// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenantaccess"
)

const benchmarkManagedByLabelValue = "cloud-workflow-engine"

// BlackBoxOptions configures a complete CWE worker process run.
type BlackBoxOptions struct {
	Binary               string
	ManagementKubeconfig string
	DSN                  string
	TemporalHostPort     string
	ReadyURL             string
	MetricsURL           string
	SteadyStateDuration  time.Duration
	Timeout              time.Duration
}

// RunBlackBox runs the complete worker against one management cluster. The
// caller supplies PostgreSQL and Temporal so their versions and resource
// limits remain explicit and reproducible.
func RunBlackBox(ctx context.Context, inventory Inventory, options BlackBoxOptions, logger *slog.Logger) (Result, error) {
	result := NewResult("black-box", inventory)
	agentSimulator, err := StartAgentSimulator(ctx, "127.0.0.1:0", 0, 0)
	if err != nil {
		return result, err
	}
	defer agentSimulator.Stop()
	_, agentPort, err := net.SplitHostPort(agentSimulator.Address())
	if err != nil {
		return result, fmt.Errorf("splitting simulated agent address: %w", err)
	}

	store, err := OpenStore(ctx, options.DSN, logger)
	if err != nil {
		return result, err
	}
	setupStart := time.Now()
	if err := store.ResetInventory(ctx, inventory); err != nil {
		_ = store.Cleanup()
		return result, err
	}
	if err := store.SeedInventory(ctx, inventory, "127.0.0.1"); err != nil {
		_ = store.Cleanup()
		return result, err
	}
	result.Metrics["databaseBeforeWorker"] = databaseStats(store.Bun.DBStats(), store.SQL.Stats())
	if err := store.Cleanup(); err != nil {
		return result, fmt.Errorf("closing fixture database: %w", err)
	}
	if err := publishTenantSecrets(ctx, options.ManagementKubeconfig, inventory); err != nil {
		return result, err
	}
	result.Phases = append(result.Phases, Phase{Name: phaseFixtureSeed, DurationMS: elapsedMS(setupStart)})

	//nolint:gosec // the benchmark operator explicitly selects the local CWE binary under test
	command := exec.CommandContext(ctx, options.Binary)
	command.Env = append(os.Environ(),
		"KUBECONFIG="+options.ManagementKubeconfig,
		"DB_CONNECTION_STRING="+options.DSN,
		"TEMPORAL_HOST_PORT="+options.TemporalHostPort,
		"TEMPORAL_NAMESPACE=default",
		"FORGE_ENVIRONMENT=test",
		"NKE_SITE_CONTROLLER_NAME=cwe-perf",
		"MANAGED_BY_LABEL_VALUE="+benchmarkManagedByLabelValue,
		"TENANT_ACCESS_ENABLED=true",
		"NODE_AGENT_PORT="+agentPort,
		"LOG_LEVEL=warn",
	)
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	startup := time.Now()
	if err := command.Start(); err != nil {
		return result, fmt.Errorf("starting CWE worker: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	if err := waitForProcessReady(ctx, options.ReadyURL, options.Timeout, exited); err != nil {
		stopProcess(command, exited)
		return result, err
	}
	result.Phases = append(result.Phases, Phase{Name: "worker-ready", DurationMS: elapsedMS(startup)})

	steadyStart := time.Now()
	select {
	case <-ctx.Done():
		stopProcess(command, exited)
		return result, ctx.Err()
	case processErr := <-exited:
		return result, fmt.Errorf("CWE worker exited during measurement: %w", processErr)
	case <-time.After(options.SteadyStateDuration):
	}
	result.Phases = append(result.Phases, Phase{Name: "steady-state", DurationMS: elapsedMS(steadyStart)})
	if metrics, metricsErr := scrapeMetrics(ctx, options.MetricsURL); metricsErr == nil {
		result.Metrics["prometheus"] = metrics
	} else {
		result.Errors = append(result.Errors, metricsErr.Error())
	}
	result.Metrics["agentSimulator"] = agentSimulator.Stats()
	if stats, statsErr := tenantAPIStats(ctx, inventory.Clusters[0].Server); statsErr == nil {
		result.Metrics["tenantAPI"] = stats
	}
	stopProcess(command, exited)
	result.AddRuntimeMetrics()
	return result, nil
}

func publishTenantSecrets(ctx context.Context, kubeconfig string, inventory Inventory) error {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("loading management kubeconfig: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("creating management Kubernetes client: %w", err)
	}
	namespaces := client.CoreV1().Namespaces()
	_, err = namespaces.Get(ctx, tenantaccess.SecretsNamespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = namespaces.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenantaccess.SecretsNamespace}}, metav1.CreateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensuring tenant Secret namespace: %w", err)
	}
	secrets := client.CoreV1().Secrets(tenantaccess.SecretsNamespace)
	for _, cluster := range inventory.Clusters {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: tenantaccess.KubeconfigSecretName(cluster.Name),
				Labels: map[string]string{
					constants.LabelKeyNkeManagedBy: benchmarkManagedByLabelValue,
					constants.LabelKeyClusterID:    cluster.ID.String(),
				},
			},
			Data: map[string][]byte{tenantaccess.KubeconfigSecretKey: cluster.Kubeconfig()},
		}
		existing, getErr := secrets.Get(ctx, secret.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			_, err = secrets.Create(ctx, secret, metav1.CreateOptions{})
		case getErr != nil:
			err = getErr
		default:
			secret.ResourceVersion = existing.ResourceVersion
			_, err = secrets.Update(ctx, secret, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("publishing kubeconfig Secret for %s: %w", cluster.Name, err)
		}
	}
	return nil
}

func waitForProcessReady(ctx context.Context, url string, timeout time.Duration, exited <-chan error) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		if err != nil {
			return fmt.Errorf("creating readiness request: %w", err)
		}
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case processErr := <-exited:
			return fmt.Errorf("CWE worker exited before readiness: %w", processErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("CWE worker did not become ready within %s", timeout)
		case <-ticker.C:
		}
	}
}

func stopProcess(command *exec.Cmd, exited <-chan error) {
	if command.Process == nil {
		return
	}
	if command.ProcessState != nil && command.ProcessState.Exited() {
		return
	}
	_ = command.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		<-exited
	}
}

func scrapeMetrics(ctx context.Context, url string) (map[string]float64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("creating metrics request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("scraping worker metrics: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scraping worker metrics returned %s", response.Status)
	}
	prefixes := []string{"go_goroutines", "go_memstats_heap_alloc_bytes", "process_cpu_seconds_total", "process_resident_memory_bytes", "workqueue_", "controller_runtime_"}
	metrics := make(map[string]float64)
	scanner := bufio.NewScanner(io.LimitReader(response.Body, 16<<20))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := strings.SplitN(fields[0], "{", 2)[0]
		if !hasMetricPrefix(name, prefixes) {
			continue
		}
		value, parseErr := strconv.ParseFloat(fields[1], 64)
		if parseErr == nil {
			metrics[name] += value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading worker metrics: %w", err)
	}
	return metrics, nil
}

func hasMetricPrefix(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
