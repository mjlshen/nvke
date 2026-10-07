// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/uptrace/bun"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/dsx-ai-factory/nke/controller/internal/constants"
	"github.com/dsx-ai-factory/nke/controller/internal/controllers/tenantaccess"
)

const (
	benchmarkManagedByLabelValue = "cloud-workflow-engine"
)

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
	SampleInterval       time.Duration
	SamplesPath          string
	AgentDelay           time.Duration
	AgentFailureEvery    uint64
	AgentOutageAfter     time.Duration
	AgentOutageDuration  time.Duration
	RestartAfter         time.Duration
	NodeEventInterval    time.Duration
	ServingCSRs          bool
	TenantOutageAfter    time.Duration
	TenantOutageDuration time.Duration
	CertificateInterval  time.Duration
}

// RunBlackBox runs the complete worker against one management cluster. The
// caller supplies PostgreSQL and Temporal so their versions and resource
// limits remain explicit and reproducible.
func RunBlackBox(ctx context.Context, inventory Inventory, options BlackBoxOptions, logger *slog.Logger) (Result, error) {
	result := NewResult("black-box", inventory)
	result.Metrics["configuration"] = map[string]any{
		"duration": options.SteadyStateDuration.String(), "sampleInterval": options.SampleInterval.String(),
		"agentDelay": options.AgentDelay.String(), "agentFailureEvery": options.AgentFailureEvery,
		"agentOutageAfter": options.AgentOutageAfter.String(), "agentOutageDuration": options.AgentOutageDuration.String(),
		"restartAfter": options.RestartAfter.String(), "agentTransport": "plaintext", "probeIdentity": "aggregate-only",
		"nodeEventInterval": options.NodeEventInterval.String(), "servingCSRs": options.ServingCSRs,
		"tenantOutageAfter": options.TenantOutageAfter.String(), "tenantOutageDuration": options.TenantOutageDuration.String(),
		"certificateInterval": options.CertificateInterval.String(), "certificateOutcome": "simulated-unchanged",
	}
	samples, err := openSampleLog(options.SamplesPath)
	if err != nil {
		return result, err
	}
	defer func() { _ = samples.Close() }()
	agentSimulator, err := StartAgentSimulator(ctx, "127.0.0.1:0", options.AgentDelay, options.AgentFailureEvery)
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
	defer store.Cleanup() //nolint:errcheck // partial results still preserve the run outcome
	setupStart := time.Now()
	if err := postAdminJSON(ctx, inventory.Clusters[0].Server, "reset", map[string]any{}); err != nil {
		return result, fmt.Errorf("resetting virtual tenant API: %w", err)
	}
	if err := store.ResetInventory(ctx, inventory); err != nil {
		return result, err
	}
	if err := store.SeedMetricsInventory(ctx, inventory, 1, 1); err != nil {
		return result, err
	}
	if _, err := store.Bun.NewUpdate().TableExpr(metricNodes).Set("ip = ?", "127.0.0.1").
		Set("conditions = jsonb_set(jsonb_set(conditions, '{provisioned,status}', ?::jsonb), '{joined,status}', ?::jsonb)", `"True"`, `"True"`).Where("cluster_id IN (?)", bun.List(clusterIDs(inventory))).Exec(ctx); err != nil {
		return result, fmt.Errorf("setting simulated agent addresses: %w", err)
	}
	if options.CertificateInterval > 0 {
		if err := seedCertificateNodes(ctx, store, inventory); err != nil {
			return result, err
		}
	}
	result.Metrics["databaseBeforeWorker"] = databaseStats(store.Bun.DBStats(), store.SQL.Stats())
	if err := publishTenantSecrets(ctx, options.ManagementKubeconfig, inventory); err != nil {
		return result, err
	}
	result.Phases = append(result.Phases, Phase{Name: phaseFixtureSeed, DurationMS: elapsedMS(setupStart)})

	environment := append(os.Environ(),
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
		"AUTH_CONFIG_PATH=",
		"SPIFFE_ENABLED=false",
		"NICO_CONTROLLERS_ENABLED=false",
		"NODE_HEALTH_CHECK_DISABLED=false",
		"POD_NAMESPACE=nke-system",
		"PROVISION_CONTROL_PLANE_WITH_CONTROL_PLANE_CONTROLLER=false",
	)
	if options.CertificateInterval > 0 {
		environment = append(environment, "CERT_RENEWAL_INTERVAL="+options.CertificateInterval.String())
	}
	startup := time.Now()
	command, exited, err := startWorker(ctx, options, environment)
	if err != nil {
		return result, err
	}
	defer func() { stopProcess(command, exited) }()
	result.Phases = append(result.Phases, Phase{Name: "worker-ready", DurationMS: elapsedMS(startup)})
	converging := time.Now()
	if err := waitUntil(ctx, options.Timeout, func() (bool, error) {
		counts, err := observeInventory(ctx, store, inventory)
		return counts["probed"] == inventory.NodeCount() && counts["kubernetesObserved"] == inventory.NodeCount(), err
	}); err != nil {
		return result, fmt.Errorf("waiting for initial fleet convergence: %w", err)
	}
	result.Phases = append(result.Phases, Phase{Name: "worker-initial-convergence", DurationMS: elapsedMS(converging)})

	return measureBlackBox(ctx, options, store, inventory, agentSimulator, environment, command, exited, samples, result)
}

type workerWorkload struct {
	events       int
	tenantOutage bool
}

func measureBlackBox(
	ctx context.Context,
	options BlackBoxOptions,
	store *Store,
	inventory Inventory,
	simulator *AgentSimulator,
	environment []string,
	command *exec.Cmd,
	exited chan error,
	samples *os.File,
	result Result,
) (Result, error) {
	defer func() { stopProcess(command, exited) }()
	measurementCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go scheduleAgentOutage(measurementCtx, simulator, options)
	steadyStart := time.Now()
	if options.ServingCSRs {
		if err := submitServingCSRs(ctx, inventory); err != nil {
			return result, err
		}
		result.Metrics["servingCSRsSubmitted"] = len(inventory.Clusters)
	}
	generation := 1
	workload := workerWorkload{}
	sampleCount, sampleErrors := 0, 0
	for {
		if options.RestartAfter > 0 && generation == 1 && time.Since(steadyStart) >= options.RestartAfter {
			restarting := time.Now()
			probesBefore := simulator.requests.Load()
			stopProcess(command, exited)
			var err error
			command, exited, err = startWorker(ctx, options, environment)
			if err != nil {
				return result, err
			}
			if err := waitUntil(ctx, options.Timeout, func() (bool, error) {
				return simulator.requests.Load() >= probesBefore+uint64(max(0, inventory.NodeCount())), nil
			}); err != nil {
				return result, fmt.Errorf("waiting for post-restart probe sweep: %w", err)
			}
			generation++
			result.Phases = append(result.Phases, Phase{Name: "worker-restart-converged", DurationMS: elapsedMS(restarting)})
		}
		if err := workload.apply(ctx, inventory, options, time.Since(steadyStart)); err != nil {
			return result, err
		}
		sampleCtx, sampleCancel := context.WithTimeout(ctx, 15*time.Second)
		sample := sampleWorker(sampleCtx, options, store, inventory, simulator, steadyStart, generation, command.Process.Pid)
		sampleCancel()
		if err := appendSample(samples, sample); err != nil {
			return result, err
		}
		sampleCount++
		sampleErrors += len(sample.Errors)
		result.Metrics["lastSample"] = sample
		result.Metrics["samples"] = sampleCount
		result.Metrics["sampleErrors"] = sampleErrors
		result.Metrics["samplesPath"] = options.SamplesPath
		if time.Since(steadyStart) >= options.SteadyStateDuration {
			break
		}
		if err := waitWorkerInterval(ctx, exited, min(options.SampleInterval, options.SteadyStateDuration-time.Since(steadyStart))); err != nil {
			return result, err
		}
	}
	result.Phases = append(result.Phases, Phase{Name: "steady-state", DurationMS: elapsedMS(steadyStart)})
	if err := finishWorkerWorkload(ctx, options, store, inventory, workload); err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	finalCtx, finalCancel := context.WithTimeout(ctx, 15*time.Second)
	final := sampleWorker(finalCtx, options, store, inventory, simulator, steadyStart, generation, command.Process.Pid)
	finalCancel()
	if err := appendSample(samples, final); err != nil {
		return result, err
	}
	result.Metrics["finalSample"] = final
	checks := map[string]bool{
		"inventoryPresent":               final.Inventory[metricNodes] == inventory.NodeCount(),
		"tenantStateRecovered":           final.Inventory["kubernetesReady"] == expectedReadyNodes(inventory),
		"agentStateRecovered":            final.Inventory["agentReady"] == inventory.NodeCount(),
		"nodeMetricInventoryComplete":    final.Metrics["harness_samples_nke_controller_node_info"] == float64(inventory.NodeCount()),
		"clusterMetricInventoryComplete": final.Metrics["harness_samples_nke_controller_cluster_info"] == float64(len(inventory.Clusters)),
		"servingCSRsApproved":            final.TenantAPI["csrApprovals"] == float64(len(inventory.Clusters)),
		"samplesSuccessful":              sampleErrors == 0 && len(final.Errors) == 0,
		"listAPIInventoryComplete":       final.API["clusters"] == len(inventory.Clusters),
	}
	result.Metrics["checks"] = checks
	skipped := map[string]string{}
	if !options.ServingCSRs {
		delete(checks, "servingCSRsApproved")
		skipped["servingCSRsApproved"] = "CSR workload not requested"
	}
	if options.AgentFailureEvery > 0 {
		delete(checks, "agentStateRecovered")
		skipped["agentStateRecovered"] = "persistent failure injection enabled"
	}
	if options.CertificateInterval > 0 {
		checks["certificateWorkflowsDelivered"] = simulator.certStarts.Load() >= uint64(max(0, len(inventory.Clusters)))
	}
	result.Metrics["skippedChecks"] = skipped
	result.Metrics["totalSamples"] = sampleCount + 1
	result.Metrics["agentSimulator"] = simulator.Stats()
	result.Metrics["nodeEventsInjected"] = workload.events
	result.Metrics["runtimeScope"] = "benchmark-driver; worker runtime is in samples"
	result.AddRuntimeMetrics()
	return result, nil
}

func expectedReadyNodes(inventory Inventory) int {
	count := 0
	for _, cluster := range inventory.Clusters {
		for _, node := range cluster.Nodes {
			if node.Ready {
				count++
			}
		}
	}
	return count
}

func finishWorkerWorkload(ctx context.Context, options BlackBoxOptions, store *Store, inventory Inventory, workload workerWorkload) error {
	if workload.tenantOutage {
		if err := setTenantOutage(ctx, inventory, false); err != nil {
			return err
		}
	}
	if workload.events%2 != 0 {
		cluster := inventory.Clusters[(workload.events/2)%len(inventory.Clusters)]
		node := cluster.Nodes[0]
		if err := postNodeEvent(ctx, cluster.Server, cluster.Index, node.Name, node.Ready); err != nil {
			return err
		}
	}
	return waitUntil(ctx, options.Timeout, func() (bool, error) {
		counts, err := observeInventory(ctx, store, inventory)
		if err != nil {
			return false, err
		}
		if options.ServingCSRs {
			stats, statsErr := tenantAPIStats(ctx, inventory.Clusters[0].Server)
			if statsErr != nil {
				return false, statsErr
			}
			if stats["csrApprovals"] != float64(len(inventory.Clusters)) {
				return false, nil
			}
		}
		return counts["kubernetesReady"] == expectedReadyNodes(inventory) && (options.AgentFailureEvery > 0 || counts["agentReady"] == inventory.NodeCount()), nil
	})
}

func scheduleAgentOutage(ctx context.Context, simulator *AgentSimulator, options BlackBoxOptions) {
	if options.AgentOutageDuration <= 0 || !waitInterval(ctx, options.AgentOutageAfter) {
		return
	}
	simulator.SetOutage(true)
	if waitInterval(ctx, options.AgentOutageDuration) {
		simulator.SetOutage(false)
	}
}

func waitWorkerInterval(ctx context.Context, exited <-chan error, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-exited:
		return fmt.Errorf("worker exited during measurement: %w", err)
	case <-timer.C:
		return nil
	}
}

func (w *workerWorkload) apply(ctx context.Context, inventory Inventory, options BlackBoxOptions, elapsed time.Duration) error {
	outage := options.TenantOutageDuration > 0 && elapsed >= options.TenantOutageAfter && elapsed < options.TenantOutageAfter+options.TenantOutageDuration
	if outage != w.tenantOutage {
		if err := setTenantOutage(ctx, inventory, outage); err != nil {
			return err
		}
		w.tenantOutage = outage
	}
	if options.NodeEventInterval <= 0 || elapsed < time.Duration(w.events+1)*options.NodeEventInterval {
		return nil
	}
	cluster := inventory.Clusters[(w.events/2)%len(inventory.Clusters)]
	node := cluster.Nodes[0]
	ready := node.Ready
	if w.events%2 == 0 {
		ready = !ready
	}
	if err := postNodeEvent(ctx, cluster.Server, cluster.Index, node.Name, ready); err != nil {
		return err
	}
	w.events++
	return nil
}

func startWorker(ctx context.Context, options BlackBoxOptions, environment []string) (*exec.Cmd, chan error, error) {
	//nolint:gosec // operator selects the benchmark binary
	command := exec.CommandContext(ctx, options.Binary)
	command.Env = environment
	command.Stdout, command.Stderr = os.Stderr, os.Stderr
	if err := command.Start(); err != nil {
		return nil, nil, fmt.Errorf("starting worker: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait(); close(exited) }()
	if err := waitForProcessReady(ctx, options.ReadyURL, options.Timeout, exited); err != nil {
		stopProcess(command, exited)
		return nil, nil, err
	}
	return command, exited, nil
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
	if command == nil || command.Process == nil {
		return
	}
	select {
	case <-exited:
		return
	default:
	}
	_ = command.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		<-exited
	}
}

func hasMetricPrefix(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
