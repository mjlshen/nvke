// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	nkeproto "github.com/dsx-ai-factory/nke/agent/pkg/agent/proto"
)

// AgentSimulator serves every virtual node on one shared gRPC endpoint.
type AgentSimulator struct {
	nkeproto.UnimplementedNodeAgentServer
	server          *grpc.Server
	listener        net.Listener
	delay           time.Duration
	failureEvery    uint64
	requests        atomic.Uint64
	failures        atomic.Uint64
	outage          atomic.Bool
	slowEvery       uint64
	slowDelay       time.Duration
	failedNodeEvery uint64
	slowRequests    atomic.Uint64
	timeouts        atomic.Uint64
	mu              sync.Mutex
	lastProbe       map[string]time.Time
	maxProbeGap     time.Duration
	certOperations  map[string]struct{}
	certStarts      atomic.Uint64
}

// StartAgentSimulator starts a deterministic node-agent endpoint.
func StartAgentSimulator(ctx context.Context, address string, delay time.Duration, failureEvery uint64) (*AgentSimulator, error) {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listening for simulated node agents: %w", err)
	}
	simulator := &AgentSimulator{
		server:         grpc.NewServer(),
		listener:       listener,
		delay:          delay,
		failureEvery:   failureEvery,
		lastProbe:      make(map[string]time.Time),
		certOperations: make(map[string]struct{}),
	}
	nkeproto.RegisterNodeAgentServer(simulator.server, simulator)
	go func() {
		_ = simulator.server.Serve(listener)
	}()
	return simulator, nil
}

// StartKubernetesCertificateRenewal simulates a successful certificate check
// with unchanged certificates. It exercises orchestration, not node commands.
func (s *AgentSimulator) StartKubernetesCertificateRenewal(ctx context.Context, request *nkeproto.StartKubernetesCertificateRenewalRequest) (*nkeproto.StartKubernetesCertificateRenewalResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if s.outage.Load() {
		return nil, status.Error(codes.Unavailable, "simulated agent outage")
	}
	s.certStarts.Add(1)
	s.mu.Lock()
	s.certOperations[request.GetOperationId()] = struct{}{}
	s.mu.Unlock()
	return &nkeproto.StartKubernetesCertificateRenewalResponse{Operation: simulatedCertificateResult(request.GetOperationId())}, nil
}

// GetKubernetesCertificateRenewalStatus returns the idempotent simulated result.
func (s *AgentSimulator) GetKubernetesCertificateRenewalStatus(ctx context.Context, request *nkeproto.GetKubernetesCertificateRenewalStatusRequest) (*nkeproto.GetKubernetesCertificateRenewalStatusResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	_, exists := s.certOperations[request.GetOperationId()]
	s.mu.Unlock()
	if !exists {
		return nil, status.Error(codes.NotFound, "unknown simulated operation")
	}
	return &nkeproto.GetKubernetesCertificateRenewalStatusResponse{Operation: simulatedCertificateResult(request.GetOperationId())}, nil
}

func simulatedCertificateResult(operationID string) *nkeproto.KubernetesCertificateRenewalOperation {
	return &nkeproto.KubernetesCertificateRenewalOperation{
		Operation: &nkeproto.NodeAgentOperationStatus{OperationId: operationID, Phase: nkeproto.NodeAgentOperationPhase_NODE_AGENT_OPERATION_PHASE_SUCCEEDED},
		Outcome:   nkeproto.KubernetesCertificateRenewalOperation_OUTCOME_UNCHANGED,
	}
}

// GetKubeadmStatus implements the production readiness probe RPC.
func (s *AgentSimulator) GetKubeadmStatus(ctx context.Context, _ *nkeproto.KubeadmStatusRequest) (*nkeproto.KubeadmStatusResponse, error) {
	request := s.requests.Add(1)
	ordinal := request
	identity := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("x-perf-node-id"); len(values) == 1 {
			identity = values[0]
		}
		if values := md.Get("x-perf-node-ordinal"); len(values) == 1 {
			if parsed, err := strconv.ParseUint(values[0], 10, 64); err == nil {
				ordinal = parsed
			}
		}
	}
	s.recordProbe(identity)
	delay := s.delay
	if s.slowEvery > 0 && ordinal%s.slowEvery == 0 {
		delay += s.slowDelay
		s.slowRequests.Add(1)
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			s.timeouts.Add(1)
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if s.outage.Load() || (s.failureEvery > 0 && request%s.failureEvery == 0) || (s.failedNodeEvery > 0 && ordinal%s.failedNodeEvery == 0) {
		s.failures.Add(1)
		return nil, status.Error(codes.Unavailable, "deterministic simulated failure")
	}
	return &nkeproto.KubeadmStatusResponse{AgentVersion: "cwe-perf-agent/v1"}, nil
}

// ConfigureCohort selects persistent nodes when the driver supplies identity;
// otherwise the same controls select every Nth request in the black-box lane.
// Call before starting traffic.
func (s *AgentSimulator) ConfigureCohort(slowEvery uint64, slowDelay time.Duration, failedNodeEvery uint64) {
	s.slowEvery, s.slowDelay, s.failedNodeEvery = slowEvery, slowDelay, failedNodeEvery
}

func (s *AgentSimulator) recordProbe(identity string) {
	if identity == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if previous, ok := s.lastProbe[identity]; ok {
		s.maxProbeGap = max(s.maxProbeGap, now.Sub(previous))
	}
	s.lastProbe[identity] = now
}

// Address returns the bound endpoint.
func (s *AgentSimulator) Address() string { return s.listener.Addr().String() }

// SetOutage switches all readiness RPCs between unavailable and normal service.
func (s *AgentSimulator) SetOutage(unavailable bool) { s.outage.Store(unavailable) }

// Stop terminates the gRPC server.
func (s *AgentSimulator) Stop() { s.server.GracefulStop() }

// Stats returns simulator counters.
func (s *AgentSimulator) Stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxAge time.Duration
	for _, at := range s.lastProbe {
		maxAge = max(maxAge, time.Since(at))
	}
	return map[string]any{
		"requests": s.requests.Load(), "failures": s.failures.Load(), "address": s.Address(),
		"outage": s.outage.Load(), "slowRequests": s.slowRequests.Load(), "timeouts": s.timeouts.Load(),
		"identifiedNodes": len(s.lastProbe), "maxProbeGapMs": durationMS(s.maxProbeGap), "maxProbeAgeMs": durationMS(maxAge),
		"certificateStarts": s.certStarts.Load(), "certificateOperations": len(s.certOperations),
	}
}
