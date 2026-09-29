// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nkeproto "github.com/dsx-ai-factory/nke/agent/pkg/agent/proto"
)

// AgentSimulator serves every virtual node on one shared gRPC endpoint.
type AgentSimulator struct {
	nkeproto.UnimplementedNodeAgentServer
	server       *grpc.Server
	listener     net.Listener
	delay        time.Duration
	failureEvery uint64
	requests     atomic.Uint64
	failures     atomic.Uint64
}

// StartAgentSimulator starts a deterministic node-agent endpoint.
func StartAgentSimulator(ctx context.Context, address string, delay time.Duration, failureEvery uint64) (*AgentSimulator, error) {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listening for simulated node agents: %w", err)
	}
	simulator := &AgentSimulator{
		server:       grpc.NewServer(),
		listener:     listener,
		delay:        delay,
		failureEvery: failureEvery,
	}
	nkeproto.RegisterNodeAgentServer(simulator.server, simulator)
	go func() {
		_ = simulator.server.Serve(listener)
	}()
	return simulator, nil
}

// GetKubeadmStatus implements the production readiness probe RPC.
func (s *AgentSimulator) GetKubeadmStatus(ctx context.Context, _ *nkeproto.KubeadmStatusRequest) (*nkeproto.KubeadmStatusResponse, error) {
	request := s.requests.Add(1)
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if s.failureEvery > 0 && request%s.failureEvery == 0 {
		s.failures.Add(1)
		return nil, status.Error(codes.Unavailable, "deterministic simulated failure")
	}
	return &nkeproto.KubeadmStatusResponse{AgentVersion: "cwe-perf-agent/v1"}, nil
}

// Address returns the bound endpoint.
func (s *AgentSimulator) Address() string { return s.listener.Addr().String() }

// Stop terminates the gRPC server.
func (s *AgentSimulator) Stop() { s.server.GracefulStop() }

// Stats returns simulator counters.
func (s *AgentSimulator) Stats() map[string]any {
	return map[string]any{"requests": s.requests.Load(), "failures": s.failures.Load(), "address": s.Address()}
}
