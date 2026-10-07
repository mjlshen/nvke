// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package perf

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	nkeproto "github.com/dsx-ai-factory/nke/agent/pkg/agent/proto"
)

func TestAgentCohortsRemainAttachedToNodeIdentity(t *testing.T) {
	t.Parallel()
	simulator, err := StartAgentSimulator(context.Background(), "127.0.0.1:0", 0, 0)
	require.NoError(t, err)
	t.Cleanup(simulator.Stop)
	simulator.ConfigureCohort(2, time.Second, 3)
	failed := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-perf-node-id", "failed", "x-perf-node-ordinal", "3"))
	healthy := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-perf-node-id", "healthy", "x-perf-node-ordinal", "1"))
	for range 2 {
		_, err = simulator.GetKubeadmStatus(failed, &nkeproto.KubeadmStatusRequest{})
		require.Equal(t, codes.Unavailable, status.Code(err))
		_, err = simulator.GetKubeadmStatus(healthy, &nkeproto.KubeadmStatusRequest{})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	slow := metadata.NewIncomingContext(ctx, metadata.Pairs("x-perf-node-id", "slow", "x-perf-node-ordinal", "2"))
	_, err = simulator.GetKubeadmStatus(slow, &nkeproto.KubeadmStatusRequest{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 3, simulator.Stats()["identifiedNodes"])
	require.EqualValues(t, 1, simulator.Stats()["timeouts"])
}

func TestCertificateSimulationIsIdempotentAndAgentOutageRecovers(t *testing.T) {
	t.Parallel()
	simulator, err := StartAgentSimulator(context.Background(), "127.0.0.1:0", 0, 0)
	require.NoError(t, err)
	t.Cleanup(simulator.Stop)
	request := &nkeproto.StartKubernetesCertificateRenewalRequest{OperationId: "example-operation", ThresholdDays: 30}
	for range 2 {
		response, err := simulator.StartKubernetesCertificateRenewal(context.Background(), request)
		require.NoError(t, err)
		require.Equal(t, nkeproto.KubernetesCertificateRenewalOperation_OUTCOME_UNCHANGED, response.GetOperation().GetOutcome())
	}
	require.EqualValues(t, 1, simulator.Stats()["certificateOperations"])
	simulator.SetOutage(true)
	_, err = simulator.GetKubeadmStatus(context.Background(), &nkeproto.KubeadmStatusRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))
	simulator.SetOutage(false)
	_, err = simulator.GetKubeadmStatus(context.Background(), &nkeproto.KubeadmStatusRequest{})
	require.NoError(t, err)
}
