package handler

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestShouldHoldOpenAIWSPoolLeaseUsesActualSelectionLease(t *testing.T) {
	release := func() {}
	require.False(t, shouldHoldOpenAIWSPoolLease(nil, release))
	require.False(t, shouldHoldOpenAIWSPoolLease(&service.AccountSelectionResult{}, release))
	require.False(t, shouldHoldOpenAIWSPoolLease(&service.AccountSelectionResult{
		Egress: &service.ResolvedAccountEgress{},
	}, release))
	require.False(t, shouldHoldOpenAIWSPoolLease(&service.AccountSelectionResult{
		Egress: &service.ResolvedAccountEgress{Lease: &service.AccountEgressLease{}},
	}, nil))
	require.True(t, shouldHoldOpenAIWSPoolLease(&service.AccountSelectionResult{
		Egress: &service.ResolvedAccountEgress{Lease: &service.AccountEgressLease{}},
	}, release))
}
