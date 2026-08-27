package service

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorNormalizationAndChecksumAreCanonical(t *testing.T) {
	first := validTrafficDirectorSpec()
	first.HealthMode = " observe "
	first.Pools = []domain.TrafficDirectorPool{
		{
			Key:             " overflow ",
			WeightBPS:       0,
			AccountIDs:      []int64{4, 3},
			MinAvailable:    1,
			FallbackPoolKey: "",
		},
		{
			Key:             " primary ",
			WeightBPS:       10000,
			AccountIDs:      []int64{2, 1},
			MinAvailable:    1,
			FallbackPoolKey: " overflow ",
		},
	}
	second := validTrafficDirectorSpec()
	second.HealthMode = domain.TrafficDirectorHealthModeObserve
	second.Pools = []domain.TrafficDirectorPool{
		{
			Key:             "primary",
			WeightBPS:       10000,
			AccountIDs:      []int64{1, 2},
			MinAvailable:    1,
			FallbackPoolKey: "overflow",
		},
		{
			Key:          "overflow",
			WeightBPS:    0,
			AccountIDs:   []int64{3, 4},
			MinAvailable: 1,
		},
	}

	normalizedFirst, err := NormalizeTrafficDirectorSpec(first)
	require.NoError(t, err)
	normalizedSecond, err := NormalizeTrafficDirectorSpec(second)
	require.NoError(t, err)
	require.Equal(t, normalizedSecond, normalizedFirst)
	require.Equal(t, []string{"overflow", "primary"}, []string{
		normalizedFirst.Pools[0].Key,
		normalizedFirst.Pools[1].Key,
	})
	require.Equal(t, []int64{1, 2}, normalizedFirst.Pools[1].AccountIDs)

	firstChecksum, err := TrafficDirectorSpecChecksum(first)
	require.NoError(t, err)
	secondChecksum, err := TrafficDirectorSpecChecksum(second)
	require.NoError(t, err)
	require.Equal(t, secondChecksum, firstChecksum)
	require.Len(t, firstChecksum, sha256HexLength)

	second.HealthMode = domain.TrafficDirectorHealthModeEnforce
	enforceChecksum, err := TrafficDirectorSpecChecksum(second)
	require.NoError(t, err)
	require.NotEqual(t, firstChecksum, enforceChecksum)

	// Normalization must not sort caller-owned slices in place.
	require.Equal(t, []int64{4, 3}, first.Pools[0].AccountIDs)
}

func TestNormalizeTrafficDirectorSpecValidation(t *testing.T) {
	tooManyPools := make([]domain.TrafficDirectorPool, domain.TrafficDirectorMaxPools+1)
	for i := range tooManyPools {
		tooManyPools[i] = domain.TrafficDirectorPool{
			Key:        fmt.Sprintf("pool-%d", i),
			AccountIDs: []int64{int64(i + 1)},
		}
	}
	tooManyPools[0].WeightBPS = domain.TrafficDirectorWeightTotalBPS

	tooManyAccounts := make([]int64, domain.TrafficDirectorMaxAccountReferences+1)
	for i := range tooManyAccounts {
		tooManyAccounts[i] = int64(i + 1)
	}

	oversizedCanonicalIDs := make([]int64, domain.TrafficDirectorMaxAccountReferences)
	for i := range oversizedCanonicalIDs {
		oversizedCanonicalIDs[i] = math.MaxInt64 - int64(i)
	}

	tests := []struct {
		name      string
		mutate    func(*domain.TrafficDirectorSpec)
		wantError string
	}{
		{
			name: "schema version",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.SchemaVersion = 2
			},
			wantError: "schema_version",
		},
		{
			name: "health mode",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.HealthMode = "active"
			},
			wantError: "health_mode",
		},
		{
			name: "empty pools",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools = nil
			},
			wantError: "at least one pool",
		},
		{
			name: "pool limit",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools = tooManyPools
			},
			wantError: "pool count",
		},
		{
			name: "account reference limit",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].AccountIDs = tooManyAccounts
				spec.Pools[0].MinAvailable = 1
			},
			wantError: "account reference count",
		},
		{
			name: "invalid key",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].Key = "primary/bad"
			},
			wantError: "must match",
		},
		{
			name: "duplicate key after normalization",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[1].Key = " primary "
			},
			wantError: "is duplicated",
		},
		{
			name: "negative weight",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].WeightBPS = -1
			},
			wantError: "weight_bps",
		},
		{
			name: "weight total",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].WeightBPS = 4999
			},
			wantError: "total must equal 10000",
		},
		{
			name: "non-positive account ID",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].AccountIDs[0] = 0
			},
			wantError: "non-positive account ID",
		},
		{
			name: "duplicate account globally",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[1].AccountIDs = append(spec.Pools[1].AccountIDs, spec.Pools[0].AccountIDs[0])
			},
			wantError: "appears in both pool",
		},
		{
			name: "negative minimum",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].MinAvailable = -1
			},
			wantError: "min_available",
		},
		{
			name: "zero minimum",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].MinAvailable = 0
			},
			wantError: "min_available",
		},
		{
			name: "minimum exceeds accounts",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].MinAvailable = len(spec.Pools[0].AccountIDs) + 1
			},
			wantError: "min_available",
		},
		{
			name: "missing fallback",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].FallbackPoolKey = "missing"
			},
			wantError: "missing fallback pool",
		},
		{
			name: "fallback cycle",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools[0].FallbackPoolKey = "secondary"
				spec.Pools[1].FallbackPoolKey = "primary"
			},
			wantError: "fallback cycle",
		},
		{
			name: "unreferenced zero-weight pool",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools = append(spec.Pools, domain.TrafficDirectorPool{
					Key:          "overflow",
					AccountIDs:   []int64{5},
					MinAvailable: 1,
				})
			},
			wantError: "must be referenced as a fallback",
		},
		{
			name: "canonical JSON limit",
			mutate: func(spec *domain.TrafficDirectorSpec) {
				spec.Pools = []domain.TrafficDirectorPool{
					{
						Key:          "primary",
						WeightBPS:    domain.TrafficDirectorWeightTotalBPS,
						AccountIDs:   oversizedCanonicalIDs,
						MinAvailable: 1,
					},
				}
			},
			wantError: "canonical JSON size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := validTrafficDirectorSpec()
			tt.mutate(&spec)

			_, err := NormalizeTrafficDirectorSpec(spec)
			require.ErrorIs(t, err, ErrInvalidTrafficDirectorSpec)
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

func TestNormalizeTrafficDirectorSpecAcceptsFullPoolKeyAlphabet(t *testing.T) {
	spec := validTrafficDirectorSpec()
	spec.Pools[0].Key = "Primary.v1_A"

	normalized, err := NormalizeTrafficDirectorSpec(spec)
	require.NoError(t, err)
	require.Equal(t, "Primary.v1_A", normalized.Pools[0].Key)
}

func TestValidateTrafficDirectorSpecChecksMembershipAndReportsUnassigned(t *testing.T) {
	groupAccountIDs := map[int64]struct{}{
		1: {},
		2: {},
		3: {},
		4: {},
		5: {},
		6: {},
	}

	result, err := ValidateTrafficDirectorSpec(validTrafficDirectorSpec(), groupAccountIDs)
	require.NoError(t, err)
	require.Equal(t, []int64{5, 6}, result.UnassignedAccountIDs)
	require.Equal(t, validTrafficDirectorSpec(), result.NormalizedSpec)

	foreign := validTrafficDirectorSpec()
	foreign.Pools[0].AccountIDs[0] = 99
	_, err = ValidateTrafficDirectorSpec(foreign, groupAccountIDs)
	require.ErrorIs(t, err, ErrInvalidTrafficDirectorSpec)
	require.ErrorContains(t, err, "does not belong to the group")

	draft, err := ValidateTrafficDirectorSpec(foreign, nil)
	require.NoError(t, err)
	require.Nil(t, draft.UnassignedAccountIDs)
}

func TestNormalizeTrafficDirectorSpecAllowsZeroWeightOverflowPool(t *testing.T) {
	spec := validTrafficDirectorSpec()
	spec.Pools = append(spec.Pools, domain.TrafficDirectorPool{
		Key:          "overflow",
		WeightBPS:    0,
		AccountIDs:   []int64{5},
		MinAvailable: 1,
	})
	spec.Pools[0].FallbackPoolKey = "overflow"

	_, err := NormalizeTrafficDirectorSpec(spec)
	require.NoError(t, err)
}

func TestEvaluateTrafficDirectorIsStableAndReturnsExplicitFallbackChain(t *testing.T) {
	spec := domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeOff,
		Pools: []domain.TrafficDirectorPool{
			{
				Key:             "primary",
				WeightBPS:       domain.TrafficDirectorWeightTotalBPS,
				AccountIDs:      []int64{1},
				MinAvailable:    1,
				FallbackPoolKey: "overflow-a",
			},
			{
				Key:             "overflow-a",
				WeightBPS:       0,
				AccountIDs:      []int64{2},
				MinAvailable:    1,
				FallbackPoolKey: "overflow-b",
			},
			{
				Key:          "overflow-b",
				WeightBPS:    0,
				AccountIDs:   []int64{3},
				MinAvailable: 1,
			},
		},
	}

	first, err := EvaluateTrafficDirector(spec, 42, "customer-123")
	require.NoError(t, err)
	require.Equal(t, "primary", first.HomePoolKey)
	require.Equal(t, []string{"overflow-a", "overflow-b"}, first.FallbackPoolKeys)

	reordered := spec
	reordered.HealthMode = domain.TrafficDirectorHealthModeEnforce
	reordered.Pools = slices.Clone(spec.Pools)
	slices.Reverse(reordered.Pools)
	second, err := EvaluateTrafficDirector(reordered, 42, "customer-123")
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestFollowFallbackIncludesStartAndDefendsAgainstCycles(t *testing.T) {
	spec := domain.TrafficDirectorSpec{
		Pools: []domain.TrafficDirectorPool{
			{Key: "primary", FallbackPoolKey: "overflow-a"},
			{Key: "overflow-a", FallbackPoolKey: "overflow-b"},
			{Key: "overflow-b"},
		},
	}

	chain, err := FollowFallback(spec, "primary")
	require.NoError(t, err)
	require.Equal(t, []string{"primary", "overflow-a", "overflow-b"}, chain)

	spec.Pools[2].FallbackPoolKey = "primary"
	_, err = FollowFallback(spec, "primary")
	require.ErrorIs(t, err, ErrInvalidTrafficDirectorSpec)
	require.ErrorContains(t, err, "fallback cycle")
}

func TestEvaluateTrafficDirectorIgnoresHealthModeAndPoolOrderForHomeAssignment(t *testing.T) {
	spec := validTrafficDirectorSpec()
	first, err := EvaluateTrafficDirector(spec, 7, "stable-routing-key")
	require.NoError(t, err)

	spec.HealthMode = domain.TrafficDirectorHealthModeObserve
	slices.Reverse(spec.Pools)
	second, err := EvaluateTrafficDirector(spec, 7, "stable-routing-key")
	require.NoError(t, err)
	require.Equal(t, first.HomePoolKey, second.HomePoolKey)

	third, err := EvaluateTrafficDirector(spec, 7, "stable-routing-key")
	require.NoError(t, err)
	require.Equal(t, second, third)
}

func TestEvaluateTrafficDirectorRejectsInvalidInputs(t *testing.T) {
	spec := validTrafficDirectorSpec()

	_, err := EvaluateTrafficDirector(spec, 0, "routing-key")
	require.ErrorIs(t, err, ErrInvalidTrafficDirectorEvaluation)
	require.ErrorContains(t, err, "group ID")

	_, err = EvaluateTrafficDirector(spec, 1, " \t")
	require.ErrorIs(t, err, ErrInvalidTrafficDirectorEvaluation)
	require.ErrorContains(t, err, "routing key")
}

func TestTrafficDirectorWeightedRendezvousDistribution(t *testing.T) {
	spec := validTrafficDirectorSpec()
	spec.Pools = append(spec.Pools, domain.TrafficDirectorPool{
		Key:          "overflow",
		WeightBPS:    0,
		AccountIDs:   []int64{5},
		MinAvailable: 1,
	})
	spec.Pools[0].FallbackPoolKey = "overflow"
	normalized, err := NormalizeTrafficDirectorSpec(spec)
	require.NoError(t, err)

	const sampleSize = 100000
	counts := map[string]int{}
	for i := 0; i < sampleSize; i++ {
		poolKey := chooseTrafficDirectorHomePool(normalized.Pools, 99, fmt.Sprintf("key-%d", i))
		counts[poolKey]++
	}

	primaryRatio := float64(counts["primary"]) / sampleSize
	require.InDelta(t, 0.10, primaryRatio, 0.005)
	require.Equal(t, 0, counts["overflow"])
}

func TestTrafficDirectorWeightedRendezvousHashIncludesWeight(t *testing.T) {
	scoreAt1000 := trafficDirectorWeightedRendezvousScore(42, "stable-key", "primary", 1000)
	scoreAt2000 := trafficDirectorWeightedRendezvousScore(42, "stable-key", "primary", 2000)

	// If weight were only the divisor, these products would be identical.
	require.NotEqual(t, scoreAt1000*1000, scoreAt2000*2000)
}

const sha256HexLength = 64

func validTrafficDirectorSpec() domain.TrafficDirectorSpec {
	return domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeOff,
		Pools: []domain.TrafficDirectorPool{
			{
				Key:          "primary",
				WeightBPS:    1000,
				AccountIDs:   []int64{1, 2},
				MinAvailable: 1,
			},
			{
				Key:          "secondary",
				WeightBPS:    9000,
				AccountIDs:   []int64{3, 4},
				MinAvailable: 1,
			},
		},
	}
}
