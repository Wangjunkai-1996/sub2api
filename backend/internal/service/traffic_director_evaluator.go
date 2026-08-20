package service

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

var (
	ErrInvalidTrafficDirectorSpec       = errors.New("invalid traffic director spec")
	ErrInvalidTrafficDirectorEvaluation = errors.New("invalid traffic director evaluation input")

	trafficDirectorPoolKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

// TrafficDirectorEvaluation contains the stable home assignment and its configured overflow path.
type TrafficDirectorEvaluation struct {
	HomePoolKey      string   `json:"home_pool_key"`
	FallbackPoolKeys []string `json:"fallback_pool_keys"`
}

// TrafficDirectorValidationResult contains the canonical policy and accounts
// which belong to the group but are intentionally outside every configured pool.
type TrafficDirectorValidationResult struct {
	NormalizedSpec       domain.TrafficDirectorSpec
	UnassignedAccountIDs []int64
}

// ValidateTrafficDirectorSpec validates a policy for a group. A nil account set
// skips membership checks, which is useful while editing a draft policy.
func ValidateTrafficDirectorSpec(
	spec domain.TrafficDirectorSpec,
	groupAccountIDs map[int64]struct{},
) (TrafficDirectorValidationResult, error) {
	normalized, _, err := normalizeTrafficDirectorSpec(spec)
	if err != nil {
		return TrafficDirectorValidationResult{}, err
	}

	assignedAccountIDs := make(map[int64]struct{})
	for _, pool := range normalized.Pools {
		for _, accountID := range pool.AccountIDs {
			if groupAccountIDs != nil {
				if _, exists := groupAccountIDs[accountID]; !exists {
					return TrafficDirectorValidationResult{}, invalidTrafficDirectorSpec(
						"account ID %d in pool %q does not belong to the group", accountID, pool.Key,
					)
				}
			}
			assignedAccountIDs[accountID] = struct{}{}
		}
	}

	var unassignedAccountIDs []int64
	if groupAccountIDs != nil {
		unassignedAccountIDs = make([]int64, 0, len(groupAccountIDs)-len(assignedAccountIDs))
		for accountID := range groupAccountIDs {
			if _, assigned := assignedAccountIDs[accountID]; !assigned {
				unassignedAccountIDs = append(unassignedAccountIDs, accountID)
			}
		}
		sort.Slice(unassignedAccountIDs, func(i, j int) bool {
			return unassignedAccountIDs[i] < unassignedAccountIDs[j]
		})
	}

	return TrafficDirectorValidationResult{
		NormalizedSpec:       normalized,
		UnassignedAccountIDs: unassignedAccountIDs,
	}, nil
}

// NormalizeTrafficDirectorSpec returns a validated canonical representation without mutating spec.
func NormalizeTrafficDirectorSpec(spec domain.TrafficDirectorSpec) (domain.TrafficDirectorSpec, error) {
	normalized, _, err := normalizeTrafficDirectorSpec(spec)
	return normalized, err
}

// TrafficDirectorSpecChecksum returns the SHA-256 checksum of the canonical JSON policy.
func TrafficDirectorSpecChecksum(spec domain.TrafficDirectorSpec) (string, error) {
	_, canonical, err := normalizeTrafficDirectorSpec(spec)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// EvaluateTrafficDirector selects a home pool and follows only its explicit fallback chain.
func EvaluateTrafficDirector(
	spec domain.TrafficDirectorSpec,
	groupID int64,
	routingKey string,
) (TrafficDirectorEvaluation, error) {
	if groupID <= 0 {
		return TrafficDirectorEvaluation{}, invalidTrafficDirectorEvaluation("group ID must be positive")
	}
	if strings.TrimSpace(routingKey) == "" {
		return TrafficDirectorEvaluation{}, invalidTrafficDirectorEvaluation("routing key must not be empty")
	}

	normalized, _, err := normalizeTrafficDirectorSpec(spec)
	if err != nil {
		return TrafficDirectorEvaluation{}, err
	}

	return evaluateNormalizedTrafficDirector(normalized, groupID, routingKey)
}

func normalizeTrafficDirectorSpec(
	spec domain.TrafficDirectorSpec,
) (domain.TrafficDirectorSpec, []byte, error) {
	if spec.SchemaVersion != domain.TrafficDirectorSchemaVersion {
		return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
			"schema_version must equal %d", domain.TrafficDirectorSchemaVersion,
		)
	}

	healthMode := strings.TrimSpace(spec.HealthMode)
	switch healthMode {
	case domain.TrafficDirectorHealthModeOff,
		domain.TrafficDirectorHealthModeObserve,
		domain.TrafficDirectorHealthModeEnforce:
	default:
		return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
			"health_mode must be one of off, observe, or enforce",
		)
	}

	if len(spec.Pools) == 0 {
		return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec("at least one pool is required")
	}
	if len(spec.Pools) > domain.TrafficDirectorMaxPools {
		return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
			"pool count %d exceeds limit %d", len(spec.Pools), domain.TrafficDirectorMaxPools,
		)
	}

	totalAccountReferences := 0
	for _, pool := range spec.Pools {
		if len(pool.AccountIDs) > domain.TrafficDirectorMaxAccountReferences-totalAccountReferences {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"account reference count exceeds limit %d", domain.TrafficDirectorMaxAccountReferences,
			)
		}
		totalAccountReferences += len(pool.AccountIDs)
	}

	normalized := domain.TrafficDirectorSpec{
		SchemaVersion: spec.SchemaVersion,
		HealthMode:    healthMode,
		Pools:         make([]domain.TrafficDirectorPool, len(spec.Pools)),
	}
	for i, pool := range spec.Pools {
		accountIDs := make([]int64, len(pool.AccountIDs))
		copy(accountIDs, pool.AccountIDs)
		normalized.Pools[i] = domain.TrafficDirectorPool{
			Key:             strings.TrimSpace(pool.Key),
			WeightBPS:       pool.WeightBPS,
			AccountIDs:      accountIDs,
			MinAvailable:    pool.MinAvailable,
			FallbackPoolKey: strings.TrimSpace(pool.FallbackPoolKey),
		}
		sort.Slice(normalized.Pools[i].AccountIDs, func(a, b int) bool {
			return normalized.Pools[i].AccountIDs[a] < normalized.Pools[i].AccountIDs[b]
		})
	}
	sort.Slice(normalized.Pools, func(i, j int) bool {
		return normalized.Pools[i].Key < normalized.Pools[j].Key
	})

	poolsByKey := make(map[string]domain.TrafficDirectorPool, len(normalized.Pools))
	accountPoolByID := make(map[int64]string, totalAccountReferences)
	totalPositiveWeight := int64(0)
	for _, pool := range normalized.Pools {
		if !trafficDirectorPoolKeyPattern.MatchString(pool.Key) {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"pool key %q must match %s", pool.Key, trafficDirectorPoolKeyPattern.String(),
			)
		}
		if _, exists := poolsByKey[pool.Key]; exists {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"pool key %q is duplicated", pool.Key,
			)
		}
		if pool.WeightBPS < 0 || pool.WeightBPS > domain.TrafficDirectorWeightTotalBPS {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"pool %q weight_bps must be between 0 and %d",
				pool.Key,
				domain.TrafficDirectorWeightTotalBPS,
			)
		}
		if pool.WeightBPS > 0 {
			totalPositiveWeight += int64(pool.WeightBPS)
		}
		if pool.MinAvailable < 1 || pool.MinAvailable > len(pool.AccountIDs) {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"pool %q min_available must be between 1 and its account count %d",
				pool.Key,
				len(pool.AccountIDs),
			)
		}

		for _, accountID := range pool.AccountIDs {
			if accountID <= 0 {
				return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
					"pool %q contains non-positive account ID %d", pool.Key, accountID,
				)
			}
			if existingPoolKey, exists := accountPoolByID[accountID]; exists {
				return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
					"account ID %d appears in both pool %q and pool %q",
					accountID,
					existingPoolKey,
					pool.Key,
				)
			}
			accountPoolByID[accountID] = pool.Key
		}
		poolsByKey[pool.Key] = pool
	}

	if totalPositiveWeight != int64(domain.TrafficDirectorWeightTotalBPS) {
		return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
			"positive weight_bps total must equal %d, got %d",
			domain.TrafficDirectorWeightTotalBPS,
			totalPositiveWeight,
		)
	}

	fallbackTargets := make(map[string]struct{}, len(normalized.Pools))
	for _, pool := range normalized.Pools {
		if pool.FallbackPoolKey == "" {
			continue
		}
		if _, exists := poolsByKey[pool.FallbackPoolKey]; !exists {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"pool %q references missing fallback pool %q", pool.Key, pool.FallbackPoolKey,
			)
		}
		fallbackTargets[pool.FallbackPoolKey] = struct{}{}
	}
	for _, pool := range normalized.Pools {
		if pool.WeightBPS != 0 {
			continue
		}
		if _, referenced := fallbackTargets[pool.Key]; !referenced {
			return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
				"zero-weight pool %q must be referenced as a fallback", pool.Key,
			)
		}
	}
	if err := validateTrafficDirectorFallbackGraph(normalized.Pools, poolsByKey); err != nil {
		return domain.TrafficDirectorSpec{}, nil, err
	}

	canonical, err := json.Marshal(normalized)
	if err != nil {
		return domain.TrafficDirectorSpec{}, nil, fmt.Errorf("marshal canonical traffic director spec: %w", err)
	}
	if len(canonical) > domain.TrafficDirectorMaxCanonicalJSONBytes {
		return domain.TrafficDirectorSpec{}, nil, invalidTrafficDirectorSpec(
			"canonical JSON size %d exceeds limit %d",
			len(canonical),
			domain.TrafficDirectorMaxCanonicalJSONBytes,
		)
	}

	return normalized, canonical, nil
}

func validateTrafficDirectorFallbackGraph(
	pools []domain.TrafficDirectorPool,
	poolsByKey map[string]domain.TrafficDirectorPool,
) error {
	const (
		trafficDirectorPoolVisiting = 1
		trafficDirectorPoolVisited  = 2
	)

	states := make(map[string]uint8, len(pools))
	var visit func(string) error
	visit = func(poolKey string) error {
		switch states[poolKey] {
		case trafficDirectorPoolVisiting:
			return invalidTrafficDirectorSpec("fallback cycle detected at pool %q", poolKey)
		case trafficDirectorPoolVisited:
			return nil
		}

		states[poolKey] = trafficDirectorPoolVisiting
		if fallbackPoolKey := poolsByKey[poolKey].FallbackPoolKey; fallbackPoolKey != "" {
			if err := visit(fallbackPoolKey); err != nil {
				return err
			}
		}
		states[poolKey] = trafficDirectorPoolVisited
		return nil
	}

	for _, pool := range pools {
		if err := visit(pool.Key); err != nil {
			return err
		}
	}
	return nil
}

func evaluateNormalizedTrafficDirector(
	spec domain.TrafficDirectorSpec,
	groupID int64,
	routingKey string,
) (TrafficDirectorEvaluation, error) {
	homePoolKey := chooseTrafficDirectorHomePool(spec.Pools, groupID, routingKey)
	fallbackChain, err := FollowFallback(spec, homePoolKey)
	if err != nil {
		return TrafficDirectorEvaluation{}, err
	}
	fallbackPoolKeys := []string{}
	if len(fallbackChain) > 1 {
		fallbackPoolKeys = fallbackChain[1:]
	}

	return TrafficDirectorEvaluation{
		HomePoolKey:      homePoolKey,
		FallbackPoolKeys: fallbackPoolKeys,
	}, nil
}

// FollowFallback returns the configured chain in order, including startKey.
// It performs its own missing-target and cycle checks so callers are protected
// even when handed a policy which has not passed validation.
func FollowFallback(spec domain.TrafficDirectorSpec, startKey string) ([]string, error) {
	startKey = strings.TrimSpace(startKey)
	if startKey == "" {
		return nil, invalidTrafficDirectorEvaluation("fallback start key must not be empty")
	}

	poolsByKey := make(map[string]domain.TrafficDirectorPool, len(spec.Pools))
	for _, pool := range spec.Pools {
		pool.Key = strings.TrimSpace(pool.Key)
		pool.FallbackPoolKey = strings.TrimSpace(pool.FallbackPoolKey)
		if _, exists := poolsByKey[pool.Key]; exists {
			return nil, invalidTrafficDirectorSpec("pool key %q is duplicated", pool.Key)
		}
		poolsByKey[pool.Key] = pool
	}

	chain := make([]string, 0, len(spec.Pools))
	seen := make(map[string]struct{}, len(spec.Pools))
	for poolKey := startKey; poolKey != ""; {
		if _, visited := seen[poolKey]; visited {
			return nil, invalidTrafficDirectorSpec("fallback cycle detected at pool %q", poolKey)
		}
		pool, exists := poolsByKey[poolKey]
		if !exists {
			return nil, invalidTrafficDirectorSpec("fallback chain references missing pool %q", poolKey)
		}
		seen[poolKey] = struct{}{}
		chain = append(chain, poolKey)
		poolKey = pool.FallbackPoolKey
	}
	return chain, nil
}

func chooseTrafficDirectorHomePool(
	pools []domain.TrafficDirectorPool,
	groupID int64,
	routingKey string,
) string {
	bestPoolKey := ""
	bestScore := math.Inf(1)
	for _, pool := range pools {
		if pool.WeightBPS == 0 {
			continue
		}

		score := trafficDirectorWeightedRendezvousScore(groupID, routingKey, pool.Key, pool.WeightBPS)
		if score < bestScore || (score == bestScore && (bestPoolKey == "" || pool.Key < bestPoolKey)) {
			bestScore = score
			bestPoolKey = pool.Key
		}
	}
	return bestPoolKey
}

func trafficDirectorWeightedRendezvousScore(
	groupID int64,
	routingKey string,
	poolKey string,
	weightBPS int,
) float64 {
	hasher := sha256.New()
	var lengthPrefix [8]byte
	// Every part is framed with its unsigned 64-bit big-endian byte length.
	for _, part := range []string{
		"traffic-director/v1",
		strconv.FormatInt(groupID, 10),
		routingKey,
		poolKey,
		strconv.Itoa(weightBPS),
	} {
		binary.BigEndian.PutUint64(lengthPrefix[:], uint64(len(part)))
		_, _ = hasher.Write(lengthPrefix[:])
		_, _ = hasher.Write([]byte(part))
	}
	digest := hasher.Sum(nil)

	hashValue := binary.BigEndian.Uint64(digest[:8])
	hashUniformDenominator := math.Ldexp(1, 64) + 1
	var exponentialCost float64
	if hashValue >= uint64(1)<<63 {
		// For the upper half, evaluate -ln(u) as -log1p(-(1-u)).
		// This avoids rounding u=(n+1)/(2^64+1) to exactly 1 near MaxUint64.
		complement := (float64(^uint64(0)-hashValue) + 1) / hashUniformDenominator
		exponentialCost = -math.Log1p(-complement)
	} else {
		uniform := (float64(hashValue) + 1) / hashUniformDenominator
		exponentialCost = -math.Log(uniform)
	}
	return exponentialCost / float64(weightBPS)
}

func invalidTrafficDirectorSpec(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidTrafficDirectorSpec, fmt.Sprintf(format, args...))
}

func invalidTrafficDirectorEvaluation(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidTrafficDirectorEvaluation, fmt.Sprintf(format, args...))
}
