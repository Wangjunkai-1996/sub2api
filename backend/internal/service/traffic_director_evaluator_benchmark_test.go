package service

import (
	"strconv"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

var benchmarkTrafficDirectorEvaluation TrafficDirectorEvaluation

func BenchmarkTrafficDirectorCompiledEvaluate(b *testing.B) {
	for _, poolCount := range []int{2, domain.TrafficDirectorMaxPools - 1} {
		b.Run(strconv.Itoa(poolCount)+"_weighted_pools", func(b *testing.B) {
			spec := benchmarkTrafficDirectorSpec(poolCount)
			normalized, _, err := normalizeTrafficDirectorSpec(spec)
			if err != nil {
				b.Fatal(err)
			}
			compiled, err := compileNormalizedTrafficDirectorSpec(normalized)
			if err != nil {
				b.Fatal(err)
			}

			routingKeys := make([]string, 1024)
			for i := range routingKeys {
				routingKeys[i] = "benchmark-routing-key-" + strconv.Itoa(i)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				evaluation, err := compiled.evaluate(42, routingKeys[i%len(routingKeys)])
				if err != nil {
					b.Fatal(err)
				}
				benchmarkTrafficDirectorEvaluation = evaluation
			}
		})
	}
}

func benchmarkTrafficDirectorSpec(weightedPoolCount int) domain.TrafficDirectorSpec {
	const backupPoolKey = "backup"
	pools := make([]domain.TrafficDirectorPool, 0, weightedPoolCount+1)
	baseWeight := domain.TrafficDirectorWeightTotalBPS / weightedPoolCount
	remainder := domain.TrafficDirectorWeightTotalBPS % weightedPoolCount
	for i := 0; i < weightedPoolCount; i++ {
		weight := baseWeight
		if i < remainder {
			weight++
		}
		pools = append(pools, domain.TrafficDirectorPool{
			Key:             "pool-" + strconv.Itoa(i),
			WeightBPS:       weight,
			AccountIDs:      []int64{int64(i + 1)},
			MinAvailable:    1,
			FallbackPoolKey: backupPoolKey,
		})
	}
	pools = append(pools, domain.TrafficDirectorPool{
		Key:          backupPoolKey,
		AccountIDs:   []int64{int64(weightedPoolCount + 1)},
		MinAvailable: 1,
	})
	return domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeOff,
		Pools:         pools,
	}
}
