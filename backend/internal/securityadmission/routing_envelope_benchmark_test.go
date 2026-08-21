package securityadmission

import (
	"sort"
	"strconv"
	"testing"
	"time"
)

const benchmarkP99Samples = 512

func reportFixedP99(b *testing.B, run func() error) {
	b.Helper()
	samples := make([]int64, benchmarkP99Samples)
	for i := range samples {
		startedAt := time.Now()
		if err := run(); err != nil {
			b.Fatal(err)
		}
		samples[i] = time.Since(startedAt).Nanoseconds()
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	index := (len(samples)*99 + 99) / 100
	if index > len(samples) {
		index = len(samples)
	}
	b.ReportMetric(float64(samples[index-1]), "p99-ns/op")
}

func BenchmarkClassifyP99(b *testing.B) {
	for _, size := range []int{256 << 10, DefaultBodyCapBytes, DefaultBodyCapBytes + 1} {
		body := benchmarkBody(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			run := func() error {
				_, err := Classify(string(ProtocolOpenAIResponses), body, Options{})
				return err
			}
			b.ReportMetric(float64(len(body)), "bytes/request")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := run(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportFixedP99(b, run)
		})
	}
}

func BenchmarkRoutingEnvelopeBounded(b *testing.B) {
	for _, size := range []int{DefaultBodyCapBytes + 1, 4 << 20, 32 << 20} {
		prefix := []byte(`{"model":"bench","stream":true,"input":"`)
		body := make([]byte, size)
		copy(body, prefix)
		for i := len(prefix); i < len(body); i++ {
			body[i] = 'x'
		}
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			run := func() error {
				_, err := ExtractRoutingEnvelope(string(ProtocolOpenAIResponses), body)
				return err
			}
			b.ReportMetric(float64(len(body)), "bytes/request")
			b.ReportMetric(float64(RoutingEnvelopeWindowBytes), "window-bytes/request")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := run(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportFixedP99(b, run)
		})
	}
}
