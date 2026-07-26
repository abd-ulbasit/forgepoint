// metrics.go — the serving-metrics accumulator: the HPA scaling signal.
//
// ============================================================================
// WHY THIS IS ITS OWN TYPE (and why it's the heart of the Sidecar+HPA pattern)
// ============================================================================
//
// In "HPA on custom metrics", the autoscaler does NOT scale on CPU — it scales
// on INFLIGHT REQUESTS (queue depth / concurrency), because inference latency is
// dominated by queuing once the CPU is busy, so inflight crosses the danger
// threshold BEFORE CPU saturates. The number the HPA reads therefore has to be
// CORRECT and CONCURRENCY-SAFE: many goroutines (one per in-flight Predict)
// mutate it simultaneously. This type owns that math behind a mutex so the rest
// of the service never touches a counter directly and can't get the locking
// wrong.
//
// WHY a mutex and not atomics: we update SEVERAL fields together per call
// (inflight, total/failed, last/percentile latency). A mutex makes that group a
// single consistent transaction; mixing atomics for some fields and a lock for
// others is the classic source of torn reads (the HPA scraping a half-updated
// snapshot). One lock, one snapshot — simpler and correct. Under -race this is
// what keeps the concurrent-predict test clean.
//
// LATENCY PERCENTILES: a production pod would feed latencies into an HDR
// histogram or t-digest for true p50/p99. For the domain we keep a small ring
// buffer and compute the percentiles by sorting a copy on read. WHY this is
// acceptable here: the scrape interval is seconds and the window is small
// (bounded memory), so the O(k log k) sort on read is negligible, and it keeps
// the domain dependency-free (no histogram library). The interface (P50/P99) is
// what matters and is honest; the estimator can be swapped without changing the
// ServingMetrics contract. The tradeoff is called out explicitly: the "real"
// answer is streaming histograms; the simpler estimator is a deliberate choice
// for a small, bounded window.
// ============================================================================
package domain

import (
	"sort"
	"sync"
	"time"
)

// latencyWindow is how many recent inference latencies we retain for the
// rolling p50/p99 estimate. Small = bounded memory and a fast sort on read;
// large enough that the percentile is meaningful for an HPA scrape window.
const latencyWindow = 256

// metricsAccumulator holds the live RED + inflight metrics for the pod's
// inference work. All access goes through its methods, which lock.
type metricsAccumulator struct {
	mu sync.Mutex

	inflight      int32         // requests currently executing (THE HPA signal)
	total         int64         // monotonic count of all predictions attempted
	failed        int64         // monotonic count of failed predictions
	lastLatency   time.Duration // most recent single-call latency (liveness signal)
	memoryBytes   int64         // resident model memory (set at load)
	latencies     []time.Duration
	latenciesHead int // ring-buffer write position
}

// newMetricsAccumulator returns an accumulator with a preallocated ring buffer.
func newMetricsAccumulator() *metricsAccumulator {
	return &metricsAccumulator{latencies: make([]time.Duration, 0, latencyWindow)}
}

// beginRequest increments inflight and returns. It is called at the START of
// Predict — BEFORE the engine runs — so the HPA observes queue depth WHILE the
// call is in flight, not only after it completes. (The InflightObservedDuring
// test asserts exactly this.) The matching endRequest MUST run on every exit
// path (success AND error), which the service guarantees with a defer.
func (m *metricsAccumulator) beginRequest() {
	m.mu.Lock()
	m.inflight++
	m.mu.Unlock()
}

// endRequest decrements inflight, bumps the total (and failed, if failed),
// records the call's latency, and updates the last-latency liveness signal. One
// locked transaction so a concurrent GetSnapshot never sees a torn update.
//
// WHY total is incremented HERE (on completion) and not in beginRequest: total
// counts COMPLETED predictions (the denominator of an error rate). Counting at
// completion keeps total and failed consistent — every failure is also a
// completion. inflight, by contrast, is the in-progress gauge and is the pair
// inc/dec around the call.
func (m *metricsAccumulator) endRequest(latency time.Duration, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inflight--
	m.total++
	if failed {
		m.failed++
	}
	m.lastLatency = latency
	m.recordLatencyLocked(latency)
}

// recordLatencyLocked appends a latency into the fixed-size ring buffer. Caller
// must hold the lock. The ring keeps memory bounded regardless of throughput.
func (m *metricsAccumulator) recordLatencyLocked(d time.Duration) {
	if len(m.latencies) < latencyWindow {
		m.latencies = append(m.latencies, d)
		return
	}
	m.latencies[m.latenciesHead] = d
	m.latenciesHead = (m.latenciesHead + 1) % latencyWindow
}

// setMemory records the resident model memory (called once at load). Locked so
// a concurrent snapshot is consistent.
func (m *metricsAccumulator) setMemory(bytes int64) {
	m.mu.Lock()
	m.memoryBytes = bytes
	m.mu.Unlock()
}

// snapshot returns a consistent point-in-time ServingMetrics. It computes the
// rolling p50/p99 by copying and sorting the latency window under the lock —
// see the percentile tradeoff note at the top of the file.
func (m *metricsAccumulator) snapshot() ServingMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	p50, p99 := percentilesLocked(m.latencies)
	return ServingMetrics{
		InflightRequests:     m.inflight,
		TotalRequests:        m.total,
		FailedRequests:       m.failed,
		P50Latency:           p50,
		P99Latency:           p99,
		LastInferenceLatency: m.lastLatency,
		ModelMemoryBytes:     m.memoryBytes,
	}
}

// lastInferenceLatency returns just the most recent latency (the raw liveness
// signal HealthCheck needs) without computing percentiles.
func (m *metricsAccumulator) lastInferenceLatency() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLatency
}

// percentilesLocked computes p50 and p99 from a latency slice. Caller holds the
// lock; we copy before sorting so we never reorder the live ring buffer (which
// would corrupt its insertion order). Returns (0,0) for an empty window.
//
// PERCENTILE INDEXING: we use the "nearest-rank" method — p-th percentile is the
// value at ceil(p/100 * N), 1-indexed, clamped to the last element. It is simple
// and exact for the discrete samples we hold (no interpolation ambiguity).
func percentilesLocked(window []time.Duration) (p50, p99 time.Duration) {
	n := len(window)
	if n == 0 {
		return 0, 0
	}
	cp := make([]time.Duration, n)
	copy(cp, window)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[rankIndex(50, n)], cp[rankIndex(99, n)]
}

// rankIndex returns the 0-based index of the p-th percentile by nearest rank
// over n samples. ceil(p/100 * n) gives the 1-based rank; subtract 1 for the
// 0-based index, clamped to [0, n-1].
func rankIndex(p, n int) int {
	// ceil(p*n/100) without floats: (p*n + 99) / 100.
	rank := (p*n + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return rank - 1
}
