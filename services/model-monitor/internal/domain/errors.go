// errors.go — sentinel errors owned by the Model Monitor domain.
//
// WHY domain (business) errors are distinct from storage sentinels (ports.go):
// a storage outcome ("no row") is not the same fact as a business outcome
// ("monitor not found for this model"). The service is the single translation
// point — it catches a storage sentinel and returns the matching business error,
// so the handler maps ONE vocabulary to gRPC status codes and never needs to know
// storage exists.
//
// errors.Is-friendly: callers (handler, tests) match with errors.Is(err, ErrX).
// Validation errors are wrapped with %w around ErrValidation so a caller gets a
// specific message AND can still classify the error class.
package domain

import "errors"

var (
	// ErrValidation classifies malformed input the service rejects before
	// touching any port (empty model name, warn > critical, window_size over the
	// cap, etc.). Wrapped with %w so callers get a precise message while still
	// being able to errors.Is(err, ErrValidation). Handler maps → InvalidArgument.
	ErrValidation = errors.New("monitor: validation failed")

	// ErrMonitorNotFound is returned when an operation targets a model that has no
	// monitor (GetModelHealth/GetMonitorStatus/ResetBaseline on an unmonitored
	// model). Handler maps → NotFound. Business translation of ErrRepoNotFound.
	ErrMonitorNotFound = errors.New("monitor: monitor not found")

	// ErrReportNotFound is returned by GetDriftReport when no report has that id.
	// Handler maps → NotFound.
	ErrReportNotFound = errors.New("monitor: drift report not found")

	// ErrBaselineUnavailable is returned when the monitor cannot resolve a
	// training-time baseline for a model version (Registry/Experiment-Tracker has
	// none, or the version does not exist). The monitor stays PENDING_BASELINE and
	// refuses to score rather than inventing a baseline. Handler maps → FailedPrecondition.
	ErrBaselineUnavailable = errors.New("monitor: baseline unavailable")

	// ErrRetrainPipelineRequired is returned by ConfigureMonitor when AutoRetrain
	// is enabled but no RetrainPipelineID is set. WHY a dedicated sentinel: arming
	// the closed loop without a pipeline to call would silently no-op every
	// breach — a misconfiguration we fail-fast on. Handler maps → InvalidArgument.
	ErrRetrainPipelineRequired = errors.New("monitor: auto_retrain requires a retrain_pipeline_id")

	// ErrInsufficientSamples is returned by the scorer when a window has fewer than
	// MinSamples. It is NOT an error the handler surfaces to clients — the scorer
	// uses it internally to keep the monitor WARMING_UP and skip scoring. A drift
	// score over a near-empty window is noise, not signal.
	ErrInsufficientSamples = errors.New("monitor: window has insufficient samples to score")

	// ErrEmptyDistribution is returned by the drift math when a distribution has no
	// mass (every bin zero), which makes PSI/KL/KS undefined. Callers treat it as
	// "cannot score yet", not as drift.
	ErrEmptyDistribution = errors.New("monitor: distribution has no mass")

	// ErrBinMismatch is returned when two distributions being compared do not share
	// the same bin layout. PSI/KL compare bin-for-bin; mismatched bins would
	// compare apples to oranges. Surfaced as a programming/data error, not drift.
	ErrBinMismatch = errors.New("monitor: distributions have mismatched bins")
)
