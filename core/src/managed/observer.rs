//! Physical observation of one managed rewrite attempt.
//!
//! The observer is a reporting seam, not a control seam. Emission is
//! synchronous and infallible, applies no backpressure, and can neither
//! reorder nor suppress work. Events carry the attempt identity and a stable
//! logical ordinal so a caller can attribute every physical object a rewrite
//! opened, including objects whose close was still in flight when the attempt
//! was cancelled.
//!
//! Events carry no tenant authority and no commit permission. Deciding what a
//! reported object means, and whether it may be published, stays with the
//! caller.

use std::fmt::Debug;
use std::sync::Arc;

use iceberg::writer::file_writer::rolling_writer::RollingCloseReason;
use uuid::Uuid;

/// Identity of exactly one publication attempt.
///
/// A rewrite that is retried is a new attempt with a new identity, so outputs
/// produced by an abandoned attempt can never be confused with outputs of the
/// attempt that actually publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttemptId(Uuid);

impl AttemptId {
    /// Creates a fresh attempt identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Creates an attempt identity from a caller-owned UUID.
    ///
    /// Wyrd mints attempt identity in its durable plan, so the core must be
    /// able to adopt that identity rather than inventing a second one.
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for AttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AttemptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One physical output object an attempt opened.
///
/// `settled` distinguishes an object whose close completed and whose contents
/// are therefore known, from an object whose close was still outstanding when
/// the attempt ended. Both are reported: an unsettled object may or may not
/// exist in storage, and the caller must treat it as possibly-produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputIdentity {
    /// Ordinal assigned when the output was opened, stable across concurrency.
    pub logical_ordinal: u64,
    /// Storage path of the output object.
    pub path: String,
    /// Whether the close for this output completed before the attempt ended.
    pub settled: bool,
}

/// Closed set of physical events a managed rewrite reports.
///
/// The set is closed so a caller can exhaustively match it and so no future
/// event can silently go unhandled.
#[derive(Debug, Clone, PartialEq)]
pub enum RewriteEvent {
    /// A new output object was opened and assigned its logical ordinal.
    OutputOpened {
        /// Attempt that opened the output.
        attempt_id: AttemptId,
        /// Ordinal assigned at open time.
        logical_ordinal: u64,
        /// Storage path of the output object.
        path: String,
    },
    /// The writer decided to close an output.
    ///
    /// `written_size_estimate_bytes` is the writer's anticipated encoded size
    /// at the moment of the decision. It is an estimate, never a guarantee of
    /// the object's physical byte length.
    RollDecided {
        /// Attempt that made the decision.
        attempt_id: AttemptId,
        /// Ordinal of the output being closed.
        logical_ordinal: u64,
        /// Storage path of the output object.
        path: String,
        /// Why the output is being closed.
        reason: RollingCloseReason,
        /// Configured target the decision was made against.
        target_file_size_bytes: usize,
        /// Anticipated encoded size observed at the decision.
        written_size_estimate_bytes: usize,
    },
    /// A close finished, successfully or not.
    OutputClosed {
        /// Attempt that closed the output.
        attempt_id: AttemptId,
        /// Ordinal assigned when the output was opened.
        logical_ordinal: u64,
        /// Ordinal reflecting the order closes actually settled.
        completion_ordinal: u64,
        /// Storage path of the output object.
        path: String,
        /// Why the output was closed.
        reason: RollingCloseReason,
        /// Number of data files the close produced, or `None` when it failed.
        output_files: Option<usize>,
    },
    /// Peak memory reserved from the leased pool during the attempt.
    PeakMemory {
        /// Attempt the measurement belongs to.
        attempt_id: AttemptId,
        /// Highest simultaneous reservation observed.
        peak_bytes: usize,
        /// Capacity of the leased pool, when it is bounded.
        pool_capacity_bytes: Option<usize>,
    },
    /// A query operator spilled to disk under memory pressure.
    OperatorSpill {
        /// Attempt the spill belongs to.
        attempt_id: AttemptId,
        /// Number of spill events the plan reported.
        spill_count: usize,
        /// Bytes the plan reported spilling.
        spilled_bytes: u64,
        /// Rows the plan reported spilling.
        spilled_rows: u64,
    },
    /// Accounting for the caller-leased scratch root.
    ScratchSpill {
        /// Attempt the accounting belongs to.
        attempt_id: AttemptId,
        /// Bytes currently resident under the scratch root.
        current_bytes: u64,
        /// Highest byte usage observed under the scratch root.
        peak_bytes: u64,
    },
    /// The attempt produced its complete output set.
    Succeeded {
        /// Attempt that succeeded.
        attempt_id: AttemptId,
        /// Every output the attempt produced.
        outputs: Vec<OutputIdentity>,
        /// Total physical bytes across the produced outputs.
        output_bytes: u64,
    },
    /// The attempt failed.
    ///
    /// Outputs are still reported: a failure after some objects were written
    /// leaves those objects in storage, and the caller owns their cleanup.
    Failed {
        /// Attempt that failed.
        attempt_id: AttemptId,
        /// Rendered error, for diagnostics only.
        message: String,
        /// Every output the attempt produced or may have produced.
        outputs: Vec<OutputIdentity>,
    },
    /// The attempt was cancelled and fully drained.
    Cancelled {
        /// Attempt that was cancelled.
        attempt_id: AttemptId,
        /// Every output the attempt produced or may have produced.
        outputs: Vec<OutputIdentity>,
    },
}

impl RewriteEvent {
    /// Returns the attempt this event belongs to.
    #[must_use]
    pub fn attempt_id(&self) -> AttemptId {
        match self {
            Self::OutputOpened { attempt_id, .. }
            | Self::RollDecided { attempt_id, .. }
            | Self::OutputClosed { attempt_id, .. }
            | Self::PeakMemory { attempt_id, .. }
            | Self::OperatorSpill { attempt_id, .. }
            | Self::ScratchSpill { attempt_id, .. }
            | Self::Succeeded { attempt_id, .. }
            | Self::Failed { attempt_id, .. }
            | Self::Cancelled { attempt_id, .. } => *attempt_id,
        }
    }

    /// Returns whether this event terminates the attempt.
    ///
    /// Exactly one terminal event is emitted per attempt.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

/// Receives physical events from a managed rewrite.
///
/// Implementations must be cheap and must not block: emission happens inline
/// on the writer path, and a slow observer would apply backpressure the writer
/// does not model.
pub trait RewriteObserver: Debug + Send + Sync {
    /// Delivers one event. Must not panic and must not block.
    fn on_event(&self, event: RewriteEvent);
}

/// Observer that discards every event.
///
/// The default for callers that want managed execution without observation.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRewriteObserver;

impl RewriteObserver for NoopRewriteObserver {
    fn on_event(&self, _event: RewriteEvent) {}
}

/// Returns a shared no-op observer.
#[must_use]
pub fn noop_observer() -> Arc<dyn RewriteObserver> {
    Arc::new(NoopRewriteObserver)
}
