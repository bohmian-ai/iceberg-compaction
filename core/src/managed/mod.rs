//! Wyrd-managed extensions to the compaction core.
//!
//! Two capabilities live here, and nothing else:
//!
//! * **Caller-bound execution** ([`context`], [`observer`], [`boundary`]) — a
//!   caller leases the runtime, memory budget, scratch root, cancellation
//!   token, and observer for exactly one non-committing publication attempt,
//!   and receives a complete account of every output that attempt opened.
//! * **Selection reports** ([`selection`]) — the durable account of which files
//!   an upstream planning pass selected, and against which snapshot.
//!
//! No durable behavior lives here. There is no tenant identity, no lease
//! negotiation, no SQL, no audit, no commit, no reconciliation, and no object
//! garbage collection: those belong to the caller, and moving any of them into
//! the core would make the core the authority on someone else's data.

pub mod boundary;
pub mod bridge;
pub mod context;
pub mod observer;
pub mod selection;

pub use boundary::NonCommittingCompaction;
pub use bridge::{AttemptLedger, RollingObserverBridge};
pub use context::{ManagedExecutionContext, ManagedExecutionContextBuilder, SpillLease};
pub use observer::{
    AttemptId, NoopRewriteObserver, OutputIdentity, RewriteEvent, RewriteObserver, noop_observer,
};
pub use selection::{SelectedFile, SelectionReason, SelectionReport, SelectionStrategyKind};
