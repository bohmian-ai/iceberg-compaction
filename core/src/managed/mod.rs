//! Wyrd-managed extensions to the compaction core.
//!
//! Two capabilities live here, and nothing else:
//!
//! * **Caller-bound execution** ([`context`], [`observer`]) — a caller leases
//!   the runtime, memory budget, scratch root, cancellation token, and observer
//!   for exactly one publication attempt, and receives a complete physical
//!   account of what that attempt did.
//! * **Identity-aware selection** ([`selection`]) — a core-owned selection
//!   policy that treats schema, partition-spec, sort-order, and writer-recipe
//!   drift as first-class reasons to rewrite a file, alongside size.
//!
//! No durable behavior lives here. There is no tenant identity, no lease
//! negotiation, no SQL, no audit, no commit, no reconciliation, and no object
//! garbage collection: those belong to the caller, and moving any of them into
//! the core would make the core the authority on someone else's data.

pub mod bridge;
pub mod context;
pub mod observer;
pub mod selection;

pub use bridge::{AttemptLedger, RollingObserverBridge};
pub use context::{
    ManagedExecutionContext, ManagedExecutionContextBuilder, PeakTrackingMemoryPool, SpillLease,
};
pub use selection::{
    CandidateIdentity, IdentityAwareSelector, OpenPartitionPolicy, PolicyIdentity, SelectedFile,
    SelectionGroup, SelectionReason, SelectionReport, SelectionStrategyKind, WriterRecipeResolver,
    WyrdSelectionPolicy,
};
pub use observer::{
    AttemptId, NoopRewriteObserver, OutputIdentity, RewriteEvent, RewriteObserver, noop_observer,
};
