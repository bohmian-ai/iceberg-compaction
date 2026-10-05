//! Caller-bound execution context for exactly one managed rewrite attempt.
//!
//! The context carries the resources a caller leases to the core for the
//! duration of one publication attempt: the `DataFusion` runtime, a memory pool,
//! a scratch root whose lifetime the caller owns, a cancellation token, and the
//! rewrite observer.
//!
//! Leasing rather than constructing is the point. The core does not decide how
//! much memory an attempt may use, where it may spill, or how long that scratch
//! lives; it charges the caller's pool, spills under the caller's root, and
//! stops when told to. No durable behavior
//! moves into the core as a result: the context holds no tenant identity, no
//! catalog authority, and no commit permission.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use tokio_util::sync::CancellationToken;

use super::bridge::AttemptLedger;
use super::observer::{AttemptId, RewriteObserver, noop_observer};
use crate::error::{CompactionError, Result};

/// A caller-leased scratch root.
///
/// The core never creates or removes the root: the caller owns its lifetime,
/// which is what makes scratch usage auditable against a lease rather than
/// against whatever the OS temp directory happens to allow.
#[derive(Debug, Clone)]
pub struct SpillLease {
    root: PathBuf,
}

impl SpillLease {
    /// Creates a lease over an existing directory.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when `root` is not an existing
    /// directory. Creating it is the caller's responsibility precisely because
    /// the caller owns when it goes away.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.is_dir() {
            return Err(CompactionError::Config(format!(
                "spill root {} does not exist or is not a directory",
                root.display()
            )));
        }
        Ok(Self { root })
    }

    /// Returns the leased scratch root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// Resources a caller leases to the core for one publication attempt.
///
/// Construct with [`ManagedExecutionContextBuilder`]. The context is shared
/// behind an `Arc` by the executor and by every rewrite it runs, so a single
/// attempt's memory, scratch, cancellation, and observation are one unit even
/// when several rewrites run concurrently.
#[derive(Debug)]
pub struct ManagedExecutionContext {
    attempt_id: AttemptId,
    runtime_env: Arc<RuntimeEnv>,
    cancellation: CancellationToken,
    observer: Arc<dyn RewriteObserver>,
    ledger: Arc<AttemptLedger>,
}

impl ManagedExecutionContext {
    /// Starts building a context.
    #[must_use]
    pub fn builder() -> ManagedExecutionContextBuilder {
        ManagedExecutionContextBuilder::default()
    }

    /// Returns the attempt this context belongs to.
    #[must_use]
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the leased `DataFusion` runtime.
    ///
    /// Every rewrite builds its own isolated session over this one runtime, so
    /// the memory and disk budget is shared while session catalog state is not.
    #[must_use]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.runtime_env)
    }

    /// Returns the caller's cancellation token.
    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns whether the caller has requested cancellation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns the rewrite observer.
    #[must_use]
    pub fn observer(&self) -> &Arc<dyn RewriteObserver> {
        &self.observer
    }

    /// Returns the attempt's one output ledger.
    ///
    /// The ledger belongs to the attempt, not to a rewrite call: an attempt
    /// that executes several plans must number every object it opens in one
    /// strictly increasing space and must be able to report the cumulative set
    /// at its terminal event. A per-call ledger would restart ordinals at zero
    /// for every plan, so two objects from one attempt could carry ordinal `0`,
    /// and a later plan's terminal event would name only that plan's objects —
    /// silently discarding evidence about objects an earlier plan left in
    /// storage.
    #[must_use]
    pub fn ledger(&self) -> &Arc<AttemptLedger> {
        &self.ledger
    }
}

/// Builds a [`ManagedExecutionContext`].
///
/// Every field has a safe default, so a caller that only wants cancellation
/// need not describe a memory or scratch policy it does not have.
#[derive(Debug, Default)]
pub struct ManagedExecutionContextBuilder {
    attempt_id: Option<AttemptId>,
    memory_pool: Option<Arc<dyn MemoryPool>>,
    spill: Option<SpillLease>,
    cancellation: Option<CancellationToken>,
    observer: Option<Arc<dyn RewriteObserver>>,
}

impl ManagedExecutionContextBuilder {
    /// Adopts the caller's attempt identity instead of minting one.
    #[must_use]
    pub fn with_attempt_id(mut self, attempt_id: AttemptId) -> Self {
        self.attempt_id = Some(attempt_id);
        self
    }

    /// Leases a memory pool.
    ///
    /// The runtime charges this pool directly, so the caller's own limit and
    /// spill semantics govern every reservation the attempt makes.
    #[must_use]
    pub fn with_memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = Some(pool);
        self
    }

    /// Leases a scratch root for operator spilling.
    #[must_use]
    pub fn with_spill_lease(mut self, spill: SpillLease) -> Self {
        self.spill = Some(spill);
        self
    }

    /// Binds the caller's cancellation token.
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    /// Installs the rewrite observer.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn RewriteObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Builds the context, constructing the leased runtime.
    ///
    /// The runtime is built here rather than accepted whole so the leased pool
    /// and the leased scratch root are guaranteed to be installed: a caller
    /// cannot hand in a runtime that silently spills somewhere the lease does
    /// not account for.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::DataFusion`] when the runtime cannot be built.
    pub fn build(self) -> Result<Arc<ManagedExecutionContext>> {
        let pool = self
            .memory_pool
            .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()) as Arc<dyn MemoryPool>);
        let mut runtime = RuntimeEnvBuilder::new().with_memory_pool(pool);
        if let Some(spill) = self.spill.as_ref() {
            runtime = runtime.with_disk_manager_builder(
                datafusion::execution::disk_manager::DiskManagerBuilder::default().with_mode(
                    datafusion::execution::disk_manager::DiskManagerMode::Directories(vec![
                        spill.root().to_path_buf(),
                    ]),
                ),
            );
        }

        let attempt_id = self.attempt_id.unwrap_or_default();
        let observer = self.observer.unwrap_or_else(noop_observer);
        let ledger = AttemptLedger::new(attempt_id, Arc::clone(&observer));

        Ok(Arc::new(ManagedExecutionContext {
            attempt_id,
            runtime_env: runtime.build_arc()?,
            cancellation: self.cancellation.unwrap_or_default(),
            observer,
            ledger,
        }))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};

    use super::*;

    /// The runtime charges the leased pool and spills under the leased root.
    ///
    /// Both leases are the caller's governance: a reservation the leased pool
    /// does not see, or a spill file outside the leased root, would escape the
    /// budget the caller admitted.
    ///
    /// # Panics
    ///
    /// Panics when the context cannot be built, when a reservation bypasses the
    /// leased pool, or when a spill file lands outside the leased root.
    #[test]
    fn managed_context_charges_leased_pool_and_spills_under_leased_root() {
        let root = tempfile::tempdir().expect("scratch root");
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let context = ManagedExecutionContext::builder()
            .with_memory_pool(Arc::clone(&pool))
            .with_spill_lease(SpillLease::new(root.path()).expect("the scratch root exists"))
            .build()
            .expect("a leased context builds");
        let runtime = context.runtime_env();

        let reservation = MemoryConsumer::new("lease").register(&runtime.memory_pool);
        reservation.try_grow(512).expect("a grant inside the lease");
        assert_eq!(pool.reserved(), 512, "the runtime charges the leased pool");
        assert!(
            reservation.try_grow(1024).is_err(),
            "the leased pool's own limit refuses an overdraw"
        );

        let spill = runtime
            .disk_manager
            .create_tmp_file("managed scratch lease")
            .expect("a spill file inside the lease is created");
        assert!(
            spill
                .path()
                .is_some_and(|path| path.starts_with(root.path())),
            "spill files land under the leased root"
        );
    }
}
