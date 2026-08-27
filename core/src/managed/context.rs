//! Caller-bound execution context for exactly one managed rewrite attempt.
//!
//! The context carries the resources a caller leases to the core for the
//! duration of one publication attempt: the `DataFusion` runtime, a memory pool
//! whose peak is tracked, a scratch root whose lifetime and byte usage the
//! caller owns, a cancellation token, and the rewrite observer.
//!
//! Leasing rather than constructing is the point. The core does not decide how
//! much memory an attempt may use, where it may spill, or how long that scratch
//! lives; it reports what it used and stops when told to. No durable behavior
//! moves into the core as a result: the context holds no tenant identity, no
//! catalog authority, and no commit permission.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use tokio_util::sync::CancellationToken;

use super::observer::{AttemptId, RewriteObserver, noop_observer};
use crate::error::{CompactionError, Result};

/// Memory pool that records the highest simultaneous reservation it served.
///
/// Wraps a caller-supplied pool rather than replacing it, so the caller keeps
/// control of the actual limit and spill behavior while the core gains an
/// honest peak figure to report. The peak is a high-water mark of `reserved()`
/// sampled at every grow, so it never overstates: a value that was never
/// simultaneously held cannot be recorded.
#[derive(Debug)]
pub struct PeakTrackingMemoryPool {
    inner: Arc<dyn MemoryPool>,
    peak_bytes: Arc<AtomicUsize>,
}

impl PeakTrackingMemoryPool {
    /// Wraps `inner`, publishing its peak through the returned handle.
    #[must_use]
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            peak_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the shared peak handle, readable while the pool is in use.
    #[must_use]
    pub fn peak_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.peak_bytes)
    }

    /// Records the current reservation as the peak when it is a new high.
    fn observe_peak(&self) {
        let current = self.inner.reserved();
        self.peak_bytes.fetch_max(current, Ordering::Relaxed);
    }
}

impl std::fmt::Display for PeakTrackingMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PeakTracking(peak={}, inner={})",
            self.peak_bytes.load(Ordering::Relaxed),
            self.inner
        )
    }
}

impl MemoryPool for PeakTrackingMemoryPool {
    /// Reports the wrapped pool's name so diagnostics still identify the real
    /// limit implementation rather than this transparent wrapper.
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.observe_peak();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.observe_peak();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// Accounting handle for a caller-leased scratch root.
///
/// The core never creates or removes the root: the caller owns its lifetime,
/// which is what makes scratch usage auditable against a lease rather than
/// against whatever the OS temp directory happens to allow. The core only
/// measures what it put there.
#[derive(Debug, Clone)]
pub struct SpillLease {
    root: PathBuf,
    current_bytes: Arc<AtomicU64>,
    peak_bytes: Arc<AtomicU64>,
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
        Ok(Self {
            root,
            current_bytes: Arc::new(AtomicU64::new(0)),
            peak_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Returns the leased scratch root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Re-measures the bytes currently resident under the root.
    ///
    /// Recursion is bounded by the directory tree `DataFusion` itself creates, so
    /// this is a shallow walk in practice. Entries that vanish between listing
    /// and stat are skipped rather than failing the attempt: a spill file being
    /// reclaimed mid-measurement is normal, not an error.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Io`] when the root itself cannot be read.
    pub fn measure(&self) -> Result<u64> {
        let bytes = Self::directory_bytes(&self.root)?;
        self.current_bytes.store(bytes, Ordering::Relaxed);
        self.peak_bytes.fetch_max(bytes, Ordering::Relaxed);
        Ok(bytes)
    }

    /// Returns the most recently measured resident bytes.
    #[must_use]
    pub fn current_bytes(&self) -> u64 {
        self.current_bytes.load(Ordering::Relaxed)
    }

    /// Returns the highest measured byte usage.
    #[must_use]
    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    fn directory_bytes(dir: &Path) -> Result<u64> {
        let mut total = 0_u64;
        for entry in std::fs::read_dir(dir)? {
            let Ok(entry) = entry else { continue };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                total = total.saturating_add(Self::directory_bytes(&entry.path()).unwrap_or(0));
            } else if let Ok(metadata) = entry.metadata() {
                total = total.saturating_add(metadata.len());
            }
        }
        Ok(total)
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
    peak_memory_bytes: Arc<AtomicUsize>,
    pool_capacity_bytes: Option<usize>,
    spill: Option<SpillLease>,
    cancellation: CancellationToken,
    observer: Arc<dyn RewriteObserver>,
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

    /// Returns the highest simultaneous memory reservation observed so far.
    #[must_use]
    pub fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes.load(Ordering::Relaxed)
    }

    /// Returns the leased pool's capacity when it is bounded.
    #[must_use]
    pub fn pool_capacity_bytes(&self) -> Option<usize> {
        self.pool_capacity_bytes
    }

    /// Returns the scratch lease, when the caller supplied one.
    #[must_use]
    pub fn spill(&self) -> Option<&SpillLease> {
        self.spill.as_ref()
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
}

/// Builds a [`ManagedExecutionContext`].
///
/// Every field has a safe default, so a caller that only wants cancellation
/// need not describe a memory or scratch policy it does not have.
#[derive(Debug, Default)]
pub struct ManagedExecutionContextBuilder {
    attempt_id: Option<AttemptId>,
    memory_pool: Option<Arc<dyn MemoryPool>>,
    pool_capacity_bytes: Option<usize>,
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

    /// Leases a memory pool, recording `capacity_bytes` as its bound.
    ///
    /// The pool is wrapped so its peak is tracked; the caller's own limit and
    /// spill semantics are unchanged.
    #[must_use]
    pub fn with_memory_pool(
        mut self,
        pool: Arc<dyn MemoryPool>,
        capacity_bytes: Option<usize>,
    ) -> Self {
        self.memory_pool = Some(pool);
        self.pool_capacity_bytes = capacity_bytes;
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
    /// The runtime is built here rather than accepted whole so the peak-tracking
    /// wrapper and the leased scratch root are guaranteed to be installed: a
    /// caller cannot hand in a runtime that silently spills somewhere the lease
    /// does not account for.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::DataFusion`] when the runtime cannot be built.
    pub fn build(self) -> Result<Arc<ManagedExecutionContext>> {
        let inner_pool = self
            .memory_pool
            .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()) as Arc<dyn MemoryPool>);
        let tracking = PeakTrackingMemoryPool::new(inner_pool);
        let peak_memory_bytes = tracking.peak_handle();

        let mut runtime = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(tracking));
        if let Some(spill) = self.spill.as_ref() {
            runtime = runtime.with_disk_manager_builder(
                datafusion::execution::disk_manager::DiskManagerBuilder::default().with_mode(
                    datafusion::execution::disk_manager::DiskManagerMode::Directories(vec![
                        spill.root().to_path_buf(),
                    ]),
                ),
            );
        }

        Ok(Arc::new(ManagedExecutionContext {
            attempt_id: self.attempt_id.unwrap_or_default(),
            runtime_env: runtime.build_arc()?,
            peak_memory_bytes,
            pool_capacity_bytes: self.pool_capacity_bytes,
            spill: self.spill,
            cancellation: self.cancellation.unwrap_or_default(),
            observer: self.observer.unwrap_or_else(noop_observer),
        }))
    }
}
