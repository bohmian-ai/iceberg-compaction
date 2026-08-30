//! Core-owned, identity-aware file selection.
//!
//! Size is not the only reason a data file needs rewriting. A file written
//! under a superseded schema, partition spec, sort order, or writer recipe is
//! *correct* but *obsolete*: readers still resolve it, yet it no longer matches
//! how the table is written today, and no amount of size-based compaction will
//! ever fix that. Upstream's `SmallFiles`, `Full`, and `FilesWithDeletes`
//! policies cannot express this, because the identity that distinguishes an
//! obsolete file is erased before their input exists.
//!
//! This module owns that judgment. The caller supplies immutable policy inputs
//! — what "current" means, what sizes are acceptable, which partitions are
//! still receiving writes — and the core decides which files are selected, why,
//! and how they are grouped. The caller never supplies a selection.
//!
//! The reason vocabulary is closed and the precedence is fixed:
//!
//! ```text
//! ObsoleteSchema
//! ObsoletePartitionSpec
//! ObsoleteSortOrder
//! ObsoleteWriterRecipe
//! Oversized
//! packable Undersized
//! ```
//!
//! Precedence matters because a single file can qualify under several reasons
//! at once, and the reason is persisted as part of the durable plan. A stable
//! precedence makes the recorded reason a function of the file's identity
//! alone, not of evaluation order.

use std::collections::BTreeSet;

use iceberg::spec::Struct;

use crate::error::{CompactionError, Result};

/// Why a file was selected for rewriting.
///
/// The set is closed. Six reasons belong to the identity-aware policy; three
/// name an upstream policy that selected the file without a per-file reason,
/// so a persisted report always records which policy made the choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectionReason {
    /// The file was written under a schema that is no longer current.
    ObsoleteSchema,
    /// The file was written under a partition spec that is no longer current.
    ObsoletePartitionSpec,
    /// The file was written under a sort order that is no longer current.
    ObsoleteSortOrder,
    /// The file was written by a writer recipe that is no longer current.
    ObsoleteWriterRecipe,
    /// The file exceeds the policy's maximum acceptable size.
    Oversized,
    /// The file is below the small-file threshold and is packable with peers.
    Undersized,
    /// Selected by upstream's small-files policy.
    UpstreamSmallFiles,
    /// Selected by upstream's full-compaction policy.
    UpstreamFull,
    /// Selected by upstream's files-with-deletes policy.
    UpstreamFilesWithDeletes,
}

impl SelectionReason {
    /// Returns the stable wire spelling used in a persisted report.
    ///
    /// Spellings are part of the durable plan and must never change: an
    /// unknown spelling is rejected on read rather than silently reinterpreted.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ObsoleteSchema => "ObsoleteSchema",
            Self::ObsoletePartitionSpec => "ObsoletePartitionSpec",
            Self::ObsoleteSortOrder => "ObsoleteSortOrder",
            Self::ObsoleteWriterRecipe => "ObsoleteWriterRecipe",
            Self::Oversized => "Oversized",
            Self::Undersized => "Undersized",
            Self::UpstreamSmallFiles => "UpstreamSmallFiles",
            Self::UpstreamFull => "UpstreamFull",
            Self::UpstreamFilesWithDeletes => "UpstreamFilesWithDeletes",
        }
    }

    /// Parses a wire spelling.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] for an unknown spelling. An unknown
    /// reason means the report was written by a version this build does not
    /// understand, which must fail rather than degrade.
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ObsoleteSchema" => Ok(Self::ObsoleteSchema),
            "ObsoletePartitionSpec" => Ok(Self::ObsoletePartitionSpec),
            "ObsoleteSortOrder" => Ok(Self::ObsoleteSortOrder),
            "ObsoleteWriterRecipe" => Ok(Self::ObsoleteWriterRecipe),
            "Oversized" => Ok(Self::Oversized),
            "Undersized" => Ok(Self::Undersized),
            "UpstreamSmallFiles" => Ok(Self::UpstreamSmallFiles),
            "UpstreamFull" => Ok(Self::UpstreamFull),
            "UpstreamFilesWithDeletes" => Ok(Self::UpstreamFilesWithDeletes),
            other => Err(CompactionError::Config(format!(
                "unknown selection reason '{other}'"
            ))),
        }
    }

    /// Returns whether this reason may be recorded by `strategy`.
    ///
    /// A reason names the judgment that selected a file, so a reason from a
    /// policy that did not run is a contradiction: the report would claim a
    /// decision nobody took.
    #[must_use]
    pub fn belongs_to(&self, strategy: SelectionStrategyKind) -> bool {
        match strategy.uniform_reason() {
            Some(uniform) => *self == uniform,
            None => !matches!(
                self,
                Self::UpstreamSmallFiles | Self::UpstreamFull | Self::UpstreamFilesWithDeletes
            ),
        }
    }

    /// Returns whether this reason makes the file individually actionable.
    ///
    /// An individually actionable file is rewritten on its own: pairing an
    /// obsolete-identity or oversized file with unrelated peers would hide why
    /// it was rewritten and could produce an output larger than the policy
    /// allows.
    #[must_use]
    pub fn is_individually_actionable(&self) -> bool {
        matches!(
            self,
            Self::ObsoleteSchema
                | Self::ObsoletePartitionSpec
                | Self::ObsoleteSortOrder
                | Self::ObsoleteWriterRecipe
                | Self::Oversized
        )
    }
}

impl std::fmt::Display for SelectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which policy produced a selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionStrategyKind {
    /// Upstream small-files policy.
    UpstreamSmallFiles,
    /// Upstream full-compaction policy.
    UpstreamFull,
    /// Upstream files-with-deletes policy.
    UpstreamFilesWithDeletes,
    /// The core-owned identity-aware policy.
    WyrdIdentityAware,
}

impl SelectionStrategyKind {
    /// Returns the stable wire spelling used in a persisted report.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UpstreamSmallFiles => "UpstreamSmallFiles",
            Self::UpstreamFull => "UpstreamFull",
            Self::UpstreamFilesWithDeletes => "UpstreamFilesWithDeletes",
            Self::WyrdIdentityAware => "WyrdIdentityAware",
        }
    }

    /// Returns the reason an upstream policy records for every file it selects.
    ///
    /// Returns `None` for the identity-aware policy, which records a distinct
    /// reason per file.
    #[must_use]
    pub fn uniform_reason(&self) -> Option<SelectionReason> {
        match self {
            Self::UpstreamSmallFiles => Some(SelectionReason::UpstreamSmallFiles),
            Self::UpstreamFull => Some(SelectionReason::UpstreamFull),
            Self::UpstreamFilesWithDeletes => Some(SelectionReason::UpstreamFilesWithDeletes),
            Self::WyrdIdentityAware => None,
        }
    }
}

impl std::fmt::Display for SelectionStrategyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One file the policy selected, with the reason it was selected.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SelectedFile {
    /// Exact data-file path. This is the file's identity in the report.
    pub file_path: String,
    /// Why the file was selected.
    pub reason: SelectionReason,
}

/// A complete account of one planning pass.
///
/// Complete is the operative word: the report names every file the plans will
/// rewrite, and nothing else. The caller persists it in the durable plan before
/// an attempt exists, so an attempt can be checked against the decision that
/// authorised it rather than against a re-derivation that might have drifted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionReport {
    /// Policy that produced the selection.
    pub strategy: SelectionStrategyKind,
    /// Snapshot the selection was derived from.
    pub base_snapshot_id: i64,
    /// Identity-policy inputs, when the identity-aware policy ran.
    pub policy: Option<PolicyIdentity>,
    /// Selected files, sorted by path and unique by path.
    pub selected: Vec<SelectedFile>,
}

impl SelectionReport {
    /// Builds a report, sorting and validating its selection.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when the report is bound to no real
    /// snapshot, when the strategy and the presence of a declared policy
    /// identity contradict, when a file path appears more than once, or when a
    /// recorded reason belongs to a policy that did not run. Each of these
    /// makes the persisted decision unverifiable, so it must fail before the
    /// plan is hashed rather than after an attempt has acted on it.
    pub fn new(
        strategy: SelectionStrategyKind,
        base_snapshot_id: i64,
        policy: Option<PolicyIdentity>,
        mut selected: Vec<SelectedFile>,
    ) -> Result<Self> {
        if base_snapshot_id <= 0 {
            return Err(CompactionError::Config(format!(
                "selection report is bound to no snapshot (base_snapshot_id {base_snapshot_id})"
            )));
        }
        let declares_policy = matches!(strategy, SelectionStrategyKind::WyrdIdentityAware);
        if declares_policy != policy.is_some() {
            return Err(CompactionError::Config(format!(
                "selection strategy '{strategy}' and its declared policy identity contradict"
            )));
        }

        selected.sort();
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for entry in &selected {
            if !seen.insert(entry.file_path.as_str()) {
                return Err(CompactionError::Config(format!(
                    "selection report contains duplicate file identity '{}'",
                    entry.file_path
                )));
            }
            if !entry.reason.belongs_to(strategy) {
                return Err(CompactionError::Config(format!(
                    "selection reason '{}' does not belong to strategy '{strategy}'",
                    entry.reason
                )));
            }
        }
        Ok(Self {
            strategy,
            base_snapshot_id,
            policy,
            selected,
        })
    }

    /// Returns the paths in the report, sorted.
    #[must_use]
    pub fn selected_paths(&self) -> Vec<&str> {
        self.selected
            .iter()
            .map(|entry| entry.file_path.as_str())
            .collect()
    }
}

/// The immutable identity inputs the caller declared for one planning pass.
///
/// Recorded verbatim in the report so a later reader can tell whether a plan
/// was made against the table's current identity or a stale view of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyIdentity {
    /// Schema id considered current.
    pub schema_id: i32,
    /// Partition-spec id considered current.
    pub partition_spec_id: i32,
    /// Sort-order id considered current.
    pub sort_order_id: i32,
    /// Writer recipe considered current.
    pub writer_recipe: String,
    /// Target output size in bytes.
    pub target_file_size_bytes: u64,
    /// Exclusive lower bound below which a file counts as undersized.
    pub small_file_threshold_bytes: u64,
    /// Inclusive upper bound above which a file counts as oversized.
    pub max_file_size_bytes: u64,
    /// Whether tails of still-open partitions may be emitted.
    pub emit_open_partition_tail: bool,
}

/// Resolves the writer recipe that produced a data file.
///
/// Wyrd writes data under `/data/forge/<recipe>/...`, so the recipe is part of
/// the path and needs no side table to recover. The resolver is closed: it
/// recognises exactly that shape and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterRecipeResolver {
    marker: String,
}

impl WriterRecipeResolver {
    /// Creates a resolver for the Wyrd Forge path shape.
    #[must_use]
    pub fn forge() -> Self {
        Self {
            marker: "/data/forge/".to_owned(),
        }
    }

    /// Returns the recipe segment of `path`, when the path has the Forge shape.
    ///
    /// Returns `None` for any other path shape. A file whose path does not
    /// carry a recipe was not written by the current writer, so it can never
    /// match the current recipe — the caller treats `None` as a mismatch rather
    /// than as "unknown, assume fine".
    #[must_use]
    pub fn resolve<'path>(&self, path: &'path str) -> Option<&'path str> {
        let start = path.find(&self.marker)? + self.marker.len();
        let rest = &path[start..];
        let end = rest.find('/')?;
        if end == 0 { None } else { Some(&rest[..end]) }
    }
}

/// Which partitions are still receiving writes.
///
/// A still-open partition's trailing undersized files are deliberately left
/// alone: they are about to gain more peers, and compacting them now guarantees
/// re-compacting them later. Closed partitions have no such future.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenPartitionPolicy {
    /// Every partition is closed.
    AllClosed,
    /// The listed partition keys are still open.
    Open(BTreeSet<String>),
}

impl OpenPartitionPolicy {
    /// Returns whether `partition_key` is still receiving writes.
    #[must_use]
    pub fn is_open(&self, partition_key: &str) -> bool {
        match self {
            Self::AllClosed => false,
            Self::Open(keys) => keys.contains(partition_key),
        }
    }
}

/// Immutable policy inputs for the identity-aware selector.
///
/// Everything here is a declaration by the caller about the table and its
/// write policy. Nothing here selects a file: the selector does that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WyrdSelectionPolicy {
    /// Schema id considered current.
    pub schema_id: i32,
    /// Partition-spec id considered current.
    pub partition_spec_id: i32,
    /// Sort-order id considered current.
    pub sort_order_id: i32,
    /// Writer recipe considered current.
    pub writer_recipe: String,
    /// Resolver for the recipe embedded in a data-file path.
    pub recipe_resolver: WriterRecipeResolver,
    /// Target output size in bytes.
    pub target_file_size_bytes: u64,
    /// Exclusive lower bound: `size < threshold` is undersized.
    pub small_file_threshold_bytes: u64,
    /// Which partitions are still receiving writes.
    pub open_partitions: OpenPartitionPolicy,
    /// Whether the tail of a still-open partition may be emitted anyway.
    pub emit_open_partition_tail: bool,
    /// Field id whose bounds order candidates within a partition.
    ///
    /// Ordering by event time keeps a rewrite's inputs contiguous in time,
    /// which is what makes the output's own bounds tight. Without it the
    /// ordering falls back to spec, partition, and path, which is still total
    /// and deterministic but produces wider output bounds.
    pub event_time_field_id: Option<i32>,
}

impl WyrdSelectionPolicy {
    /// Returns the inclusive upper bound above which a file is oversized.
    ///
    /// Defined as `floor(target * 180 / 100)`: a file may run up to 80% over
    /// target before it is worth splitting, because the rolling writer's target
    /// is an estimate and a modest overshoot is expected rather than a defect.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when `target * 180` overflows `u64`,
    /// which would otherwise wrap into a nonsensical bound.
    pub fn max_file_size_bytes(&self) -> Result<u64> {
        self.target_file_size_bytes
            .checked_mul(180)
            .map(|scaled| scaled / 100)
            .ok_or_else(|| {
                CompactionError::Config(format!(
                    "target_file_size_bytes {} overflows the oversized bound",
                    self.target_file_size_bytes
                ))
            })
    }

    /// Validates the policy's internal consistency.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when the target is zero, when the
    /// oversized bound overflows, or when the small-file threshold is not below
    /// the target — a threshold at or above target would classify a
    /// target-sized file as undersized and compact it forever.
    pub fn validate(&self) -> Result<()> {
        if self.target_file_size_bytes == 0 {
            return Err(CompactionError::Config(
                "target_file_size_bytes must be greater than zero".to_owned(),
            ));
        }
        let max = self.max_file_size_bytes()?;
        if self.small_file_threshold_bytes >= self.target_file_size_bytes {
            return Err(CompactionError::Config(format!(
                "small_file_threshold_bytes {} must be below target_file_size_bytes {}",
                self.small_file_threshold_bytes, self.target_file_size_bytes
            )));
        }
        if max < self.target_file_size_bytes {
            return Err(CompactionError::Config(format!(
                "oversized bound {} must not be below target_file_size_bytes {}",
                max, self.target_file_size_bytes
            )));
        }
        Ok(())
    }

    /// Captures the policy's identity inputs for the durable report.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when the oversized bound overflows.
    pub fn identity(&self) -> Result<PolicyIdentity> {
        Ok(PolicyIdentity {
            schema_id: self.schema_id,
            partition_spec_id: self.partition_spec_id,
            sort_order_id: self.sort_order_id,
            writer_recipe: self.writer_recipe.clone(),
            target_file_size_bytes: self.target_file_size_bytes,
            small_file_threshold_bytes: self.small_file_threshold_bytes,
            max_file_size_bytes: self.max_file_size_bytes()?,
            emit_open_partition_tail: self.emit_open_partition_tail,
        })
    }
}

/// Identity a data file carries in its manifest.
///
/// Captured while reading manifests, because a scan task drops schema,
/// partition-spec, and sort-order identity on its way to the executor. Without
/// this capture the policy could only see size, which is exactly the limitation
/// it exists to remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateIdentity {
    /// Exact data-file path.
    pub file_path: String,
    /// Schema id of the manifest that carries this entry.
    pub schema_id: i32,
    /// Partition spec the file was written under.
    pub partition_spec_id: i32,
    /// Sort order the file was written under, when it declared one.
    pub sort_order_id: Option<i32>,
    /// Partition value, rendered canonically for grouping and ordering.
    pub partition_key: String,
    /// Physical size of the file.
    pub file_size_in_bytes: u64,
    /// Lower bound of the event-time field, when available.
    pub min_event_time: Option<i64>,
    /// Upper bound of the event-time field, when available.
    pub max_event_time: Option<i64>,
}

impl CandidateIdentity {
    /// Renders a partition value as a canonical, stable ordering key.
    ///
    /// The rendering is an internal ordering and grouping key only. It is never
    /// a plan-hash input, so it need not be a specified encoding — only stable
    /// within a build and identical for identical partition values.
    #[must_use]
    pub fn partition_key_of(partition: Option<&Struct>) -> String {
        match partition {
            Some(partition) => format!("{partition:?}"),
            None => String::new(),
        }
    }

    /// Returns the total ordering key for this candidate.
    ///
    /// Ordering is by partition spec, then partition, then event-time bounds,
    /// then path. Path is last and is unique, so the order is total: two
    /// planning passes over the same snapshot produce the same sequence, and
    /// therefore the same groups.
    #[must_use]
    fn ordering_key(&self) -> (i32, &str, i64, i64, &str) {
        (
            self.partition_spec_id,
            self.partition_key.as_str(),
            self.min_event_time.unwrap_or(i64::MIN),
            self.max_event_time.unwrap_or(i64::MIN),
            self.file_path.as_str(),
        )
    }
}

/// One selected batch of files that will be rewritten together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionGroup {
    /// Files in the group, in selection order.
    pub files: Vec<CandidateIdentity>,
    /// Reason the group was selected.
    ///
    /// A single-file group carries that file's individual reason; a packed
    /// group carries [`SelectionReason::Undersized`].
    pub reason: SelectionReason,
}

impl SelectionGroup {
    /// Returns the total physical bytes in the group.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.files
            .iter()
            .map(|file| file.file_size_in_bytes)
            .sum::<u64>()
    }
}

/// Applies the identity-aware policy to a set of candidates.
///
/// Owns the policy and the whole decision: classification, precedence,
/// partition and spec isolation, ordering, and packing. A caller gets groups
/// and a report; it never gets a hook to override an individual choice.
#[derive(Debug)]
pub struct IdentityAwareSelector {
    policy: WyrdSelectionPolicy,
    max_file_size_bytes: u64,
}

impl IdentityAwareSelector {
    /// Creates a selector, validating the policy.
    ///
    /// # Errors
    ///
    /// Propagates [`WyrdSelectionPolicy::validate`].
    pub fn new(policy: WyrdSelectionPolicy) -> Result<Self> {
        policy.validate()?;
        let max_file_size_bytes = policy.max_file_size_bytes()?;
        Ok(Self {
            policy,
            max_file_size_bytes,
        })
    }

    /// Returns the policy this selector applies.
    #[must_use]
    pub fn policy(&self) -> &WyrdSelectionPolicy {
        &self.policy
    }

    /// Classifies one candidate against the policy.
    ///
    /// Returns `None` when the file is current and adequately sized, which is
    /// the common case and must never be selected: rewriting a healthy file
    /// costs IO and produces no improvement.
    ///
    /// The order of the checks *is* the precedence. Identity drift outranks
    /// size because an obsolete file must be rewritten regardless of how large
    /// it is, and recording it as merely "undersized" would lose the only
    /// signal explaining why compaction keeps touching it.
    #[must_use]
    pub fn classify(&self, candidate: &CandidateIdentity) -> Option<SelectionReason> {
        if candidate.schema_id != self.policy.schema_id {
            return Some(SelectionReason::ObsoleteSchema);
        }
        if candidate.partition_spec_id != self.policy.partition_spec_id {
            return Some(SelectionReason::ObsoletePartitionSpec);
        }
        if candidate.sort_order_id != Some(self.policy.sort_order_id) {
            return Some(SelectionReason::ObsoleteSortOrder);
        }
        let recipe = self.policy.recipe_resolver.resolve(&candidate.file_path);
        if recipe != Some(self.policy.writer_recipe.as_str()) {
            return Some(SelectionReason::ObsoleteWriterRecipe);
        }
        if candidate.file_size_in_bytes > self.max_file_size_bytes {
            return Some(SelectionReason::Oversized);
        }
        if candidate.file_size_in_bytes < self.policy.small_file_threshold_bytes {
            return Some(SelectionReason::Undersized);
        }
        None
    }

    /// Selects and groups candidates.
    ///
    /// Candidates are ordered totally, then walked once. Individually
    /// actionable files become single-file groups, after any pending undersized
    /// work is flushed so the emitted order still reflects the input order.
    /// Undersized files accumulate only within one partition spec and partition
    /// value, and a pending run is emitted before adding a file that would push
    /// it past target.
    ///
    /// At a spec, partition, or end-of-input boundary a pending run is emitted
    /// only when it holds at least two files and either the partition is closed
    /// or the run already reaches target. Otherwise the run is dropped: a lone
    /// small file has nothing to merge with, and the live tail of an open
    /// partition is about to gain peers, so compacting it now guarantees
    /// compacting it again later.
    ///
    /// # Errors
    ///
    /// This selection is total and cannot fail; the signature is fallible so
    /// that future validation can be added without a breaking change to
    /// callers, and so it composes with the fallible report construction.
    pub fn select(&self, mut candidates: Vec<CandidateIdentity>) -> Result<Vec<SelectionGroup>> {
        candidates.sort_by(|left, right| left.ordering_key().cmp(&right.ordering_key()));

        let mut groups: Vec<SelectionGroup> = Vec::new();
        let mut pending: Vec<CandidateIdentity> = Vec::new();
        let mut pending_bytes = 0_u64;
        let mut pending_scope: Option<(i32, String)> = None;

        for candidate in candidates {
            let Some(reason) = self.classify(&candidate) else {
                continue;
            };

            let scope = (candidate.partition_spec_id, candidate.partition_key.clone());
            if pending_scope.as_ref().is_some_and(|open| *open != scope) {
                let closed_scope = pending_scope.take().expect("checked as Some above");
                Self::flush_boundary(
                    &self.policy,
                    &closed_scope.1,
                    &mut pending,
                    &mut pending_bytes,
                    &mut groups,
                );
            }

            if reason.is_individually_actionable() {
                // Flush first so the single-file group lands after the peers
                // that preceded it, preserving input order in the output.
                if let Some(open_scope) = pending_scope.clone() {
                    Self::flush_boundary(
                        &self.policy,
                        &open_scope.1,
                        &mut pending,
                        &mut pending_bytes,
                        &mut groups,
                    );
                    pending_scope = None;
                }
                groups.push(SelectionGroup {
                    files: vec![candidate],
                    reason,
                });
                continue;
            }

            let would_be = pending_bytes.saturating_add(candidate.file_size_in_bytes);
            if !pending.is_empty()
                && pending.len() >= 2
                && would_be > self.policy.target_file_size_bytes
            {
                groups.push(SelectionGroup {
                    files: std::mem::take(&mut pending),
                    reason: SelectionReason::Undersized,
                });
                pending_bytes = 0;
            }

            pending_bytes = pending_bytes.saturating_add(candidate.file_size_in_bytes);
            pending.push(candidate);
            pending_scope = Some(scope);
        }

        if let Some(open_scope) = pending_scope {
            Self::flush_boundary(
                &self.policy,
                &open_scope.1,
                &mut pending,
                &mut pending_bytes,
                &mut groups,
            );
        }

        Ok(groups)
    }

    /// Emits or discards a pending undersized run at a boundary.
    fn flush_boundary(
        policy: &WyrdSelectionPolicy,
        partition_key: &str,
        pending: &mut Vec<CandidateIdentity>,
        pending_bytes: &mut u64,
        groups: &mut Vec<SelectionGroup>,
    ) {
        let bytes = *pending_bytes;
        let files = std::mem::take(pending);
        *pending_bytes = 0;
        if files.len() < 2 {
            return;
        }
        let partition_open = policy.open_partitions.is_open(partition_key);
        let worth_emitting = !partition_open
            || policy.emit_open_partition_tail
            || bytes >= policy.target_file_size_bytes;
        if worth_emitting {
            groups.push(SelectionGroup {
                files,
                reason: SelectionReason::Undersized,
            });
        }
    }

    /// Builds the durable report for a set of groups.
    ///
    /// # Errors
    ///
    /// Propagates [`SelectionReport::new`], which rejects duplicate identities.
    pub fn report(
        &self,
        base_snapshot_id: i64,
        groups: &[SelectionGroup],
    ) -> Result<SelectionReport> {
        let selected = groups
            .iter()
            .flat_map(|group| {
                group.files.iter().map(|file| SelectedFile {
                    file_path: file.file_path.clone(),
                    reason: group.reason,
                })
            })
            .collect();
        SelectionReport::new(
            SelectionStrategyKind::WyrdIdentityAware,
            base_snapshot_id,
            Some(self.policy.identity()?),
            selected,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a policy whose "current" identity is schema 7, spec 3, sort 5,
    /// recipe `v2`, with a 1000-byte target.
    fn policy() -> WyrdSelectionPolicy {
        WyrdSelectionPolicy {
            schema_id: 7,
            partition_spec_id: 3,
            sort_order_id: 5,
            writer_recipe: "v2".to_owned(),
            recipe_resolver: WriterRecipeResolver::forge(),
            target_file_size_bytes: 1000,
            small_file_threshold_bytes: 400,
            open_partitions: OpenPartitionPolicy::AllClosed,
            emit_open_partition_tail: false,
            event_time_field_id: None,
        }
    }

    /// Builds a candidate that is current under [`policy`] in every respect.
    fn current(path_recipe: &str, name: &str, size: u64) -> CandidateIdentity {
        CandidateIdentity {
            file_path: format!("s3://bucket/data/forge/{path_recipe}/part-0/{name}.parquet"),
            schema_id: 7,
            partition_spec_id: 3,
            sort_order_id: Some(5),
            partition_key: "part-0".to_owned(),
            file_size_in_bytes: size,
            min_event_time: None,
            max_event_time: None,
        }
    }

    #[test]
    fn wyrd_selection_policy_covers_all_reasons_and_precedence() {
        let selector = IdentityAwareSelector::new(policy()).unwrap();

        // A file that is current and adequately sized is never selected.
        assert_eq!(selector.classify(&current("v2", "healthy", 900)), None);
        // The threshold is an exclusive lower bound: exactly at it is fine.
        assert_eq!(selector.classify(&current("v2", "at_floor", 400)), None);
        // The oversized bound is floor(1000 * 180 / 100) = 1800, inclusive.
        assert_eq!(selector.policy().max_file_size_bytes().unwrap(), 1800);
        assert_eq!(selector.classify(&current("v2", "at_ceiling", 1800)), None);

        // Each reason in isolation.
        let mut undersized = current("v2", "small", 399);
        assert_eq!(
            selector.classify(&undersized),
            Some(SelectionReason::Undersized)
        );
        undersized.file_size_in_bytes = 1801;
        assert_eq!(
            selector.classify(&undersized),
            Some(SelectionReason::Oversized)
        );

        let recipe_drift = current("v1", "old_recipe", 900);
        assert_eq!(
            selector.classify(&recipe_drift),
            Some(SelectionReason::ObsoleteWriterRecipe)
        );

        let mut sort_drift = current("v2", "old_sort", 900);
        sort_drift.sort_order_id = Some(4);
        assert_eq!(
            selector.classify(&sort_drift),
            Some(SelectionReason::ObsoleteSortOrder)
        );
        // An absent sort order is drift too: the current table declares one.
        sort_drift.sort_order_id = None;
        assert_eq!(
            selector.classify(&sort_drift),
            Some(SelectionReason::ObsoleteSortOrder)
        );

        let mut spec_drift = current("v2", "old_spec", 900);
        spec_drift.partition_spec_id = 2;
        assert_eq!(
            selector.classify(&spec_drift),
            Some(SelectionReason::ObsoletePartitionSpec)
        );

        let mut schema_drift = current("v2", "old_schema", 900);
        schema_drift.schema_id = 6;
        assert_eq!(
            selector.classify(&schema_drift),
            Some(SelectionReason::ObsoleteSchema)
        );

        // Precedence: a file that qualifies under every reason at once reports
        // the highest-precedence one, and peeling each cause off in order walks
        // the precedence down exactly one step at a time.
        let mut all_at_once = current("v1", "everything", 1801);
        all_at_once.schema_id = 6;
        all_at_once.partition_spec_id = 2;
        all_at_once.sort_order_id = Some(4);
        let expected = [
            SelectionReason::ObsoleteSchema,
            SelectionReason::ObsoletePartitionSpec,
            SelectionReason::ObsoleteSortOrder,
            SelectionReason::ObsoleteWriterRecipe,
            SelectionReason::Oversized,
        ];
        let mut observed = Vec::new();
        for step in 0..expected.len() {
            observed.push(selector.classify(&all_at_once).unwrap());
            match step {
                0 => all_at_once.schema_id = 7,
                1 => all_at_once.partition_spec_id = 3,
                2 => all_at_once.sort_order_id = Some(5),
                3 => {
                    all_at_once.file_path =
                        "s3://bucket/data/forge/v2/part-0/everything.parquet".to_owned();
                }
                _ => {}
            }
        }
        assert_eq!(observed, expected);
        // With every identity cause removed it is merely oversized, and
        // shrinking it below the ceiling makes it healthy.
        all_at_once.file_size_in_bytes = 900;
        assert_eq!(selector.classify(&all_at_once), None);

        // A path with no recipe segment can never match the current recipe.
        let mut foreign = current("v2", "foreign", 900);
        foreign.file_path = "s3://bucket/elsewhere/foreign.parquet".to_owned();
        assert_eq!(
            selector.classify(&foreign),
            Some(SelectionReason::ObsoleteWriterRecipe)
        );

        // Individually actionable reasons never share a group.
        let groups = selector
            .select(vec![
                schema_drift.clone(),
                spec_drift.clone(),
                current("v2", "tiny_a", 100),
                current("v2", "tiny_b", 100),
            ])
            .unwrap();
        for group in &groups {
            if group.reason.is_individually_actionable() {
                assert_eq!(group.files.len(), 1, "{:?} must stand alone", group.reason);
            }
        }
        let packed: Vec<&SelectionGroup> = groups
            .iter()
            .filter(|group| group.reason == SelectionReason::Undersized)
            .collect();
        assert_eq!(packed.len(), 1);
        assert_eq!(packed[0].files.len(), 2);
    }

    #[test]
    fn wyrd_selection_policy_preserves_partition_spec_and_open_tail_rules() {
        let selector = IdentityAwareSelector::new(policy()).unwrap();

        // Undersized files never accumulate across a partition boundary...
        let mut other_partition = current("v2", "other_a", 100);
        other_partition.partition_key = "part-1".to_owned();
        let mut other_partition_b = current("v2", "other_b", 100);
        other_partition_b.partition_key = "part-1".to_owned();
        let groups = selector
            .select(vec![
                current("v2", "a", 100),
                current("v2", "b", 100),
                other_partition.clone(),
                other_partition_b.clone(),
            ])
            .unwrap();
        assert_eq!(groups.len(), 2);
        for group in &groups {
            let keys: BTreeSet<&str> = group
                .files
                .iter()
                .map(|file| file.partition_key.as_str())
                .collect();
            assert_eq!(keys.len(), 1, "a group never spans partitions");
        }

        // ...nor across a partition-spec boundary, even at the same partition
        // value: the two specs describe different physical layouts.
        let mut other_spec = current("v2", "spec_a", 100);
        other_spec.partition_spec_id = 3;
        let mut legacy_spec = current("v2", "spec_b", 100);
        legacy_spec.partition_spec_id = 2;
        let mut legacy_spec_peer = current("v2", "spec_c", 100);
        legacy_spec_peer.partition_spec_id = 2;
        let groups = selector
            .select(vec![other_spec, legacy_spec, legacy_spec_peer])
            .unwrap();
        for group in &groups {
            let specs: BTreeSet<i32> = group
                .files
                .iter()
                .map(|file| file.partition_spec_id)
                .collect();
            assert_eq!(specs.len(), 1, "a group never spans partition specs");
        }

        // A pending run is emitted before it would cross target.
        let selector = IdentityAwareSelector::new(policy()).unwrap();
        let groups = selector
            .select(vec![
                current("v2", "p0", 390),
                current("v2", "p1", 390),
                current("v2", "p2", 390),
                current("v2", "p3", 390),
            ])
            .unwrap();
        assert_eq!(groups.len(), 2);
        for group in &groups {
            assert_eq!(group.files.len(), 2);
            assert!(group.total_bytes() <= 1000);
        }

        // A lone undersized file is never emitted: it has nothing to merge with.
        let groups = selector.select(vec![current("v2", "lonely", 100)]).unwrap();
        assert!(groups.is_empty());

        // An open partition's tail stays live...
        let open = WyrdSelectionPolicy {
            open_partitions: OpenPartitionPolicy::Open(BTreeSet::from(["part-0".to_owned()])),
            ..policy()
        };
        let selector_open = IdentityAwareSelector::new(open.clone()).unwrap();
        let tail = vec![current("v2", "t0", 100), current("v2", "t1", 100)];
        assert!(
            selector_open.select(tail.clone()).unwrap().is_empty(),
            "an under-target tail of an open partition is left to grow"
        );

        // ...unless it already reaches target on its own, in which case there
        // is nothing to gain by waiting for more peers.
        let selector_open_small_target = IdentityAwareSelector::new(WyrdSelectionPolicy {
            target_file_size_bytes: 700,
            ..open.clone()
        })
        .unwrap();
        let full_tail = vec![current("v2", "t0", 399), current("v2", "t1", 399)];
        assert_eq!(
            selector_open_small_target.select(full_tail).unwrap().len(),
            1
        );

        // ...or the caller opts into emitting open tails...
        let selector_forced = IdentityAwareSelector::new(WyrdSelectionPolicy {
            emit_open_partition_tail: true,
            ..open.clone()
        })
        .unwrap();
        assert_eq!(selector_forced.select(tail.clone()).unwrap().len(), 1);

        // ...or the partition is closed.
        assert_eq!(selector.select(tail).unwrap().len(), 1);
    }

    #[test]
    fn wyrd_selection_policy_rejects_duplicate_or_stale_manifest_identities() {
        // Two reasons for one identity make the persisted reason ambiguous.
        let duplicate =
            SelectionReport::new(SelectionStrategyKind::WyrdIdentityAware, 42, None, vec![
                SelectedFile {
                    file_path: "a.parquet".to_owned(),
                    reason: SelectionReason::Undersized,
                },
                SelectedFile {
                    file_path: "a.parquet".to_owned(),
                    reason: SelectionReason::Oversized,
                },
            ]);
        assert!(matches!(duplicate, Err(CompactionError::Config(_))));

        // A report is sorted and stable regardless of insertion order.
        let report = SelectionReport::new(SelectionStrategyKind::UpstreamFull, 42, None, vec![
            SelectedFile {
                file_path: "b.parquet".to_owned(),
                reason: SelectionReason::UpstreamFull,
            },
            SelectedFile {
                file_path: "a.parquet".to_owned(),
                reason: SelectionReason::UpstreamFull,
            },
        ])
        .unwrap();
        assert_eq!(report.selected_paths(), vec!["a.parquet", "b.parquet"]);

        // Unknown reasons are rejected rather than reinterpreted.
        assert!(SelectionReason::parse("NotAReason").is_err());
        for reason in [
            SelectionReason::ObsoleteSchema,
            SelectionReason::ObsoletePartitionSpec,
            SelectionReason::ObsoleteSortOrder,
            SelectionReason::ObsoleteWriterRecipe,
            SelectionReason::Oversized,
            SelectionReason::Undersized,
            SelectionReason::UpstreamSmallFiles,
            SelectionReason::UpstreamFull,
            SelectionReason::UpstreamFilesWithDeletes,
        ] {
            assert_eq!(SelectionReason::parse(reason.as_str()).unwrap(), reason);
        }

        // An inconsistent policy is rejected before it can select anything.
        assert!(
            IdentityAwareSelector::new(WyrdSelectionPolicy {
                target_file_size_bytes: 0,
                ..policy()
            })
            .is_err()
        );
        assert!(
            IdentityAwareSelector::new(WyrdSelectionPolicy {
                small_file_threshold_bytes: 1000,
                ..policy()
            })
            .is_err()
        );
        assert!(
            WyrdSelectionPolicy {
                target_file_size_bytes: u64::MAX,
                ..policy()
            }
            .max_file_size_bytes()
            .is_err()
        );

        // Every reason a group can carry round-trips into the report.
        let selector = IdentityAwareSelector::new(policy()).unwrap();
        let mut schema_drift = current("v2", "schema", 900);
        schema_drift.schema_id = 6;
        let groups = selector
            .select(vec![
                schema_drift,
                current("v2", "x", 100),
                current("v2", "y", 100),
            ])
            .unwrap();
        let report = selector.report(42, &groups).unwrap();
        assert_eq!(report.base_snapshot_id, 42);
        assert_eq!(report.strategy, SelectionStrategyKind::WyrdIdentityAware);
        assert_eq!(report.policy.as_ref().unwrap().max_file_size_bytes, 1800);
        assert_eq!(report.selected.len(), 3);
    }
}
