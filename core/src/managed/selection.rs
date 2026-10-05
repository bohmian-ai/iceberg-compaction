//! The durable account of one upstream planning pass.
//!
//! Upstream planning returns plans but not *why* each file is in them. A
//! caller that persists a plan before any attempt exists needs that answer as
//! data: which policy ran, against which snapshot, and exactly which files it
//! selected. [`SelectionReport`] is that answer, derived from the same plans
//! the caller will execute so the two cannot disagree.

use std::collections::BTreeSet;

use crate::error::{CompactionError, Result};

/// Why a file was selected for rewriting.
///
/// Every upstream policy selects files without a per-file reason, so the
/// reason names the policy that made the choice and a persisted report always
/// records which policy that was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectionReason {
    /// Selected by upstream's small-files policy.
    UpstreamSmallFiles,
    /// Selected by upstream's full-compaction policy.
    UpstreamFull,
    /// Selected by upstream's files-with-deletes policy.
    UpstreamFilesWithDeletes,
    /// Selected by upstream's unified auto-planning policy.
    UpstreamAuto,
}

impl SelectionReason {
    /// Returns the stable wire spelling used in a persisted report.
    ///
    /// Spellings are part of the durable plan and must never change.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UpstreamSmallFiles => "UpstreamSmallFiles",
            Self::UpstreamFull => "UpstreamFull",
            Self::UpstreamFilesWithDeletes => "UpstreamFilesWithDeletes",
            Self::UpstreamAuto => "UpstreamAuto",
        }
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
    /// Upstream unified auto-planning policy.
    UpstreamAuto,
}

impl SelectionStrategyKind {
    /// Returns the stable wire spelling used in a persisted report.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UpstreamSmallFiles => "UpstreamSmallFiles",
            Self::UpstreamFull => "UpstreamFull",
            Self::UpstreamFilesWithDeletes => "UpstreamFilesWithDeletes",
            Self::UpstreamAuto => "UpstreamAuto",
        }
    }

    /// Returns the reason this policy records for every file it selects.
    #[must_use]
    pub fn uniform_reason(&self) -> SelectionReason {
        match self {
            Self::UpstreamSmallFiles => SelectionReason::UpstreamSmallFiles,
            Self::UpstreamFull => SelectionReason::UpstreamFull,
            Self::UpstreamFilesWithDeletes => SelectionReason::UpstreamFilesWithDeletes,
            Self::UpstreamAuto => SelectionReason::UpstreamAuto,
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
    /// Selected files, sorted by path and unique by path.
    pub selected: Vec<SelectedFile>,
}

impl SelectionReport {
    /// Builds a report, sorting and validating its selection.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when the report is bound to no real
    /// snapshot, when a file path appears more than once, or when a recorded
    /// reason belongs to a policy that did not run. Each of these makes the
    /// persisted decision unverifiable, so it must fail before the plan is
    /// hashed rather than after an attempt has acted on it.
    pub fn new(
        strategy: SelectionStrategyKind,
        base_snapshot_id: i64,
        mut selected: Vec<SelectedFile>,
    ) -> Result<Self> {
        if base_snapshot_id <= 0 {
            return Err(CompactionError::Config(format!(
                "selection report is bound to no snapshot (base_snapshot_id {base_snapshot_id})"
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
            if entry.reason != strategy.uniform_reason() {
                return Err(CompactionError::Config(format!(
                    "selection reason '{}' does not belong to strategy '{strategy}'",
                    entry.reason
                )));
            }
        }
        Ok(Self {
            strategy,
            base_snapshot_id,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, reason: SelectionReason) -> SelectedFile {
        SelectedFile {
            file_path: path.to_owned(),
            reason,
        }
    }

    /// A report is sorted, unique, snapshot-bound, and reason-consistent.
    #[test]
    fn wyrd_selection_report_is_canonical_or_refused() {
        let small = SelectionStrategyKind::UpstreamSmallFiles;
        let reason = small.uniform_reason();

        let report =
            SelectionReport::new(small, 7, vec![file("b", reason), file("a", reason)]).unwrap();
        assert_eq!(report.selected_paths(), vec!["a", "b"], "sorted by path");

        assert!(
            SelectionReport::new(small, 0, vec![]).is_err(),
            "a report must name a real snapshot"
        );
        assert!(
            SelectionReport::new(small, 7, vec![file("a", reason), file("a", reason)]).is_err(),
            "a duplicate identity must be refused"
        );
        assert!(
            SelectionReport::new(small, 7, vec![file("a", SelectionReason::UpstreamFull)]).is_err(),
            "a reason from a policy that did not run must be refused"
        );
    }
}
