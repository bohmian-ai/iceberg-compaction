//! Captures per-file identity from manifests before a scan erases it.
//!
//! `FileScanTask` is built for reading: it carries what an executor needs to
//! decode rows, and drops the schema, partition-spec, and sort-order identity
//! the file was written under. That identity only exists in the manifest, and
//! only until the scan has been planned.
//!
//! This index reads the manifests directly and keys the captured identity by
//! exact data-file path, so the identity-aware policy can consult it while the
//! scan's own delete attachment and projection remain untouched. Reading the
//! manifests is strictly a read: nothing here writes, commits, or expires.

use std::collections::HashMap;

use iceberg::spec::{DataContentType, ManifestContentType, PrimitiveLiteral};
use iceberg::table::Table;

use crate::error::{CompactionError, Result};
use crate::managed::selection::CandidateIdentity;

/// Per-path identity captured from one snapshot's data manifests.
#[derive(Debug, Default)]
pub struct ManifestIdentityIndex {
    by_path: HashMap<String, CandidateIdentity>,
}

impl ManifestIdentityIndex {
    /// Reads every data manifest reachable from `snapshot_id` and indexes the
    /// identity of each live data entry.
    ///
    /// Delete manifests are skipped: a delete file is never a rewrite
    /// candidate, only evidence attached to one. Dead entries are skipped
    /// because they are already logically removed.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionError::Config`] when `snapshot_id` is not present in
    /// the table metadata, and propagates Iceberg IO or decode failures from
    /// reading the manifest list and manifests.
    pub async fn load(
        table: &Table,
        snapshot_id: i64,
        event_time_field_id: Option<i32>,
    ) -> Result<Self> {
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                CompactionError::Config(format!("snapshot {snapshot_id} not found in table"))
            })?;

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let mut by_path = HashMap::new();

        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file.load_manifest(table.file_io()).await?;
            let schema_id = manifest.metadata().schema_id();
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    continue;
                }
                let identity = CandidateIdentity {
                    file_path: data_file.file_path().to_owned(),
                    schema_id,
                    partition_spec_id: data_file.partition_spec_id(),
                    sort_order_id: data_file.sort_order_id(),
                    partition_key: CandidateIdentity::partition_key_of(Some(data_file.partition())),
                    file_size_in_bytes: data_file.file_size_in_bytes(),
                    min_event_time: event_time_field_id
                        .and_then(|field_id| bound_as_i64(data_file.lower_bounds(), field_id)),
                    max_event_time: event_time_field_id
                        .and_then(|field_id| bound_as_i64(data_file.upper_bounds(), field_id)),
                };
                by_path.insert(identity.file_path.clone(), identity);
            }
        }

        Ok(Self { by_path })
    }

    /// Returns the captured identity for `file_path`, when it was indexed.
    #[must_use]
    pub fn get(&self, file_path: &str) -> Option<&CandidateIdentity> {
        self.by_path.get(file_path)
    }

    /// Returns every captured identity.
    #[must_use]
    pub fn identities(&self) -> Vec<CandidateIdentity> {
        self.by_path.values().cloned().collect()
    }

    /// Returns how many identities were captured.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_path.len()
    }

    /// Returns whether no identities were captured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_path.is_empty()
    }
}

/// Extracts an integral bound as `i64`.
///
/// Timestamps and dates are the only bounds the ordering uses, and both are
/// stored as integral literals. Any other literal type yields `None` rather
/// than a lossy coercion: a wrong ordering key is worse than no ordering key,
/// because the fallback ordering is still total.
fn bound_as_i64(
    bounds: &HashMap<i32, iceberg::spec::Datum>,
    field_id: i32,
) -> Option<i64> {
    match bounds.get(&field_id)?.literal() {
        PrimitiveLiteral::Long(value) => Some(*value),
        PrimitiveLiteral::Int(value) => Some(i64::from(*value)),
        _ => None,
    }
}
