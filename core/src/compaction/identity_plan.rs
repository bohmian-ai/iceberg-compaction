//! Joins identity-aware selection onto the scan tasks a plan will actually read.
//!
//! Selection is decided from manifests; execution reads scan tasks. Those are
//! two views of one snapshot, and they can disagree at the edges — a manifest
//! entry the scan does not produce cannot be rewritten, so a plan never covers
//! it. Deriving the durable report from the *selection* rather than from the
//! surviving join would therefore record files no attempt will ever touch.
//!
//! This module owns that join, and it is the single derivation both the plans
//! and the report come from. One pass, one truth: a plan/report disagreement
//! stops being possible rather than merely unlikely.

use std::collections::HashMap;

use iceberg::scan::FileScanTask;

use crate::managed::selection::SelectionGroup;

/// One selection group trimmed to the files whose scan tasks exist.
///
/// `group.files` and `tasks` name the same files in the same order, so the
/// group's reason describes exactly the plan built from `tasks`.
#[derive(Debug)]
pub struct JoinedGroup {
    /// The selection group, trimmed to files the scan produced.
    pub group: SelectionGroup,
    /// The scan tasks for those files, in the group's order.
    pub tasks: Vec<FileScanTask>,
}

/// Joins selection groups onto scan tasks planned from the same snapshot.
///
/// Files with no scan task are dropped from their group, and a group left with
/// no files is dropped entirely — a plan cannot be built from nothing, and a
/// report entry with no plan would be unverifiable. Each task is consumed by
/// at most one group, which the selection report's uniqueness rule already
/// guarantees; consuming rather than copying makes that guarantee structural.
#[must_use]
pub fn join_groups_to_tasks(
    groups: Vec<SelectionGroup>,
    tasks: Vec<FileScanTask>,
) -> Vec<JoinedGroup> {
    let mut tasks_by_path: HashMap<String, FileScanTask> = tasks
        .into_iter()
        .map(|task| (task.data_file_path.clone(), task))
        .collect();

    let mut joined = Vec::with_capacity(groups.len());
    for mut group in groups {
        let mut group_tasks = Vec::with_capacity(group.files.len());
        group
            .files
            .retain(|file| match tasks_by_path.remove(&file.file_path) {
                Some(task) => {
                    group_tasks.push(task);
                    true
                }
                None => false,
            });
        if group.files.is_empty() {
            continue;
        }
        joined.push(JoinedGroup {
            group,
            tasks: group_tasks,
        });
    }
    joined
}
