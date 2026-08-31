/*
 * Copyright 2025 iceberg-compaction
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use iceberg::spec::{DataFile, MAIN_BRANCH, Snapshot};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::{Catalog, ErrorKind, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::common::{CompactionMetricsRecorder, Metrics};
use crate::compaction::identity_plan::{JoinedGroup, join_groups_to_tasks};
use crate::compaction::validator::CompactionValidator;
use crate::config::{CompactionExecutionConfig, CompactionPlanningConfig};
use crate::executor::{
    ExecutorType, RewriteFilesRequest, RewriteFilesResponse, RewriteFilesStat, TableSortOrder,
    create_compaction_executor,
};
use crate::file_selection::{FileGroup, FileSelector, ManifestIdentityIndex};
use crate::managed::selection::{
    IdentityAwareSelector, SelectedFile, SelectionGroup, SelectionReport, SelectionStrategyKind,
};
use crate::{CompactionConfig, CompactionError, CompactionExecutor, Result};

mod identity_plan;
mod validator;

const UNASSIGNED_SNAPSHOT_ID: i64 = -1;

/// Validates that all rewrite results target the same snapshot and branch.
///
/// # Errors
///
/// Returns `CompactionError::InvalidInput` if any result has mismatched `to_branch` or `snapshot_id`.
fn validate_rewrite_results_consistency(
    rewrite_results: &[RewriteResult],
    expected_snapshot_id: i64,
    expected_branch: &str,
) -> Result<()> {
    for result in rewrite_results {
        if result.plan.to_branch != expected_branch {
            return Err(CompactionError::Execution(format!(
                "Compaction plan branch '{}' does not match configured branch '{}'",
                result.plan.to_branch, expected_branch
            )));
        }

        if result.plan.snapshot_id != expected_snapshot_id {
            return Err(CompactionError::Execution(format!(
                "Compaction plan snapshot '{}' does not match other plans snapshot '{}'",
                result.plan.snapshot_id, expected_snapshot_id
            )));
        }
    }
    Ok(())
}

/// Builder for `Compaction` with optional configuration.
///
/// # Examples
///
/// ```ignore
/// let compaction = CompactionBuilder::new(catalog, table_ident)
///     .with_config(config)
///     .with_executor_type(ExecutorType::DataFusion)
///     .build();
/// ```
pub struct CompactionBuilder {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,

    catalog_name: Option<Cow<'static, str>>,
    config: Option<Arc<CompactionConfig>>,
    executor_type: Option<ExecutorType>,
    executor: Option<Box<dyn CompactionExecutor>>,
    registry: Option<BoxedRegistry>,
    commit_retry_config: Option<CommitManagerRetryConfig>,
    to_branch: Option<Cow<'static, str>>,
}

impl CompactionBuilder {
    /// Creates a new builder with required catalog and table identifier.
    pub fn new(catalog: Arc<dyn Catalog>, table_ident: TableIdent) -> Self {
        Self {
            catalog,
            table_ident,

            catalog_name: None,
            config: None,
            executor_type: None,
            executor: None,
            registry: None,
            commit_retry_config: None,
            to_branch: None,
        }
    }

    /// Sets the compaction configuration.
    pub fn with_config(mut self, config: Arc<CompactionConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the executor type. Defaults to `ExecutorType::DataFusion`.
    pub fn with_executor_type(mut self, executor_type: ExecutorType) -> Self {
        self.executor_type = Some(executor_type);
        self
    }

    /// Sets the catalog name for metrics labels.
    pub fn with_catalog_name(mut self, catalog_name: impl Into<Cow<'static, str>>) -> Self {
        self.catalog_name = Some(catalog_name.into());
        self
    }

    /// Injects an already-constructed executor.
    ///
    /// This is how a caller binds compaction to resources it owns — a leased
    /// runtime, a memory budget, a scratch root, a cancellation token — without
    /// the core deciding any of them. It takes precedence over
    /// [`with_executor_type`](Self::with_executor_type), which remains the
    /// construction path for callers that have no such resources to lease.
    #[must_use]
    pub fn with_executor(mut self, executor: Box<dyn CompactionExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Sets the metrics registry. Defaults to `NoopMetricsRegistry`.
    pub fn with_registry(mut self, registry: BoxedRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Sets commit retry configuration for transient failures.
    pub fn with_retry_config(mut self, retry_config: CommitManagerRetryConfig) -> Self {
        self.commit_retry_config = Some(retry_config);
        self
    }

    /// Sets the target branch for compaction commits. Defaults to `main`.
    pub fn with_to_branch(mut self, to_branch: impl Into<Cow<'static, str>>) -> Self {
        self.to_branch = Some(to_branch.into());
        self
    }

    /// Builds the `Compaction` instance with configured values.
    pub fn build(self) -> Compaction {
        let executor = self.executor.unwrap_or_else(|| {
            create_compaction_executor(self.executor_type.unwrap_or(ExecutorType::DataFusion))
        });

        let metrics = if let Some(registry) = self.registry {
            Arc::new(Metrics::new(registry))
        } else {
            Arc::new(Metrics::new(Box::new(NoopMetricsRegistry)))
        };

        let commit_retry_config = self.commit_retry_config.unwrap_or_default();

        let to_branch = self
            .to_branch
            .unwrap_or_else(|| MAIN_BRANCH.to_owned().into());

        let catalog_name = self
            .catalog_name
            .unwrap_or_else(|| "default".to_owned().into());

        let table_ident_name = Cow::Owned(self.table_ident.name().to_owned());

        Compaction {
            config: self.config,
            executor,
            catalog: self.catalog,
            metrics,
            table_ident: self.table_ident,
            table_ident_name,
            catalog_name,
            commit_retry_config,
            to_branch,
        }
    }
}

/// Iceberg table compaction orchestrator supporting managed and plan-driven workflows.
///
/// # Workflows
///
/// **Managed workflow**: [`compact()`](Self::compact) handles planning, execution, and commit atomically.
///
/// **Plan-driven workflow**: Caller controls each phase:
/// 1. [`plan_compaction()`](Self::plan_compaction) → generate plans
/// 2. [`rewrite_plan()`](Self::rewrite_plan) → execute rewrites
/// 3. [`commit_rewrite_results()`](Self::commit_rewrite_results) → commit transaction
///
/// # Fields
///
/// - `config`: Optional global config for managed workflow. Plan-driven workflow provides config per-plan.
pub struct Compaction {
    /// Optional global configuration for managed workflows
    pub config: Option<Arc<CompactionConfig>>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
    pub metrics: Arc<Metrics>,
    pub table_ident: TableIdent,
    pub table_ident_name: Cow<'static, str>,
    pub catalog_name: Cow<'static, str>,

    pub commit_retry_config: CommitManagerRetryConfig,
    pub to_branch: Cow<'static, str>,
}

/// Intermediate result from `rewrite_plan()` before commit.
#[derive(Debug, Clone)]
pub struct RewriteResult {
    pub output_data_files: Vec<DataFile>,
    pub stats: RewriteFilesStat,
    pub plan: CompactionPlan,
    /// Validation info for creating `CompactionValidator` later
    pub validation_info: Option<ValidationInfo>,
}

/// Information for deferred `CompactionValidator` creation.
#[derive(Debug, Clone)]
pub struct ValidationInfo {
    pub file_group: FileGroup,
    pub executor_parallelism: usize,
}

/// Result of a successful compaction containing rewritten files and metadata.
#[derive(Default)]
pub struct CompactionResult {
    /// Newly written data files from compaction
    pub data_files: Vec<DataFile>,
    /// Statistics about the compaction operation
    pub stats: RewriteFilesStat,
    /// Updated table metadata after commit (if available)
    pub table: Option<Table>,
}

impl Compaction {
    /// Runs managed compaction: planning, execution, commit, and optional validation.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(CompactionResult))` if files were compacted
    /// - `Ok(None)` if no files needed compaction
    /// - `Err(_)` if `config` is `None` or operation failed
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `self.config` is `None`
    /// - Planning, execution, commit, or validation fails
    pub async fn compact(&self) -> Result<Option<CompactionResult>> {
        if let Some(config) = &self.config {
            let overall_start_time = std::time::Instant::now();

            // 1. Get all compaction plans
            let plans = self.plan_compaction().await?;

            if plans.is_empty() {
                return Ok(None);
            }

            let table = self.catalog.load_table(&self.table_ident).await?;

            // 2. Concurrently execute rewrite for all plans
            let rewrite_results = self
                .concurrent_rewrite_plans(plans, &config.execution, &table)
                .await?;

            if rewrite_results.is_empty() {
                return Ok(None);
            }

            // 3. Commit all rewrite results in a single transaction
            let commit_start_time = std::time::Instant::now();
            let final_table = self.commit_rewrite_results(rewrite_results.clone()).await?;

            // 4. Run validations if enabled
            if config.execution.enable_validate_compaction {
                self.run_validations(rewrite_results.clone(), &final_table)
                    .await?;
            }

            // 6. Update metrics for the entire compaction operation
            self.record_overall_metrics(&rewrite_results, overall_start_time, commit_start_time);

            // 7. Merge results for response
            let merged_result =
                self.merge_rewrite_results_to_compaction_result(rewrite_results, Some(final_table));
            Ok(Some(merged_result))
        } else {
            Err(crate::error::CompactionError::Execution(
                "CompactionConfig is required".to_owned(),
            ))
        }
    }

    /// Records metrics for overall compaction duration and statistics.
    pub(crate) fn record_overall_metrics(
        &self,
        rewrite_results: &[RewriteResult],
        overall_start_time: std::time::Instant,
        commit_start_time: std::time::Instant,
    ) {
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident_name.clone(),
        );

        // Record commit duration
        metrics_recorder.record_commit_duration(commit_start_time.elapsed().as_millis() as _);

        // Record total compaction duration
        metrics_recorder.record_compaction_duration(overall_start_time.elapsed().as_millis() as _);

        // Record plan-level metrics for each rewrite result
        for result in rewrite_results {
            metrics_recorder.record_plan_file_count(result.stats.input_files_count);
            metrics_recorder.record_plan_size_bytes(result.stats.input_total_bytes);
        }

        // Merge all stats and record completion
        let merged_stats = self.merge_rewrite_stats(rewrite_results);
        metrics_recorder.record_compaction_complete(&merged_stats);
    }

    /// Merges statistics from multiple rewrite results into a single aggregate.
    pub(crate) fn merge_rewrite_stats(
        &self,
        rewrite_results: &[RewriteResult],
    ) -> RewriteFilesStat {
        let mut merged_stats = RewriteFilesStat::default();

        for result in rewrite_results {
            merged_stats.input_files_count += result.stats.input_files_count;
            merged_stats.output_files_count += result.stats.output_files_count;
            merged_stats.input_total_bytes += result.stats.input_total_bytes;
            merged_stats.output_total_bytes += result.stats.output_total_bytes;
            merged_stats.input_data_file_count += result.stats.input_data_file_count;
            merged_stats.input_position_delete_file_count +=
                result.stats.input_position_delete_file_count;
            merged_stats.input_equality_delete_file_count +=
                result.stats.input_equality_delete_file_count;
            merged_stats.input_data_file_total_bytes += result.stats.input_data_file_total_bytes;
            merged_stats.input_position_delete_file_total_bytes +=
                result.stats.input_position_delete_file_total_bytes;
            merged_stats.input_equality_delete_file_total_bytes +=
                result.stats.input_equality_delete_file_total_bytes;
        }

        merged_stats
    }

    /// Executes rewrite for a single plan without committing.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `plan.to_branch != self.to_branch`
    /// - Snapshot with `plan.snapshot_id` does not exist
    /// - Executor rewrite operation fails
    pub async fn rewrite_plan(
        &self,
        plan: CompactionPlan,
        execution_config: &CompactionExecutionConfig,
        table: &Table,
    ) -> Result<RewriteResult> {
        if plan.to_branch != *self.to_branch {
            return Err(CompactionError::Execution(format!(
                "Compaction plan branch '{}' does not match configured branch '{}'",
                plan.to_branch, self.to_branch
            )));
        }

        // Check if the current snapshot exists
        if let Some(_branch_snapshot) = table.metadata().snapshot_by_id(plan.snapshot_id) {
            let now = std::time::Instant::now();
            let metrics_recorder = CompactionMetricsRecorder::new(
                self.metrics.clone(),
                self.catalog_name.clone(),
                self.table_ident_name.clone(),
            );

            // Step 1: Create rewrite request
            let rewrite_files_request =
                self.create_rewrite_request(table, &plan.file_group, execution_config)?;

            // Step 2: Execute rewrite
            let RewriteFilesResponse {
                data_files: output_data_files,
                stats,
            } = match self.executor.rewrite_files(rewrite_files_request).await {
                Ok(response) => response,
                Err(e) => {
                    metrics_recorder.record_executor_error();
                    return Err(e);
                }
            };

            // Step 3: (Delayed) Input file collection moved to commit phase to avoid duplicate IO

            // Step 4: Setup validation info if enabled
            let validation_info = if execution_config.enable_validate_compaction {
                Some(ValidationInfo {
                    file_group: plan.file_group.clone(),
                    executor_parallelism: plan.file_group.executor_parallelism,
                })
            } else {
                None
            };

            // Step 5: Update metrics - record plan-level metrics
            metrics_recorder.record_plan_execution_duration(now.elapsed().as_millis() as _);
            metrics_recorder.record_plan_file_count(stats.input_files_count);
            metrics_recorder.record_plan_size_bytes(stats.input_total_bytes);

            Ok(RewriteResult {
                output_data_files,
                stats,
                plan,
                validation_info,
            })
        } else {
            Err(CompactionError::Execution(format!(
                "Snapshot {} not found",
                plan.snapshot_id
            )))
        }
    }

    /// Generates compaction plans without executing them.
    ///
    /// # Returns
    ///
    /// Vector of `CompactionPlan` based on `self.config.planning`.
    ///
    /// # Errors
    ///
    /// Returns error if `self.config` is `None` or planning fails.
    pub async fn plan_compaction(&self) -> Result<Vec<CompactionPlan>> {
        if let Some(config) = &self.config {
            let table = self.catalog.load_table(&self.table_ident).await?;
            let compaction_planner = CompactionPlanner::new(config.planning.clone());

            compaction_planner
                .plan_compaction_with_branch(&table, &self.to_branch)
                .await
        } else {
            Err(crate::error::CompactionError::Execution(
                "CompactionConfig is required for planning".to_owned(),
            ))
        }
    }

    /// Generates compaction plans together with the durable selection report.
    ///
    /// The additive counterpart to [`plan_compaction`](Self::plan_compaction):
    /// same plans, plus the complete account of why each file is in them. The
    /// caller persists the report in its durable plan before an attempt exists,
    /// so a later attempt is checkable against the decision that authorised it.
    ///
    /// # Errors
    ///
    /// Returns an error when no config is set, when the table cannot be loaded,
    /// or when planning or report construction fails.
    pub async fn plan_compaction_with_report(
        &self,
    ) -> Result<(Vec<CompactionPlan>, SelectionReport)> {
        let Some(config) = &self.config else {
            return Err(CompactionError::Execution(
                "CompactionConfig is required for planning".to_owned(),
            ));
        };
        let table = self.catalog.load_table(&self.table_ident).await?;
        CompactionPlanner::new(config.planning.clone())
            .plan_compaction_with_report(&table, &self.to_branch)
            .await
    }

    /// Commits multiple rewrite results in a single Iceberg transaction.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `rewrite_results` is empty
    /// - Results have inconsistent `to_branch` or `snapshot_id`
    /// - Snapshot does not exist
    /// - Commit fails
    pub async fn commit_rewrite_results(
        &self,
        rewrite_results: Vec<RewriteResult>,
    ) -> Result<Table> {
        if rewrite_results.is_empty() {
            return Err(CompactionError::Execution(
                "No rewrite results to commit".to_owned(),
            ));
        }

        let table = self.catalog.load_table(&self.table_ident).await?;
        let snapshot_id = rewrite_results[0].plan.snapshot_id;

        // verify all rewrite results are from the same branch and snapshot
        validate_rewrite_results_consistency(&rewrite_results, snapshot_id, &self.to_branch)?;

        // Create commit manager and delegate the complex logic to it
        if let Some(snapshot) = table.metadata().snapshot_by_id(snapshot_id) {
            let consistency_params = CommitConsistencyParams {
                starting_snapshot_id: snapshot.snapshot_id(),
                use_starting_sequence_number: true,
                basic_schema_id: table.metadata().current_schema().schema_id(),
            };

            let commit_manager = CommitManager::new(
                self.commit_retry_config.clone(),
                self.catalog.clone(),
                self.table_ident.clone(),
                self.table_ident_name.clone(),
                self.catalog_name.clone(),
                self.metrics.clone(),
                consistency_params,
            );

            // Delegate to CommitManager's high-level interface
            commit_manager
                .rewrite_files_from_results(rewrite_results, &self.to_branch)
                .await
        } else {
            Err(CompactionError::Execution(format!(
                "Snapshot {} not found",
                snapshot_id
            )))
        }
    }

    /// Executes multiple plans concurrently using `futures::stream`.
    ///
    /// # Performance
    ///
    /// Uses buffered stream for concurrent execution.
    pub(crate) async fn concurrent_rewrite_plans(
        &self,
        plans: Vec<CompactionPlan>,
        execution_config: &CompactionExecutionConfig,
        table: &Table,
    ) -> Result<Vec<RewriteResult>> {
        use futures::stream::{self, StreamExt};

        let results: Result<Vec<RewriteResult>> = stream::iter(plans)
            .map(|plan| async move { self.rewrite_plan(plan, execution_config, table).await })
            .buffer_unordered(execution_config.max_concurrent_compaction_plans) // Limit concurrency based on config
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect();

        results
    }

    /// Runs `CompactionValidator` for each result if validation info is present.
    pub(crate) async fn run_validations(
        &self,
        rewrite_results: Vec<RewriteResult>,
        committed_table: &Table,
    ) -> Result<()> {
        for rewrite_result in rewrite_results {
            if let Some(validation_info) = rewrite_result.validation_info {
                let mut validator = CompactionValidator::new(
                    validation_info.file_group,
                    rewrite_result.output_data_files,
                    validation_info.executor_parallelism,
                    committed_table.metadata().current_schema().clone(),
                    committed_table.metadata().current_schema().clone(),
                    committed_table.clone(),
                    self.catalog_name.clone(),
                    self.to_branch.clone(),
                )
                .await?;

                validator.validate().await?;
                tracing::info!(
                    "Compaction validation completed successfully for table '{}'",
                    self.table_ident
                );
            }
        }
        Ok(())
    }

    /// Merges multiple rewrite results into a single `CompactionResult`.
    pub(crate) fn merge_rewrite_results_to_compaction_result(
        &self,
        results: Vec<RewriteResult>,
        table: Option<Table>,
    ) -> CompactionResult {
        // Reuse the existing stats merger to avoid duplication
        let merged_stats = self.merge_rewrite_stats(&results);

        // Collect all output data files
        let mut merged_data_files = Vec::new();
        for result in results {
            merged_data_files.extend(result.output_data_files);
        }

        CompactionResult {
            data_files: merged_data_files,
            stats: merged_stats,
            table,
        }
    }

    /// Creates a `RewriteFilesRequest` for the executor.
    ///
    /// Default implementation creates standard request. Override for customization.
    fn create_rewrite_request(
        &self,
        table: &Table,
        file_group: &FileGroup,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<RewriteFilesRequest> {
        let schema = table.metadata().current_schema().clone();
        let location_generator = DefaultLocationGenerator::new(table.metadata()).unwrap();
        let metrics_recorder = CompactionMetricsRecorder::new(
            self.metrics.clone(),
            self.catalog_name.clone(),
            self.table_ident_name.clone(),
        );

        Ok(RewriteFilesRequest {
            file_io: table.file_io().clone(),
            schema,
            file_group: file_group.clone(),
            execution_config: Arc::new(execution_config.clone()),
            location_generator,
            partition_spec: table.metadata().default_partition_spec().clone(),
            metrics_recorder: Some(metrics_recorder),
            sort_order: {
                let default_sort_order_id = table.metadata().default_sort_order_id();
                table
                    .metadata()
                    .sort_order_by_id(default_sort_order_id)
                    .cloned()
                    .map(|order| TableSortOrder {
                        id: default_sort_order_id,
                        order,
                    })
            },
            format_version: table.metadata().format_version(),
        })
    }

    /// Compacts the table using a single provided plan.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(_))` if files were compacted
    /// - `Ok(None)` if plan has no files
    ///
    /// # Errors
    ///
    /// Returns error if rewrite, commit, or validation fails.
    pub async fn compact_with_plan(
        &self,
        plan: CompactionPlan,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<Option<CompactionResult>> {
        // Check if there are files to compact
        if plan.file_count() == 0 {
            return Ok(None);
        }

        let overall_start_time = std::time::Instant::now();

        let table = self.catalog.load_table(&self.table_ident).await?;

        // Use the new rewrite_plan method
        let rewrite_result = self.rewrite_plan(plan, execution_config, &table).await?;

        // Commit the single rewrite result
        let commit_start_time = std::time::Instant::now();
        let final_table = self
            .commit_rewrite_results(vec![rewrite_result.clone()])
            .await?;

        // Run validation if enabled
        if execution_config.enable_validate_compaction
            && let Some(validation_info) = &rewrite_result.validation_info
        {
            let mut validator = CompactionValidator::new(
                validation_info.file_group.clone(),
                rewrite_result.output_data_files.clone(),
                validation_info.executor_parallelism,
                final_table.metadata().current_schema().clone(),
                final_table.metadata().current_schema().clone(),
                final_table.clone(),
                self.catalog_name.clone(),
                self.to_branch.clone(),
            )
            .await?;

            validator.validate().await?;
            tracing::info!(
                "Compaction validation completed successfully for table '{}'",
                self.table_ident
            );
        }

        // Record metrics for single plan compaction
        self.record_overall_metrics(
            std::slice::from_ref(&rewrite_result),
            overall_start_time,
            commit_start_time,
        );

        // Convert to CompactionResult
        let result = CompactionResult {
            data_files: rewrite_result.output_data_files,
            stats: rewrite_result.stats,
            table: Some(final_table),
        };

        Ok(Some(result))
    }

    /// Returns the metrics registry for this compaction instance.
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Builds a `CommitManager` with the given consistency parameters.
    pub fn build_commit_manager(
        &self,
        consistency_params: CommitConsistencyParams,
    ) -> CommitManager {
        CommitManager::new(
            self.commit_retry_config.clone(),
            self.catalog.clone(),
            self.table_ident.clone(),
            self.table_ident_name.clone(),
            self.catalog_name.clone(),
            self.metrics.clone(),
            consistency_params,
        )
    }
}

/// Number of manifests loaded concurrently when resolving rewrite inputs.
///
/// Mirrors `iceberg`'s own `DEFAULT_LOAD_CONCURRENCY_LIMIT`, which is crate-private
/// and therefore cannot be reused here.
const MANIFEST_LOAD_CONCURRENCY: usize = 16;

/// Resolves the `wanted` data file paths to their `DataFile` records in `snapshot`.
///
/// # Performance
///
/// Entries are filtered against `wanted` *during* the manifest scan, so the resolved map
/// is `O(wanted)` rather than `O(files in the table)`. Peak memory is that plus a bounded
/// manifest working set, not strictly `O(wanted)`: `load_manifest` fully materializes each
/// manifest, and `buffer_unordered` keeps up to `MANIFEST_LOAD_CONCURRENCY` decoded
/// manifests in flight at once. The scan stops as soon as every wanted path has been
/// found.
///
/// Deleted entries are skipped. A path can appear both as a live entry and as a tombstone
/// from an earlier lifecycle, and only the live record is a valid rewrite input; skipping
/// tombstones is also what makes the early exit safe, since it guarantees a path is only
/// ever recorded from a live entry.
///
/// # Errors
///
/// Returns error if manifest list or manifest loading fails.
async fn resolve_data_files_by_path(
    snapshot: &Arc<Snapshot>,
    table: &Table,
    wanted: &HashSet<&str>,
) -> Result<HashMap<String, DataFile>> {
    use futures::StreamExt;

    if wanted.is_empty() {
        return Ok(HashMap::new());
    }

    let manifest_list = table
        .object_cache()
        .get_manifest_list(snapshot, &table.metadata_ref())
        .await?;

    // The manifest futures deliberately own their inputs rather than borrowing. Borrowed
    // futures inside `buffer_unordered` make the enclosing future's `Send` bound
    // higher-ranked, which rustc cannot discharge, and that surfaces as a confusing
    // "`Send` is not general enough" error in downstream callers that spawn compaction.
    // Cloning is cheap: `FileIO` is a handle, and this list is one entry per manifest, not
    // per data file.
    let file_io = table.file_io().clone();
    let manifest_files = manifest_list.entries().to_vec();
    let mut manifests = futures::stream::iter(manifest_files)
        .map(|manifest_file| {
            let file_io = file_io.clone();
            async move { manifest_file.load_manifest(&file_io).await }
        })
        .buffer_unordered(MANIFEST_LOAD_CONCURRENCY);

    let mut resolved: HashMap<String, DataFile> = HashMap::with_capacity(wanted.len());
    while let Some(manifest) = manifests.next().await {
        let manifest = manifest?;
        for entry in manifest.entries() {
            if !entry.is_alive() || entry.content_type() != iceberg::spec::DataContentType::Data {
                continue;
            }
            let path = entry.data_file().file_path();
            if !wanted.contains(path) {
                continue;
            }
            resolved.insert(path.to_owned(), entry.data_file().clone());
        }

        if resolved.len() == wanted.len() {
            break;
        }
    }

    Ok(resolved)
}

/// Configuration for commit retry behavior with exponential backoff.
#[derive(Debug, Clone)]
pub struct CommitManagerRetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay before the first retry
    pub retry_initial_delay: Duration,
    /// Maximum delay between retries (for exponential backoff)
    pub retry_max_delay: Duration,
}

impl Default for CommitManagerRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_initial_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

/// Manages commit operations with retry logic and consistency validation.
///
/// Uses exponential backoff for transient failures (e.g., optimistic lock conflicts).
pub struct CommitManager {
    config: CommitManagerRetryConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    /// Snapshot ID for consistency checks during commit
    starting_snapshot_id: i64,
    /// Enable sequence number validation during commit
    use_starting_sequence_number: bool,
    /// Metrics recorder for commit operations
    metrics_recorder: CompactionMetricsRecorder,
    /// Schema ID for validation
    basic_schema_id: i32,
}

/// Parameters for commit consistency validation.
pub struct CommitConsistencyParams {
    /// Base snapshot ID for consistency validation
    pub starting_snapshot_id: i64,
    /// Enable sequence number validation
    pub use_starting_sequence_number: bool,
    /// Table schema ID for validation
    pub basic_schema_id: i32,
}

impl CommitManager {
    /// Creates a new `CommitManager` with retry configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: CommitManagerRetryConfig,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        table_ident_name: impl Into<Cow<'static, str>>,
        catalog_name: impl Into<Cow<'static, str>>,
        metrics: Arc<Metrics>,
        consistency_params: CommitConsistencyParams,
    ) -> Self {
        let catalog_name = catalog_name.into();
        let table_ident_name = table_ident_name.into();

        let metrics_recorder =
            CompactionMetricsRecorder::new(metrics, catalog_name.clone(), table_ident_name.clone());

        Self {
            config,
            catalog,
            table_ident,
            starting_snapshot_id: consistency_params.starting_snapshot_id,
            use_starting_sequence_number: consistency_params.use_starting_sequence_number,
            metrics_recorder,
            basic_schema_id: consistency_params.basic_schema_id,
        }
    }

    /// Collects added and rewritten files from rewrite results by loading snapshot.
    ///
    /// # Performance
    ///
    /// Loads snapshot files once, builds `HashMap` index for efficient lookup.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `rewrite_results` is empty
    /// - Results have inconsistent `to_branch` or `snapshot_id`
    /// - Snapshot or file loading fails
    async fn collect_files_from_results(
        &self,
        rewrite_results: &[RewriteResult],
        to_branch: &str,
    ) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
        if rewrite_results.is_empty() {
            return Err(CompactionError::Execution(
                "No rewrite results to process".to_owned(),
            ));
        }

        let snapshot_id = rewrite_results[0].plan.snapshot_id;

        // Validate consistency across all rewrite results
        validate_rewrite_results_consistency(rewrite_results, snapshot_id, to_branch)?;

        // Load table and get snapshot
        let table = self.catalog.load_table(&self.table_ident).await?;
        let snapshot = table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                CompactionError::Execution(format!("Snapshot {} not found", snapshot_id))
            })?;

        // --- Batch collect input files from all plans ---

        // 1. Gather the input paths this batch replaces, in plan order.
        //    Note: Only data files are collected, delete files are excluded.
        let input_paths: Vec<&str> = rewrite_results
            .iter()
            .flat_map(|rr| {
                rr.plan
                    .file_group
                    .data_files
                    .iter()
                    .map(|task| task.data_file_path.as_str())
            })
            .collect();

        // 2. Resolve just those paths against the snapshot. Scanning for the batch's own
        //    inputs keeps this `O(batch)`; loading the whole snapshot made commit memory
        //    scale with total table size and OOM-killed large tables.
        let resolved =
            resolve_data_files_by_path(snapshot, &table, &input_paths.iter().copied().collect())
                .await?;

        // 3. Collect rewritten data files (to be replaced), preserving plan order and
        //    silently skipping paths that are no longer present in the snapshot.
        let rewritten_data_files: Vec<DataFile> = input_paths
            .iter()
            .filter_map(|path| resolved.get(*path).cloned())
            .collect();

        // 4. Collect added data files (newly written) from all plans
        let added_data_files: Vec<DataFile> = rewrite_results
            .iter()
            .flat_map(|rr| rr.output_data_files.iter().cloned())
            .collect();

        Ok((added_data_files, rewritten_data_files))
    }

    /// Rewrites files from results: file collection, validation, and commit.
    ///
    /// # Errors
    ///
    /// Propagates errors from `collect_files_from_results()` and `rewrite_files()`.
    pub async fn rewrite_files_from_results(
        &self,
        rewrite_results: Vec<RewriteResult>,
        to_branch: &str,
    ) -> Result<Table> {
        let delete_cleanup_min_data_sequence_number = rewrite_results
            .first()
            .and_then(|result| result.plan.delete_cleanup_min_data_sequence_number);
        let (added_data_files, rewritten_data_files) = self
            .collect_files_from_results(&rewrite_results, to_branch)
            .await?;
        self.rewrite_files_with_delete_cleanup_sequence(
            added_data_files,
            rewritten_data_files,
            to_branch,
            delete_cleanup_min_data_sequence_number,
        )
        .await
    }

    /// Overwrites files from results: file collection, validation, and commit.
    ///
    /// # Errors
    ///
    /// Propagates errors from `collect_files_from_results()` and `overwrite_files()`.
    pub async fn overwrite_files_from_results(
        &self,
        rewrite_results: Vec<RewriteResult>,
        to_branch: &str,
    ) -> Result<Table> {
        let (added_data_files, rewritten_data_files) = self
            .collect_files_from_results(&rewrite_results, to_branch)
            .await?;
        self.overwrite_files(added_data_files, rewritten_data_files, to_branch)
            .await
    }

    /// Rewrites files with retry on transient failures (e.g., optimistic lock).
    ///
    /// # Errors
    ///
    /// Returns error if all retries exhausted or non-retryable error occurs.
    pub async fn rewrite_files(
        &self,
        added_data_files: Vec<DataFile>,
        rewritten_data_files: Vec<DataFile>,
        to_branch: &str,
    ) -> Result<Table> {
        self.rewrite_files_with_delete_cleanup_sequence(
            added_data_files,
            rewritten_data_files,
            to_branch,
            None,
        )
        .await
    }

    async fn rewrite_files_with_delete_cleanup_sequence(
        &self,
        added_data_files: Vec<DataFile>,
        rewritten_data_files: Vec<DataFile>,
        to_branch: &str,
        delete_cleanup_min_data_sequence_number: Option<i64>,
    ) -> Result<Table> {
        let data_files = added_data_files;
        let delete_files = rewritten_data_files;

        let operation = || {
            let catalog = self.catalog.clone();
            let table_ident = self.table_ident.clone();
            let data_files = data_files.clone();
            let delete_files = delete_files.clone();
            let use_starting_sequence_number = self.use_starting_sequence_number;
            let starting_snapshot_id = self.starting_snapshot_id;
            let metrics_recorder = self.metrics_recorder.clone();

            async move {
                // reload the table to get the latest state
                let table = catalog.load_table(&table_ident).await?;

                let schema_id = table.metadata().current_schema().schema_id();
                if schema_id != self.basic_schema_id {
                    return Err(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schema ID mismatch: expected {}, found {}",
                            self.basic_schema_id, schema_id
                        ),
                    ));
                }

                let txn = Transaction::new(&table);

                // TODO: support validation of data files and delete files with starting snapshot before applying the rewrite
                let mut rewrite_action = if use_starting_sequence_number {
                    // TODO: avoid retry if the snapshot_id is not found
                    if let Some(snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) {
                        let mut action = txn
                            .rewrite_files()
                            .set_enable_delete_filter_manager(true)
                            .add_data_files(data_files)
                            .delete_files(delete_files)
                            .set_target_branch(to_branch.to_owned())
                            .set_new_data_file_sequence_number(snapshot.sequence_number())
                            .set_check_file_existence(true);
                        action.set_snapshot_properties(custom_snapshot_properties(snapshot));
                        action
                    } else {
                        return Err(iceberg::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "No snapshot found with the given snapshot_id {starting_snapshot_id}"
                            ),
                        ));
                    }
                } else {
                    let mut action = txn
                        .rewrite_files()
                        .set_enable_delete_filter_manager(true)
                        .add_data_files(data_files)
                        .delete_files(delete_files)
                        .set_target_branch(to_branch.to_owned())
                        .set_check_file_existence(true);
                    if let Some(snapshot) = table.metadata().snapshot_for_ref(to_branch) {
                        action.set_snapshot_properties(custom_snapshot_properties(snapshot));
                    }
                    action
                };

                if let Some(sequence_number) = delete_cleanup_min_data_sequence_number {
                    rewrite_action = rewrite_action
                        .set_delete_file_cleanup_min_data_sequence_number(sequence_number);
                }

                let txn = rewrite_action.apply(txn)?;
                match txn.commit(catalog.as_ref()).await {
                    Ok(table) => {
                        // Update metrics after a successful commit
                        metrics_recorder.record_commit_success();
                        Ok(table)
                    }
                    Err(commit_err) => {
                        metrics_recorder.record_commit_failure();

                        tracing::error!(
                            "Commit attempt failed for table '{}': {:?}. Will retry if applicable.",
                            table_ident,
                            commit_err
                        );
                        Err(commit_err)
                    }
                }
            }
        };

        let retry_strategy = ExponentialBuilder::default()
            .with_min_delay(self.config.retry_initial_delay)
            .with_max_delay(self.config.retry_max_delay)
            .with_max_times(self.config.max_retries as usize);

        operation
            .retry(retry_strategy)
            .when(|e| {
                matches!(e.kind(), iceberg::ErrorKind::DataInvalid)
                    || matches!(e.kind(), iceberg::ErrorKind::Unexpected)
                    || matches!(e.kind(), iceberg::ErrorKind::CatalogCommitConflicts)
            })
            .notify(|e, d| {
                // Notify the user about the error
                // TODO: add metrics
                tracing::info!("Retrying Compaction failed {:?} after {:?}", e, d);
            })
            .await
            .map_err(|e: iceberg::Error| CompactionError::from(e)) // Convert backon::Error to your CompactionError
    }

    /// Overwrites files with retry on transient failures (e.g., optimistic lock).
    ///
    /// # Errors
    ///
    /// Returns error if all retries exhausted or non-retryable error occurs.
    pub async fn overwrite_files(
        &self,
        added_data_files: Vec<DataFile>,
        rewritten_data_files: Vec<DataFile>,
        to_branch: &str,
    ) -> Result<Table> {
        let data_files = added_data_files;
        let delete_files = rewritten_data_files;

        let operation = || {
            let catalog = self.catalog.clone();
            let table_ident = self.table_ident.clone();
            let data_files = data_files.clone();
            let delete_files = delete_files.clone();
            let use_starting_sequence_number = self.use_starting_sequence_number;
            let starting_snapshot_id = self.starting_snapshot_id;
            let metrics_recorder = self.metrics_recorder.clone();

            async move {
                // reload the table to get the latest state
                let table = catalog.load_table(&table_ident).await?;

                let schema_id = table.metadata().current_schema().schema_id();
                if schema_id != self.basic_schema_id {
                    return Err(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schema ID mismatch: expected {}, found {}",
                            self.basic_schema_id, schema_id
                        ),
                    ));
                }

                let txn = Transaction::new(&table);

                // TODO: support validation of data files and delete files with starting snapshot before applying the rewrite
                let overwrite_action = if use_starting_sequence_number {
                    // TODO: avoid retry if the snapshot_id is not found
                    if let Some(snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) {
                        let mut action = txn
                            .overwrite_files()
                            .add_data_files(data_files)
                            .delete_files(delete_files)
                            .set_target_branch(to_branch.to_owned())
                            .set_new_data_file_sequence_number(snapshot.sequence_number())
                            .set_check_file_existence(true);
                        action.set_snapshot_properties(custom_snapshot_properties(snapshot));
                        action
                    } else {
                        return Err(iceberg::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "No snapshot found with the given snapshot_id {starting_snapshot_id}"
                            ),
                        ));
                    }
                } else {
                    let mut action = txn
                        .overwrite_files()
                        .add_data_files(data_files)
                        .delete_files(delete_files)
                        .set_target_branch(to_branch.to_owned())
                        .set_check_file_existence(true);
                    if let Some(snapshot) = table.metadata().snapshot_for_ref(to_branch) {
                        action.set_snapshot_properties(custom_snapshot_properties(snapshot));
                    }
                    action
                };

                let txn = overwrite_action.apply(txn)?;
                match txn.commit(catalog.as_ref()).await {
                    Ok(table) => {
                        // Update metrics after a successful commit
                        metrics_recorder.record_commit_success();
                        Ok(table)
                    }
                    Err(commit_err) => {
                        metrics_recorder.record_commit_failure();

                        tracing::error!(
                            "Commit attempt failed for table '{}': {:?}. Will retry if applicable.",
                            table_ident,
                            commit_err
                        );
                        Err(commit_err)
                    }
                }
            }
        };

        let retry_strategy = ExponentialBuilder::default()
            .with_min_delay(self.config.retry_initial_delay)
            .with_max_delay(self.config.retry_max_delay)
            .with_max_times(self.config.max_retries as usize);

        operation
            .retry(retry_strategy)
            .when(|e| {
                matches!(e.kind(), iceberg::ErrorKind::DataInvalid)
                    || matches!(e.kind(), iceberg::ErrorKind::Unexpected)
                    || matches!(e.kind(), iceberg::ErrorKind::CatalogCommitConflicts)
            })
            .notify(|e, d| {
                // Notify the user about the error
                // TODO: add metrics
                tracing::info!("Retrying Compaction failed {:?} after {:?}", e, d);
            })
            .await
            .map_err(|e: iceberg::Error| CompactionError::from(e))
    }
}

/// Known Iceberg snapshot summary keys managed by `SnapshotSummaryCollector`
/// and `update_snapshot_summaries`.
///
/// These keys are auto-computed by iceberg-rust during snapshot production and must
/// NOT be copied from the previous snapshot — doing so would overwrite the correctly
/// recalculated values. Only properties whose keys are *not* in this list (and don't
/// start with `"partitions."`) are considered custom metadata that must be preserved.
const KNOWN_SNAPSHOT_SUMMARY_KEYS: &[&str] = &[
    "added-data-files",
    "added-delete-files",
    "added-equality-delete-files",
    "added-position-delete-files",
    "added-files-size",
    "added-records",
    "added-equality-deletes",
    "added-position-deletes",
    "deleted-data-files",
    "removed-delete-files",
    "removed-equality-delete-files",
    "removed-position-delete-files",
    "removed-files-size",
    "deleted-records",
    "removed-equality-deletes",
    "removed-position-deletes",
    "total-data-files",
    "total-delete-files",
    "total-files-size",
    "total-records",
    "total-equality-deletes",
    "total-position-deletes",
    "changed-partition-count",
];

/// Extracts non-standard (custom) properties from a snapshot's summary.
fn custom_snapshot_properties(snapshot: &Snapshot) -> HashMap<String, String> {
    snapshot
        .summary()
        .additional_properties
        .iter()
        .filter(|(k, _)| {
            !KNOWN_SNAPSHOT_SUMMARY_KEYS.contains(&k.as_str()) && !k.starts_with("partitions.")
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Compaction plan describing files to rewrite and target commit location.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    /// Group of files to be compacted together
    pub file_group: FileGroup,
    /// Target branch for committing the compaction result
    pub to_branch: Cow<'static, str>,
    /// Snapshot ID from which files were selected
    pub snapshot_id: i64,
    /// Minimum data sequence among files with applicable deletes in the planned snapshot.
    delete_cleanup_min_data_sequence_number: Option<i64>,
}

impl CompactionPlan {
    /// Creates a new compaction plan.
    pub fn new(
        file_group: FileGroup,
        to_branch: impl Into<Cow<'static, str>>,
        snapshot_id: i64,
    ) -> Self {
        Self {
            file_group,
            to_branch: to_branch.into(),
            snapshot_id,
            delete_cleanup_min_data_sequence_number: None,
        }
    }

    pub(crate) fn with_delete_cleanup_min_data_sequence_number(
        mut self,
        sequence_number: Option<i64>,
    ) -> Self {
        self.delete_cleanup_min_data_sequence_number = sequence_number;
        self
    }

    /// Creates an empty plan for testing.
    pub fn dummy() -> Self {
        Self {
            file_group: FileGroup::empty(),
            to_branch: Cow::Borrowed(MAIN_BRANCH),
            snapshot_id: UNASSIGNED_SNAPSHOT_ID,
            delete_cleanup_min_data_sequence_number: None,
        }
    }

    /// Returns total number of files to be compacted.
    pub fn file_count(&self) -> usize {
        self.file_group.input_files_count()
    }

    /// Returns total size in bytes of files to be compacted.
    pub fn total_bytes(&self) -> u64 {
        self.file_group.input_total_bytes()
    }

    /// Returns whether this plan has any files to compact.
    /// Returns `false` if the file group is empty, `true` otherwise.
    pub fn has_files(&self) -> bool {
        !self.file_group.is_empty()
    }

    /// Returns recommended executor parallelism from file group.
    pub fn recommended_executor_parallelism(&self) -> usize {
        self.file_group.executor_parallelism
    }

    /// Returns recommended output parallelism from file group.
    pub fn recommended_output_parallelism(&self) -> usize {
        self.file_group.output_parallelism
    }
}

/// Planner for generating compaction plans from table snapshots.
pub struct CompactionPlanner {
    config: CompactionPlanningConfig,
}

impl CompactionPlanner {
    /// Creates a new planner with the given configuration.
    pub fn new(config: CompactionPlanningConfig) -> Self {
        Self { config }
    }

    /// Plans compaction for a specific branch.
    ///
    /// # Returns
    ///
    /// Vector of `CompactionPlan` based on file grouping strategy.
    ///
    /// # Errors
    ///
    /// Returns error if branch snapshot not found or file grouping fails.
    pub async fn plan_compaction_with_branch(
        &self,
        table: &Table,
        to_branch: &str,
    ) -> Result<Vec<CompactionPlan>> {
        if let Some(branch_snapshot) = table.metadata().snapshot_for_ref(to_branch) {
            // Step 1: Group files for compaction (extensible)
            let (file_groups, delete_cleanup_min_data_sequence_number) = self
                .group_files_for_compaction(table, branch_snapshot.snapshot_id())
                .await?;

            // Convert each FileGroup to a separate CompactionPlan
            // Filter out empty plans to avoid unnecessary processing
            let plans = file_groups
                .into_iter()
                .map(|file_group| {
                    CompactionPlan::new(
                        file_group,
                        to_branch.to_owned(),
                        branch_snapshot.snapshot_id(),
                    )
                    .with_delete_cleanup_min_data_sequence_number(
                        delete_cleanup_min_data_sequence_number,
                    )
                })
                .filter(|plan| plan.has_files())
                .collect();

            Ok(plans)
        } else {
            Ok(vec![])
        }
    }

    /// Plans compaction for the main branch.
    pub async fn plan_compaction(&self, table: &Table) -> Result<Vec<CompactionPlan>> {
        self.plan_compaction_with_branch(table, MAIN_BRANCH).await
    }

    /// Plans compaction and returns the plans together with a complete account
    /// of why each file was selected.
    ///
    /// The report is the durable half of a plan: the caller persists it before
    /// any attempt exists, so an attempt can be checked against the decision
    /// that authorised it. Plans and report come from one derivation, so every
    /// path in the report appears in exactly one returned plan and every data
    /// file in the returned plans appears in the report — a disagreement is not
    /// merely detectable, it is unconstructible.
    ///
    /// # Errors
    ///
    /// Returns an error when the branch snapshot is missing, when manifest
    /// identity cannot be read, when the policy is inconsistent, when
    /// parallelism calculation fails, or when the report would contain a
    /// duplicate identity.
    pub async fn plan_compaction_with_report(
        &self,
        table: &Table,
        to_branch: &str,
    ) -> Result<(Vec<CompactionPlan>, SelectionReport)> {
        let Some(branch_snapshot) = table.metadata().snapshot_for_ref(to_branch) else {
            return Err(CompactionError::Execution(format!(
                "branch '{to_branch}' has no snapshot to plan from"
            )));
        };
        let snapshot_id = branch_snapshot.snapshot_id();

        let CompactionPlanningConfig::WyrdIdentityAware(config) = &self.config else {
            let plans = self.plan_compaction_with_branch(table, to_branch).await?;
            let strategy = Self::upstream_strategy_kind(&self.config);
            let reason = strategy
                .uniform_reason()
                .expect("upstream strategies always declare a uniform reason");
            let selected = plans
                .iter()
                .flat_map(|plan| plan.file_group.data_files.iter())
                .map(|task| SelectedFile {
                    file_path: task.data_file_path.clone(),
                    reason,
                })
                .collect();
            let report = SelectionReport::new(strategy, snapshot_id, None, selected)?;
            return Ok((plans, report));
        };

        let selector = IdentityAwareSelector::new(config.policy.clone())?;
        let (joined, min_sequence) =
            Self::joined_identity_groups(table, snapshot_id, &selector, config).await?;

        let mut plans = Vec::with_capacity(joined.len());
        let mut groups: Vec<SelectionGroup> = Vec::with_capacity(joined.len());
        for JoinedGroup { group, tasks } in joined {
            let file_group = FileGroup::new(tasks).with_calculated_parallelism(&self.config)?;
            plans.push(
                CompactionPlan::new(file_group, to_branch.to_owned(), snapshot_id)
                    .with_delete_cleanup_min_data_sequence_number(min_sequence),
            );
            groups.push(group);
        }
        let report = selector.report(snapshot_id, &groups)?;

        Ok((plans, report))
    }

    /// Maps an upstream planning config to its report strategy.
    ///
    /// # Panics
    ///
    /// Never: the identity-aware variant is handled by its own branch before
    /// this is reached, and the fallback preserves that invariant explicitly.
    fn upstream_strategy_kind(config: &CompactionPlanningConfig) -> SelectionStrategyKind {
        match config {
            CompactionPlanningConfig::SmallFiles(_) => SelectionStrategyKind::UpstreamSmallFiles,
            CompactionPlanningConfig::Full(_) => SelectionStrategyKind::UpstreamFull,
            CompactionPlanningConfig::FilesWithDeletes(_) => {
                SelectionStrategyKind::UpstreamFilesWithDeletes
            }
            CompactionPlanningConfig::Auto(_) => SelectionStrategyKind::UpstreamAuto,
            CompactionPlanningConfig::WyrdIdentityAware(_) => {
                SelectionStrategyKind::WyrdIdentityAware
            }
        }
    }

    /// Customization point for file grouping logic.
    ///
    /// Upstream configs run the filter/grouping pipeline. The identity-aware
    /// config is routed to its own selector instead: its precedence is stateful
    /// and its grouping is ordered, neither of which the pipeline can express.
    async fn group_files_for_compaction(
        &self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<(Vec<FileGroup>, Option<i64>)> {
        use crate::file_selection::PlanStrategy;

        if let CompactionPlanningConfig::WyrdIdentityAware(config) = &self.config {
            return self
                .group_files_by_identity(table, snapshot_id, config)
                .await;
        }

        let strategy = PlanStrategy::from(&self.config);
        let tasks = FileSelector::scan_data_files(table, snapshot_id).await?;
        let min_sequence = FileSelector::delete_cleanup_min_data_sequence_number(&tasks);
        let file_groups = FileSelector::group_tasks_with_strategy(tasks, strategy, &self.config)?;
        Ok((file_groups, min_sequence))
    }

    /// Groups files using the core-owned identity-aware policy.
    ///
    /// The scan is still the source of the tasks, so delete attachment,
    /// projection, and deletion-vector handling stay exactly as upstream built
    /// them. Only the *choice* of which tasks to keep, and how to group them,
    /// comes from the policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the policy is inconsistent, when manifest identity
    /// cannot be read, or when parallelism calculation fails for a group.
    async fn group_files_by_identity(
        &self,
        table: &Table,
        snapshot_id: i64,
        config: &crate::config::WyrdIdentityAwareConfig,
    ) -> Result<(Vec<FileGroup>, Option<i64>)> {
        let selector = IdentityAwareSelector::new(config.policy.clone())?;
        let (joined, min_sequence) =
            Self::joined_identity_groups(table, snapshot_id, &selector, config).await?;
        let file_groups = joined
            .into_iter()
            .map(|joined| FileGroup::new(joined.tasks).with_calculated_parallelism(&self.config))
            .collect::<Result<Vec<_>>>()?;
        Ok((file_groups, min_sequence))
    }

    /// Runs the identity-aware policy against one snapshot and joins its groups
    /// onto that snapshot's scan tasks.
    ///
    /// This is the single derivation behind both the plans and the durable
    /// report. Running it twice would let the two disagree; running it once
    /// makes the report a function of the plans by construction. The same scan
    /// also yields upstream's delete-cleanup lower bound, which describes the
    /// snapshot's live data rather than the files this policy selected.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot is absent from the table metadata,
    /// when manifest identity cannot be read, when the scan fails, or when the
    /// caller declared a plan budget of zero.
    ///
    /// The declared plan budget is applied here, on the selector's own ordered
    /// groups, before the join and therefore before either the plans or the
    /// report exist. Capping the *groups* rather than the returned plans is what
    /// makes the budget a selection decision: the report is derived from the
    /// surviving join, so a budget-capped pass reports exactly the files its
    /// plans rewrite and nothing from the pre-cap set.
    async fn joined_identity_groups(
        table: &Table,
        snapshot_id: i64,
        selector: &IdentityAwareSelector,
        config: &crate::config::WyrdIdentityAwareConfig,
    ) -> Result<(Vec<JoinedGroup>, Option<i64>)> {
        if config.max_selection_plans == 0 {
            return Err(CompactionError::Config(
                "identity-aware planning requires a plan budget of at least one".to_owned(),
            ));
        }
        let index =
            ManifestIdentityIndex::load(table, snapshot_id, config.policy.event_time_field_id)
                .await?;
        let mut groups = selector.select(index.identities())?;
        groups.truncate(config.max_selection_plans);
        let tasks = FileSelector::scan_data_files(table, snapshot_id).await?;
        let min_sequence = FileSelector::delete_cleanup_min_data_sequence_number(&tasks);
        Ok((join_groups_to_tasks(groups, tasks), min_sequence))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
    use iceberg::spec::{DataFile, MAIN_BRANCH, NestedField, PrimitiveType, Schema, Type};
    use iceberg::table::Table;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use iceberg::writer::base_writer::position_delete_file_writer::{
        PositionDeleteFileWriterBuilder, PositionDeleteInput,
    };
    use iceberg::writer::delta_writer::{DELETE_OP, DeltaWriterBuilder, INSERT_OP};
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
    use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
    use itertools::Itertools;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::compaction::identity_plan::join_groups_to_tasks;
    // Additional imports for new tests
    use crate::compaction::{
        CommitManagerRetryConfig, CompactionPlan, RewriteResult, UNASSIGNED_SNAPSHOT_ID,
    };
    use crate::compaction::{CompactionBuilder, CompactionPlanner, resolve_data_files_by_path};
    use crate::config::{
        CompactionConfigBuilder, CompactionExecutionConfig, CompactionExecutionConfigBuilder,
        CompactionPlanningConfig, SmallFilesConfigBuilder,
    };
    use crate::error::CompactionError;
    use crate::executor::{
        CompactionExecutor, DataFusionExecutor, ExecutorType, RewriteFilesRequest, RewriteFilesStat,
    };
    use crate::file_selection::{FileSelector, ManifestIdentityIndex};
    use crate::managed::selection::IdentityAwareSelector;

    mod file_group_scope;

    // ----------------------
    // Test helpers to reduce duplication
    // ----------------------

    struct TestEnv {
        #[allow(dead_code)]
        temp_dir: TempDir,
        warehouse_location: String,
        catalog: Arc<MemoryCatalog>,
        table_ident: TableIdent,
        table: Table,
    }

    async fn create_test_env() -> TestEnv {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        warehouse_location.clone(),
                    )]),
                )
                .await
                .unwrap(),
        );

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(catalog.as_ref(), &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(catalog.as_ref(), &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        TestEnv {
            temp_dir,
            warehouse_location,
            catalog,
            table_ident,
            table,
        }
    }

    async fn append_and_commit<C: Catalog>(
        table: &Table,
        catalog: &C,
        data_files: Vec<DataFile>,
    ) -> Table {
        let transaction = Transaction::new(table);
        let append_action = transaction.fast_append().add_data_files(data_files);
        let tx = append_action.apply(transaction).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    async fn write_simple_files(
        table: &Table,
        warehouse_location: &str,
        suffix_prefix: &str,
        count: usize,
    ) -> Vec<DataFile> {
        let mut all = Vec::new();
        for i in 0..count {
            let mut writer = build_simple_data_writer(
                table,
                warehouse_location.to_owned(),
                &format!("{suffix_prefix}_{i}"),
            )
            .await;
            let batch = create_test_record_batch(&simple_table_schema());
            writer.write(batch).await.unwrap();
            let files = writer.close().await.unwrap();
            all.extend(files);
        }
        all
    }

    async fn create_namespace<C: Catalog>(catalog: &C, namespace_ident: &NamespaceIdent) {
        let _ = catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .unwrap();
    }

    fn simple_table_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap()
    }

    fn simple_table_schema_with_pos() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "pos", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    async fn create_table<C: Catalog>(catalog: &C, table_ident: &TableIdent) {
        let _ = catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .schema(simple_table_schema())
                    .build(),
            )
            .await
            .unwrap();
    }

    fn create_test_record_batch_with_pos(iceberg_schema: &Schema, insert: bool) -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let op = if insert { INSERT_OP } else { DELETE_OP };
        let pos_array = Int32Array::from(vec![op, op, op]);

        // Convert iceberg schema to arrow schema to ensure field ID consistency
        let arrow_schema = schema_to_arrow_schema(iceberg_schema).unwrap();

        RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(pos_array),
        ])
        .unwrap()
    }

    fn create_test_record_batch(iceberg_schema: &Schema) -> RecordBatch {
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        // Convert iceberg schema to arrow schema to ensure field ID consistency
        let arrow_schema = schema_to_arrow_schema(iceberg_schema).unwrap();

        RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(id_array),
            Arc::new(name_array),
        ])
        .unwrap()
    }

    /// Creates a large record batch (1000 rows) for tests that need files
    /// significantly larger than the small test files (~1.3 KB).
    fn create_test_record_batch_large(iceberg_schema: &Schema) -> RecordBatch {
        let ids: Vec<i32> = (0..1000).collect();
        let names: Vec<String> = (0..1000).map(|i| format!("name_{i:0>100}")).collect();
        let id_array = Int32Array::from(ids);
        let name_array = StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let arrow_schema = schema_to_arrow_schema(iceberg_schema).unwrap();

        RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(id_array),
            Arc::new(name_array),
        ])
        .unwrap()
    }

    async fn build_equality_delta_writer(
        table: &Table,
        warehouse_location: String,
        unique_column_ids: Vec<i32>,
    ) -> impl IcebergWriter {
        let table_schema = table.metadata().current_schema().clone();
        let unique_uuid_suffix = Uuid::now_v7().to_string();

        let location_generator =
            DefaultLocationGenerator::with_data_location(warehouse_location.clone());
        let file_name_generator = DefaultFileNameGenerator::new(
            "data".to_owned(),
            Some(unique_uuid_suffix.clone()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let data_file_builder = DataFileWriterBuilder::new(RollingFileWriterBuilder::new(
            ParquetWriterBuilder::new(WriterProperties::builder().build(), table_schema.clone()),
            1024 * 1024,
            table.file_io().clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        ));

        let position_delete_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        2147483546,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(2147483545, "pos", Type::Primitive(PrimitiveType::Long))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let position_delete_builder =
            PositionDeleteFileWriterBuilder::new(RollingFileWriterBuilder::new(
                ParquetWriterBuilder::new(WriterProperties::new(), position_delete_schema),
                1024 * 1024,
                table.file_io().clone(),
                location_generator.clone(),
                file_name_generator.clone(),
            ));

        let equality_delete_config =
            EqualityDeleteWriterConfig::new(unique_column_ids.clone(), table_schema.clone())
                .unwrap();
        let equality_delete_builder = EqualityDeleteFileWriterBuilder::new(
            RollingFileWriterBuilder::new(
                ParquetWriterBuilder::new(
                    WriterProperties::new(),
                    Arc::new(
                        Schema::builder()
                            .with_fields(
                                unique_column_ids
                                    .iter()
                                    .map(|id| table_schema.field_by_id(*id).unwrap().clone())
                                    .collect_vec(),
                            )
                            .build()
                            .unwrap(),
                    ),
                ),
                1024 * 1024,
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            ),
            equality_delete_config,
        );

        DeltaWriterBuilder::new(
            data_file_builder,
            position_delete_builder,
            equality_delete_builder,
            unique_column_ids,
            table_schema,
        )
        .build(None)
        .await
        .unwrap()
    }

    async fn build_simple_data_writer(
        table: &Table,
        warehouse_location: String,
        file_name_suffix: &str,
    ) -> impl IcebergWriter {
        let table_schema = table.metadata().current_schema();

        // Set up writer
        let location_generator = DefaultLocationGenerator::with_data_location(warehouse_location);

        let file_name_generator = DefaultFileNameGenerator::new(
            "data".to_owned(),
            Some(file_name_suffix.to_owned()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::builder().build(), table_schema.clone()),
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        data_file_builder.build(None).await.unwrap()
    }

    async fn load_data_files_from_snapshot(table: &Table, branch: &str) -> Vec<DataFile> {
        let snapshot = table.metadata().snapshot_for_ref(branch).unwrap();
        let manifest_list = table
            .object_cache()
            .get_manifest_list(snapshot, &table.metadata_ref())
            .await
            .unwrap();

        let mut data_files = Vec::new();
        for manifest in manifest_list.entries() {
            let manifest_file = manifest.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest_file.entries() {
                if entry.is_alive() {
                    data_files.push(entry.data_file().clone());
                }
            }
        }
        data_files
    }

    fn assert_compaction_stats(
        stats: &RewriteFilesStat,
        expected_input_count: usize,
        allow_output_increase: bool,
    ) {
        assert_eq!(stats.input_files_count, expected_input_count);
        if !allow_output_increase {
            assert!(stats.output_files_count <= expected_input_count);
        }
        assert!(stats.output_files_count > 0);
        assert!(stats.input_total_bytes > 0);
        assert!(stats.output_total_bytes > 0);
    }

    fn create_default_compaction(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
    ) -> crate::compaction::Compaction {
        CompactionBuilder::new(catalog, table_ident)
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .build()
    }

    #[tokio::test]
    async fn test_write_commit_and_compaction() {
        let env = create_test_env().await;
        let table = &env.table;

        let unique_column_ids = vec![1];
        let mut writer =
            build_equality_delta_writer(table, env.warehouse_location.clone(), unique_column_ids)
                .await;

        let insert_batch = create_test_record_batch_with_pos(&simple_table_schema_with_pos(), true);
        let delete_batch =
            create_test_record_batch_with_pos(&simple_table_schema_with_pos(), false);

        writer.write(insert_batch.clone()).await.unwrap();
        writer.write(delete_batch).await.unwrap();
        writer.write(insert_batch).await.unwrap();

        let data_files = writer.close().await.unwrap();
        let initial_file_count = data_files.len();

        let updated_table = append_and_commit(table, env.catalog.as_ref(), data_files).await;

        assert_eq!(updated_table.metadata().snapshots().len(), 1);
        let latest_snapshot = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();

        let reloaded_table = env.catalog.load_table(&env.table_ident).await.unwrap();
        let current_snapshot = reloaded_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        assert_eq!(
            current_snapshot.snapshot_id(),
            latest_snapshot.snapshot_id()
        );

        let execution_config = CompactionExecutionConfigBuilder::default()
            .enable_validate_compaction(true)
            .build()
            .unwrap();

        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default()
                    .execution(execution_config)
                    .build()
                    .unwrap(),
            ))
            .build();

        let result = compaction.compact().await.unwrap().unwrap();
        assert_compaction_stats(&result.stats, initial_file_count, false);
    }

    /// Resolution must return exactly the requested paths, gathering them from across
    /// multiple manifests and ignoring every other file in the snapshot. This is the
    /// property that keeps commit memory proportional to the batch instead of to the
    /// whole table.
    #[tokio::test]
    async fn test_resolve_data_files_by_path_returns_only_requested_paths() {
        let env = create_test_env().await;

        // Two appends, so the snapshot spans more than one manifest.
        let first = write_simple_files(&env.table, &env.warehouse_location, "first", 2).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), first.clone()).await;
        let second = write_simple_files(&table, &env.warehouse_location, "second", 2).await;
        let table = append_and_commit(&table, env.catalog.as_ref(), second.clone()).await;

        let snapshot = table.metadata().snapshot_for_ref(MAIN_BRANCH).unwrap();

        // Guard the premise of this test: the resolver must be crossing manifest boundaries.
        let manifest_count = table
            .object_cache()
            .get_manifest_list(snapshot, &table.metadata_ref())
            .await
            .unwrap()
            .entries()
            .len();
        assert!(
            manifest_count >= 2,
            "expected the snapshot to span multiple manifests, got {manifest_count}"
        );

        // One file from each manifest, plus a path that is not in the table at all.
        let requested = [
            first[0].file_path().to_owned(),
            second[0].file_path().to_owned(),
        ];
        let mut wanted: HashSet<&str> = requested.iter().map(String::as_str).collect();
        wanted.insert("s3://nonexistent/file.parquet");

        let resolved = resolve_data_files_by_path(snapshot, &table, &wanted)
            .await
            .unwrap();

        // The unknown path is dropped rather than erroring, and the real paths each
        // resolve to their own record.
        assert_eq!(resolved.len(), 2);
        for path in &requested {
            assert_eq!(resolved.get(path).unwrap().file_path(), path);
        }
    }

    /// An empty request must short-circuit *before* reading the manifest list, not merely
    /// happen to return an empty result because there was nothing to match. Proven with a
    /// snapshot whose `manifest_list` points at a path that does not exist: scanning a valid
    /// snapshot with an empty `wanted` set would also return an empty map even without the
    /// short-circuit, so that alone doesn't distinguish "skipped the read" from "did the read
    /// and it happened to match nothing". If the short-circuit were removed, this call would
    /// instead try to read the nonexistent path and return `Err`, so `.unwrap()` only survives
    /// while the empty-request check runs first.
    #[tokio::test]
    async fn test_resolve_data_files_by_path_short_circuits_on_empty_request() {
        let env = create_test_env().await;

        let bogus_snapshot = Arc::new(
            iceberg::spec::Snapshot::builder()
                .with_snapshot_id(1)
                .with_sequence_number(1)
                .with_timestamp_ms(0)
                .with_manifest_list(format!(
                    "{}/metadata/does-not-exist-{}.avro",
                    env.warehouse_location,
                    Uuid::new_v4()
                ))
                .with_summary(iceberg::spec::Summary {
                    operation: iceberg::spec::Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build(),
        );

        let resolved = resolve_data_files_by_path(&bogus_snapshot, &env.table, &HashSet::new())
            .await
            .unwrap();

        assert!(resolved.is_empty());
    }

    /// A live entry and a same-path tombstone from an earlier lifecycle can coexist in one
    /// manifest list -- deleting a file does not rewrite the *old* manifest that originally
    /// added it, and, empirically, the snapshot producer stops referencing that manifest once
    /// none of its entries are live rather than editing it in place. Resolution must return
    /// the live record and must not let the tombstone shadow it.
    ///
    /// Both entries are written into a *single* synthetic manifest, live first then the
    /// tombstone, deliberately rather than mirroring the two-separate-manifests shape the doc
    /// comment above describes. Two reasons:
    ///
    /// - `resolve_data_files_by_path` only re-checks its `resolved.len() == wanted.len()` early
    ///   exit *between* manifests, not between entries within one (`core/src/compaction/mod.rs`,
    ///   the `while let Some(manifest) = manifests.next().await` loop). With one path spread
    ///   across two manifests loaded concurrently via `buffer_unordered`, whichever manifest
    ///   happens to be visited first satisfies the exit on its own and the second manifest is
    ///   never even loaded -- so with a two-manifest layout, whether the tombstone is seen at
    ///   all (with or without the resolver's `is_alive` check) depends on manifest completion
    ///   order, which `buffer_unordered` does not guarantee. That made an earlier version of
    ///   this test pass 20/20 runs with the resolver's `!entry.is_alive()` check deleted, for a
    ///   different reason than the one below.
    /// - A single manifest's `entries()` are iterated in a fixed, sequential order with no
    ///   concurrency involved, so putting the live entry before the tombstone makes the
    ///   overwrite-if-both-are-inserted order deterministic: if the resolver's live/dead skip
    ///   is ever removed, the tombstone (inserted second) unconditionally overwrites the live
    ///   entry (inserted first) in the result map, every single run, not just probabilistically.
    ///
    /// The live and tombstone `DataFile`s also carry *different* `record_count`s for the same
    /// `file_path`, so the final assertion can tell which one the resolver actually returned.
    /// A real `overwrite_files().delete_files(...)` commit cannot produce that distinction to
    /// test against: `ReplaceFilesOperation::delete_entries` (`transaction/replace_files.rs`)
    /// looks up the still-live entry by path and clones *its* `DataFile` verbatim into the
    /// tombstone (`old_entry.data_file().clone()`), so a genuine tombstone is always
    /// byte-for-byte identical to the entry it replaces. An earlier version of this test built
    /// both entries from a real delete commit and asserted only `file_path`, which is identical
    /// either way regardless of the resolver's correctness. The manifest here is therefore
    /// built directly with `ManifestWriterBuilder`, independent of any commit, so the two
    /// entries can be given distinguishable `DataFile`s.
    #[tokio::test]
    async fn test_resolve_data_files_by_path_prefers_live_entry_over_tombstone() {
        let env = create_test_env().await;
        let table = &env.table;
        let schema = table.metadata().current_schema().clone();
        let partition_spec = table.metadata().default_partition_spec().as_ref().clone();

        let shared_path = format!(
            "{}/data/shadowed-{}.parquet",
            env.warehouse_location,
            Uuid::new_v4()
        );
        let build_data_file = |record_count: u64| {
            iceberg::spec::DataFileBuilder::default()
                .content(iceberg::spec::DataContentType::Data)
                .file_path(shared_path.clone())
                .file_format(iceberg::spec::DataFileFormat::Parquet)
                .partition_spec_id(partition_spec.spec_id())
                .record_count(record_count)
                .file_size_in_bytes(1)
                .build()
                .unwrap()
        };
        // Same path, deliberately different `record_count`s -- see the doc comment above.
        let live_data_file = build_data_file(1);
        let tombstone_data_file = build_data_file(999);
        assert_ne!(
            live_data_file.record_count(),
            tombstone_data_file.record_count(),
            "the live and tombstone records must be distinguishable for this test to \
             mean anything"
        );

        let manifest_path = format!(
            "{}/metadata/shadow-{}.avro",
            env.warehouse_location,
            Uuid::new_v4()
        );
        let mut manifest_writer = iceberg::spec::ManifestWriterBuilder::new(
            table.file_io().new_output(&manifest_path).unwrap(),
            Some(1),
            schema,
            partition_spec,
        )
        .build_v2_data();
        // Live first, tombstone second -- see the doc comment above for why the order
        // matters to this test's determinism.
        manifest_writer
            .add_existing_file(live_data_file.clone(), 1, 1, Some(1))
            .unwrap();
        manifest_writer
            .add_delete_file(tombstone_data_file.clone(), 1, Some(1))
            .unwrap();
        let mut manifest = manifest_writer.write_manifest_file().await.unwrap();
        // Real commits assign these at commit time; stamp them explicitly since this
        // manifest is never actually committed. Must be non-negative or the manifest list
        // writer below rejects it as "unassigned".
        manifest.sequence_number = 1;
        manifest.min_sequence_number = 1;

        let manifest_list_path = format!(
            "{}/metadata/test-manifest-list-{}.avro",
            env.warehouse_location,
            Uuid::new_v4()
        );
        let output_file = table.file_io().new_output(&manifest_list_path).unwrap();
        let file_writer = output_file.writer().await.unwrap();
        let mut writer = iceberg::spec::ManifestListWriter::v2(file_writer, 1, None, 1);
        writer.add_manifests(std::iter::once(manifest)).unwrap();
        writer.close().await.unwrap();

        let snapshot = Arc::new(
            iceberg::spec::Snapshot::builder()
                .with_snapshot_id(1)
                .with_sequence_number(1)
                .with_timestamp_ms(0)
                .with_manifest_list(manifest_list_path)
                .with_summary(iceberg::spec::Summary {
                    operation: iceberg::spec::Operation::Overwrite,
                    additional_properties: HashMap::new(),
                })
                .build(),
        );

        // Guard the premise: the manifest must contain both a live entry and a tombstone
        // for the shared path, in that order, otherwise this test isn't exercising the
        // shadowing case at all.
        let manifest_list = table
            .object_cache()
            .get_manifest_list(&snapshot, &table.metadata_ref())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 1);
        let loaded_manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        let statuses: Vec<_> = loaded_manifest
            .entries()
            .iter()
            .filter(|entry| entry.data_file().file_path() == shared_path)
            .map(|entry| entry.is_alive())
            .collect();
        assert_eq!(
            statuses,
            vec![true, false],
            "expected exactly one live entry followed by one tombstone for the shared path, \
             got is_alive()={statuses:?}"
        );

        let wanted: HashSet<&str> = std::iter::once(shared_path.as_str()).collect();
        let resolved = resolve_data_files_by_path(&snapshot, table, &wanted)
            .await
            .unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(
            resolved.get(&shared_path).unwrap().record_count(),
            live_data_file.record_count(),
            "resolver must return the live entry's record, not the tombstone's"
        );
    }

    #[tokio::test]
    async fn test_full_compaction() {
        let env = create_test_env().await;

        let data_files = write_simple_files(&env.table, &env.warehouse_location, "test", 3).await;
        let initial_file_count = data_files.len();
        let updated_table = append_and_commit(&env.table, env.catalog.as_ref(), data_files).await;

        let snapshot_before = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();

        let compaction = create_default_compaction(env.catalog.clone(), env.table_ident.clone());
        let result = compaction.compact().await.unwrap().unwrap();

        assert_compaction_stats(&result.stats, initial_file_count, false);

        let final_table = result.table.unwrap();
        let snapshot_after = final_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        assert_ne!(snapshot_before.snapshot_id(), snapshot_after.snapshot_id());
    }

    #[tokio::test]
    async fn test_small_files_compaction_with_validation() {
        let env = create_test_env().await;

        let batch = create_test_record_batch(&simple_table_schema());
        let small_files1 =
            write_simple_files(&env.table, &env.warehouse_location, "small1", 1).await;
        let small_files2 =
            write_simple_files(&env.table, &env.warehouse_location, "small2", 1).await;

        let mut large_writer =
            build_simple_data_writer(&env.table, env.warehouse_location.clone(), "large").await;
        for _ in 0..10 {
            large_writer.write(batch.clone()).await.unwrap();
        }
        let large_files = large_writer.close().await.unwrap();

        let mut all_data_files = Vec::new();
        all_data_files.extend(small_files1);
        all_data_files.extend(small_files2);
        all_data_files.extend(large_files);

        let updated_table =
            append_and_commit(&env.table, env.catalog.as_ref(), all_data_files).await;

        let data_files_before = load_data_files_from_snapshot(&updated_table, MAIN_BRANCH).await;

        let small_file_threshold = 10_000;

        let compaction_config = CompactionConfigBuilder::default()
            .planning(CompactionPlanningConfig::SmallFiles(
                SmallFilesConfigBuilder::default()
                    .small_file_threshold_bytes(small_file_threshold)
                    .build()
                    .unwrap(),
            ))
            .build()
            .unwrap();

        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(compaction_config))
            .build();

        let planner = CompactionPlanner::new(compaction.config.as_ref().unwrap().planning.clone());
        let snapshot_before = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();

        let (files_to_compact, _) = planner
            .group_files_for_compaction(&updated_table, snapshot_before.snapshot_id())
            .await
            .unwrap();

        let selected_file_paths: std::collections::HashSet<&str> = files_to_compact
            .iter()
            .flat_map(|group| &group.data_files)
            .map(|task| task.data_file_path())
            .collect();

        let small_files_count = data_files_before
            .iter()
            .filter(|file| file.file_size_in_bytes() < small_file_threshold)
            .count();

        let large_files_count = data_files_before
            .iter()
            .filter(|file| file.file_size_in_bytes() >= small_file_threshold)
            .count();

        for data_file in &data_files_before {
            if data_file.file_size_in_bytes() < small_file_threshold {
                assert!(selected_file_paths.contains(data_file.file_path()));
            } else {
                assert!(!selected_file_paths.contains(data_file.file_path()));
            }
        }

        assert!(small_files_count > 0);

        let result = compaction.compact().await.unwrap().unwrap();
        assert_eq!(result.stats.input_files_count, small_files_count);
        assert!(result.stats.output_files_count <= small_files_count);
        assert!(result.stats.output_files_count > 0);

        let final_data_files = load_data_files_from_snapshot(
            &env.catalog.load_table(&env.table_ident).await.unwrap(),
            MAIN_BRANCH,
        )
        .await;

        let expected_final_count = large_files_count + result.stats.output_files_count as usize;
        assert_eq!(final_data_files.len(), expected_final_count);

        let final_file_paths: std::collections::HashSet<&str> = final_data_files
            .iter()
            .map(|file| file.file_path())
            .collect();

        for data_file in &data_files_before {
            if data_file.file_size_in_bytes() >= small_file_threshold {
                assert!(final_file_paths.contains(data_file.file_path()));
            }
        }
    }

    /// Test empty input scenarios (table, plan, results)
    #[tokio::test]
    async fn test_empty_input_scenarios() {
        use crate::file_selection::FileGroup;

        let env = create_test_env().await;

        let planner = CompactionPlanner::new(CompactionPlanningConfig::default());
        let plan = planner.plan_compaction(&env.table).await.unwrap();
        assert!(plan.is_empty());

        let compaction = create_default_compaction(env.catalog.clone(), env.table_ident.clone());
        let result = compaction.compact().await.unwrap();
        assert!(result.is_none());

        let empty_plan =
            CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, UNASSIGNED_SNAPSHOT_ID);
        let result = compaction
            .compact_with_plan(empty_plan, &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    /// Test compaction with sort order to verify data is sorted correctly
    #[tokio::test]
    async fn test_compaction_with_sort_order() {
        let env = create_test_env().await;
        let namespace_ident = NamespaceIdent::new("test_namespace2".into());
        create_namespace(env.catalog.as_ref(), &namespace_ident).await;
        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table_order".into());

        let sort_order = iceberg::spec::SortOrder::builder()
            .with_sort_field(iceberg::spec::SortField {
                source_id: 1,
                transform: iceberg::spec::Transform::Identity,
                direction: iceberg::spec::SortDirection::Ascending,
                null_order: iceberg::spec::NullOrder::First,
            })
            .build(&simple_table_schema())
            .unwrap();
        let _ = env
            .catalog
            .create_table(
                &table_ident.namespace,
                TableCreation::builder()
                    .name(table_ident.name().into())
                    .sort_order(sort_order)
                    .schema(simple_table_schema())
                    .build(),
            )
            .await
            .unwrap();

        let table = env.catalog.load_table(&table_ident).await.unwrap();

        let data_files = write_simple_files(&env.table, &env.warehouse_location, "test", 3).await;
        let _updated_table = append_and_commit(&table, env.catalog.as_ref(), data_files).await;

        let compaction = create_default_compaction(env.catalog.clone(), table_ident.clone());
        let result = compaction.compact().await.unwrap().unwrap();

        assert_eq!(result.data_files.len(), 1);
        assert_eq!(result.data_files[0].sort_order_id(), Some(1));

        // Read the compacted Parquet file directly so the assertion validates the
        // physical row order produced by compaction rather than a query-side ORDER BY.
        let output_file = &result.data_files[0];
        let input_file = table.file_io().new_input(output_file.file_path()).unwrap();
        let input_content = input_file.read().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(input_content)
            .unwrap()
            .build()
            .unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();

        let target_ids = [1, 1, 1, 2, 2, 2, 3, 3, 3];
        let actual_ids: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap())
            })
            .collect();
        assert_eq!(actual_ids, target_ids);
    }

    /// Writes one data file holding the given rows and returns it.
    ///
    /// Each call produces its own physical object, which is what lets a test
    /// name a row by `(data file, position)` and observe whether a delete was
    /// scoped to the file it references.
    ///
    /// # Panics
    ///
    /// Panics when the batch cannot be built or the writer cannot be closed to
    /// exactly one file, either of which means the fixture is malformed.
    async fn write_rows_file(
        table: &Table,
        warehouse_location: &str,
        file_name_suffix: &str,
        ids: &[i32],
        names: &[&str],
    ) -> DataFile {
        let arrow_schema = Arc::new(schema_to_arrow_schema(&simple_table_schema()).unwrap());
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ])
        .unwrap();

        let mut writer =
            build_simple_data_writer(table, warehouse_location.to_owned(), file_name_suffix).await;
        writer.write(batch).await.unwrap();
        let mut files = writer.close().await.unwrap();
        assert_eq!(files.len(), 1, "one batch produces one data file");
        files.remove(0)
    }

    /// Writes one position-delete file naming `(referenced data file, row position)` pairs.
    ///
    /// The inputs are sorted by path and position because the Iceberg
    /// position-delete writer expects them in that order; sorting here keeps
    /// callers free to state deletions in whatever order reads best.
    ///
    /// # Panics
    ///
    /// Panics when the writer cannot be built, written, or closed to exactly
    /// one delete file.
    async fn write_position_delete_file(
        table: &Table,
        warehouse_location: &str,
        file_name_suffix: &str,
        deletes: &[(&str, i64)],
    ) -> DataFile {
        let position_delete_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        2147483546,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(2147483545, "pos", Type::Primitive(PrimitiveType::Long))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::new(), position_delete_schema),
            table.file_io().clone(),
            DefaultLocationGenerator::with_data_location(warehouse_location.to_owned()),
            DefaultFileNameGenerator::new(
                "pos-delete".to_owned(),
                Some(file_name_suffix.to_owned()),
                iceberg::spec::DataFileFormat::Parquet,
            ),
        );

        let mut sorted = deletes.to_vec();
        sorted.sort_unstable();
        let mut writer = PositionDeleteFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .unwrap();
        writer
            .write(
                sorted
                    .into_iter()
                    .map(|(path, pos)| PositionDeleteInput::new(Arc::from(path), pos))
                    .collect(),
            )
            .await
            .unwrap();
        let mut files = writer.close().await.unwrap();
        assert_eq!(files.len(), 1, "one write produces one delete file");
        files.remove(0)
    }

    /// Reads every produced data file and returns the surviving `id` values, sorted.
    ///
    /// Reading the physical objects rather than querying the table is what
    /// makes the delete assertions row-semantic: a delete that was applied to
    /// the wrong rows, or not applied at all, changes this list.
    ///
    /// # Panics
    ///
    /// Panics when an output object cannot be read back as the `(id, name)`
    /// projection the fixture wrote.
    async fn surviving_ids(table: &Table, data_files: &[DataFile]) -> Vec<i32> {
        let mut ids = Vec::new();
        for file in data_files {
            let content = table
                .file_io()
                .new_input(file.file_path())
                .unwrap()
                .read()
                .await
                .unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(content)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                ids.extend(column.iter().map(|value| value.unwrap()));
            }
        }
        ids.sort_unstable();
        ids
    }

    /// Position deletes must remove exactly the rows they reference.
    ///
    /// Two data files carrying the same row positions make the scoping
    /// observable end to end: deleting position 0 of the first file and
    /// position 1 of the second must drop exactly those two rows and keep the
    /// other four. Asserting the surviving `id` values read back from the
    /// rewritten objects — rather than file counts or generated SQL — is what
    /// catches a merge-on-read plan that resolves the delete join against the
    /// wrong rows.
    #[tokio::test]
    async fn test_position_deletes_remove_exactly_the_referenced_rows() {
        let env = create_test_env().await;

        let first = write_rows_file(
            &env.table,
            &env.warehouse_location,
            "pos_delete_first",
            &[10, 11, 12],
            &["a0", "a1", "a2"],
        )
        .await;
        let second = write_rows_file(
            &env.table,
            &env.warehouse_location,
            "pos_delete_second",
            &[20, 21, 22],
            &["b0", "b1", "b2"],
        )
        .await;
        let first_path = first.file_path().to_owned();
        let second_path = second.file_path().to_owned();
        let table = append_and_commit(&env.table, env.catalog.as_ref(), vec![first, second]).await;

        let deletes = write_position_delete_file(&table, &env.warehouse_location, "pos_delete", &[
            (first_path.as_str(), 0),
            (second_path.as_str(), 1),
        ])
        .await;
        let transaction = Transaction::new(&table);
        let table = transaction
            .overwrite_files()
            .add_data_files(vec![deletes])
            .apply(transaction)
            .unwrap()
            .commit(env.catalog.as_ref())
            .await
            .unwrap();
        drop(table);

        let result = create_default_compaction(env.catalog.clone(), env.table_ident.clone())
            .compact()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            surviving_ids(&env.table, &result.data_files).await,
            vec![11, 12, 20, 22],
            "each position delete removed its own row in its own data file"
        );
    }

    /// Writes one equality-delete file matching rows by the `id` column.
    ///
    /// # Panics
    ///
    /// Panics when the writer cannot be built, written, or closed to exactly
    /// one delete file.
    async fn write_equality_delete_file(
        table: &Table,
        warehouse_location: &str,
        file_name_suffix: &str,
        ids: &[i32],
    ) -> DataFile {
        let table_schema = table.metadata().current_schema().clone();
        let equality_ids = vec![1];
        let delete_schema = Arc::new(
            Schema::builder()
                .with_fields(
                    equality_ids
                        .iter()
                        .map(|id| table_schema.field_by_id(*id).unwrap().clone())
                        .collect_vec(),
                )
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(schema_to_arrow_schema(&delete_schema).unwrap());
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(ids.to_vec()))])
                .unwrap();

        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::new(), delete_schema),
            table.file_io().clone(),
            DefaultLocationGenerator::with_data_location(warehouse_location.to_owned()),
            DefaultFileNameGenerator::new(
                "eq-delete".to_owned(),
                Some(file_name_suffix.to_owned()),
                iceberg::spec::DataFileFormat::Parquet,
            ),
        );
        let mut writer = EqualityDeleteFileWriterBuilder::new(
            rolling,
            EqualityDeleteWriterConfig::new(equality_ids, table_schema).unwrap(),
        )
        .build(None)
        .await
        .unwrap();
        writer.write(batch).await.unwrap();
        let mut files = writer.close().await.unwrap();
        assert_eq!(files.len(), 1, "one batch produces one delete file");
        files.remove(0)
    }

    /// Position and equality deletes compose without either widening its scope.
    ///
    /// The merge-on-read plan chains one anti-join per delete kind, so a
    /// regression in either chain link is only visible on the surviving rows.
    /// Here row 0 of the first data file and the `id = 21` row of the second are
    /// deleted by different mechanisms, and every other row must survive.
    #[tokio::test]
    async fn test_position_and_equality_deletes_remove_exactly_their_targets() {
        let env = create_test_env().await;

        let first = write_rows_file(
            &env.table,
            &env.warehouse_location,
            "both_deletes_first",
            &[10, 11, 12],
            &["a0", "a1", "a2"],
        )
        .await;
        let second = write_rows_file(
            &env.table,
            &env.warehouse_location,
            "both_deletes_second",
            &[20, 21, 22],
            &["b0", "b1", "b2"],
        )
        .await;
        let first_path = first.file_path().to_owned();
        let table = append_and_commit(&env.table, env.catalog.as_ref(), vec![first, second]).await;

        let position_delete =
            write_position_delete_file(&table, &env.warehouse_location, "both_deletes", &[(
                first_path.as_str(),
                0,
            )])
            .await;
        let equality_delete =
            write_equality_delete_file(&table, &env.warehouse_location, "both_deletes", &[21])
                .await;
        let transaction = Transaction::new(&table);
        let table = transaction
            .overwrite_files()
            .add_data_files(vec![position_delete, equality_delete])
            .apply(transaction)
            .unwrap()
            .commit(env.catalog.as_ref())
            .await
            .unwrap();
        drop(table);

        let result = create_default_compaction(env.catalog.clone(), env.table_ident.clone())
            .compact()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            surviving_ids(&env.table, &result.data_files).await,
            vec![11, 12, 20, 22],
            "each delete removed only the row it names"
        );
    }

    /// Test the `plan_compaction` functionality
    #[tokio::test]
    async fn test_plan_compaction() {
        let env = create_test_env().await;

        let data_files = write_simple_files(&env.table, &env.warehouse_location, "test", 2).await;
        let expected_file_count = data_files.len();

        let updated_table = append_and_commit(&env.table, env.catalog.as_ref(), data_files).await;

        let planner = CompactionPlanner::new(CompactionPlanningConfig::Full(
            crate::config::FullCompactionConfig::default(),
        ));

        let plans = planner.plan_compaction(&updated_table).await.unwrap();

        assert!(!plans.is_empty());
        let plan = &plans[0];
        assert_eq!(plan.file_count(), expected_file_count);
        assert!(plan.total_bytes() > 0);
        assert!(plan.recommended_executor_parallelism() > 0);
        assert!(plan.recommended_output_parallelism() > 0);
        assert_eq!(plan.to_branch, MAIN_BRANCH);
        assert!(plan.has_files(), "Plan should have files");
    }

    /// Test `plan_compaction` with non-existent branch
    #[tokio::test]
    async fn test_plan_compaction_invalid_branch() {
        let env = create_test_env().await;
        let table = &env.table;

        let planner = CompactionPlanner::new(CompactionPlanningConfig::default());

        // Test with non-existent branch - should return error or empty plan
        let result = planner
            .plan_compaction_with_branch(table, "non-existent-branch")
            .await;

        // The current implementation returns an error for non-existent branch
        // If it changes to return empty plan in the future, both are acceptable
        match result {
            Err(e) => {
                let error_msg = e.to_string();
                assert!(
                    error_msg.contains("non-existent-branch")
                        || error_msg.contains("not found")
                        || error_msg.contains("snapshot"),
                    "Error should mention the branch or snapshot issue, got: {}",
                    error_msg
                );
            }
            Ok(plans) => {
                // Alternative acceptable behavior: return empty plans
                assert!(
                    plans.is_empty(),
                    "Non-existent branch should produce empty plans or error"
                );
            }
        }
    }

    /// Test the `compact_with_plan` functionality
    #[tokio::test]
    async fn test_compact_with_plan() {
        let env = create_test_env().await;

        let data_files = write_simple_files(&env.table, &env.warehouse_location, "test", 2).await;
        let initial_file_count = data_files.len();

        let updated_table = append_and_commit(&env.table, env.catalog.as_ref(), data_files).await;

        let full_compaction_config = CompactionConfigBuilder::default()
            .planning(CompactionPlanningConfig::Full(
                crate::config::FullCompactionConfig::default(),
            ))
            .build()
            .unwrap();
        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(full_compaction_config))
            .build();

        let planner = CompactionPlanner::new(compaction.config.as_ref().unwrap().planning.clone());
        let plans = planner.plan_compaction(&updated_table).await.unwrap();

        assert!(!plans.is_empty());

        let plan = &plans[0];
        let result = compaction
            .compact_with_plan(plan.clone(), &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap()
            .unwrap();

        assert_compaction_stats(&result.stats, initial_file_count, false);
    }

    /// Test `compact_with_plan` with empty plan (merged from `test_compact_with_plan_empty` an`test_compact_no_files`es)
    #[tokio::test]
    async fn test_compact_with_empty_plan() {
        use crate::file_selection::FileGroup;

        let env = create_test_env().await;

        let compaction = create_default_compaction(env.catalog.clone(), env.table_ident.clone());

        let empty_plan =
            CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, UNASSIGNED_SNAPSHOT_ID);

        let result = compaction
            .compact_with_plan(empty_plan, &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    struct BranchTestEnv {
        _temp_dir: TempDir,
        warehouse_location: String,
        catalog: Arc<MemoryCatalog>,
        table_ident: TableIdent,
        table: Table,
    }

    async fn create_branch_test_env() -> BranchTestEnv {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_location = temp_dir.path().to_str().unwrap().to_owned();
        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        warehouse_location.clone(),
                    )]),
                )
                .await
                .unwrap(),
        );

        let namespace_ident = NamespaceIdent::new("test_namespace".into());
        create_namespace(catalog.as_ref(), &namespace_ident).await;

        let table_ident = TableIdent::new(namespace_ident.clone(), "test_table".into());
        create_table(catalog.as_ref(), &table_ident).await;

        let table = catalog.load_table(&table_ident).await.unwrap();

        BranchTestEnv {
            _temp_dir: temp_dir,
            warehouse_location,
            catalog,
            table_ident,
            table,
        }
    }

    /// Test `compact_with_plan` with branch functionality
    #[tokio::test]
    async fn test_compact_with_plan_with_branch() {
        let env = create_branch_test_env().await;

        let mut writer1 =
            build_simple_data_writer(&env.table, env.warehouse_location.clone(), "branch1").await;
        let batch = create_test_record_batch(&simple_table_schema());
        writer1.write(batch.clone()).await.unwrap();
        let branch_data_files1 = writer1.close().await.unwrap();

        let mut writer2 =
            build_simple_data_writer(&env.table, env.warehouse_location.clone(), "branch2").await;
        writer2.write(batch.clone()).await.unwrap();
        let branch_data_files2 = writer2.close().await.unwrap();

        let transaction = Transaction::new(&env.table);
        let branch_name = "feature/compaction-branch";
        let append_action = transaction
            .fast_append()
            .set_target_branch(branch_name.to_owned())
            .add_data_files(branch_data_files1)
            .add_data_files(branch_data_files2);
        let tx = append_action.apply(transaction).unwrap();
        let updated_table = tx.commit(env.catalog.as_ref()).await.unwrap();

        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .with_to_branch(branch_name.to_owned())
            .build();

        let planner = CompactionPlanner::new(CompactionPlanningConfig::default());
        let plans = planner
            .plan_compaction_with_branch(&updated_table, branch_name)
            .await
            .unwrap();

        assert!(!plans.is_empty());
        let plan = &plans[0];

        assert_eq!(plan.file_count(), 2);
        assert_eq!(plan.to_branch, branch_name);

        let result = compaction
            .compact_with_plan(plan.clone(), &compaction.config.as_ref().unwrap().execution)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.stats.input_files_count, 2);
        assert!(result.stats.output_files_count > 0);
        assert!(result.stats.output_files_count <= 2);
    }

    /// Test branch functionality with small files compaction
    #[tokio::test]
    async fn test_small_files_compaction_with_branch() {
        let env = create_branch_test_env().await;

        let new_branch = "feature/small-files-compaction";

        let mut small_writer1 =
            build_simple_data_writer(&env.table, env.warehouse_location.clone(), "small-branch")
                .await;
        let batch = create_test_record_batch(&simple_table_schema());
        small_writer1.write(batch.clone()).await.unwrap();
        let small_files1 = small_writer1.close().await.unwrap();

        let large_batch = create_test_record_batch_large(&simple_table_schema());
        let mut large_writer =
            build_simple_data_writer(&env.table, env.warehouse_location.clone(), "large-branch")
                .await;
        large_writer.write(large_batch).await.unwrap();
        let large_files = large_writer.close().await.unwrap();

        let mut all_branch_files = Vec::new();
        all_branch_files.extend(small_files1);
        all_branch_files.extend(large_files);

        let transaction = Transaction::new(&env.table);
        let append_action = transaction
            .fast_append()
            .set_target_branch(new_branch.to_owned())
            .add_data_files(all_branch_files);
        let tx = append_action.apply(transaction).unwrap();
        let updated_table = tx.commit(env.catalog.as_ref()).await.unwrap();

        let small_file_threshold = 10_000u64;
        let planning_config = CompactionPlanningConfig::SmallFiles(
            SmallFilesConfigBuilder::default()
                .small_file_threshold_bytes(small_file_threshold)
                .build()
                .unwrap(),
        );

        let branch_planner = CompactionPlanner::new(planning_config.clone());

        let branch_plans = branch_planner
            .plan_compaction_with_branch(&updated_table, new_branch)
            .await
            .unwrap();

        assert!(!branch_plans.is_empty());
        let branch_plan = &branch_plans[0];

        assert_eq!(branch_plan.file_count(), 1);
        assert_eq!(branch_plan.to_branch, new_branch);
        let input_file_path = branch_plan.file_group.data_files[0].data_file_path();
        assert!(input_file_path.contains("small-branch"));

        let branch_compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
                .with_to_branch(new_branch.to_owned())
                .build();

        let result = branch_compaction
            .compact_with_plan(
                branch_plan.clone(),
                &CompactionExecutionConfigBuilder::default().build().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result.stats.input_files_count, 1);
        assert!(result.stats.output_files_count > 0);
    }

    /// Consolidated commit validation scenarios to avoid repeated init
    #[tokio::test]
    async fn test_commit_validations() {
        use crate::file_selection::FileGroup;

        // Shared environment
        let env = create_test_env().await;

        // Compaction configured for main branch for consistent checks
        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_to_branch(MAIN_BRANCH.to_owned())
            .build();

        // 1) Branch mismatch
        let plan1 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, 1);
        let plan2 = CompactionPlan::new(FileGroup::empty(), "feature-branch", 1);
        let r1 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan1,
            validation_info: None,
        };
        let r2 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan2,
            validation_info: None,
        };
        let err = compaction
            .commit_rewrite_results(vec![r1, r2])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not match configured branch"),
            "Branch mismatch message"
        );

        // 2) Snapshot mismatch (same branch)
        let plan1 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, 1);
        let plan2 = CompactionPlan::new(FileGroup::empty(), MAIN_BRANCH, 2);
        let r1 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan1,
            validation_info: None,
        };
        let r2 = RewriteResult {
            output_data_files: vec![],
            stats: RewriteFilesStat::default(),
            plan: plan2,
            validation_info: None,
        };
        let err = compaction
            .commit_rewrite_results(vec![r1, r2])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("does not match other plans snapshot"),
            "Snapshot mismatch message"
        );

        // 3) Empty results rejection
        let err = compaction
            .commit_rewrite_results(vec![])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("No rewrite results to commit"),
            "Empty results message"
        );
    }

    /// Test branch validation in `rewrite_plan` method
    #[tokio::test]
    async fn test_rewrite_plan_branch_validation() {
        use crate::config::CompactionExecutionConfigBuilder;
        use crate::file_selection::FileGroup;

        // Reuse shared env
        let env = create_test_env().await;

        // Create compaction configured for "main" branch
        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_to_branch("main".to_owned())
            .build();

        // Create a plan for a different branch
        let plan = CompactionPlan::new(FileGroup::empty(), "feature-branch", 1);

        let execution_config = CompactionExecutionConfigBuilder::default().build().unwrap();
        let table = env.catalog.load_table(&env.table_ident).await.unwrap();

        // Test should fail due to branch mismatch
        let rewrite_result = compaction
            .rewrite_plan(plan, &execution_config, &table)
            .await;
        assert!(
            rewrite_result.is_err(),
            "Branch mismatch should cause error"
        );
        let error_msg = rewrite_result.unwrap_err().to_string();
        assert!(
            error_msg.contains("does not match configured branch"),
            "Error should mention branch mismatch, got: {}",
            error_msg
        );
    }

    /// Test `CompactionBuilder` configuration
    #[tokio::test]
    async fn test_compaction_builder() {
        let env = create_test_env().await;

        // Test builder with custom settings
        let custom_registry = Box::new(mixtrics::registry::noop::NoopMetricsRegistry);
        let retry_config = CommitManagerRetryConfig {
            max_retries: 5,
            retry_initial_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
        };

        let compaction = CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default().build().unwrap(),
            ))
            .with_executor_type(ExecutorType::DataFusion)
            .with_catalog_name("test-catalog")
            .with_registry(custom_registry)
            .with_retry_config(retry_config.clone())
            .with_to_branch("custom-branch")
            .build();

        assert_eq!(compaction.to_branch, "custom-branch");
        assert_eq!(compaction.catalog_name, "test-catalog");
        assert_eq!(
            compaction.commit_retry_config.max_retries,
            retry_config.max_retries
        );
        assert!(compaction.config.is_some());
    }

    /// Test metrics are accessible
    #[tokio::test]
    async fn test_compaction_metrics() {
        let env = create_test_env().await;

        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        let metrics = compaction.metrics();
        assert!(
            Arc::ptr_eq(&metrics, &compaction.metrics),
            "Should return same metrics instance"
        );
    }

    /// Test `rewrite_plan` with invalid snapshot
    #[tokio::test]
    async fn test_rewrite_plan_invalid_snapshot() {
        let env = create_test_env().await;

        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        let table = env.catalog.load_table(&env.table_ident).await.unwrap();

        // Create a plan with non-existent snapshot ID
        let invalid_plan = CompactionPlan::new(
            crate::file_selection::FileGroup::empty(),
            MAIN_BRANCH,
            999999, // Non-existent snapshot ID
        );

        let execution_config = CompactionExecutionConfigBuilder::default().build().unwrap();

        let result = compaction
            .rewrite_plan(invalid_plan, &execution_config, &table)
            .await;

        assert!(result.is_err(), "Invalid snapshot should cause error");
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("not found") || error_msg.contains("999999"),
            "Error should mention snapshot issue, got: {}",
            error_msg
        );
    }

    /// Test compact without config should fail
    #[tokio::test]
    async fn test_compact_without_config() {
        let env = create_test_env().await;

        // Create compaction WITHOUT config
        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        assert!(compaction.config.is_none(), "Should not have config");

        let result = compaction.compact().await;

        assert!(result.is_err(), "compact() without config should fail");
        if let Err(e) = result {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("config") || error_msg.contains("required"),
                "Error should mention missing config, got: {}",
                error_msg
            );
        }
    }

    /// Test `plan_compaction` without config should fail
    #[tokio::test]
    async fn test_plan_compaction_without_config() {
        let env = create_test_env().await;

        // Create compaction WITHOUT config
        let compaction =
            CompactionBuilder::new(env.catalog.clone(), env.table_ident.clone()).build();

        let result = compaction.plan_compaction().await;

        assert!(
            result.is_err(),
            "plan_compaction() without config should fail"
        );
    }

    /// Appends data files with custom snapshot properties set on the commit.
    async fn append_and_commit_with_properties<C: Catalog>(
        table: &Table,
        catalog: &C,
        data_files: Vec<DataFile>,
        properties: HashMap<String, String>,
    ) -> Table {
        let transaction = Transaction::new(table);
        let append_action = transaction
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(properties);
        let tx = append_action.apply(transaction).unwrap();
        tx.commit(catalog).await.unwrap()
    }

    /// Custom snapshot metadata from the previous snapshot should get through compaction
    #[tokio::test]
    async fn test_custom_snapshot_metadata_preserved_after_compaction() {
        let env = create_test_env().await;

        let data_files = write_simple_files(&env.table, &env.warehouse_location, "test", 3).await;

        // Append with custom properties that simulate external system metadata
        let mut custom_props = HashMap::new();
        custom_props.insert("pipeline-id".to_owned(), "pipe-42".to_owned());
        custom_props.insert("bobsled.source-table".to_owned(), "events_raw".to_owned());
        custom_props.insert("custom.watermark-ms".to_owned(), "1700000000000".to_owned());

        let updated_table = append_and_commit_with_properties(
            &env.table,
            env.catalog.as_ref(),
            data_files,
            custom_props.clone(),
        )
        .await;

        // Verify custom properties exist on the pre-compaction snapshot
        let snapshot_before = updated_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        let summary_before = &snapshot_before.summary().additional_properties;
        for (key, value) in &custom_props {
            assert_eq!(
                summary_before.get(key).unwrap(),
                value,
                "Custom property '{key}' should be present before compaction"
            );
        }

        // Also record the total-records value before compaction
        let total_records_before = summary_before.get("total-records").cloned();

        // Run compaction
        let compaction = create_default_compaction(env.catalog.clone(), env.table_ident.clone());
        let result = compaction.compact().await.unwrap().unwrap();
        let final_table = result.table.unwrap();

        let snapshot_after = final_table
            .metadata()
            .snapshot_for_ref(MAIN_BRANCH)
            .unwrap();
        let summary_after = &snapshot_after.summary().additional_properties;

        // Custom properties must be preserved
        for (key, value) in &custom_props {
            assert_eq!(
                summary_after.get(key).unwrap(),
                value,
                "Custom property '{key}' must survive compaction"
            );
        }

        // total-records must still be correct (recalculated, not blindly copied)
        assert_eq!(
            summary_after.get("total-records"),
            total_records_before.as_ref(),
            "total-records must be preserved (no data was added or removed)"
        );

        // Snapshot operation should be "replace" for compaction
        assert_eq!(
            snapshot_after.summary().operation,
            iceberg::spec::Operation::Replace,
            "Compaction snapshot operation should be 'replace'"
        );
    }

    /// Verifies that `custom_snapshot_properties` correctly filters known keys.
    #[test]
    fn test_custom_snapshot_properties_filters_known_keys() {
        use iceberg::spec::{Operation, Snapshot, Summary};

        use super::{KNOWN_SNAPSHOT_SUMMARY_KEYS, custom_snapshot_properties};

        let mut all_properties = HashMap::new();

        // Add all known keys
        for key in KNOWN_SNAPSHOT_SUMMARY_KEYS {
            all_properties.insert(key.to_string(), "100".to_owned());
        }

        all_properties.insert(
            "partitions.date=2024-01-01".to_owned(),
            "added-data-files=1".to_owned(),
        );

        all_properties.insert("pipeline-id".to_owned(), "pipe-42".to_owned());
        all_properties.insert("bobsled.source-table".to_owned(), "events_raw".to_owned());

        let summary = Summary {
            operation: Operation::Append,
            additional_properties: all_properties,
        };

        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_timestamp_ms(1000)
            .with_sequence_number(1)
            .with_schema_id(0)
            .with_manifest_list("manifest-list.avro")
            .with_summary(summary)
            .build();

        let custom = custom_snapshot_properties(&snapshot);

        // Only custom keys should remain
        assert_eq!(custom.len(), 2);
        assert_eq!(custom.get("pipeline-id").unwrap(), "pipe-42");
        assert_eq!(custom.get("bobsled.source-table").unwrap(), "events_raw");

        // Known keys must NOT be present
        for key in KNOWN_SNAPSHOT_SUMMARY_KEYS {
            assert!(
                !custom.contains_key(*key),
                "Known key '{key}' must be filtered out"
            );
        }
        assert!(
            !custom.contains_key("partitions.date=2024-01-01"),
            "Partition keys must be filtered out"
        );
    }

    // ------------------------------------------------------------------
    // Managed execution and identity-aware selection
    // ------------------------------------------------------------------

    /// Catalog wrapper that serves reads and refuses every mutation.
    ///
    /// Planning and rewriting must be pure with respect to the catalog: they
    /// read a snapshot and produce candidate outputs, and the decision to
    /// publish belongs to the caller alone. A double that merely *counts*
    /// mutations could still let one through; this one makes any mutation an
    /// immediate, attributable failure.
    #[derive(Debug)]
    struct ReadOnlyCatalog {
        inner: Arc<dyn Catalog>,
    }

    impl ReadOnlyCatalog {
        fn new(inner: Arc<dyn Catalog>) -> Self {
            Self { inner }
        }

        fn refuse<T>(operation: &str) -> iceberg::Result<T> {
            Err(iceberg::Error::new(
                ErrorKind::FeatureUnsupported,
                format!("read-only catalog refused mutation: {operation}"),
            ))
        }
    }

    #[async_trait::async_trait]
    impl Catalog for ReadOnlyCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            Self::refuse("create_namespace")
        }

        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            Self::refuse("update_namespace")
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            Self::refuse("drop_namespace")
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<Table> {
            Self::refuse("create_table")
        }

        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            Self::refuse("drop_table")
        }

        async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            Self::refuse("purge_table")
        }

        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            Self::refuse("rename_table")
        }

        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<Table> {
            Self::refuse("register_table")
        }

        async fn update_table(&self, _commit: iceberg::TableCommit) -> iceberg::Result<Table> {
            Self::refuse("update_table")
        }
    }

    /// Observer that records every event for later assertion.
    #[derive(Debug, Default)]
    struct RecordingRewriteObserver {
        events: std::sync::Mutex<Vec<crate::managed::RewriteEvent>>,
    }

    impl RecordingRewriteObserver {
        fn events(&self) -> Vec<crate::managed::RewriteEvent> {
            self.events.lock().unwrap().clone()
        }

        fn terminal(&self) -> Option<crate::managed::RewriteEvent> {
            self.events()
                .into_iter()
                .find(crate::managed::RewriteEvent::is_terminal)
        }
    }

    impl crate::managed::RewriteObserver for RecordingRewriteObserver {
        fn on_event(&self, event: crate::managed::RewriteEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    /// Writes `count` small data files under a Forge-shaped data location.
    ///
    /// The path shape matters: the identity-aware policy recovers the writer
    /// recipe from `/data/forge/<recipe>/`, so files written anywhere else
    /// would classify as recipe drift and never exercise the size path.
    async fn write_forge_files(
        table: &Table,
        warehouse_location: &str,
        recipe: &str,
        prefix: &str,
        count: usize,
    ) -> Vec<DataFile> {
        let data_location = format!("{warehouse_location}/data/forge/{recipe}");
        let mut all = Vec::new();
        for index in 0..count {
            let location_generator =
                DefaultLocationGenerator::with_data_location(data_location.clone());
            let file_name_generator = DefaultFileNameGenerator::new(
                "data".to_owned(),
                Some(format!("{prefix}_{index}")),
                iceberg::spec::DataFileFormat::Parquet,
            );
            let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
                ParquetWriterBuilder::new(
                    WriterProperties::builder().build(),
                    table.metadata().current_schema().clone(),
                ),
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            );
            // Stamping the current sort order is what a real Wyrd writer does;
            // without it every file would read as sort-order drift.
            let mut writer = DataFileWriterBuilder::new(rolling_writer_builder)
                .sort_order_id(Some(table.metadata().default_sort_order().order_id as i32))
                .build(None)
                .await
                .unwrap();
            writer
                .write(create_test_record_batch(&simple_table_schema()))
                .await
                .unwrap();
            all.extend(writer.close().await.unwrap());
        }
        all
    }

    /// Builds a policy whose "current" identity is the table's own.
    fn forge_policy(
        table: &Table,
        recipe: &str,
        target: u64,
        threshold: u64,
    ) -> crate::managed::WyrdSelectionPolicy {
        crate::managed::WyrdSelectionPolicy {
            schema_id: table.metadata().current_schema_id(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            sort_order_id: table.metadata().default_sort_order().order_id as i32,
            writer_recipe: recipe.to_owned(),
            recipe_resolver: crate::managed::WriterRecipeResolver::forge(),
            target_file_size_bytes: target,
            small_file_threshold_bytes: threshold,
            open_partitions: crate::managed::OpenPartitionPolicy::AllClosed,
            emit_open_partition_tail: false,
            event_time_field_id: None,
        }
    }

    /// Builds a rewrite request straight against the executor.
    ///
    /// Bypassing `rewrite_plan` is deliberate for the failure case: it is the
    /// only way to hand the writer an unusable output location without
    /// corrupting the table under test.
    fn rewrite_request_for(
        table: &Table,
        plan: &CompactionPlan,
        execution_config: Arc<CompactionExecutionConfig>,
        data_location: String,
    ) -> RewriteFilesRequest {
        RewriteFilesRequest {
            file_io: table.file_io().clone(),
            schema: table.metadata().current_schema().clone(),
            file_group: plan.file_group.clone(),
            execution_config,
            partition_spec: table.metadata().default_partition_spec().clone(),
            metrics_recorder: None,
            location_generator: DefaultLocationGenerator::with_data_location(data_location),
            sort_order: None,
            format_version: table.metadata().format_version(),
        }
    }

    #[tokio::test]
    async fn wyrd_selection_report_is_canonical_and_matches_final_plans() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "sel", 4).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;
        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        let planning = CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(forge_policy(
                &table, "v2", 2_000_000, 1_000_000,
            )),
        );
        let planner = CompactionPlanner::new(planning.clone());
        let (plans, report) = planner
            .plan_compaction_with_report(&table, MAIN_BRANCH)
            .await
            .unwrap();

        assert_eq!(
            report.strategy,
            crate::managed::SelectionStrategyKind::WyrdIdentityAware
        );
        assert_eq!(report.base_snapshot_id, snapshot_id);
        assert!(!plans.is_empty(), "four small files must produce a plan");
        for plan in &plans {
            assert_eq!(
                plan.snapshot_id, snapshot_id,
                "every plan is bound to the reported snapshot"
            );
            assert_eq!(plan.to_branch, MAIN_BRANCH);
        }

        // The report is exactly the set of files the plans will rewrite:
        // nothing extra, nothing missing, no duplicates, sorted by identity.
        let mut planned: Vec<String> = plans
            .iter()
            .flat_map(|plan| plan.file_group.data_files.iter())
            .map(|task| task.data_file_path.clone())
            .collect();
        planned.sort();
        let reported: Vec<String> = report
            .selected_paths()
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(reported, planned);
        assert_eq!(
            report.selected.len(),
            plans
                .iter()
                .map(|plan| plan.file_group.data_file_count)
                .sum::<usize>(),
            "report totals equal the plans' totals"
        );

        let unique: std::collections::BTreeSet<&String> = planned.iter().collect();
        assert_eq!(unique.len(), planned.len(), "no identity appears twice");

        // Every file of one plan carries one reason, and that reason is the
        // one the report recorded: a plan is one selection group, not a mix.
        let reason_by_path: HashMap<&str, crate::managed::SelectionReason> = report
            .selected
            .iter()
            .map(|entry| (entry.file_path.as_str(), entry.reason))
            .collect();
        for plan in &plans {
            let mut reasons: std::collections::BTreeSet<crate::managed::SelectionReason> =
                std::collections::BTreeSet::new();
            for task in &plan.file_group.data_files {
                reasons.insert(reason_by_path[task.data_file_path.as_str()]);
            }
            assert_eq!(reasons.len(), 1, "one plan carries exactly one reason");
        }

        // Small current-identity files are packed, not rewritten one by one.
        for entry in &report.selected {
            assert_eq!(entry.reason, crate::managed::SelectionReason::Undersized);
        }

        // The declared policy travels with the report.
        let policy = report.policy.as_ref().unwrap();
        assert_eq!(policy.writer_recipe, "v2");
        assert_eq!(policy.target_file_size_bytes, 2_000_000);
        assert_eq!(policy.max_file_size_bytes, 3_600_000);

        // Canonical means derived from the final plans, not re-derived beside
        // them: an identity the scan did not produce is rewritten by no plan,
        // so the report must not name it either.
        let CompactionPlanningConfig::WyrdIdentityAware(config) = &planning else {
            unreachable!("planning was constructed as identity-aware");
        };
        let selector = IdentityAwareSelector::new(config.policy.clone()).unwrap();
        let index = ManifestIdentityIndex::load(&table, snapshot_id, None)
            .await
            .unwrap();
        let groups = selector.select(index.identities()).unwrap();
        let mut tasks = FileSelector::scan_data_files(&table, snapshot_id)
            .await
            .unwrap();
        let dropped = tasks
            .pop()
            .expect("the snapshot has scan tasks")
            .data_file_path;

        let joined = join_groups_to_tasks(groups, tasks);
        for entry in &joined {
            assert_eq!(
                entry.group.files.len(),
                entry.tasks.len(),
                "a joined group names exactly the files its plan will read"
            );
            assert!(
                !entry
                    .group
                    .files
                    .iter()
                    .any(|file| file.file_path == dropped),
                "an identity with no scan task cannot survive into the report"
            );
        }
    }

    #[tokio::test]
    async fn wyrd_selection_plan_budget_caps_plans_and_report_together() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "budget", 6).await;
        let largest = files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .max()
            .expect("six files were written");
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;

        // Sized so an undersized run flushes after two files: six candidates
        // therefore produce three groups, which is more than the budget under
        // test and is what makes the cap observable at all.
        let threshold = largest + 1;
        let target = largest * 2 + 1;
        let policy = crate::managed::WyrdSelectionPolicy {
            small_file_threshold_bytes: threshold,
            target_file_size_bytes: target,
            ..forge_policy(&table, "v2", target, threshold)
        };

        let unbudgeted = CompactionPlanner::new(CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(policy.clone()),
        ));
        let (all_plans, all_report) = unbudgeted
            .plan_compaction_with_report(&table, MAIN_BRANCH)
            .await
            .unwrap();
        assert!(
            all_plans.len() > 2,
            "the fixture must plan more work than the budget admits, got {}",
            all_plans.len()
        );

        let budgeted = CompactionPlanner::new(CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(policy.clone()).with_max_selection_plans(2),
        ));
        let (plans, report) = budgeted
            .plan_compaction_with_report(&table, MAIN_BRANCH)
            .await
            .unwrap();

        assert_eq!(plans.len(), 2, "the core admits exactly the declared budget");

        // The budget is a selection decision, not a post-hoc trim: the report
        // names the files the surviving plans rewrite and nothing from the
        // groups the budget refused. A caller that trimmed the returned plans
        // instead would still hold this report's pre-cap file set.
        let mut planned: Vec<String> = plans
            .iter()
            .flat_map(|plan| plan.file_group.data_files.iter())
            .map(|task| task.data_file_path.clone())
            .collect();
        planned.sort();
        let reported: Vec<String> = report
            .selected_paths()
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert_eq!(reported, planned);
        assert!(
            report.selected.len() < all_report.selected.len(),
            "a budgeted pass reports strictly less work than an unbudgeted one"
        );
        assert_eq!(
            report.selected.len(),
            plans
                .iter()
                .map(|plan| plan.file_group.data_file_count)
                .sum::<usize>(),
            "report totals equal the budgeted plans' totals"
        );

        // The admitted plans are the selector's own leading groups, so raising
        // the budget only ever adds work to the tail.
        let all_paths: Vec<String> = all_report
            .selected_paths()
            .into_iter()
            .map(str::to_owned)
            .collect();
        for path in &reported {
            assert!(
                all_paths.contains(path),
                "a budgeted selection is drawn from the unbudgeted one"
            );
        }

        // A zero budget names no work at all, so it is refused at the pass that
        // could not be planned rather than reported as an empty selection.
        let refused = CompactionPlanner::new(CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(policy).with_max_selection_plans(0),
        ))
        .plan_compaction_with_report(&table, MAIN_BRANCH)
        .await;
        assert!(
            matches!(refused, Err(CompactionError::Config(_))),
            "a zero plan budget must be refused, got {refused:?}"
        );
    }

    #[tokio::test]
    async fn wyrd_selection_refuses_unknown_or_contradictory_snapshot_evidence() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "evid", 4).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;
        let snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let policy = forge_policy(&table, "v2", 2_000_000, 1_000_000);

        // A snapshot the table does not carry is unknown evidence, not an
        // empty selection: planning it would silently report zero work for a
        // table that may be full of candidates.
        let unknown = ManifestIdentityIndex::load(&table, snapshot_id + 1_000, None).await;
        assert!(
            matches!(unknown, Err(CompactionError::Config(_))),
            "an unknown snapshot must be refused, got {unknown:?}"
        );

        // A branch with no snapshot is the same refusal at the planning seam.
        let planner = CompactionPlanner::new(CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(policy.clone()),
        ));
        let absent_branch = planner
            .plan_compaction_with_report(&table, "no_such_branch")
            .await;
        assert!(
            absent_branch.is_err(),
            "a branch with no snapshot cannot authorise a plan"
        );

        // A report must be bound to a real snapshot. The unassigned sentinel
        // names no snapshot, so a plan carrying it can never be checked
        // against the state that authorised it.
        let one = vec![crate::managed::SelectedFile {
            file_path: "s3://b/data/forge/v2/part-0/a.parquet".to_owned(),
            reason: crate::managed::SelectionReason::Undersized,
        }];
        let unbound = crate::managed::SelectionReport::new(
            crate::managed::SelectionStrategyKind::WyrdIdentityAware,
            UNASSIGNED_SNAPSHOT_ID,
            Some(policy.identity().unwrap()),
            one.clone(),
        );
        assert!(
            unbound.is_err(),
            "a report bound to no snapshot must be refused"
        );

        // One identity, one reason. Two reasons for one file make the
        // persisted reason ambiguous.
        let duplicated = crate::managed::SelectionReport::new(
            crate::managed::SelectionStrategyKind::WyrdIdentityAware,
            snapshot_id,
            Some(policy.identity().unwrap()),
            vec![one[0].clone(), crate::managed::SelectedFile {
                file_path: one[0].file_path.clone(),
                reason: crate::managed::SelectionReason::Oversized,
            }],
        );
        assert!(duplicated.is_err(), "a duplicate identity must be refused");

        // The strategy and its declared identity must agree: the identity-aware
        // policy always records its inputs, and an upstream policy has none to
        // record. Either mismatch makes the report describe a decision that
        // was never taken.
        let missing_policy = crate::managed::SelectionReport::new(
            crate::managed::SelectionStrategyKind::WyrdIdentityAware,
            snapshot_id,
            None,
            one.clone(),
        );
        assert!(
            missing_policy.is_err(),
            "identity-aware selection must record its policy"
        );
        let foreign_policy = crate::managed::SelectionReport::new(
            crate::managed::SelectionStrategyKind::UpstreamSmallFiles,
            snapshot_id,
            Some(policy.identity().unwrap()),
            vec![crate::managed::SelectedFile {
                file_path: one[0].file_path.clone(),
                reason: crate::managed::SelectionReason::UpstreamSmallFiles,
            }],
        );
        assert!(
            foreign_policy.is_err(),
            "an upstream strategy has no identity policy to declare"
        );

        // A reason must belong to the strategy that recorded it.
        let foreign_reason = crate::managed::SelectionReport::new(
            crate::managed::SelectionStrategyKind::WyrdIdentityAware,
            snapshot_id,
            Some(policy.identity().unwrap()),
            vec![crate::managed::SelectedFile {
                file_path: one[0].file_path.clone(),
                reason: crate::managed::SelectionReason::UpstreamFull,
            }],
        );
        assert!(
            foreign_reason.is_err(),
            "an upstream reason cannot appear under identity-aware selection"
        );

        // An unknown wire spelling is rejected on read rather than reinterpreted.
        assert!(crate::managed::SelectionReason::parse("NotAReason").is_err());
        assert_eq!(
            crate::managed::SelectionReason::parse("ObsoleteSchema").unwrap(),
            crate::managed::SelectionReason::ObsoleteSchema
        );

        // The accepted report is the one the production path builds.
        let (plans, report) = planner
            .plan_compaction_with_report(&table, MAIN_BRANCH)
            .await
            .unwrap();
        assert_eq!(report.base_snapshot_id, snapshot_id);
        for plan in &plans {
            assert_eq!(plan.snapshot_id, report.base_snapshot_id);
        }
    }

    #[tokio::test]
    async fn managed_plan_and_rewrite_never_mutate_catalog() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "ro", 4).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;

        let read_only: Arc<dyn Catalog> =
            Arc::new(ReadOnlyCatalog::new(env.catalog.clone() as Arc<dyn Catalog>));
        let observer = Arc::new(RecordingRewriteObserver::default());
        let context = crate::managed::ManagedExecutionContext::builder()
            .with_observer(observer.clone())
            .build()
            .unwrap();

        let compaction = CompactionBuilder::new(read_only, env.table_ident.clone())
            .with_config(Arc::new(
                CompactionConfigBuilder::default()
                    .planning(CompactionPlanningConfig::WyrdIdentityAware(
                        crate::config::WyrdIdentityAwareConfig::new(forge_policy(
                            &table, "v2", 2_000_000, 1_000_000,
                        )),
                    ))
                    .build()
                    .unwrap(),
            ))
            .with_executor(Box::new(DataFusionExecutor::with_context(context)))
            .build();

        // Planning reads only.
        let (plans, report) = compaction.plan_compaction_with_report().await.unwrap();
        assert!(!plans.is_empty());
        assert!(!report.selected.is_empty());

        // Rewriting reads and writes objects, but never touches the catalog.
        let execution_config =
            Arc::new(CompactionExecutionConfigBuilder::default().build().unwrap());
        let result = compaction
            .rewrite_plan(plans[0].clone(), &execution_config, &table)
            .await
            .unwrap();
        assert!(!result.output_data_files.is_empty());

        // The catalog's own view is untouched: same snapshot, same files.
        let reloaded = env.catalog.load_table(&env.table_ident).await.unwrap();
        assert_eq!(
            reloaded
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id(),
            table.metadata().current_snapshot().unwrap().snapshot_id()
        );

        // And the refusal is real, not merely unexercised.
        let refused = ReadOnlyCatalog::new(env.catalog.clone() as Arc<dyn Catalog>)
            .drop_table(&env.table_ident)
            .await;
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn managed_rewrite_keeps_plan_result_and_output_compatibility() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "compat", 4).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;

        let planning = CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(forge_policy(
                &table, "v2", 2_000_000, 1_000_000,
            )),
        );
        let config = Arc::new(
            CompactionConfigBuilder::default()
                .planning(planning)
                .build()
                .unwrap(),
        );
        let execution_config =
            Arc::new(CompactionExecutionConfigBuilder::default().build().unwrap());

        // Unmanaged: the historical construction path, unchanged.
        let unmanaged = CompactionBuilder::new(
            env.catalog.clone() as Arc<dyn Catalog>,
            env.table_ident.clone(),
        )
        .with_config(config.clone())
        .build();
        let unmanaged_plans = unmanaged.plan_compaction().await.unwrap();
        let unmanaged_result = unmanaged
            .rewrite_plan(unmanaged_plans[0].clone(), &execution_config, &table)
            .await
            .unwrap();

        // Managed: same plans, same result shape, same rows.
        let observer = Arc::new(RecordingRewriteObserver::default());
        let context = crate::managed::ManagedExecutionContext::builder()
            .with_observer(observer.clone())
            .build()
            .unwrap();
        let managed = CompactionBuilder::new(
            env.catalog.clone() as Arc<dyn Catalog>,
            env.table_ident.clone(),
        )
        .with_config(config)
        .with_executor(Box::new(DataFusionExecutor::with_context(context)))
        .build();
        let managed_plans = managed.plan_compaction().await.unwrap();
        let managed_result = managed
            .rewrite_plan(managed_plans[0].clone(), &execution_config, &table)
            .await
            .unwrap();

        assert_eq!(managed_plans.len(), unmanaged_plans.len());
        assert_eq!(
            managed_plans[0].file_count(),
            unmanaged_plans[0].file_count()
        );
        assert_eq!(
            managed_plans[0].total_bytes(),
            unmanaged_plans[0].total_bytes()
        );
        assert_eq!(
            managed_result.stats.input_files_count,
            unmanaged_result.stats.input_files_count
        );
        assert_eq!(
            managed_result.stats.output_files_count,
            unmanaged_result.stats.output_files_count
        );

        let managed_rows: u64 = managed_result
            .output_data_files
            .iter()
            .map(iceberg::spec::DataFile::record_count)
            .sum();
        let unmanaged_rows: u64 = unmanaged_result
            .output_data_files
            .iter()
            .map(iceberg::spec::DataFile::record_count)
            .sum();
        assert_eq!(managed_rows, unmanaged_rows);
        assert!(managed_rows > 0);

        // Observation is additive, never a substitute for the returned result.
        assert!(matches!(
            observer.terminal(),
            Some(crate::managed::RewriteEvent::Succeeded { .. })
        ));
    }

    #[tokio::test]
    async fn wyrd_execution_uses_only_caller_admitted_resources() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "iso", 6).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;

        let observer = Arc::new(RecordingRewriteObserver::default());
        let spill_dir = TempDir::new().unwrap();
        let lease = crate::managed::SpillLease::new(spill_dir.path().to_path_buf()).unwrap();
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = Arc::new(
            datafusion::execution::memory_pool::FairSpillPool::new(64 * 1024 * 1024),
        );
        let context = crate::managed::ManagedExecutionContext::builder()
            .with_memory_pool(pool, Some(64 * 1024 * 1024))
            .with_spill_lease(lease.clone())
            .with_observer(observer.clone())
            .build()
            .unwrap();

        // The leased runtime is the one the executor runs on, and the leased
        // scratch root is the only place it may spill.
        let runtime = context.runtime_env();
        assert!(Arc::ptr_eq(&runtime, &context.runtime_env()));
        assert_eq!(context.pool_capacity_bytes(), Some(64 * 1024 * 1024));
        assert_eq!(context.peak_memory_bytes(), 0, "nothing reserved yet");
        assert_eq!(context.spill().unwrap().root(), spill_dir.path());

        let executor = DataFusionExecutor::with_context(Arc::clone(&context));
        assert!(
            Arc::ptr_eq(&executor.context().unwrap().runtime_env(), &runtime),
            "the executor runs on the caller's runtime, not one it built"
        );
        let compaction = CompactionBuilder::new(
            env.catalog.clone() as Arc<dyn Catalog>,
            env.table_ident.clone(),
        )
        .with_config(Arc::new(
            CompactionConfigBuilder::default()
                .planning(CompactionPlanningConfig::WyrdIdentityAware(
                    crate::config::WyrdIdentityAwareConfig::new(forge_policy(
                        &table, "v2", 2_000_000, 1_000_000,
                    )),
                ))
                .build()
                .unwrap(),
        ))
        .with_executor(Box::new(executor))
        .build();

        let plans = compaction.plan_compaction().await.unwrap();
        assert!(!plans.is_empty());
        let execution_config =
            Arc::new(CompactionExecutionConfigBuilder::default().build().unwrap());

        // Two rewrites over the same leased runtime. Isolated sessions mean
        // both can register their tables under the same names; a shared
        // session would collide or cross-contaminate. Running the same plan
        // twice is the sharpest form of that collision.
        for _ in 0..2 {
            let result = compaction
                .rewrite_plan(plans[0].clone(), &execution_config, &table)
                .await
                .unwrap();
            assert!(!result.output_data_files.is_empty());
        }

        // The leased pool actually served the work, and its peak is reported
        // against the capacity the caller admitted.
        assert!(
            context.peak_memory_bytes() > 0,
            "the injected pool must be the one that served the reservations"
        );
        let peaks: Vec<(usize, Option<usize>)> = observer
            .events()
            .into_iter()
            .filter_map(|event| match event {
                crate::managed::RewriteEvent::PeakMemory {
                    peak_bytes,
                    pool_capacity_bytes,
                    ..
                } => Some((peak_bytes, pool_capacity_bytes)),
                _ => None,
            })
            .collect();
        assert_eq!(peaks.len(), 2, "one peak report per completed attempt");
        for (peak, capacity) in peaks {
            assert!(peak > 0);
            assert_eq!(capacity, Some(64 * 1024 * 1024));
            assert!(peak <= 64 * 1024 * 1024, "the admitted bound was honoured");
        }

        // A caller that has already withdrawn its admission gets a refusal,
        // not a partially executed attempt: nothing is read, nothing is
        // opened, and there is no resource usage to account for.
        let refused_observer = Arc::new(RecordingRewriteObserver::default());
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let refused_context = crate::managed::ManagedExecutionContext::builder()
            .with_cancellation(token)
            .with_spill_lease(lease)
            .with_observer(refused_observer.clone())
            .build()
            .unwrap();
        let refusing = DataFusionExecutor::with_context(Arc::clone(&refused_context));
        let refused = refusing
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[0],
                execution_config,
                format!("{}/data/forge/v9", env.warehouse_location),
            ))
            .await;
        let Err(CompactionError::Cancelled {
            attempt_id,
            outputs,
        }) = refused
        else {
            panic!("a withdrawn admission must refuse, got {refused:?}");
        };
        assert_eq!(attempt_id, refused_context.attempt_id().to_string());
        assert!(outputs.is_empty(), "a refused attempt opened nothing");
        let refused_events = refused_observer.events();
        assert_eq!(
            refused_events.len(),
            1,
            "a refusal before IO reports only its terminal, got {refused_events:?}"
        );
        assert!(refused_events[0].is_terminal());
    }

    /// Observer that withdraws the caller's admission the first time an output
    /// is opened *after the test arms it*.
    ///
    /// Cancelling from inside the observer is the caller acting on its own
    /// token, not the observer steering the core: the token belongs to the
    /// attempt's context, and the core's decisions are unchanged by the
    /// observation itself. It is used here because it is the only way to
    /// cancel at a known point in the writer's life — after work has started
    /// and before it can finish — without a timing race.
    ///
    /// Arming is what lets one attempt complete a plan before the cancel
    /// lands: an attempt that executes several plans must be observable at the
    /// exact moment a *later* plan has opened an object while an earlier
    /// plan's objects are already settled.
    #[derive(Debug)]
    struct CancelOnFirstOutput {
        events: std::sync::Mutex<Vec<crate::managed::RewriteEvent>>,
        token: tokio_util::sync::CancellationToken,
        armed: std::sync::atomic::AtomicBool,
    }

    impl CancelOnFirstOutput {
        /// Creates a disarmed observer bound to the attempt's cancellation
        /// token; it records events but cancels nothing until armed.
        fn disarmed(token: tokio_util::sync::CancellationToken) -> Self {
            Self {
                events: std::sync::Mutex::new(Vec::new()),
                token,
                armed: std::sync::atomic::AtomicBool::new(false),
            }
        }

        /// Arms the observer so the next opened output cancels the attempt.
        fn arm(&self) {
            self.armed
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        /// Returns the events recorded so far, in emission order.
        fn events(&self) -> Vec<crate::managed::RewriteEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl crate::managed::RewriteObserver for CancelOnFirstOutput {
        fn on_event(&self, event: crate::managed::RewriteEvent) {
            let first_open = matches!(event, crate::managed::RewriteEvent::OutputOpened { .. })
                && self.armed.load(std::sync::atomic::Ordering::SeqCst)
                && !self.token.is_cancelled();
            self.events.lock().unwrap().push(event);
            if first_open {
                self.token.cancel();
            }
        }
    }

    #[tokio::test]
    async fn wyrd_cancellation_drains_writers_and_reports_possible_outputs() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "cancel", 6).await;
        let largest = files
            .iter()
            .map(|file| file.file_size_in_bytes())
            .max()
            .expect("six files were written");
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;

        let token = tokio_util::sync::CancellationToken::new();
        // Disarmed: the attempt must complete one plan before the cancel lands,
        // so the reported possible outputs can be checked for the union of an
        // earlier plan's objects and the cancelled plan's own.
        let observer = Arc::new(CancelOnFirstOutput::disarmed(token.clone()));
        let context = crate::managed::ManagedExecutionContext::builder()
            .with_cancellation(token)
            .with_observer(observer.clone())
            .build()
            .unwrap();

        // Sized so an undersized run flushes after two files: the attempt gets
        // more than one plan, which is the only way a per-call ledger and an
        // attempt-owned one can be told apart.
        let threshold = largest + 1;
        let target = largest * 2 + 1;
        let planner = CompactionPlanner::new(CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(crate::managed::WyrdSelectionPolicy {
                small_file_threshold_bytes: threshold,
                target_file_size_bytes: target,
                ..forge_policy(&table, "v2", target, threshold)
            }),
        ));
        let plans = planner
            .plan_compaction_with_branch(&table, MAIN_BRANCH)
            .await
            .unwrap();
        assert!(
            plans.len() >= 2,
            "one attempt must hold at least two plans, got {}",
            plans.len()
        );

        // A tiny target rolls a new output almost every batch, so the attempt
        // is guaranteed to have opened an object before the cancel lands.
        let execution_config = Arc::new(
            CompactionExecutionConfigBuilder::default()
                .target_file_size_bytes(1_u64)
                .build()
                .unwrap(),
        );
        let executor = DataFusionExecutor::with_context(Arc::clone(&context));

        // First plan runs to completion under the same attempt. Its objects are
        // real files in storage, so the attempt still owes the caller their
        // identities if a later plan is withdrawn.
        executor
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[0],
                Arc::clone(&execution_config),
                format!("{}/data/forge/v8", env.warehouse_location),
            ))
            .await
            .expect("the first plan of the attempt completes");
        let settled_before: std::collections::BTreeSet<u64> = context
            .ledger()
            .outputs()
            .iter()
            .map(|output| output.logical_ordinal)
            .collect();
        assert!(
            !settled_before.is_empty(),
            "the completed plan left objects the attempt owns"
        );

        observer.arm();
        let cancelled = executor
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[1],
                execution_config,
                format!("{}/data/forge/v8", env.warehouse_location),
            ))
            .await;

        let Err(CompactionError::Cancelled {
            attempt_id,
            outputs,
        }) = cancelled
        else {
            panic!("a cancelled attempt must not report success, got {cancelled:?}");
        };
        assert_eq!(attempt_id, context.attempt_id().to_string());
        assert!(
            !outputs.is_empty(),
            "an attempt cancelled after it opened an object must report it"
        );

        // The reported set is the attempt's, not the cancelled call's: the
        // earlier plan's objects are still named, and the ordinals never
        // restarted at zero for the second plan.
        let reported_all: std::collections::BTreeSet<u64> = outputs
            .iter()
            .map(|output| output.logical_ordinal)
            .collect();
        assert!(
            settled_before.is_subset(&reported_all),
            "a completed plan's objects survive into the cancelled attempt's report"
        );
        assert!(
            reported_all.len() > settled_before.len(),
            "the cancelled plan's own objects are reported alongside them"
        );
        let highest_before = *settled_before.last().expect("checked non-empty above");
        assert!(
            reported_all
                .iter()
                .any(|ordinal| *ordinal > highest_before),
            "a later plan draws fresh ordinals rather than reusing the first plan's"
        );

        let events = observer.events();

        // Every object the attempt opened is reported as a possible output.
        // Losing one would leave an object in storage that the caller never
        // learns about and therefore never reclaims.
        let opened: std::collections::BTreeSet<u64> = events
            .iter()
            .filter_map(|event| match event {
                crate::managed::RewriteEvent::OutputOpened {
                    logical_ordinal, ..
                } => Some(*logical_ordinal),
                _ => None,
            })
            .collect();
        let reported: std::collections::BTreeSet<u64> = outputs
            .iter()
            .map(|output| output.logical_ordinal)
            .collect();
        assert!(!opened.is_empty());
        assert_eq!(
            reported, opened,
            "every output the attempt opened, across both plans, is reported"
        );
        assert_eq!(
            reported.len(),
            outputs.len(),
            "no ordinal is reported twice"
        );
        for output in &outputs {
            assert!(!output.path.is_empty());
        }

        // Drained, not detached: every writer that opened an object was closed
        // before the attempt returned, so no task is still writing behind us.
        let closed: std::collections::BTreeSet<u64> = events
            .iter()
            .filter_map(|event| match event {
                crate::managed::RewriteEvent::OutputClosed {
                    logical_ordinal, ..
                } => Some(*logical_ordinal),
                _ => None,
            })
            .collect();
        assert_eq!(closed, opened, "every opened writer output was closed");

        // Exactly one terminal event, and it names the same outputs the error
        // carries.
        let terminals: Vec<crate::managed::RewriteEvent> = events
            .into_iter()
            .filter(crate::managed::RewriteEvent::is_terminal)
            .collect();
        assert_eq!(
            terminals.len(),
            2,
            "one terminal event per plan: the completed one and the cancelled one"
        );
        assert!(
            matches!(terminals[0], crate::managed::RewriteEvent::Succeeded { .. }),
            "the first plan reported success, got {:?}",
            terminals[0]
        );
        let crate::managed::RewriteEvent::Cancelled {
            outputs: terminal_outputs,
            ..
        } = &terminals[1]
        else {
            panic!(
                "expected a cancellation terminal event, got {:?}",
                terminals[1]
            );
        };
        assert_eq!(terminal_outputs, &outputs);
    }

    /// Names one event variant.
    ///
    /// The match is exhaustive and wildcard-free on purpose: the event set is
    /// closed, so adding a variant must break this function and force the
    /// caller to decide what the new effect means rather than silently
    /// dropping it.
    fn event_name(event: &crate::managed::RewriteEvent) -> &'static str {
        use crate::managed::RewriteEvent as E;
        match event {
            E::OutputOpened { .. } => "OutputOpened",
            E::RollDecided { .. } => "RollDecided",
            E::OutputClosed { .. } => "OutputClosed",
            E::PeakMemory { .. } => "PeakMemory",
            E::OperatorSpill { .. } => "OperatorSpill",
            E::ScratchSpill { .. } => "ScratchSpill",
            E::Succeeded { .. } => "Succeeded",
            E::Failed { .. } => "Failed",
            E::Cancelled { .. } => "Cancelled",
        }
    }

    #[tokio::test]
    async fn wyrd_observer_is_closed_non_semantic_and_balanced() {
        let env = create_test_env().await;
        let files = write_forge_files(&env.table, &env.warehouse_location, "v2", "term", 4).await;
        let table = append_and_commit(&env.table, env.catalog.as_ref(), files).await;

        let planning = CompactionPlanningConfig::WyrdIdentityAware(
            crate::config::WyrdIdentityAwareConfig::new(forge_policy(
                &table, "v2", 2_000_000, 1_000_000,
            )),
        );
        let config = Arc::new(
            CompactionConfigBuilder::default()
                .planning(planning)
                .build()
                .unwrap(),
        );
        let execution_config =
            Arc::new(CompactionExecutionConfigBuilder::default().build().unwrap());

        let planner = CompactionPlanner::new(config.planning.clone());
        let plans = planner
            .plan_compaction_with_branch(&table, MAIN_BRANCH)
            .await
            .unwrap();
        assert!(!plans.is_empty());

        // Success, with resource accounting.
        let spill_dir = TempDir::new().unwrap();
        let success_observer = Arc::new(RecordingRewriteObserver::default());
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = Arc::new(
            datafusion::execution::memory_pool::FairSpillPool::new(64 * 1024 * 1024),
        );
        let success_context = crate::managed::ManagedExecutionContext::builder()
            .with_memory_pool(pool, Some(64 * 1024 * 1024))
            .with_spill_lease(
                crate::managed::SpillLease::new(spill_dir.path().to_path_buf()).unwrap(),
            )
            .with_observer(success_observer.clone())
            .build()
            .unwrap();
        let executor = DataFusionExecutor::with_context(Arc::clone(&success_context));
        let response = executor
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[0],
                execution_config.clone(),
                format!("{}/data/forge/v3", env.warehouse_location),
            ))
            .await
            .unwrap();
        assert!(!response.data_files.is_empty());

        let success_events = success_observer.events();
        assert!(
            success_events
                .iter()
                .any(|event| matches!(event, crate::managed::RewriteEvent::PeakMemory { peak_bytes, .. } if *peak_bytes > 0)),
            "a completed rewrite reports a non-zero peak against the leased pool"
        );
        assert!(
            success_events
                .iter()
                .any(|event| matches!(event, crate::managed::RewriteEvent::ScratchSpill { .. })),
            "a leased scratch root is always accounted for"
        );

        // Balanced: every output the attempt opened reached a close, and the
        // terminal event accounts for all of them as settled. A leaked active
        // output would mean an object nobody closed and nobody reported.
        let opened: std::collections::BTreeSet<u64> = success_events
            .iter()
            .filter_map(|event| match event {
                crate::managed::RewriteEvent::OutputOpened {
                    logical_ordinal, ..
                } => Some(*logical_ordinal),
                _ => None,
            })
            .collect();
        let closed: std::collections::BTreeSet<u64> = success_events
            .iter()
            .filter_map(|event| match event {
                crate::managed::RewriteEvent::OutputClosed {
                    logical_ordinal, ..
                } => Some(*logical_ordinal),
                _ => None,
            })
            .collect();
        assert!(!opened.is_empty(), "every output open is reported");
        assert_eq!(closed, opened, "no output is left active");

        let terminals: Vec<&crate::managed::RewriteEvent> = success_events
            .iter()
            .filter(|event| event.is_terminal())
            .collect();
        assert_eq!(terminals.len(), 1, "exactly one terminal per rewrite call");
        let crate::managed::RewriteEvent::Succeeded {
            outputs,
            output_bytes: first_bytes,
            ..
        } = terminals[0]
        else {
            panic!("expected a success terminal, got {:?}", terminals[0]);
        };
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.logical_ordinal)
                .collect::<std::collections::BTreeSet<u64>>(),
            opened,
            "the terminal accounts for every output the attempt opened"
        );
        assert!(
            outputs.iter().all(|output| output.settled),
            "a successful attempt settles every output it opened"
        );

        // The event set is closed: every recorded event names one known
        // variant, and the naming itself is wildcard-free.
        for event in &success_events {
            assert!(!event_name(event).is_empty());
        }

        // A terminal event ends a plan, not the attempt. Rewriting again under
        // the same context must neither reset the attempt's accumulated
        // evidence nor change what the executor decides to do: the second
        // terminal names the first plan's objects as well as its own, and the
        // rewrite itself produces exactly what it produced before.
        let first_outputs = outputs.clone();
        let first_bytes = *first_bytes;
        let repeated = executor
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[0],
                execution_config.clone(),
                format!("{}/data/forge/v9", env.warehouse_location),
            ))
            .await
            .unwrap();
        assert_eq!(
            repeated.data_files.len(),
            response.data_files.len(),
            "a repeated terminal does not change the executor's decisions"
        );
        assert_eq!(
            repeated.stats.input_files_count,
            response.stats.input_files_count
        );

        let repeated_events = success_observer.events();
        let repeated_terminals: Vec<&crate::managed::RewriteEvent> = repeated_events
            .iter()
            .filter(|event| event.is_terminal())
            .collect();
        assert_eq!(
            repeated_terminals.len(),
            2,
            "one terminal per rewrite, two rewrites"
        );
        let crate::managed::RewriteEvent::Succeeded {
            outputs: second_outputs,
            output_bytes: second_bytes,
            ..
        } = repeated_terminals[1]
        else {
            panic!(
                "expected a second success terminal, got {:?}",
                repeated_terminals[1]
            );
        };
        for output in &first_outputs {
            assert!(
                second_outputs.contains(output),
                "the second terminal still names the first plan's objects"
            );
        }
        assert!(
            second_outputs.len() > first_outputs.len(),
            "the second terminal adds its own objects to the attempt's set"
        );
        let second_ordinals: Vec<u64> = second_outputs
            .iter()
            .map(|output| output.logical_ordinal)
            .collect();
        assert!(
            second_ordinals.windows(2).all(|pair| pair[0] < pair[1]),
            "the attempt numbers every object in one strictly increasing space"
        );
        assert!(
            second_outputs.iter().all(|output| output.settled),
            "two completed plans settle every object between them"
        );
        assert!(
            *second_bytes > first_bytes,
            "reported bytes accumulate across the attempt rather than restarting"
        );

        // Non-semantic: attaching the observation seam changes nothing the
        // caller can act on. The same plan, executed unobserved, produces the
        // same objects and the same rows.
        let silent = DataFusionExecutor::new()
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[0],
                execution_config.clone(),
                format!("{}/data/forge/v6", env.warehouse_location),
            ))
            .await
            .unwrap();
        assert_eq!(silent.data_files.len(), response.data_files.len());
        assert_eq!(
            silent.stats.output_files_count,
            response.stats.output_files_count
        );
        assert_eq!(
            silent.stats.input_files_count,
            response.stats.input_files_count
        );
        let observed_rows: u64 = response
            .data_files
            .iter()
            .map(iceberg::spec::DataFile::record_count)
            .sum();
        let silent_rows: u64 = silent
            .data_files
            .iter()
            .map(iceberg::spec::DataFile::record_count)
            .sum();
        assert_eq!(observed_rows, silent_rows);
        assert!(observed_rows > 0);

        // Failure: a projection the input files cannot satisfy fails the
        // rewrite, and the attempt still reports exactly one terminal event.
        let failure_observer = Arc::new(RecordingRewriteObserver::default());
        let failure_context = crate::managed::ManagedExecutionContext::builder()
            .with_observer(failure_observer.clone())
            .build()
            .unwrap();
        let failing = DataFusionExecutor::with_context(failure_context);
        let mut failing_request = rewrite_request_for(
            &table,
            &plans[0],
            execution_config.clone(),
            format!("{}/data/forge/v5", env.warehouse_location),
        );
        failing_request.schema = Arc::new(
            Schema::builder()
                .with_schema_id(99)
                .with_fields(vec![
                    NestedField::required(99, "absent_column", Type::Primitive(PrimitiveType::Int))
                        .into(),
                ])
                .build()
                .unwrap(),
        );
        let failure = failing.rewrite_files(failing_request).await;
        assert!(
            failure.is_err(),
            "a rewrite whose projection the inputs cannot satisfy must fail"
        );
        assert_eq!(
            failure_observer
                .events()
                .iter()
                .filter(|event| event.is_terminal())
                .count(),
            1,
            "a failed attempt reports exactly one terminal"
        );
        assert!(matches!(
            failure_observer.terminal(),
            Some(crate::managed::RewriteEvent::Failed { .. })
        ));

        // Cancellation: the third and final terminal outcome.
        let cancel_observer = Arc::new(RecordingRewriteObserver::default());
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        let cancel_context = crate::managed::ManagedExecutionContext::builder()
            .with_cancellation(token)
            .with_observer(cancel_observer.clone())
            .build()
            .unwrap();
        let cancelling = DataFusionExecutor::with_context(cancel_context);
        let cancelled = cancelling
            .rewrite_files(rewrite_request_for(
                &table,
                &plans[0],
                execution_config,
                format!("{}/data/forge/v4", env.warehouse_location),
            ))
            .await;
        assert!(matches!(cancelled, Err(CompactionError::Cancelled { .. })));
        assert_eq!(
            cancel_observer
                .events()
                .iter()
                .filter(|event| event.is_terminal())
                .count(),
            1,
            "a cancelled attempt reports exactly one terminal"
        );
        assert!(matches!(
            cancel_observer.terminal(),
            Some(crate::managed::RewriteEvent::Cancelled { .. })
        ));
    }
}
