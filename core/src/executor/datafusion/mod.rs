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

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::StreamExt;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
};
use iceberg::spec::{DataFile, FormatVersion, PartitionSpec, Schema};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::{
    RollingFileWriterBuilder, RollingWriterObserver,
};
use iceberg::writer::{IcebergWriter, TaskWriter};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::{CompactionExecutor, RewriteFilesStat};
use crate::CompactionError;
use crate::config::CompactionExecutionConfig;
use crate::error::Result;
use crate::managed::bridge::AttemptLedger;
use crate::managed::context::ManagedExecutionContext;
use crate::managed::observer::RewriteEvent;
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;
pub mod iceberg_partition_expr;

/// DataFusion-backed rewrite executor.
///
/// Without a managed context every rewrite shares one executor-wide runtime and
/// spawns writer tasks that report nothing. With a context installed the
/// executor is bound to exactly one publication attempt — sharing that
/// attempt's leased runtime, refusing before any IO once that attempt's
/// admission is withdrawn, and reporting every physical object it produced.
#[derive(Default)]
pub struct DataFusionExecutor {
    /// Caller-owned publication attempt this executor is bound to, if managed.
    context: Option<Arc<ManagedExecutionContext>>,
    /// Runtime (bounded `FairSpillPool` + `DiskManager`) built once and shared
    /// across every `rewrite_files` call on this executor. igloo runs all the
    /// concurrent plans of one invocation through a single `Compaction`, which
    /// holds a single `DataFusionExecutor`, so caching the runtime here makes
    /// `max_memory_bytes` a *pod-wide* ceiling instead of a per-plan one:
    /// two concurrent unsorted plans would otherwise hold two independent
    /// `max_memory_bytes` pools and blow the pod's memory limit (F6 OOM).
    ///
    /// `OnceLock` so every call after the first successful build is a lock-free
    /// read. Lazily initialized from the first request whose `execution_config`
    /// sets a budget; all plans in an invocation share the same config, so the
    /// first config governs the shared pool for that invocation. `None`
    /// (unbudgeted) requests keep the previous per-call, unbounded behavior.
    ///
    /// Only the success case is cached here: `build_spilling_runtime_env` is
    /// fallible (e.g. it can fail to create `spill_dir`), and `OnceLock`'s
    /// initializer must be infallible, so an `Err` is never stored — it's
    /// returned to that caller and retried on the next call, same as before
    /// this was split out of the `build_lock`-guarded slow path below.
    shared_runtime: OnceLock<Arc<RuntimeEnv>>,
    /// Serializes concurrent first-time builds so only one `RuntimeEnv` is ever
    /// constructed even when multiple plans race on the very first call —
    /// otherwise two racing builds could each pass the `shared_runtime.get()`
    /// check, transiently defeating the single-pool guarantee this cache exists
    /// for. Held only across the synchronous build, never across an `.await`.
    build_lock: Mutex<()>,
}

impl std::fmt::Debug for DataFusionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFusionExecutor").finish_non_exhaustive()
    }
}

impl DataFusionExecutor {
    /// Returns the shared bounded runtime for this executor, building it once on
    /// first use, or `None` when no memory budget is configured (unbounded,
    /// per-call behavior preserved).
    ///
    /// Sync; the fast path (already built) never locks. The slow path (first
    /// build) briefly holds `build_lock`, dropped before the caller `.await`s,
    /// so it never crosses an await point.
    fn shared_runtime_env(
        &self,
        execution_config: &CompactionExecutionConfig,
    ) -> Result<Option<Arc<RuntimeEnv>>> {
        let Some(max_memory_bytes) = execution_config.max_memory_bytes.filter(|n| *n > 0) else {
            return Ok(None);
        };

        if let Some(runtime_env) = self.shared_runtime.get() {
            return Ok(Some(runtime_env.clone()));
        }

        let _guard = self.build_lock.lock().map_err(|e| {
            CompactionError::Unexpected(format!("shared runtime build lock poisoned: {e}"))
        })?;
        // Someone else may have finished building while we waited for the lock.
        if let Some(runtime_env) = self.shared_runtime.get() {
            return Ok(Some(runtime_env.clone()));
        }
        let runtime_env = datafusion_processor::build_spilling_runtime_env(
            max_memory_bytes,
            execution_config.spill_dir.as_deref(),
        )?;
        // Can only fail if another thread set it between our check and here,
        // which `build_lock` rules out.
        let _ = self.shared_runtime.set(runtime_env.clone());
        Ok(Some(runtime_env))
    }

    /// Creates an unmanaged executor.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds the executor to one caller-owned publication attempt.
    ///
    /// The returned executor must not outlive that attempt: its runtime lease,
    /// scratch lease, and cancellation token all belong to the attempt, and
    /// reusing it for a second attempt would attribute the second attempt's
    /// outputs to the first attempt's identity.
    #[must_use]
    pub fn with_context(context: Arc<ManagedExecutionContext>) -> Self {
        Self {
            context: Some(context),
            ..Self::default()
        }
    }

    /// Returns the bound attempt context, when the executor is managed.
    #[must_use]
    pub fn context(&self) -> Option<&Arc<ManagedExecutionContext>> {
        self.context.as_ref()
    }
}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        // Refuse before any IO. A caller that has already withdrawn its
        // admission gets nothing read, nothing planned, and nothing opened, so
        // there are no possible outputs to reconcile and no resource usage to
        // account for. Discovering the withdrawal after the scan would leave
        // the caller holding evidence about work it never authorised.
        // The reported outputs are the attempt's, not this call's: a second plan
        // refused before IO must still surface the objects an earlier plan in
        // the same attempt already opened, or the caller would reclaim nothing.
        if let Some(context) = self.context.as_ref()
            && context.is_cancelled()
        {
            let attempt_id = context.attempt_id();
            let outputs = context.ledger().outputs();
            context.observer().on_event(RewriteEvent::Cancelled {
                attempt_id,
                outputs: outputs.clone(),
            });
            return Err(CompactionError::Cancelled {
                attempt_id: attempt_id.to_string(),
                outputs,
            });
        }

        let RewriteFilesRequest {
            file_io,
            schema,
            file_group,
            execution_config,
            partition_spec,
            metrics_recorder,
            location_generator,
            sort_order,
            format_version,
        } = request;
        let mut stats = RewriteFilesStat::default();
        stats.record_input(&file_group);
        let sort_order_id = sort_order.clone().map(|sort_order| sort_order.id as i32);

        // Extract parallelism before file_group is moved
        let executor_parallelism = file_group.executor_parallelism;
        let output_parallelism = file_group.output_parallelism;

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema.clone())
            .with_format_version(format_version)
            .with_input_data_files(file_group)
            .with_sort_order(sort_order.clone())
            .with_partition_spec(partition_spec.clone())
            .build()?;
        // A managed attempt's leased runtime wins: it already encodes the
        // caller's budget and scratch lease. Otherwise every rewrite on this
        // executor shares one pod-wide runtime.
        let runtime = match self.context.as_ref() {
            Some(context) => Some(context.runtime_env()),
            None => self.shared_runtime_env(&execution_config)?,
        };
        let (batches, input_schema) = DatafusionProcessor::new(
            execution_config.clone(),
            executor_parallelism,
            file_io.clone(),
            runtime,
        )?
        .execute(datafusion_task_ctx, output_parallelism)
        .await?;
        let arc_input_schema = Arc::new(input_schema);

        // The attempt owns the ledger, so every plan this attempt rewrites draws
        // its logical ordinals from one strictly increasing space and adds to
        // one cumulative possible-output set.
        let ledger: Option<Arc<AttemptLedger>> = self
            .context
            .as_ref()
            .map(|context| Arc::clone(context.ledger()));
        let cancellation = self
            .context
            .as_ref()
            .map(|context| context.cancellation().clone());

        // A JoinSet rather than detached handles: dropping it aborts every
        // writer task, so no writer survives an early return and keeps writing
        // objects nobody is waiting for.
        let mut writers: JoinSet<std::result::Result<Vec<DataFile>, CompactionError>> =
            JoinSet::new();
        let preserve_lineage = format_version >= FormatVersion::V3;

        // build iceberg writer for each partition
        for mut batch_stream in batches {
            let location_generator = location_generator.clone();
            let schema = arc_input_schema.clone();
            let execution_config = execution_config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let metrics_recorder = metrics_recorder.clone();
            let writer_observer = ledger
                .as_ref()
                .map(|ledger| ledger.writer_bridge() as Arc<dyn RollingWriterObserver>);
            let task_cancellation = cancellation.clone();

            writers.spawn(async move {
                let mut data_file_writer = build_iceberg_data_file_writer(
                    execution_config.data_file_prefix.clone(),
                    location_generator,
                    schema,
                    file_io,
                    partition_spec,
                    sort_order_id,
                    execution_config,
                    writer_observer,
                )?;

                // Process each record batch with metrics
                let mut fetch_batch_start = Instant::now();
                loop {
                    // Cancellation stops new reads and new writes. The writer is
                    // still closed below, so objects already opened surface as
                    // evidence instead of being abandoned untracked.
                    let mut stream = batch_stream.as_mut();
                    let next = match task_cancellation.as_ref() {
                        Some(token) => {
                            tokio::select! {
                                biased;
                                () = token.cancelled() => None,
                                next = stream.next() => next,
                            }
                        }
                        None => stream.next().await,
                    };
                    let Some(batch_result) = next else { break };

                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_datafusion_batch_fetch_duration(
                            fetch_batch_start.elapsed().as_millis() as f64,
                        );
                    }

                    let batch = batch_result?;
                    if preserve_lineage {
                        validate_row_lineage(&batch)?;
                    }

                    let record_count = batch.num_rows() as u64;
                    let batch_bytes = batch.get_array_memory_size() as u64;

                    // Write the batch
                    let write_start = Instant::now();
                    data_file_writer.write(batch).await?;
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_datafusion_batch_write_duration(
                            write_start.elapsed().as_millis() as f64,
                        );
                    }

                    // Record detailed batch stats
                    if let Some(metrics_recorder) = &metrics_recorder {
                        metrics_recorder.record_batch_stats(record_count, batch_bytes);
                    }

                    fetch_batch_start = Instant::now(); // Reset for next batch
                }

                Ok(data_file_writer.close().await?)
            });
        }

        // Drain every writer regardless of outcome. Returning on the first
        // error would leave siblings running and their objects unaccounted for.
        let mut output_data_files: Vec<DataFile> = Vec::new();
        let mut first_error: Option<CompactionError> = None;
        while let Some(joined) = writers.join_next().await {
            match joined {
                Ok(Ok(files)) => output_data_files.extend(files),
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(CompactionError::Execution(err.to_string()));
                    }
                }
            }
        }

        let Some(context) = self.context.as_ref() else {
            if let Some(err) = first_error {
                return Err(err);
            }
            stats.record_output(&output_data_files);
            return Ok(RewriteFilesResponse {
                data_files: output_data_files,
                stats,
            });
        };

        let ledger = ledger.expect("a managed context always installs a ledger");
        let outputs = ledger.outputs();
        let attempt_id = context.attempt_id();

        // Cancellation is decided after the drain so the reported identities
        // are complete: a writer that settled while we were cancelling still
        // produced an object the caller must know about.
        if context.is_cancelled() {
            ledger.emit(RewriteEvent::Cancelled {
                attempt_id,
                outputs: outputs.clone(),
            });
            return Err(CompactionError::Cancelled {
                attempt_id: attempt_id.to_string(),
                outputs,
            });
        }

        if let Some(err) = first_error {
            ledger.emit(RewriteEvent::Failed {
                attempt_id,
                message: err.to_string(),
                outputs,
            });
            return Err(err);
        }

        stats.record_output(&output_data_files);
        ledger.emit(RewriteEvent::Succeeded {
            attempt_id,
            outputs,
            output_bytes: ledger.add_output_bytes(stats.output_total_bytes),
        });

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stats,
        })
    }
}

/// Checks one rewrite batch's row-lineage columns before it is written.
///
/// A v3 rewrite must write the exact `_row_id` and `_last_updated_sequence_number` of every
/// surviving row. Either column missing, not `Int64`, or holding a null fails the rewrite
/// before the batch is written, so no output with lost lineage is ever returned for commit.
///
/// # Errors
///
/// Returns [`CompactionError::Execution`] when a lineage column is missing, has a non-`Int64`
/// type, or contains a null.
fn validate_row_lineage(batch: &RecordBatch) -> Result<()> {
    let lineage_column = |name: &str| -> Result<()> {
        let column = batch.column_by_name(name).ok_or_else(|| {
            CompactionError::Execution(format!("v3 rewrite batch is missing lineage column {name}"))
        })?;
        let values = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                CompactionError::Execution(format!(
                    "v3 rewrite lineage column {name} must be Int64, got {}",
                    column.data_type()
                ))
            })?;
        if values.null_count() > 0 {
            return Err(CompactionError::Execution(format!(
                "v3 rewrite lineage column {name} has {} null values",
                values.null_count()
            )));
        }
        Ok(())
    };
    lineage_column(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)?;
    lineage_column(RESERVED_COL_NAME_ROW_ID)?;
    Ok(())
}

/// Builds the writer stack for one output partition of a rewrite.
///
/// `observer`, when present, is attached to the rolling writer so every output
/// this stack opens and closes is reported. It is purely additive: the roll
/// decision, the objects produced, and their order are identical with and
/// without it.
///
/// # Errors
///
/// Returns [`CompactionError::Config`] when the configured target file size
/// exceeds the platform's `usize`, and propagates partition-splitter
/// construction failures.
#[allow(clippy::too_many_arguments)]
pub fn build_iceberg_data_file_writer(
    data_file_prefix: String,
    location_generator: DefaultLocationGenerator,
    schema: Arc<Schema>,
    file_io: FileIO,
    partition_spec: Arc<PartitionSpec>,
    sort_order_id: Option<i32>,
    execution_config: Arc<CompactionExecutionConfig>,
    observer: Option<Arc<dyn RollingWriterObserver>>,
) -> Result<Box<dyn IcebergWriter>> {
    let target_file_size =
        usize::try_from(execution_config.target_file_size_bytes).map_err(|_| {
            CompactionError::Config(format!(
                "target_file_size_bytes {} exceeds platform usize",
                execution_config.target_file_size_bytes
            ))
        })?;

    let data_file_builder = {
        let parquet_writer_builder = ParquetWriterBuilder::new(
            execution_config.write_parquet_properties.clone(),
            schema.clone(),
        );

        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            data_file_prefix,
            Some(unique_uuid_suffix.to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let mut rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size,
            file_io,
            location_generator,
            file_name_generator,
        )
        .with_max_concurrent_closes(execution_config.max_concurrent_closes);
        if let Some(observer) = observer {
            rolling_writer_builder = rolling_writer_builder.with_observer(observer);
        }

        DataFileWriterBuilder::new(rolling_writer_builder).sort_order_id(sort_order_id)
    };

    let partition_splitter = if partition_spec.is_unpartitioned() {
        None
    } else {
        Some(RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            partition_spec.clone(),
        )?)
    };

    let iceberg_task_writer = TaskWriter::new_with_partition_splitter(
        data_file_builder,
        true,
        schema,
        partition_spec,
        partition_splitter,
    );

    Ok(Box::new(iceberg_task_writer))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch};

    use super::validate_row_lineage;

    /// Builds a batch from named columns.
    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    /// Missing, null, and mistyped lineage each fail; complete lineage passes.
    #[test]
    fn row_lineage_is_complete() {
        let ids = || Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef;
        let seqs = || Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef;

        validate_row_lineage(&batch(vec![
            ("_row_id", ids()),
            ("_last_updated_sequence_number", seqs()),
        ]))
        .unwrap();

        for bad in [
            batch(vec![("_row_id", ids())]),
            batch(vec![("_last_updated_sequence_number", seqs())]),
            batch(vec![
                (
                    "_row_id",
                    Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef,
                ),
                ("_last_updated_sequence_number", seqs()),
            ]),
            batch(vec![
                (
                    "_row_id",
                    Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef,
                ),
                ("_last_updated_sequence_number", seqs()),
            ]),
        ] {
            assert!(validate_row_lineage(&bad).is_err());
        }
    }
}
