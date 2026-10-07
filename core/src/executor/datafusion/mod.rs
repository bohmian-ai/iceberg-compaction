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
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFile, PartitionSpec, Schema};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::{
    RollingFileWriterBuilder, RollingWriterObserver,
};
use iceberg::writer::file_writer::variant_shredding::{VariantLayout, VariantShreddingPolicy};
use iceberg::writer::file_writer::{ParquetWriterBuilder, VariantParquetWriterBuilder};
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
        let variant_layout = combined_variant_layout(
            &file_io,
            &schema,
            &file_group.data_files,
            &execution_config.variant_shredding,
        )
        .await?;
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
            let variant_layout = variant_layout.clone();

            writers.spawn(async move {
                let mut data_file_writer = build_iceberg_data_file_writer(
                    execution_config.data_file_prefix.clone(),
                    location_generator,
                    schema,
                    file_io,
                    partition_spec,
                    sort_order_id,
                    execution_config,
                    variant_layout,
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

/// Source footers loaded at once while choosing a rewrite's Variant layout.
const MAX_CONCURRENT_FOOTER_LOADS: usize = 16;

/// Chooses the one Variant layout every output of a rewrite shreds with.
///
/// Loads the footer of each distinct source data file, one small range read
/// per file with no data pages, and combines the layouts those files already
/// chose from their shredded-leaf counts. A table without a top-level Variant
/// column, or the default policy, reads nothing and shreds nothing.
///
/// # Errors
///
/// Returns [`CompactionError::Iceberg`] when the schema cannot be converted to
/// Arrow, a source cannot be opened, or its footer cannot be read or combined.
async fn combined_variant_layout(
    file_io: &FileIO,
    schema: &Schema,
    sources: &[FileScanTask],
    policy: &VariantShreddingPolicy,
) -> Result<VariantLayout> {
    let has_variant = schema
        .as_struct()
        .fields()
        .iter()
        .any(|field| matches!(*field.field_type, iceberg::spec::Type::Variant(_)));
    if !has_variant || *policy == VariantShreddingPolicy::default() {
        return Ok(VariantLayout::default());
    }
    let logical = iceberg::arrow::schema_to_arrow_schema(schema)?;
    let mut seen = std::collections::HashSet::new();
    let distinct: Vec<(String, u64)> = sources
        .iter()
        .filter(|task| seen.insert(task.data_file_path.as_str()))
        .map(|task| (task.data_file_path.clone(), task.file_size_in_bytes))
        .collect();
    let footers: Vec<Arc<parquet::file::metadata::ParquetMetaData>> =
        futures::stream::iter(distinct.into_iter().map(|(path, size)| {
            let file_io = file_io.clone();
            async move {
                let input = file_io.new_input(&path)?;
                let metadata = iceberg::io::FileMetadata { size };
                let mut reader =
                    iceberg::arrow::ArrowFileReader::new(metadata, input.reader().await?);
                parquet::arrow::async_reader::AsyncFileReader::get_metadata(&mut reader, None)
                    .await
                    .map_err(|err| {
                        iceberg::Error::new(
                            iceberg::ErrorKind::DataInvalid,
                            format!("read the footer of {path}"),
                        )
                        .with_source(err)
                    })
            }
        }))
        .buffered(MAX_CONCURRENT_FOOTER_LOADS)
        .try_collect()
        .await?;
    Ok(VariantLayout::combine(&logical, &footers, policy)?)
}

/// Builds the writer stack for one output partition of a rewrite.
///
/// Every output shreds its Variant columns with `variant_layout`.
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
    variant_layout: VariantLayout,
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
        let parquet_writer_builder = VariantParquetWriterBuilder::new(
            ParquetWriterBuilder::new(
                execution_config.write_parquet_properties.clone(),
                schema.clone(),
            ),
            variant_layout,
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

    use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::compute::{cast, concat_batches};
    use iceberg::arrow::schema_to_arrow_schema;
    use iceberg::io::FileIO;
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type, VariantType};
    use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
    use iceberg::writer::file_writer::variant_shredding::{
        VariantLayout, VariantSampler, VariantShreddingPolicy,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::variant::{VariantArray, json_to_variant, unshred_variant, variant_to_json};

    use super::{build_iceberg_data_file_writer, combined_variant_layout};
    use crate::config::CompactionExecutionConfigBuilder;

    /// The policy values Wyrd passes.
    const POLICY: VariantShreddingPolicy = VariantShreddingPolicy {
        confidence_z: 2.5758,
        margin: 0.02,
        min_stratum_rows: 30,
        min_frequency: 0.10,
        max_tracked_children: 1_000,
        max_emitted_children: 300,
        max_depth: 50,
    };

    /// Every output of a rewrite shreds with the layout combined from its source footers.
    ///
    /// Two source files are written first, one shredding only `a` and one only
    /// `b`. Their footers combine into a layout shredding both. A one-byte
    /// target then rolls the `{a}` and `{b}` batches into separate outputs,
    /// and each output must shred both fields, keep its logical row count, and
    /// read back every value unchanged.
    #[tokio::test]
    async fn rolled_outputs_share_the_combined_source_layout() {
        let temp_dir = tempfile::tempdir().expect("output directory");
        let file_io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).expect("arrow schema"));
        let batch = |ids: [i64; 2], json: [&str; 2]| {
            let json: ArrayRef = Arc::new(StringArray::from(json.to_vec()));
            let variant: ArrayRef = json_to_variant(&json).expect("variant").into();
            let variant = cast(&variant, arrow_schema.field(1).data_type()).expect("storage");
            RecordBatch::try_new(arrow_schema.clone(), vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                variant,
            ])
            .expect("batch")
        };
        let first = batch([1, 2], [r#"{"a":1}"#, r#"{"a":300}"#]);
        let second = batch([3, 4], [r#"{"b":"x"}"#, r#"{"b":"y"}"#]);
        let writer_stack = |target: u64, layout: VariantLayout| {
            let config = CompactionExecutionConfigBuilder::default()
                .target_file_size_bytes(target)
                .variant_shredding(POLICY)
                .build()
                .expect("config");
            build_iceberg_data_file_writer(
                "test".to_owned(),
                DefaultLocationGenerator::with_data_location(
                    temp_dir.path().to_str().expect("utf-8 path").to_owned(),
                ),
                schema.clone(),
                file_io.clone(),
                Arc::new(PartitionSpec::unpartition_spec()),
                None,
                Arc::new(config),
                layout,
                None,
            )
            .expect("writer stack")
        };

        let mut sources = Vec::new();
        for source in [&first, &second] {
            let mut sampler = VariantSampler::new(&arrow_schema, POLICY, 7, &[2]);
            sampler.offer(source, &[0, 0], 0).expect("sample");
            let mut writer = writer_stack(u64::MAX >> 1, sampler.layout());
            writer.write(source.clone()).await.expect("source batch");
            for data_file in writer.close().await.expect("source close") {
                sources.push(
                    FileScanTask::builder()
                        .with_file_size_in_bytes(data_file.file_size_in_bytes())
                        .with_start(0)
                        .with_length(data_file.file_size_in_bytes())
                        .with_data_file_path(data_file.file_path().to_owned())
                        .with_data_file_format(iceberg::spec::DataFileFormat::Parquet)
                        .with_schema(schema.clone())
                        .with_project_field_ids(vec![1, 2])
                        .with_case_sensitive(true)
                        .build(),
                );
            }
        }
        assert_eq!(sources.len(), 2, "one file per source");
        let layout = combined_variant_layout(&file_io, &schema, &sources, &POLICY)
            .await
            .expect("combined layout");

        let mut writer = writer_stack(1, layout);
        writer.write(first).await.expect("first batch");
        writer.write(second).await.expect("second batch");
        let data_files = writer.close().await.expect("close");
        assert_eq!(data_files.len(), 2, "one output per batch");

        let mut shredded = Vec::new();
        let mut values = Vec::new();
        for data_file in &data_files {
            assert_eq!(data_file.record_count(), 2);
            let bytes = file_io
                .new_input(data_file.file_path())
                .expect("input")
                .read()
                .await
                .expect("bytes");
            let read = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .expect("reader")
                .build()
                .expect("batches")
                .collect::<Result<Vec<_>, _>>()
                .expect("rows");
            let read = concat_batches(&read[0].schema(), &read).expect("one batch");
            let variant = VariantArray::try_new(read.column_by_name("v").expect("v").as_ref())
                .expect("variant storage");
            shredded.push(
                match variant.typed_value_column().map(|typed| typed.data_type()) {
                    Some(datafusion::arrow::datatypes::DataType::Struct(fields)) => fields
                        .iter()
                        .map(|field| field.name().clone())
                        .collect::<Vec<_>>(),
                    other => panic!("expected an object layout, got {other:?}"),
                },
            );
            let logical: ArrayRef = unshred_variant(&variant).expect("unshred").into();
            values.extend(
                variant_to_json(&logical)
                    .expect("json")
                    .iter()
                    .map(|json| json.expect("non-null").to_owned()),
            );
        }
        let both = vec!["a".to_owned(), "b".to_owned()];
        assert_eq!(shredded, [both.clone(), both]);
        values.sort();
        assert_eq!(values, [
            r#"{"a":1}"#,
            r#"{"a":300}"#,
            r#"{"b":"x"}"#,
            r#"{"b":"y"}"#
        ]);
    }
}
