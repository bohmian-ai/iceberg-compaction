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

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::StreamExt;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionSpec, Schema};
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

/// DataFusion-backed rewrite executor.
///
/// Without a managed context the executor is stateless and behaves exactly as
/// before: it builds its own runtime per rewrite, spawns detached writer tasks,
/// and reports nothing. With a context installed it becomes bound to exactly
/// one publication attempt — sharing that attempt's leased runtime, honoring
/// its cancellation token, and reporting every physical object it produced.
#[derive(Debug, Default)]
pub struct DataFusionExecutor {
    context: Option<Arc<ManagedExecutionContext>>,
}

impl DataFusionExecutor {
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
        }
    }

    /// Returns the bound attempt context, when the executor is managed.
    #[must_use]
    pub fn context(&self) -> Option<&Arc<ManagedExecutionContext>> {
        self.context.as_ref()
    }

    /// Reports the attempt's resource usage before its terminal event.
    ///
    /// Peak memory is always reported; scratch accounting only when the caller
    /// leased a root. A scratch measurement that fails is skipped rather than
    /// failing the attempt: losing an accounting figure must not destroy an
    /// otherwise complete rewrite.
    fn report_resource_usage(context: &ManagedExecutionContext) {
        let attempt_id = context.attempt_id();
        context.observer().on_event(RewriteEvent::PeakMemory {
            attempt_id,
            peak_bytes: context.peak_memory_bytes(),
            pool_capacity_bytes: context.pool_capacity_bytes(),
        });
        if let Some(spill) = context.spill()
            && spill.measure().is_ok()
        {
            context.observer().on_event(RewriteEvent::ScratchSpill {
                attempt_id,
                current_bytes: spill.current_bytes(),
                peak_bytes: spill.peak_bytes(),
            });
        }
    }
}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
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
            .build()?;
        // A leased runtime means every rewrite in this attempt draws on one
        // shared budget while keeping its own isolated session catalog.
        let processor = match self.context.as_ref() {
            Some(context) => DatafusionProcessor::new_with_runtime_env(
                execution_config.clone(),
                executor_parallelism,
                file_io.clone(),
                context.runtime_env(),
            )?,
            None => DatafusionProcessor::new(
                execution_config.clone(),
                executor_parallelism,
                file_io.clone(),
            )?,
        };
        let (batches, input_schema) = processor
            .execute(datafusion_task_ctx, output_parallelism)
            .await?;
        let arc_input_schema = Arc::new(input_schema);

        let ledger = self.context.as_ref().map(|context| {
            AttemptLedger::new(context.attempt_id(), Arc::clone(context.observer()))
        });
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
        Self::report_resource_usage(context);
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
            output_bytes: stats.output_total_bytes,
        });

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stats,
        })
    }
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
