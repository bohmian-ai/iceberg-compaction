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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompactionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Execution failed: {0}")]
    Execution(String),

    #[error("Iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Test error: {0}")]
    Test(String),

    #[error("Compaction validator failed: {0}")]
    CompactionValidator(String),

    #[error("Compaction unexpected failed: {0}")]
    Unexpected(String),

    /// A managed attempt was cancelled by its caller and fully drained.
    ///
    /// `outputs` names every object the attempt produced or may have produced.
    /// It is evidence, not a deletion instruction: the caller owns whether an
    /// unpublished object is reclaimed, retained, or reconciled against a
    /// commit whose outcome is uncertain.
    #[error("Compaction attempt {attempt_id} was cancelled with {} output(s)", outputs.len())]
    Cancelled {
        /// Identity of the cancelled attempt.
        attempt_id: String,
        /// Every produced or possibly-produced output.
        outputs: Vec<crate::managed::observer::OutputIdentity>,
    },
}

pub type Result<T> = std::result::Result<T, CompactionError>;
