// Copyright 2024 TAKKT Industrial & Packaging GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

mod compat;
mod consts;
mod de;
mod result;

use crate::{
    compat::ByteStreamExt,
    consts::{
        MAXIMUM_NUMBER_OF_PARTS,
        MAXIMUM_OBJECT_SIZE,
        MAXIMUM_PART_NUMBER,
        MAXIMUM_PART_SIZE,
        MINIMUM_PART_NUMBER,
        MINIMUM_PART_SIZE,
    },
    result::{
        bail,
        AnyhowResultExt,
        Error,
        Result,
        StdResultExt,
    },
};
use anyhow::Context;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{
        CompletedMultipartUpload,
        CompletedPart,
    },
};
use clap::{
    Args,
    Parser,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::path::{
    Path,
    PathBuf,
};
use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
};
use tracing::{
    debug,
    error,
    info,
    warn,
};
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct State {
    s3_bucket: String,
    s3_key: String,
    file_to_upload: PathBuf,
    file_size_in_bytes: u64,
    part_size: u64,
    number_of_parts: u64,
    upload_id: String,
    last_successful_part: u64,
    #[serde(with = "de::completed_parts")]
    completed_parts: Vec<CompletedPart>,
}

impl State {
    async fn write_to_file(&self, file: impl AsRef<Path>) -> Result<()> {
        let file = file.as_ref().to_owned();

        // serde_json does not support asynchronous writers, so we make sure to spawn the task such
        // that it doesn't block the executor.
        tokio::task::block_in_place(|| {
            serde_json::to_writer(
                std::fs::File::create(file)
                    .context("Failed to open state file")
                    .into_unrecoverable()?,
                self,
            )
            .context("Failed to serialize state file")
            .into_unrecoverable()
        })
    }
}

#[derive(Debug, Parser)]
#[command(version)]
enum Cli {
    /// Upload a file to S3.
    Upload(Upload),
    /// Resume the upload of a file to S3.
    Resume(Resume),
}

#[derive(Debug, Args)]
struct Upload {
    /// The name of the S3 bucket to upload the file to.
    #[arg(long)]
    s3_bucket: String,
    /// The S3 key where to upload the file to.
    #[arg(long)]
    s3_key: String,
    /// Path to the file to upload.
    #[arg(long)]
    file_to_upload: PathBuf,
    /// Explicit part-size to use.
    #[arg(long)]
    override_part_size: Option<u64>,
    /// Path to where the state-file will be saved.
    ///
    /// The state-file is used to make resumable uploads possible. This file is only written if the
    /// upload has failed.
    #[arg(long)]
    state_file: PathBuf,
}

impl Upload {
    async fn run(mut self) -> Result<()> {
        debug!("Running upload command: {:?}", self);

        debug!("Verifying that the state-file doesn't exist yet. If it does, we don't allow the start of a new upload against the same file.");
        if tokio::fs::try_exists(&self.state_file)
            .await
            .into_unrecoverable()?
        {
            bail!("The state-file already exists, and we don't allow starting a new upload against the same file. If you want to resume the upload, use the 'resume' command instead. If you want to start a new upload, please remove the state-file first, or use a different one.");
        }

        self.file_to_upload = self
            .file_to_upload
            .canonicalize()
            .context("Failed to canonicalize file path")
            .into_unrecoverable()?;

        let file_size_in_bytes = {
            let file = tokio::fs::File::open(&self.file_to_upload)
                .await
                .into_unrecoverable()?;
            file.metadata().await.into_unrecoverable()?.len()
        };
        if file_size_in_bytes < MINIMUM_PART_SIZE {
            bail!("File is too small for multipart upload, and a regular upload is not yet supported by persevere")
        } else if file_size_in_bytes > MAXIMUM_OBJECT_SIZE {
            bail!("File exceeds the maximum object size of S3 and thus can't be uploaded")
        }

        let part_size = if let Some(override_part_size) = self.override_part_size {
            if override_part_size < MINIMUM_PART_SIZE {
                bail!(
                    "The part size is too small, it must be at least {} bytes",
                    MINIMUM_PART_SIZE
                );
            } else if override_part_size > MAXIMUM_PART_SIZE {
                bail!(
                    "The part size is too large, it must be at most {} bytes",
                    MAXIMUM_PART_SIZE
                );
            }
            if file_size_in_bytes.div_ceil(override_part_size) > MAXIMUM_PART_NUMBER {
                bail!("The number of parts exceeds the maximum number of parts allowed by S3");
            }
            override_part_size
        } else {
            // The size of the parts we want to upload must at least be `MINIMUM_PART_SIZE`, but if the
            // file is so large that this part-size would result in more than `MAXIMUM_NUMBER_OF_PARTS`, we
            // need to adjust the part size to ensure we don't exceed this limit.
            let part_size =
                MINIMUM_PART_SIZE.max(file_size_in_bytes.div_ceil(MAXIMUM_NUMBER_OF_PARTS));
            if part_size > MAXIMUM_PART_SIZE {
                bail!("The part size exceeds the maximum part size allowed by S3");
            }
            part_size
        };

        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let s3 = aws_sdk_s3::Client::new(&config);

        let multipart_upload = s3
            .create_multipart_upload()
            .bucket(&self.s3_bucket)
            .key(&self.s3_key)
            .send()
            .await
            .into_retryable()?;
        let upload_id = multipart_upload
            .upload_id
            .context("Creating multipart upload probably failed, because no upload ID was returned")
            .into_retryable()?;
        info!(
            "Created multipart upload with ID {} for: s3://{}/{}",
            upload_id, self.s3_bucket, self.s3_key,
        );

        let state = State {
            s3_bucket: self.s3_bucket,
            s3_key: self.s3_key,
            file_to_upload: self.file_to_upload,
            file_size_in_bytes,
            part_size,
            number_of_parts: file_size_in_bytes.div_ceil(part_size),
            upload_id,
            last_successful_part: 0,
            completed_parts: vec![],
        };

        match upload(&s3, &self.state_file, state.clone()).await {
            Err(Error::Unrecoverable(err)) => {
                error!(
                    "Unrecoverable failure during upload, aborting multipart upload: {}",
                    err,
                );
                s3.abort_multipart_upload()
                    .bucket(&state.s3_bucket)
                    .key(&state.s3_key)
                    .upload_id(&state.upload_id)
                    .send()
                    .await
                    .into_retryable()?;
                return Err(Error::Unrecoverable(err));
            }
            result => result,
        }?;
        Ok(())
    }
}

#[derive(Debug, Args)]
struct Resume {
    /// Path to where the state-file of a previous upload.
    ///
    /// This state-file is used to resume the upload in question. The state-file will automatically
    /// be removed if the upload finishes successfully.
    #[arg(long)]
    state_file: PathBuf,
}

impl Resume {
    async fn run(&self) -> Result<()> {
        debug!("Running resume command: {:?}", self);

        // Serde does not support asynchronous readers, so we make sure to spawn the task away from
        // the main thread.
        let state: State = tokio::task::spawn_blocking({
            let state_file = self.state_file.clone();
            || {
                serde_json::from_reader(
                    std::fs::File::open(state_file)
                        .context("Failed to open state file")
                        .into_unrecoverable()?,
                )
                .context("Failed to deserialize state file")
                .into_unrecoverable()
            }
        })
        .await
        .expect("Failed to await synchronous read of state file")?;

        let current_file_size_in_bytes = {
            let file = tokio::fs::File::open(&state.file_to_upload)
                .await
                .into_unrecoverable()?;
            file.metadata().await.into_unrecoverable()?.len()
        };
        if current_file_size_in_bytes != state.file_size_in_bytes {
            bail!(
                "The file has changed since the last upload. The file size was {} bytes, but is now {} bytes. The upload cannot be resumed, and should be aborted! Upload ID: {}",
                state.file_size_in_bytes,
                current_file_size_in_bytes,
                state.upload_id,
            );
        }

        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let s3 = aws_sdk_s3::Client::new(&config);

        match upload(&s3, &self.state_file, state.clone()).await {
            Err(Error::Unrecoverable(err)) => {
                error!(
                    "Unrecoverable failure during upload, aborting multipart upload: {}",
                    err,
                );
                s3.abort_multipart_upload()
                    .bucket(&state.s3_bucket)
                    .key(&state.s3_key)
                    .upload_id(&state.upload_id)
                    .send()
                    .await
                    .into_retryable()?;
                return Err(Error::Unrecoverable(err));
            }
            result => result,
        }?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Part {
    number: i32,
    offset: u64,
    size: u64,
}

#[tracing::instrument(skip_all)]
async fn upload_part(s3: &aws_sdk_s3::Client, state: &State, part: Part) -> Result<CompletedPart> {
    info!(
        "Starting upload of part {} of {} ({} bytes)...",
        part.number, state.number_of_parts, part.size,
    );
    debug!(
        "Opening file for reading: {}",
        state.file_to_upload.display()
    );
    let mut file = tokio::fs::File::open(&state.file_to_upload)
        .await
        .into_unrecoverable()?;
    debug!("Seeking to the start of the part: {}", part.offset);
    file.seek(tokio::io::SeekFrom::Start(part.offset))
        .await
        .into_unrecoverable()?;

    let part_reader = file.take(part.size);
    let byte_stream = ByteStream::from_reader(part_reader);

    let uploaded_part = s3
        .upload_part()
        .bucket(&state.s3_bucket)
        .key(&state.s3_key)
        .upload_id(&state.upload_id)
        .part_number(part.number)
        .content_length(part.size as i64)
        .body(byte_stream)
        .send()
        .await
        .into_retryable()?;

    info!(
        "Finished upload of part {} of {} ({} bytes)",
        part.number, state.number_of_parts, part.size,
    );

    Ok(CompletedPart::builder()
        .set_checksum_crc32(uploaded_part.checksum_crc32)
        .set_checksum_crc32_c(uploaded_part.checksum_crc32_c)
        .set_checksum_sha1(uploaded_part.checksum_sha1)
        .set_checksum_sha256(uploaded_part.checksum_sha256)
        .set_e_tag(uploaded_part.e_tag)
        .part_number(part.number)
        .build())
}

#[tracing::instrument(skip_all)]
async fn upload(s3: &aws_sdk_s3::Client, state_file: &Path, mut state: State) -> Result<()> {
    debug!(
        "File size: {} bytes. Part size: {} bytes. Number of parts to upload: {}.",
        state.file_size_in_bytes, state.part_size, state.number_of_parts,
    );
    if state.number_of_parts > MAXIMUM_PART_NUMBER {
        bail!("The number of parts exceeds the maximum number of parts allowed by S3");
    }

    info!(
        "Uploading the file in {} parts of {} bytes each",
        state.number_of_parts, state.part_size,
    );

    let first_part_number = if state.last_successful_part > 0 {
        state.last_successful_part + 1
    } else {
        MINIMUM_PART_NUMBER
    };
    let mut offset = (first_part_number - 1) * state.part_size;
    for part_number in first_part_number..(MINIMUM_PART_NUMBER + state.number_of_parts) {
        let actual_part_size = if part_number == state.number_of_parts {
            let potential_part_size = state.file_size_in_bytes % state.part_size;
            if potential_part_size == 0 {
                state.part_size
            } else {
                potential_part_size
            }
        } else {
            state.part_size
        };

        let mut last_retry_error: Option<Error> = None;
        for attempt in 1..=3 {
            let part = Part {
                number: part_number as i32,
                offset,
                size: actual_part_size,
            };
            match upload_part(s3, &state, part).await {
                Ok(completed_part) => {
                    state.completed_parts.push(completed_part);
                    offset += actual_part_size;
                    last_retry_error = None;
                    state.last_successful_part = part_number;
                    break;
                }
                Err(Error::Retryable(err)) => {
                    warn!(
                        "Failed to upload part {}, retrying (attempt {}): {}",
                        part_number, attempt, err,
                    );
                    last_retry_error = Some(Error::Retryable(err));
                    continue;
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        if let Some(error) = last_retry_error {
            state.write_to_file(&state_file).await?;
            error!(
                "Failed to upload part {} after 3 attempts. Multipart upload will not be aborted, to allow resuming.",
                part_number,
            );
            error!("Process failed with a retryable error. To resume the upload, run the following command:");
            error!("persevere resume --state-file '{}'", state_file.display());
            return Err(error);
        }
    }

    // We verify that the offset we reached matches up with the file size.
    if offset != state.file_size_in_bytes {
        bail!("In theory we finished the upload, but in practice there were still more bytes to be read from the file. This is unexpected, and we don't really have a way to recover from this, besides maybe trying to reupload the file.");
    }

    let completed_multipart_upload = s3
        .complete_multipart_upload()
        .bucket(state.s3_bucket)
        .key(state.s3_key)
        .upload_id(&state.upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(state.completed_parts))
                .build(),
        )
        .send()
        .await
        .into_retryable()?;
    info!(
        "Successfully uploaded the file. ETag: {}",
        completed_multipart_upload
            .e_tag
            .as_deref()
            .unwrap_or("<unknown>"),
    );

    debug!("Removing state-file: {}", state_file.display());
    match tokio::fs::remove_file(state_file).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!("The state-file did not exist, probably because it was never written, likely because the upload worked first try.")
        }
        result => result.into_unrecoverable()?,
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_file(true)
                .with_line_number(true)
                .with_target(false),
        )
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let command = Cli::parse();
    match command {
        Cli::Upload(cmd) => cmd.run().await,
        Cli::Resume(cmd) => cmd.run().await,
    }
}
