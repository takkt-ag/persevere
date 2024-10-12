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
};
use anyhow::{
    Context,
    Result,
};
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
use std::path::{
    Path,
    PathBuf,
};
use tokio::io::AsyncReadExt;
use tracing::{
    debug,
    error,
    info,
};
use tracing_subscriber::prelude::*;

#[derive(Debug, Parser)]
#[command(version)]
enum Cli {
    /// Upload a file to S3.
    Upload(Upload),
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
}

impl Upload {
    async fn run(&self) -> Result<()> {
        debug!("Running upload command: {:?}", self);
        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let s3 = aws_sdk_s3::Client::new(&config);
        upload(
            &s3,
            &self.s3_bucket,
            &self.s3_key,
            &self.file_to_upload,
            &self.override_part_size,
        )
        .await?;
        Ok(())
    }
}

#[tracing::instrument(skip_all)]
async fn upload_part(
    s3: &aws_sdk_s3::Client,
    s3_bucket: &str,
    s3_key: &str,
    upload_id: &str,
    file: tokio::fs::File,
    part_number: u64,
    number_of_parts: u64,
    part_size: u64,
) -> Result<CompletedPart> {
    info!(
        "Starting upload of part {} of {} ({} bytes)...",
        part_number, number_of_parts, part_size,
    );
    let part_reader = file.take(part_size);
    let byte_stream = ByteStream::from_reader(part_reader);

    let uploaded_part = s3
        .upload_part()
        .bucket(s3_bucket)
        .key(s3_key)
        .upload_id(upload_id)
        .part_number(part_number as i32)
        .content_length(part_size as i64)
        .body(byte_stream)
        .send()
        .await?;

    info!(
        "Finished upload of part {} of {} ({} bytes)",
        part_number, number_of_parts, part_size,
    );

    Ok(CompletedPart::builder()
        .set_checksum_crc32(uploaded_part.checksum_crc32)
        .set_checksum_crc32_c(uploaded_part.checksum_crc32_c)
        .set_checksum_sha1(uploaded_part.checksum_sha1)
        .set_checksum_sha256(uploaded_part.checksum_sha256)
        .set_e_tag(uploaded_part.e_tag)
        .part_number(part_number as i32)
        .build())
}

#[tracing::instrument(skip_all)]
async fn upload(
    s3: &aws_sdk_s3::Client,
    s3_bucket: &str,
    s3_key: &str,
    file_to_upload: &Path,
    override_part_size: &Option<u64>,
) -> Result<()> {
    let mut file = tokio::fs::File::open(file_to_upload).await?;

    let file_size_in_bytes = file.metadata().await?.len();
    if file_size_in_bytes < MINIMUM_PART_SIZE {
        anyhow::bail!("File is too small for multipart upload, and a regular upload is not yet supported by persevere")
    } else if file_size_in_bytes > MAXIMUM_OBJECT_SIZE {
        anyhow::bail!("File exceeds the maximum object size of S3 and thus can't be uploaded")
    }

    let part_size = if let Some(override_part_size) = override_part_size {
        if *override_part_size < MINIMUM_PART_SIZE {
            anyhow::bail!(
                "The part size is too small, it must be at least {} bytes",
                MINIMUM_PART_SIZE
            );
        } else if *override_part_size > MAXIMUM_PART_SIZE {
            anyhow::bail!(
                "The part size is too large, it must be at most {} bytes",
                MAXIMUM_PART_SIZE
            );
        }
        if file_size_in_bytes.div_ceil(*override_part_size) > MAXIMUM_PART_NUMBER {
            anyhow::bail!("The number of parts exceeds the maximum number of parts allowed by S3");
        }
        *override_part_size
    } else {
        // The size of the parts we want to upload must at least be `MINIMUM_PART_SIZE`, but if the
        // file is so large that this part-size would result in more than `MAXIMUM_NUMBER_OF_PARTS`, we
        // need to adjust the part size to ensure we don't exceed this limit.
        let part_size = MINIMUM_PART_SIZE.max(file_size_in_bytes.div_ceil(MAXIMUM_NUMBER_OF_PARTS));
        if part_size > MAXIMUM_PART_SIZE {
            anyhow::bail!("The part size exceeds the maximum part size allowed by S3");
        }
        part_size
    };

    let number_of_parts = file_size_in_bytes.div_ceil(part_size);
    debug!(
        "File size: {} bytes. Part size: {} bytes. Number of parts to upload: {}.",
        file_size_in_bytes, part_size, number_of_parts,
    );
    if number_of_parts > MAXIMUM_PART_NUMBER {
        anyhow::bail!("The number of parts exceeds the maximum number of parts allowed by S3");
    }

    let multipart_upload = s3
        .create_multipart_upload()
        .bucket(s3_bucket)
        .key(s3_key)
        .send()
        .await?;
    let upload_id = multipart_upload
        .upload_id
        .context("Creating multipart upload probably failed, because no upload ID was returned")?;
    info!(
        "Created multipart upload with ID {} for: s3://{}/{}",
        upload_id, s3_bucket, s3_key,
    );

    info!(
        "Uploading the file in {} parts of {} bytes each",
        number_of_parts, part_size,
    );
    let mut completed_parts: Vec<CompletedPart> = Vec::with_capacity(number_of_parts as usize);
    for part_number in MINIMUM_PART_NUMBER..(MINIMUM_PART_NUMBER + number_of_parts) {
        let actual_part_size = if part_number == number_of_parts {
            info!(
                "Last part is smaller than the rest: {} bytes",
                file_size_in_bytes % part_size
            );
            let potential_part_size = file_size_in_bytes % part_size;
            if potential_part_size == 0 {
                part_size
            } else {
                potential_part_size
            }
        } else {
            part_size
        };

        match upload_part(
            s3,
            s3_bucket,
            s3_key,
            &upload_id,
            file.try_clone().await?,
            part_number,
            number_of_parts,
            actual_part_size,
        )
        .await
        {
            Ok(completed_part) => {
                completed_parts.push(completed_part);
            }
            Err(err) => {
                error!(
                    "Failed to upload part {}, aborting multipart upload: {}",
                    part_number, err,
                );
                s3.abort_multipart_upload()
                    .bucket(s3_bucket)
                    .key(s3_key)
                    .upload_id(&upload_id)
                    .send()
                    .await?;
                return Err(err).context("Part upload failed, aborted multipart upload");
            }
        }
    }

    // We assert that the file was read until the end by trying to read one more byte. If the number
    // of bytes read is 0, we know we've reached the end of the file, matching our assumption.
    let bytes_read = file.read(&mut [0; 1]).await?;
    if bytes_read != 0 {
        // FIXME: return a "unrecoverable" error type to ensure the multipart upload is aborted,
        //        because this state should never occur and is not recoverable.
        anyhow::bail!("In theory we finished the upload, but in practice there were still more bytes to be read from the file. This is unexpected, and we don't really have a way to recover from this, besides maybe trying to reupload the file.");
    }

    let completed_multipart_upload = s3
        .complete_multipart_upload()
        .bucket(s3_bucket)
        .key(s3_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await?;
    info!(
        "Successfully uploaded the file. ETag: {}",
        completed_multipart_upload
            .e_tag
            .as_deref()
            .unwrap_or("<unknown>"),
    );

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
    }
}
