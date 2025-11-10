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

use crate::{
    get_aws_config,
    result::{
        bail,
        AnyhowResultExt,
        Error,
        Result,
        StdResultExt,
    },
};
use anyhow::Context;
use aws_sdk_s3::types::ObjectAttributes;
use clap::{
    Args,
    Subcommand,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::path::{
    Path,
    PathBuf,
};
use tokio::io::AsyncSeekExt;
use tracing::{
    debug,
    error,
    info,
    warn,
};

#[derive(Debug, Deserialize, Serialize)]
struct State {
    s3_bucket: String,
    s3_key: String,
    output: PathBuf,
    object_size: u64,
    part_size: u64,
    number_of_parts: u64,
    last_successful_part: Option<u64>,
}

impl State {
    async fn from_file(file: impl AsRef<Path>) -> Result<Self> {
        let file = file.as_ref().to_owned();

        // serde_json does not support asynchronous readers, so we make sure to spawn the task away
        // from the main thread.
        tokio::task::spawn_blocking(|| {
            serde_json::from_reader(
                std::fs::File::open(file)
                    .context("Failed to open state file")
                    .into_unrecoverable()?,
            )
            .context("Failed to deserialize state file")
            .into_unrecoverable()
        })
        .await
        .expect("Failed to await synchronous read of state file")
    }

    // NOTE: `self` is taken mutably here, even though it isn't required by the method itself. By
    //       requiring mutability, we guarantee that there is only ever one task that can write the
    //       state file at a time, ensuring the file is always in a consistent state that.
    async fn write_to_file(&mut self, file: impl AsRef<Path>) -> Result<()> {
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

#[derive(Debug, Subcommand)]
pub(crate) enum Download {
    /// Start the download of a file from S3.
    ///
    /// Persevere will take care of downloading the file in a manner that is resilient, such that
    /// intermittent errors do not result in losing all progress on the download, as well as
    /// resumable, e.g. in case the system you are downloading crashed or there is a more
    /// persistent, but still recoverable, error.
    ///
    /// This is achieved through a state-file which keeps track of the state of the download.
    /// Resuming a download is done through the `resume` subcommand, by providing the same
    /// state-file again.
    ///
    /// You need the following AWS permissions for the S3-object ARN you are trying to download from:
    ///
    /// * `s3:GetObject`
    /// * `s3:GetObjectAttributes`
    ///
    /// Persevere will automatically discover valid AWS credentials like most AWS SDKs. This means
    /// you can provide environment variables such as `AWS_PROFILE` to select the profile you want
    /// to download a file with, or provide the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    /// directly.
    Start(Start),
    /// Resume the download of a file from S3.
    ///
    /// You only have to provide the state-file of a previous invocation to `download start`, and
    /// Persevere will resume your download where it left off.
    ///
    /// You can not provide any other parameters to modify how the download is handled, all choices
    /// made when you started the download have to remain the same. If you modify the state-file
    /// manually, chances are you'll either have the download fail outright, or you'll end up with a
    /// corrupt file locally (and won't know that it is corrupt).
    ///
    ///
    /// You need the following AWS permissions for the S3-object ARN you are trying to download from:
    ///
    /// * `s3:GetObject`
    /// * `s3:GetObjectAttributes`
    ///
    /// Persevere will automatically discover valid AWS credentials like most AWS SDKs. This means
    /// you can provide environment variables such as `AWS_PROFILE` to select the profile you want
    /// to download a file with, or provide the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
    /// directly.
    Resume(Resume),
    /// Abort the download of a file from S3.
    ///
    /// If you previously started a download using the `start` subcommand which has failed with a
    /// recoverable error, but you no longer want to finish the download you can invoke this
    /// subcommand with the state-file.
    Abort(Abort),
}

impl Download {
    pub(crate) async fn run(self) -> Result<()> {
        match self {
            Download::Start(start) => start.run().await,
            Download::Resume(resume) => resume.run().await,
            Download::Abort(abort) => abort.run().await,
        }
    }
}

#[derive(Debug, Args)]
pub(crate) struct Start {
    /// The name of the S3 bucket to download the file from.
    #[arg(long)]
    s3_bucket: String,
    /// The S3 key to download the file from.
    #[arg(long)]
    s3_key: String,
    /// Path to the local file to download to.
    #[arg(long)]
    output: PathBuf,
    /// Explicit part-size, in bytes, to use.
    ///
    /// The default is 100 MiB.
    #[arg(long, default_value = "104857600")]
    part_size: u64,
    /// Path to where the state-file will be saved.
    ///
    /// The state-file is used to make resumable downloads possible. It will automatically be removed if the download
    /// finishes successfully.
    #[arg(long)]
    state_file: PathBuf,
}

impl Start {
    pub(crate) async fn run(self) -> Result<()> {
        debug!("Running download command: {:?}", self);

        debug!("Verifying that the state-file doesn't exist yet. If it does, we don't allow the start of a new download against the same file.");
        if tokio::fs::try_exists(&self.state_file)
            .await
            .into_unrecoverable()?
        {
            bail!("The state-file already exists, and we don't allow starting a new download against the same file. If you want to resume the download, use the 'resume' command instead. If you want to start a new download, please remove the state-file first, or use a different one.");
        }

        if self.output.exists() {
            bail!("The output file already exists. We don't allow overwriting existing files.");
        }

        let config = get_aws_config().await;
        let s3 = aws_sdk_s3::Client::new(&config);

        let object_attributes = s3
            .get_object_attributes()
            .bucket(&self.s3_bucket)
            .key(&self.s3_key)
            .object_attributes(ObjectAttributes::ObjectSize)
            .max_parts(224)
            .send()
            .await
            .into_unrecoverable()?;
        let object_size = object_attributes
            .object_size()
            .ok_or_else(|| anyhow::anyhow!("Object size is required"))
            .into_unrecoverable()? as u64;
        let number_of_parts = object_size.div_ceil(self.part_size);

        debug!("Truncating local file to be of object's size");
        tokio::fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.output)
            .await
            .into_unrecoverable()?
            .set_len(object_size)
            .await
            .into_unrecoverable()?;

        let mut state = State {
            s3_bucket: self.s3_bucket,
            s3_key: self.s3_key,
            output: self.output,
            object_size,
            part_size: self.part_size,
            number_of_parts,
            last_successful_part: None,
        };

        download(&s3, &self.state_file, &mut state).await?;

        Ok(())
    }
}

#[derive(Debug, Args)]
pub(crate) struct Resume {
    /// Path to the state-file to resume the download from.
    ///
    /// This state-file is used to resume the download in question. The state-file will
    /// automatically be removed if the download finishes successfully.
    #[arg(long)]
    state_file: PathBuf,
}

impl Resume {
    async fn run(&self) -> Result<()> {
        debug!("Running resume command: {:?}", self);

        let mut state = State::from_file(&self.state_file).await?;

        let config = get_aws_config().await;
        let s3 = aws_sdk_s3::Client::new(&config);

        download(&s3, &self.state_file, &mut state).await?;

        Ok(())
    }
}

#[derive(Debug, Args)]
pub(crate) struct Abort {
    /// Path to the state-file to abort the download from.
    ///
    /// This state-file is used to abort the download in question. The state-file will
    /// automatically be removed if the download finishes successfully.
    #[arg(long)]
    state_file: PathBuf,
}

impl Abort {
    async fn run(&self) -> Result<()> {
        debug!("Running abort command: {:?}", self);

        debug!("Removing state-file: {}", self.state_file.display());
        tokio::fs::remove_file(&self.state_file)
            .await
            .context("Failed to remove state-file")
            .into_unrecoverable()?;

        Ok(())
    }
}

#[tracing::instrument(skip_all)]
async fn download_part(s3: &aws_sdk_s3::Client, state: &State, part_number: u64) -> Result<()> {
    info!(
        "Starting download of part {} of {} ({} bytes)...",
        part_number + 1,
        state.number_of_parts,
        state.part_size,
    );
    let offset_start = part_number * state.part_size;
    let mut offset_end = offset_start + state.part_size - 1;
    if offset_end > state.object_size {
        offset_end = state.object_size - 1; // TODO: is `- 1` correct here?
    }
    let range = format!("bytes={}-{}", offset_start, offset_end);

    debug!("Opening file for writing: {}", state.output.display());
    let mut file = tokio::fs::File::options()
        .write(true)
        .open(&state.output)
        .await
        .into_unrecoverable()?;
    debug!("Seeking to the start of the part: {}", offset_start);
    file.seek(tokio::io::SeekFrom::Start(offset_start))
        .await
        .into_unrecoverable()?;

    debug!("Retrieving range from S3");
    let get_part = s3
        .get_object()
        .bucket(&state.s3_bucket)
        .key(&state.s3_key)
        .range(range)
        .send()
        .await
        .into_retryable()?;

    debug!("Copying S3 stream to local file");
    tokio::io::copy(&mut get_part.body.into_async_read(), &mut file)
        .await
        .into_retryable()?;

    info!(
        "Finished download of part {} of {} ({} bytes)",
        part_number + 1,
        state.number_of_parts,
        state.part_size,
    );
    Ok(())
}

#[tracing::instrument(skip_all)]
async fn download(s3: &aws_sdk_s3::Client, state_file: &Path, state: &mut State) -> Result<()> {
    debug!(
        "Object size: {} bytes. Part size: {} bytes. Number of parts to download: {}.",
        state.object_size, state.part_size, state.number_of_parts,
    );
    info!(
        "Download the object in {} parts of {} bytes each",
        state.number_of_parts, state.part_size,
    );

    let first_part_number = state.last_successful_part.unwrap_or(0);
    for part_number in first_part_number..state.number_of_parts {
        let mut last_retry_error: Option<Error> = None;
        for attempt in 1..=3 {
            match download_part(s3, state, part_number).await {
                Ok(_) => {
                    last_retry_error = None;
                    state.last_successful_part = Some(part_number);
                    break;
                }
                Err(Error::Retryable(err)) => {
                    warn!(
                        "Failed to download part {}, retrying (attempt {}): {}",
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

        state.write_to_file(&state_file).await?;
        if let Some(error) = last_retry_error {
            error!(
                "Failed to download part {} after 3 attempts. Download will not be aborted, to allow resuming.",
                part_number,
            );
            error!("Process failed with a retryable error. To resume the download, run the following command:");
            error!(
                "persevere download resume --state-file '{}'",
                state_file.display()
            );
            return Err(error);
        }
    }

    info!("Successfully downloaded the file.");

    debug!("Removing state-file: {}", state_file.display());
    match tokio::fs::remove_file(state_file).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            debug!("The state-file did not exist, probably because it was never written, likely because the download worked first try.")
        }
        result => result.into_unrecoverable()?,
    }

    Ok(())
}
