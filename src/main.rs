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
mod download;
mod result;
mod upload;

use crate::result::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::RequestChecksumCalculation;
use clap::Parser;
use tracing_subscriber::prelude::*;

pub(crate) async fn get_aws_config() -> aws_config::SdkConfig {
    aws_config::load_defaults(BehaviorVersion::v2025_01_17())
        .await
        .into_builder()
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .build()
}

/// With Persevere you can upload and download huge files to and from S3 without worrying about
/// network interruptions or other issues. Persevere will allow you to resume the upload or download
/// where it was left off, even in the case of a system crash.
///
/// Source: <https://github.com/takkt-ag/persevere>
#[derive(Debug, Parser)]
#[command(version, max_term_width = 100)]
enum Cli {
    /// Upload a file to S3.
    ///
    /// The contents of the file you upload are always streamed, which means the memory usage of
    /// Persevere is minimal, usually below 10 MB. This makes it possible to upload files of any size
    /// supported by S3, even if they are larger than the available memory of your system.
    #[command(subcommand)]
    Upload(upload::Upload),
    /// Download a file from S3.
    ///
    /// The local file will be made the full size required immediately, to make sure you have enough
    /// disk-space for the full download.
    #[command(subcommand)]
    Download(download::Download),
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_file(false)
                .with_line_number(false)
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
        Cli::Download(download) => download.run().await,
    }
}
