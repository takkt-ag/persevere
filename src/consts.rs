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

#[allow(non_upper_case_globals)]
pub(crate) const KiB: u64 = 1024;
#[allow(non_upper_case_globals)]
pub(crate) const MiB: u64 = 1024 * KiB;
#[allow(non_upper_case_globals)]
pub(crate) const GiB: u64 = 1024 * MiB;
#[allow(non_upper_case_globals)]
pub(crate) const TiB: u64 = 1024 * GiB;

/// Maximum object size: 5 TiB
///
/// Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const MAXIMUM_OBJECT_SIZE: u64 = 5 * TiB;

/// Maximum number of parts per upload: 10,000
///
/// Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const MAXIMUM_NUMBER_OF_PARTS: u64 = 10_000;

/// Part numbers: 1 to 10,000 (inclusive)
///
/// Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const MINIMUM_PART_NUMBER: u64 = 1;
/// Part numbers: 1 to 10,000 (inclusive)
///
/// Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const MAXIMUM_PART_NUMBER: u64 = 10_000;

/// Minimum part size: 5 MiB
///
/// There is no minimum size limit on the last part of your multipart upload.
///
/// Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const MINIMUM_PART_SIZE: u64 = 5 * MiB;

/// Maximum part size: 5 GiB
///
/// Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
pub(crate) const MAXIMUM_PART_SIZE: u64 = 5 * GiB;
