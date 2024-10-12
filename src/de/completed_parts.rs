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

use aws_sdk_s3::types::CompletedPart;
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    Serializer,
};

#[derive(Deserialize, Serialize)]
struct CompletedPartRemote {
    e_tag: Option<String>,
    checksum_crc32: Option<String>,
    checksum_crc32_c: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
    part_number: Option<i32>,
}

impl From<CompletedPart> for CompletedPartRemote {
    fn from(part: CompletedPart) -> Self {
        Self {
            e_tag: part.e_tag,
            checksum_crc32: part.checksum_crc32,
            checksum_crc32_c: part.checksum_crc32_c,
            checksum_sha1: part.checksum_sha1,
            checksum_sha256: part.checksum_sha256,
            part_number: part.part_number,
        }
    }
}

impl From<CompletedPartRemote> for CompletedPart {
    fn from(part: CompletedPartRemote) -> Self {
        Self::builder()
            .set_checksum_crc32(part.checksum_crc32)
            .set_checksum_crc32_c(part.checksum_crc32_c)
            .set_checksum_sha1(part.checksum_sha1)
            .set_checksum_sha256(part.checksum_sha256)
            .set_e_tag(part.e_tag)
            .set_part_number(part.part_number)
            .build()
    }
}

pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<CompletedPart>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Vec<CompletedPartRemote> = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(Into::into).collect())
}

pub(crate) fn serialize<S>(parts: &[CompletedPart], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    parts
        .iter()
        .map(ToOwned::to_owned)
        .map(Into::<CompletedPartRemote>::into)
        .collect::<Vec<_>>()
        .serialize(serializer)
}
