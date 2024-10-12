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

use aws_sdk_s3::primitives::ByteStream;
use tokio::io::AsyncRead;

/// A simple decoder that wraps a [`tokio_util::codec::BytesCodec`] and wraps the output in an
/// [`http_body::Frame`].
///
/// This is for use with the AWS `SdkBody` type, which supports dynamic body types only through the
/// `http_body` crate, specifically requiring the data-stream to be `Frame`d.
pub(crate) struct HttpBodyFrameCodec(tokio_util::codec::BytesCodec);

impl tokio_util::codec::Decoder for HttpBodyFrameCodec {
    type Item = http_body::Frame<tokio_util::bytes::Bytes>;
    type Error = std::io::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> anyhow::Result<Option<Self::Item>, tokio::io::Error> {
        self.0
            .decode(src)
            .map(|result| result.map(|bytes_mut| http_body::Frame::data(bytes_mut.into())))
    }
}

/// Extends the [`ByteStream`] type with helper methods.
pub(crate) trait ByteStreamExt {
    /// Creates a new dynamic `ByteStream` from an [`AsyncRead`] instance.
    fn from_reader<R>(reader: R) -> ByteStream
    where
        R: AsyncRead + Send + Sync + 'static;
}

impl ByteStreamExt for ByteStream {
    fn from_reader<R>(reader: R) -> ByteStream
    where
        R: AsyncRead + Send + Sync + 'static,
    {
        let framed_reader = tokio_util::codec::FramedRead::new(
            reader,
            HttpBodyFrameCodec(tokio_util::codec::BytesCodec::new()),
        );
        let stream_body = http_body_util::StreamBody::new(framed_reader);
        ByteStream::from_body_1_x(stream_body)
    }
}
