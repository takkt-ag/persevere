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

use std::fmt::{
    Display,
    Formatter,
};

macro_rules! bail {
    ($($tt:tt)*) => {
        return Err(anyhow::anyhow!($($tt)*)).into_unrecoverable()
    };
}
pub(crate) use bail;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub(crate) enum Error {
    Retryable(anyhow::Error),
    Unrecoverable(anyhow::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Retryable(err) => write!(f, "Retryable error: {}", err),
            Error::Unrecoverable(err) => write!(f, "Unrecoverable error: {}", err),
        }
    }
}

pub(crate) trait StdResultExt<T, E> {
    fn into_retryable(self) -> Result<T, Error>;

    fn into_unrecoverable(self) -> Result<T, Error>;
}

impl<T, E> StdResultExt<T, E> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn into_retryable(self) -> Result<T, Error> {
        self.map_err(|err| Error::Retryable(anyhow::Error::new(err)))
    }

    fn into_unrecoverable(self) -> Result<T, Error> {
        self.map_err(|err| Error::Unrecoverable(anyhow::Error::new(err)))
    }
}

pub(crate) trait AnyhowResultExt<T> {
    fn into_retryable(self) -> Result<T, Error>;

    fn into_unrecoverable(self) -> Result<T, Error>;
}

impl<T> AnyhowResultExt<T> for std::result::Result<T, anyhow::Error> {
    fn into_retryable(self) -> Result<T, Error> {
        self.map_err(Error::Retryable)
    }

    fn into_unrecoverable(self) -> Result<T, Error> {
        self.map_err(Error::Unrecoverable)
    }
}
