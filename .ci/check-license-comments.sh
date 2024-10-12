#!/bin/bash
# Copyright 2024 TAKKT Industrial & Packaging GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

PATTERN_COPYRIGHT='^(//|#) Copyright [[:digit:]]+ TAKKT Industrial & Packaging GmbH'
PATTERN_SPDX='^(//|#) SPDX-License-Identifier: Apache-2.0'
ERRORS=0

while read -r -d $'\0' file
do
  if ! grep -qE "${PATTERN_COPYRIGHT}" "$file"; then
    echo "$file: missing/malformed copyright-notice"
    ERRORS=$((ERRORS + 1))
  fi
  if ! grep -qE "${PATTERN_SPDX}" "$file"; then
    echo "$file: missing/malformed SPDX license identifier"
    ERRORS=$((ERRORS + 1))
  fi
done < <(\
  git ls-files -z -- \
    '*.rs' \
    '*.sh' \
)

if [[ "$ERRORS" -gt 0 ]]; then
  exit 1
fi
