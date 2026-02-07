# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Helpers for handling MCP virtual dataset identifiers.
"""

from __future__ import annotations

VIRTUAL_DATASET_PREFIX = "virtual:"


def is_virtual_dataset_identifier(dataset_id: int | str) -> bool:
    """Return True when dataset_id uses the virtual:{uuid} prefix."""
    return isinstance(dataset_id, str) and dataset_id.startswith(
        VIRTUAL_DATASET_PREFIX
    )


def normalize_virtual_dataset_id(dataset_id: str) -> str:
    """Normalize dataset IDs by removing the optional virtual: prefix."""
    if dataset_id.startswith(VIRTUAL_DATASET_PREFIX):
        return dataset_id[len(VIRTUAL_DATASET_PREFIX) :]
    return dataset_id


def extract_virtual_dataset_id(dataset_id: int | str) -> str | None:
    """Extract raw virtual dataset UUID when dataset_id is prefixed."""
    if not is_virtual_dataset_identifier(dataset_id):
        return None
    if not isinstance(dataset_id, str):
        return None
    normalized = normalize_virtual_dataset_id(dataset_id).strip()
    return normalized or None


def to_virtual_dataset_id(dataset_id: str) -> str:
    """Create a virtual:{uuid} identifier from a raw virtual dataset UUID."""
    normalized = normalize_virtual_dataset_id(dataset_id)
    return f"{VIRTUAL_DATASET_PREFIX}{normalized}"
