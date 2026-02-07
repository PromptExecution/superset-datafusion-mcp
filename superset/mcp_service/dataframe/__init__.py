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
DataFrame MCP Module

Provides DataFrame ingestion and virtual dataset capabilities for the MCP service.
Enables AI agents to create visualizations directly from DataFrame data without
requiring database storage.
"""

from superset.mcp_service.dataframe.registry import (
    get_registry,
    VirtualDataset,
    VirtualDatasetRegistry,
)
from superset.mcp_service.dataframe.schemas import (
    ColumnSchema,
    IngestDataFrameRequest,
    IngestDataFrameResponse,
    VirtualDatasetInfo,
)

__all__ = [
    "VirtualDataset",
    "VirtualDatasetRegistry",
    "get_registry",
    "ColumnSchema",
    "IngestDataFrameRequest",
    "IngestDataFrameResponse",
    "VirtualDatasetInfo",
]
