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
List Virtual Datasets MCP Tool

Lists all virtual datasets available in the current session.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    ListVirtualDatasetsResponse,
    VirtualDatasetInfo,
)

logger = logging.getLogger(__name__)


@tool(tags=["dataframe"])
async def list_virtual_datasets(ctx: Context) -> ListVirtualDatasetsResponse:
    """
    List all virtual datasets in the current session.

    Virtual datasets are in-memory DataFrames that have been ingested
    via the ingest_dataframe tool. They can be used with generate_chart
    and other visualization tools using the 'virtual:{dataset_id}' format.

    Returns:
        List of virtual datasets with their metadata.
    """
    await ctx.info("Listing virtual datasets")

    try:
        # Get session ID from context
        session_id = getattr(ctx, "session_id", None) or "default_session"

        # Get registry and list datasets
        registry = get_registry()
        datasets_raw = registry.list_datasets(session_id=session_id)

        # Convert to response format
        datasets = []
        for ds in datasets_raw:
            # Get full dataset for column info
            ds_id = ds["id"]
            if not isinstance(ds_id, str):
                continue  # Skip invalid entries

            full_dataset = registry.get(ds_id)
            columns = full_dataset.get_column_info() if full_dataset else []

            # Safe type assertions with defaults
            name = ds.get("name", "")
            row_count = ds.get("row_count", 0)
            column_count = ds.get("column_count", 0)
            size_bytes = ds.get("size_bytes", 0)
            created_at = ds.get("created_at")
            expires_at = ds.get("expires_at")

            # Ensure proper types
            if not isinstance(name, str):
                name = str(name) if name else ""
            if not isinstance(row_count, int):
                row_count = int(str(row_count)) if row_count else 0
            if not isinstance(column_count, int):
                column_count = int(str(column_count)) if column_count else 0
            if not isinstance(size_bytes, int):
                size_bytes = int(str(size_bytes)) if size_bytes else 0
            if not isinstance(created_at, datetime):
                created_at = datetime.now(timezone.utc)

            size_mb = round(size_bytes / 1024 / 1024, 2)

            datasets.append(
                VirtualDatasetInfo(
                    id=ds_id,
                    name=name,
                    row_count=row_count,
                    column_count=column_count,
                    size_bytes=size_bytes,
                    size_mb=size_mb,
                    created_at=created_at,
                    expires_at=expires_at if isinstance(expires_at, datetime) else None,
                    columns=columns,
                )
            )

        total_size_mb = sum(ds.size_mb for ds in datasets)

        await ctx.info(
            "Found %d virtual datasets, total size: %.2f MB"
            % (len(datasets), total_size_mb)
        )

        return ListVirtualDatasetsResponse(
            datasets=datasets,
            total_count=len(datasets),
            total_size_mb=round(total_size_mb, 2),
        )

    except Exception as e:
        logger.exception("Error listing virtual datasets: %s", e)
        await ctx.error("Failed to list virtual datasets: %s" % str(e))
        return ListVirtualDatasetsResponse(
            datasets=[],
            total_count=0,
            total_size_mb=0.0,
        )
