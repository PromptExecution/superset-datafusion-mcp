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
            full_dataset = registry.get(ds["id"])
            columns = full_dataset.get_column_info() if full_dataset else []

            datasets.append(
                VirtualDatasetInfo(
                    id=ds["id"],
                    name=ds["name"],
                    row_count=ds["row_count"],
                    column_count=ds["column_count"],
                    size_bytes=ds["size_bytes"],
                    size_mb=round(ds["size_bytes"] / 1024 / 1024, 2),
                    created_at=ds["created_at"],
                    expires_at=ds["expires_at"],
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
