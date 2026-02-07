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
Ingest DataFrame MCP Tool

Allows AI agents to upload DataFrame data directly as a virtual dataset
that can be used for chart and dashboard generation.
"""

from __future__ import annotations

import base64
import logging
from datetime import timedelta

import pyarrow as pa
from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    IngestDataFrameRequest,
    IngestDataFrameResponse,
    VirtualDatasetInfo,
)
from superset.mcp_service.utils.schema_utils import parse_request
from superset.utils.core import get_user_id

logger = logging.getLogger(__name__)


@tool(tags=["dataframe", "mutate"])
@parse_request(IngestDataFrameRequest)
async def ingest_dataframe(
    request: IngestDataFrameRequest, ctx: Context
) -> IngestDataFrameResponse:
    """
    Ingest a DataFrame from Arrow IPC format for visualization.

    This tool allows AI agents to upload DataFrame data directly without
    requiring database storage. The data is registered as a virtual dataset
    that can be used with generate_chart and other visualization tools.

    IMPORTANT: Use 'virtual:{dataset_id}' format when referencing the
    dataset in generate_chart or other tools.

    Example workflow (Python):
    ```python
    import pyarrow as pa
    import base64

    # 1. Create or load your data as an Arrow table
    table = pa.table({
        'date': pd.date_range('2024-01-01', periods=30),
        'sales': [100 + i * 10 for i in range(30)],
        'region': ['North'] * 15 + ['South'] * 15
    })

    # 2. Serialize to Arrow IPC format
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    data = base64.b64encode(sink.getvalue().to_pybytes()).decode()

    # 3. Ingest via MCP
    result = await ingest_dataframe({
        "name": "sales_analysis",
        "data": data,
        "ttl_minutes": 60
    })

    # 4. Use with generate_chart
    chart = await generate_chart({
        "dataset_id": f"virtual:{result.dataset_id}",
        "config": {
            "chart_type": "xy",
            "x": {"name": "date"},
            "y": [{"name": "sales", "aggregate": "SUM"}],
            "kind": "line"
        }
    })
    ```

    Returns:
        Virtual dataset info including dataset_id for use with other tools.
    """
    await ctx.info(
        "Ingesting DataFrame: name=%s, ttl_minutes=%s"
        % (request.name, request.ttl_minutes)
    )

    try:
        registry = get_registry()

        estimated_bytes = (len(request.data) * 3) // 4
        if estimated_bytes > registry.max_size_bytes:
            return IngestDataFrameResponse(
                success=False,
                error=(
                    "Dataset payload exceeds size limit "
                    f"({registry.max_size_bytes / 1024 / 1024:.2f} MB)"
                ),
                error_code="PAYLOAD_TOO_LARGE",
            )

        # Decode base64 data
        try:
            raw_data = base64.b64decode(request.data)
        except Exception as e:
            logger.error("Failed to decode base64 data: %s", e)
            return IngestDataFrameResponse(
                success=False,
                error="Invalid base64 encoding in data field",
                error_code="INVALID_BASE64",
            )

        # Parse Arrow IPC stream
        try:
            reader = pa.ipc.open_stream(pa.BufferReader(raw_data))
            table = reader.read_all()
        except Exception as e:
            logger.error("Failed to parse Arrow IPC data: %s", e)
            return IngestDataFrameResponse(
                success=False,
                error=f"Invalid Arrow IPC format: {str(e)}",
                error_code="INVALID_ARROW_IPC",
            )

        await ctx.debug(
            "Parsed Arrow table: rows=%d, columns=%d"
            % (table.num_rows, table.num_columns)
        )

        # Get session ID from context
        session_id = getattr(ctx, "session_id", None) or "default_session"
        try:
            user_id = get_user_id()
        except Exception:
            user_id = None

        # Calculate TTL
        ttl = (
            timedelta(minutes=request.ttl_minutes) if request.ttl_minutes > 0 else None
        )

        # Register with the virtual dataset registry
        try:
            dataset_id = registry.register(
                name=request.name,
                table=table,
                session_id=session_id,
                user_id=user_id,
                ttl=ttl,
                allow_cross_session=request.allow_cross_session,
            )
        except ValueError as e:
            logger.error("Failed to register virtual dataset: %s", e)
            return IngestDataFrameResponse(
                success=False,
                error=str(e),
                error_code="REGISTRATION_FAILED",
            )

        # Retrieve the registered dataset for response
        dataset = registry.get(dataset_id, session_id=session_id, user_id=user_id)
        if dataset is None:
            return IngestDataFrameResponse(
                success=False,
                error="Dataset registration succeeded but retrieval failed",
                error_code="INTERNAL_ERROR",
            )

        # Build response
        dataset_info = VirtualDatasetInfo(
            id=dataset.id,
            name=dataset.name,
            row_count=dataset.row_count,
            column_count=len(dataset.column_names),
            size_bytes=dataset.size_bytes,
            size_mb=round(dataset.size_bytes / 1024 / 1024, 2),
            created_at=dataset.created_at,
            expires_at=dataset.expires_at,
            columns=dataset.get_column_info(),
            description=request.description,
        )

        await ctx.info(
            "DataFrame ingested successfully: dataset_id=%s, rows=%d, size_mb=%.2f"
            % (
                dataset_id,
                dataset.row_count,
                dataset_info.size_mb,
            )
        )

        return IngestDataFrameResponse(
            success=True,
            dataset=dataset_info,
            dataset_id=dataset_id,
            usage_hint=(
                f"Use 'virtual:{dataset_id}' as the dataset_id in generate_chart "
                "or other visualization tools."
            ),
        )

    except Exception as e:
        logger.exception("Unexpected error ingesting DataFrame: %s", e)
        await ctx.error("DataFrame ingestion failed: %s" % str(e))
        return IngestDataFrameResponse(
            success=False,
            error=f"Unexpected error: {str(e)}",
            error_code="UNEXPECTED_ERROR",
        )
