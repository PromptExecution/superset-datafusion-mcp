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
DataFusion Query MCP Tool

Execute SQL against DataFusion with Parquet, Arrow IPC, and virtual dataset
sources.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pyarrow as pa
from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.identifiers import to_virtual_dataset_id
from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    DataFrameSourceCapability,
    DataFusionQueryRequest,
    DataFusionQueryResponse,
)
from superset.mcp_service.dataframe.tool.common import (
    build_virtual_dataset_info,
    normalize_sql,
    table_to_columns,
    table_to_rows,
    validate_read_only_sql,
)
from superset.mcp_service.dataframe.tool.context import resolve_session_and_user
from superset.mcp_service.dataframe.tool.source_adapters import (
    get_datafusion_source_adapter,
)
from superset.mcp_service.utils.schema_utils import parse_request

logger = logging.getLogger(__name__)


@tool(tags=["dataframe", "source"])
@parse_request(DataFusionQueryRequest)
async def query_datafusion(
    request: DataFusionQueryRequest,
    ctx: Context,
) -> DataFusionQueryResponse:
    """Execute SQL using DataFusion across registered Parquet/Arrow sources."""
    await ctx.info(
        "Executing DataFusion query: sources=%d, ingest=%s"
        % (len(request.sources), request.ingest_result)
    )

    try:
        try:
            from datafusion import SessionContext
        except ImportError:
            return DataFusionQueryResponse(
                success=False,
                error=(
                    "DataFusion is required for this tool. Install with "
                    "`pip install datafusion`."
                ),
                error_code="DATAFUSION_NOT_INSTALLED",
            )

        validation_error = validate_read_only_sql(request.sql)
        if validation_error:
            return DataFusionQueryResponse(
                success=False,
                error=validation_error,
                error_code="INVALID_SQL",
            )

        session_id, user_id = resolve_session_and_user(ctx)
        registry = get_registry()
        source_capabilities: list[DataFrameSourceCapability] = []

        session_ctx = SessionContext()
        for source in request.sources:
            adapter = get_datafusion_source_adapter(source.source_type)
            if adapter is None:
                return DataFusionQueryResponse(
                    success=False,
                    error=f"Unsupported source_type '{source.source_type}'",
                    error_code="UNSUPPORTED_SOURCE_TYPE",
                    source_capabilities=source_capabilities,
                )
            try:
                adapter.register_source(
                    session_ctx=session_ctx,
                    source=source,
                    registry=registry,
                    session_id=session_id,
                    user_id=user_id,
                )
            except ValueError as ex:
                error_code = "INVALID_SOURCE_CONFIG"
                if source.source_type == "virtual_dataset":
                    error_code = "VIRTUAL_DATASET_NOT_FOUND"
                return DataFusionQueryResponse(
                    success=False,
                    error=str(ex),
                    error_code=error_code,
                    source_capabilities=source_capabilities,
                )
            source_capabilities.append(
                DataFrameSourceCapability.model_validate(adapter.capability.__dict__)
            )

        normalized_sql = normalize_sql(request.sql)
        sql = f"SELECT * FROM ({normalized_sql}) AS subq LIMIT {request.limit}"
        await ctx.debug("Executing DataFusion SQL: %s" % sql[:200])

        query_df = session_ctx.sql(sql)
        result_batches = query_df.collect()
        result_table = (
            pa.Table.from_batches(result_batches)
            if result_batches
            else pa.Table.from_pylist([])
        )

        rows = table_to_rows(result_table)
        columns = table_to_columns(result_table) if result_table.num_columns > 0 else []

        response = DataFusionQueryResponse(
            success=True,
            rows=rows,
            columns=columns,
            row_count=len(rows),
            source_capabilities=source_capabilities,
        )

        if request.ingest_result:
            if not session_id:
                response.warning = (
                    "DataFusion query succeeded, but result ingestion was skipped due "
                    "to missing session/user context"
                )
                return response

            result_name = request.result_dataset_name or "datafusion_query_result"
            ttl = (
                timedelta(minutes=request.ttl_minutes)
                if request.ttl_minutes > 0
                else timedelta(seconds=0)
            )
            dataset_id = registry.register(
                name=result_name,
                table=result_table,
                session_id=session_id,
                user_id=user_id,
                ttl=ttl,
                allow_cross_session=request.allow_cross_session,
            )
            dataset = registry.get(dataset_id, session_id=session_id, user_id=user_id)
            if dataset is not None:
                response.dataset = build_virtual_dataset_info(dataset)
                response.dataset_id = dataset_id
                response.virtual_dataset_id = to_virtual_dataset_id(dataset_id)

        return response
    except Exception as ex:
        logger.exception("DataFusion query failed: %s", ex)
        await ctx.error("DataFusion query failed: %s" % str(ex))
        return DataFusionQueryResponse(
            success=False,
            error=f"Unexpected error: {ex}",
            error_code="UNEXPECTED_ERROR",
        )
