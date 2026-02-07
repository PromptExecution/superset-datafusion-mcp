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
Query Virtual Dataset MCP Tool

Allows executing SQL queries against virtual datasets using DuckDB.
"""

from __future__ import annotations

import logging

from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.identifiers import normalize_virtual_dataset_id
from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    QueryVirtualDatasetRequest,
    QueryVirtualDatasetResponse,
)
from superset.mcp_service.dataframe.tool.common import (
    normalize_sql,
    table_to_columns,
    table_to_rows,
    validate_read_only_sql,
)
from superset.mcp_service.dataframe.tool.context import resolve_session_and_user
from superset.mcp_service.utils.schema_utils import parse_request

logger = logging.getLogger(__name__)

@tool(tags=["dataframe"])
@parse_request(QueryVirtualDatasetRequest)
async def query_virtual_dataset(
    request: QueryVirtualDatasetRequest, ctx: Context
) -> QueryVirtualDatasetResponse:
    """
    Execute a SQL query against a virtual dataset.

    This tool uses DuckDB to execute SQL queries against in-memory
    virtual datasets. The virtual dataset is available as the 'data' table.

    Example queries:
    - SELECT * FROM data LIMIT 10
    - SELECT region, SUM(sales) as total FROM data GROUP BY region
    - SELECT date, sales FROM data WHERE sales > 100 ORDER BY date

    Args:
        dataset_id: The virtual dataset ID (raw UUID or virtual:{uuid})
        sql: SQL query to execute
        limit: Maximum rows to return (default 1000)

    Returns:
        Query results as rows with column metadata.
    """
    await ctx.info(
        "Querying virtual dataset: dataset_id=%s, limit=%d"
        % (request.dataset_id, request.limit)
    )

    try:
        # Get the virtual dataset
        registry = get_registry()

        # Determine session and user context
        session_id, user_id = resolve_session_and_user(ctx)

        if not session_id:
            logger.warning(
                "Missing session_id and user_id; refusing to query virtual dataset "
                "for dataset_id=%s",
                request.dataset_id,
            )
            return QueryVirtualDatasetResponse(
                success=False,
                error=(
                    "Cannot query virtual dataset: missing session and user "
                    "context. Please retry after re-authenticating."
                ),
            )
        dataset = registry.get(
            normalize_virtual_dataset_id(request.dataset_id),
            session_id=session_id,
            user_id=user_id,
        )

        if dataset is None:
            return QueryVirtualDatasetResponse(
                success=False,
                error=(
                    f"Virtual dataset '{request.dataset_id}' not found, expired, "
                    "or access denied"
                ),
            )

        # Try to use DuckDB for SQL execution
        try:
            import duckdb
        except ImportError:
            logger.error("DuckDB not installed - required for virtual dataset queries")
            return QueryVirtualDatasetResponse(
                success=False,
                error=(
                    "DuckDB is required for virtual dataset queries. "
                    "Install with: pip install duckdb"
                ),
            )

        validation_error = validate_read_only_sql(request.sql)
        if validation_error:
            return QueryVirtualDatasetResponse(success=False, error=validation_error)

        # Always enforce an outer LIMIT to cap the result size.
        normalized_sql = normalize_sql(request.sql)
        sql = f"SELECT * FROM ({normalized_sql}) AS subq LIMIT {request.limit}"

        # Create DuckDB connection and register the table
        conn = duckdb.connect()
        conn.register("data", dataset.table)

        await ctx.debug("Executing SQL: %s" % sql[:200])

        try:
            result = conn.execute(sql)
            result_table = result.arrow()
        except duckdb.Error as e:
            logger.error("DuckDB query error: %s", e)
            return QueryVirtualDatasetResponse(
                success=False,
                error=f"SQL execution error: {str(e)}",
            )
        finally:
            conn.close()

        # Convert to response format
        rows = table_to_rows(result_table)
        columns = table_to_columns(result_table)

        await ctx.info(
            "Query completed: rows=%d, columns=%d" % (len(rows), len(columns))
        )

        return QueryVirtualDatasetResponse(
            success=True,
            rows=rows,
            columns=columns,
            row_count=len(rows),
        )

    except Exception as e:
        logger.exception("Error querying virtual dataset: %s", e)
        await ctx.error("Query failed: %s" % str(e))
        return QueryVirtualDatasetResponse(
            success=False,
            error=f"Unexpected error: {str(e)}",
        )
