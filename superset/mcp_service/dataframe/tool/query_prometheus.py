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
Prometheus Query MCP Tool

Query Prometheus via HTTP API and optionally ingest results as a virtual dataset.
"""

from __future__ import annotations

import logging
import math
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pyarrow as pa
from fastmcp import Context
from superset_core.mcp import tool

from superset.mcp_service.dataframe.identifiers import to_virtual_dataset_id
from superset.mcp_service.dataframe.registry import get_registry
from superset.mcp_service.dataframe.schemas import (
    DataFrameSourceCapability,
    PrometheusQueryRequest,
    PrometheusQueryResponse,
)
from superset.mcp_service.dataframe.tool.common import (
    build_virtual_dataset_info,
    convert_value,
    table_to_columns,
)
from superset.mcp_service.dataframe.tool.context import resolve_session_and_user
from superset.mcp_service.dataframe.tool.source_adapters import (
    PROMETHEUS_SOURCE_CAPABILITY,
)
from superset.mcp_service.utils.schema_utils import parse_request
from superset.utils import json

logger = logging.getLogger(__name__)


def _to_timestamp(value: datetime) -> float:
    """Convert datetime to a UTC unix timestamp."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp()


def _format_timestamp(value: Any) -> str:
    """Format Prometheus timestamp values as ISO-8601 UTC strings."""
    try:
        ts = float(value)
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (TypeError, ValueError):
        return str(value)


def _parse_prometheus_scalar(value: Any) -> str | int | float | bool | None:
    """Parse Prometheus scalar text values into JSON-safe primitives."""
    parsed = convert_value(value)
    if isinstance(parsed, float):
        if not math.isfinite(parsed):
            return str(value)
    return parsed


def _flatten_prometheus_result(
    result_type: str,
    result: Any,
) -> list[dict[str, str | int | float | bool | None]]:
    """Flatten Prometheus result payload into row records."""
    rows: list[dict[str, str | int | float | bool | None]] = []
    if result_type == "matrix":
        if not isinstance(result, list):
            return rows
        for series in result:
            if not isinstance(series, dict):
                continue
            metric = series.get("metric", {})
            metric_labels = {str(k): str(v) for k, v in metric.items()}
            for point in series.get("values", []):
                if not isinstance(point, list | tuple) or len(point) != 2:
                    continue
                rows.append(
                    {
                        **metric_labels,
                        "timestamp": _format_timestamp(point[0]),
                        "value": _parse_prometheus_scalar(point[1]),
                    }
                )
        return rows

    if result_type == "vector":
        if not isinstance(result, list):
            return rows
        for series in result:
            if not isinstance(series, dict):
                continue
            metric = series.get("metric", {})
            metric_labels = {str(k): str(v) for k, v in metric.items()}
            point = series.get("value", [])
            if isinstance(point, list | tuple) and len(point) == 2:
                rows.append(
                    {
                        **metric_labels,
                        "timestamp": _format_timestamp(point[0]),
                        "value": _parse_prometheus_scalar(point[1]),
                    }
                )
        return rows

    if result_type in {"scalar", "string"}:
        if isinstance(result, list | tuple) and len(result) == 2:
            rows.append(
                {
                    "timestamp": _format_timestamp(result[0]),
                    "value": _parse_prometheus_scalar(result[1]),
                }
            )
        return rows

    return rows


@tool(tags=["dataframe", "source"])
@parse_request(PrometheusQueryRequest)
async def query_prometheus(
    request: PrometheusQueryRequest,
    ctx: Context,
) -> PrometheusQueryResponse:
    """
    Execute a PromQL query against Prometheus.

    For range queries, results are flattened into one row per sample with label
    columns, a UTC timestamp column, and a value column.
    """
    await ctx.info(
        "Querying Prometheus: query_type=%s, ingest=%s"
        % (request.query_type, request.ingest_as_virtual_dataset)
    )

    try:
        base_url = request.base_url.rstrip("/")
        if request.query_type == "range":
            endpoint = f"{base_url}/api/v1/query_range"
            end_time = request.end_time or datetime.now(timezone.utc)
            start_time = request.start_time or (end_time - timedelta(hours=1))
            params = {
                "query": request.promql,
                "start": _to_timestamp(start_time),
                "end": _to_timestamp(end_time),
                "step": request.step_seconds,
            }
        else:
            endpoint = f"{base_url}/api/v1/query"
            time_value = request.end_time or datetime.now(timezone.utc)
            params = {
                "query": request.promql,
                "time": _to_timestamp(time_value),
            }

        params["timeout"] = f"{request.timeout_seconds}s"
        url = f"{endpoint}?{urlencode(params)}"

        await ctx.debug("Prometheus request URL prepared")

        ssl_context = ssl.create_default_context()
        if not request.verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        req = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(
                req,
                timeout=request.timeout_seconds,
                context=ssl_context,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as ex:
            logger.error("Prometheus HTTP error: %s", ex)
            return PrometheusQueryResponse(
                success=False,
                error=f"Prometheus HTTP error: {ex.code}",
                error_code="PROMETHEUS_HTTP_ERROR",
            )
        except URLError as ex:
            logger.error("Prometheus connection error: %s", ex)
            return PrometheusQueryResponse(
                success=False,
                error=f"Prometheus connection error: {ex}",
                error_code="PROMETHEUS_CONNECTION_ERROR",
            )

        if payload.get("status") != "success":
            error_message = payload.get("error") or "Prometheus query failed"
            return PrometheusQueryResponse(
                success=False,
                error=str(error_message),
                error_code="PROMETHEUS_QUERY_FAILED",
            )

        data = payload.get("data", {})
        result_type = str(data.get("resultType") or "")
        result = data.get("result", [])

        rows = _flatten_prometheus_result(result_type, result)
        table = pa.Table.from_pylist(rows) if rows else pa.Table.from_pylist([])
        columns = table_to_columns(table) if rows else []

        response = PrometheusQueryResponse(
            success=True,
            result_type=result_type,
            rows=rows,
            columns=columns,
            row_count=len(rows),
            source_capabilities=[
                DataFrameSourceCapability.model_validate(
                    PROMETHEUS_SOURCE_CAPABILITY.__dict__
                )
            ],
        )

        if request.ingest_as_virtual_dataset:
            session_id, user_id = resolve_session_and_user(ctx)
            if not session_id:
                response.warning = (
                    "Prometheus query succeeded, but ingestion was skipped due to "
                    "missing session/user context"
                )
                return response

            dataset_name = request.dataset_name or "prometheus_query_result"
            ttl = (
                timedelta(minutes=request.ttl_minutes)
                if request.ttl_minutes > 0
                else timedelta(seconds=0)
            )
            registry = get_registry()
            dataset_id = registry.register(
                name=dataset_name,
                table=table,
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
        logger.exception("Prometheus query tool failed: %s", ex)
        await ctx.error("Prometheus query failed: %s" % str(ex))
        return PrometheusQueryResponse(
            success=False,
            error=f"Unexpected error: {ex}",
            error_code="UNEXPECTED_ERROR",
        )
