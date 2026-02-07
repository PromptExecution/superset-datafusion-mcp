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

from superset.mcp_service.chart.virtual_dataset_bridge import (
    build_virtual_dataset_query,
)


def test_build_virtual_dataset_query_raw_mode() -> None:
    """Raw mode query selects configured columns with row limit."""
    sql = build_virtual_dataset_query(
        {
            "query_mode": "raw",
            "all_columns": ["region", "sales"],
        },
        limit=50,
    )
    assert 'SELECT "region", "sales" FROM data' in sql
    assert "LIMIT 50" in sql


def test_build_virtual_dataset_query_aggregate_with_filters() -> None:
    """Aggregate mode query applies dimensions, metrics, and filters."""
    sql = build_virtual_dataset_query(
        {
            "x_axis": "region",
            "metrics": [
                {
                    "aggregate": "SUM",
                    "column": {"column_name": "sales"},
                    "label": "Total Sales",
                }
            ],
            "adhoc_filters": [
                {
                    "expressionType": "SIMPLE",
                    "subject": "status",
                    "operator": "==",
                    "comparator": "active",
                }
            ],
        },
        limit=100,
    )
    assert 'SELECT "region", SUM("sales") AS "Total Sales" FROM data' in sql
    assert "WHERE \"status\" = 'active'" in sql
    assert 'GROUP BY "region"' in sql
    assert "LIMIT 100" in sql
