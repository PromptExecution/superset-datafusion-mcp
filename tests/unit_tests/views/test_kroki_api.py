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
from unittest.mock import Mock, patch

import pytest
from requests import RequestException


def test_kroki_render_success(client, full_api_access) -> None:
    with patch("superset.views.api.requests.post") as post_mock:
        post_mock.return_value = Mock(
            status_code=200,
            text='<svg onload="alert(1)"><script>alert(1)</script><rect/></svg>',
        )

        response = client.post(
            "/api/v1/kroki/render/",
            json={
                "diagram_type": "mermaid",
                "diagram_source": "graph TD; A-->B;",
                "output_format": "svg",
            },
        )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload is not None
    assert payload["result"]["diagram_type"] == "mermaid"
    assert payload["result"]["output_format"] == "svg"
    assert "<script" not in payload["result"]["svg"].lower()
    assert "onload=" not in payload["result"]["svg"].lower()


def test_kroki_render_rejects_non_svg_output(client, full_api_access) -> None:
    response = client.post(
        "/api/v1/kroki/render/",
        json={
            "diagram_type": "mermaid",
            "diagram_source": "graph TD; A-->B;",
            "output_format": "png",
        },
    )

    assert response.status_code == 400
    payload = response.get_json()
    assert payload is not None
    assert "output_format" in payload["message"]


@pytest.mark.parametrize(
    "diagram_type",
    [
        "unsupported_type",
        "",
    ],
)
def test_kroki_render_rejects_invalid_diagram_type(
    client, full_api_access, diagram_type: str
) -> None:
    response = client.post(
        "/api/v1/kroki/render/",
        json={
            "diagram_type": diagram_type,
            "diagram_source": "graph TD; A-->B;",
            "output_format": "svg",
        },
    )

    assert response.status_code == 400


def test_kroki_render_handles_sidecar_failure(client, full_api_access) -> None:
    with patch("superset.views.api.requests.post") as post_mock:
        post_mock.side_effect = RequestException("connection refused")

        response = client.post(
            "/api/v1/kroki/render/",
            json={
                "diagram_type": "mermaid",
                "diagram_source": "graph TD; A-->B;",
                "output_format": "svg",
            },
        )

    assert response.status_code == 502
    payload = response.get_json()
    assert payload is not None
    assert "Kroki" in payload["message"]
