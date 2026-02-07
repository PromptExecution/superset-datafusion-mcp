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
from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING
from urllib.parse import urljoin

import requests
from flask import current_app, request
from flask_appbuilder import expose
from flask_appbuilder.api import rison
from flask_appbuilder.security.decorators import has_access_api
from flask_babel import lazy_gettext as _
from requests import RequestException

from superset import db, event_logger
from superset.commands.chart.exceptions import (
    TimeRangeAmbiguousError,
    TimeRangeParseFailError,
)
from superset.legacy import update_time_range
from superset.models.slice import Slice
from superset.superset_typing import FlaskResponse
from superset.utils import json
from superset.utils.date_parser import get_since_until
from superset.utils.core import sanitize_svg_content
from superset.views.base import api, BaseSupersetView
from superset.views.error_handling import handle_api_exception

if TYPE_CHECKING:
    from superset.common.query_context_factory import QueryContextFactory

logger = logging.getLogger(__name__)

get_time_range_schema = {
    "type": ["string", "array"],
    "items": {
        "type": "object",
        "properties": {
            "timeRange": {"type": "string"},
            "shift": {"type": "string"},
        },
    },
}


class Api(BaseSupersetView):
    query_context_factory = None

    @event_logger.log_this
    @api
    @handle_api_exception
    @has_access_api
    @expose("/v1/query/", methods=("POST",))
    def query(self) -> FlaskResponse:
        """
        Take a query_obj constructed in the client and returns payload data response
        for the given query_obj.

        raises SupersetSecurityException: If the user cannot access the resource
        """
        query_context = self.get_query_context_factory().create(
            **json.loads(request.form["query_context"])
        )
        query_context.raise_for_access()
        result = query_context.get_payload()
        payload_json = result["queries"]
        return json.dumps(payload_json, default=json.json_int_dttm_ser, ignore_nan=True)

    @event_logger.log_this
    @api
    @handle_api_exception
    @has_access_api
    @expose("/v1/form_data/", methods=("GET",))
    def query_form_data(self) -> FlaskResponse:
        """
        Get the form_data stored in the database for existing slice.
        params: slice_id: integer
        """
        form_data = {}
        if slice_id := request.args.get("slice_id"):
            slc = db.session.query(Slice).filter_by(id=slice_id).one_or_none()
            if slc:
                form_data = slc.form_data.copy()

        update_time_range(form_data)

        return self.json_response(form_data)

    @api
    @handle_api_exception
    @has_access_api
    @rison(get_time_range_schema)
    @expose("/v1/time_range/", methods=("GET",))
    def time_range(self, **kwargs: Any) -> FlaskResponse:
        """Get actually time range from human-readable string or datetime expression."""
        time_ranges = kwargs["rison"]
        try:
            if isinstance(time_ranges, str):
                time_ranges = [{"timeRange": time_ranges}]

            rv = []
            for time_range in time_ranges:
                since, until = get_since_until(
                    time_range=time_range["timeRange"],
                    time_shift=time_range.get("shift"),
                )
                rv.append(
                    {
                        "since": since.isoformat() if since else "",
                        "until": until.isoformat() if until else "",
                        "timeRange": time_range["timeRange"],
                        "shift": time_range.get("shift"),
                    }
                )
            return self.json_response({"result": rv})
        except (ValueError, TimeRangeParseFailError, TimeRangeAmbiguousError) as error:
            error_msg = {"message": _("Unexpected time range: %(error)s", error=error)}
            return self.json_response(error_msg, 400)

    @event_logger.log_this
    @api
    @handle_api_exception
    @has_access_api
    @expose("/v1/kroki/render/", methods=("POST",))
    def kroki_render(self) -> FlaskResponse:
        """
        Render a diagram via a Kroki sidecar and return SVG only.
        """
        payload = request.get_json(silent=True) or {}

        diagram_type = str(payload.get("diagram_type", "")).strip().lower()
        diagram_source = payload.get("diagram_source")
        output_format = str(payload.get("output_format", "svg")).strip().lower()

        if output_format != "svg":
            return self.json_response(
                {"message": _("Kroki output_format must be svg.")},
                400,
            )

        if not diagram_type:
            return self.json_response(
                {"message": _("Missing required field: diagram_type.")},
                400,
            )

        if not isinstance(diagram_source, str) or not diagram_source.strip():
            return self.json_response(
                {"message": _("Missing required field: diagram_source.")},
                400,
            )

        max_source_length = int(current_app.config.get("KROKI_MAX_SOURCE_LENGTH", 0))
        if max_source_length > 0 and len(diagram_source) > max_source_length:
            return self.json_response(
                {
                    "message": _(
                        "diagram_source exceeds max length of %(length)s characters.",
                        length=max_source_length,
                    )
                },
                400,
            )

        allowed_diagram_types = {
            diagram.lower()
            for diagram in current_app.config.get("KROKI_ALLOWED_DIAGRAM_TYPES", [])
            if isinstance(diagram, str) and diagram.strip()
        }
        if allowed_diagram_types and diagram_type not in allowed_diagram_types:
            return self.json_response(
                {
                    "message": _(
                        "Unsupported diagram_type: %(diagram_type)s",
                        diagram_type=diagram_type,
                    )
                },
                400,
            )

        kroki_base_url = str(current_app.config.get("KROKI_BASE_URL", "")).strip()
        if not kroki_base_url:
            return self.json_response(
                {"message": _("KROKI_BASE_URL is not configured.")},
                500,
            )

        kroki_timeout = int(current_app.config.get("KROKI_REQUEST_TIMEOUT", 10))
        kroki_url = urljoin(kroki_base_url.rstrip("/") + "/", f"{diagram_type}/svg")

        try:
            response = requests.post(
                kroki_url,
                data=diagram_source.encode("utf-8"),
                headers={
                    "Accept": "image/svg+xml",
                    "Content-Type": "text/plain; charset=utf-8",
                },
                timeout=kroki_timeout,
            )
        except RequestException as ex:
            logger.warning("Kroki sidecar request failed: %s", ex)
            return self.json_response(
                {"message": _("Failed to reach Kroki renderer.")},
                502,
            )

        if response.status_code >= 400:
            logger.warning(
                "Kroki sidecar returned status %s for diagram_type=%s",
                response.status_code,
                diagram_type,
            )
            return self.json_response(
                {"message": _("Kroki failed to render diagram.")},
                502,
            )

        svg = sanitize_svg_content(response.text)
        return self.json_response(
            {
                "result": {
                    "diagram_type": diagram_type,
                    "output_format": "svg",
                    "svg": svg,
                }
            },
            200,
        )

    def get_query_context_factory(self) -> QueryContextFactory:
        if self.query_context_factory is None:
            # pylint: disable=import-outside-toplevel
            from superset.common.query_context_factory import QueryContextFactory

            self.query_context_factory = QueryContextFactory()
        return self.query_context_factory
