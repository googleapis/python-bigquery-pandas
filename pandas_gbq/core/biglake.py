# Copyright (c) 2026 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from __future__ import annotations

import dataclasses

import google.auth.transport.requests
import google.oauth2.credentials

_ICEBERG_REST_CATALOG_URI = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
_TABLE_METADATA_PATH = (
    "/v1/projects/{project}/catalogs/{catalog}/namespaces/{namespace}/tables/{table}"
)


@dataclasses.dataclass(frozen=True)
class BigLakeTableId:
    project: str
    catalog: str
    namespace: str
    table: str


def get_table_metadata(
    *,
    table_id: str,
    credentials: google.oauth2.credentials.Credentials,
    billing_project_id: str,
):
    """
    Docstring for get_table_metadata

    https://iceberg.apache.org/spec/#metrics;

     curl -X GET -H "Authorization: Bearer \"$(gcloud auth application-default print-access-token)\"" \
              -H "Content-Type: application/json; charset=utf-8" \
    -H 'x-goog-user-project: swast-scratch' \
    -H 'X-Iceberg-Access-Delegation: vended-credentials' \
    """
    # https://iceberg.apache.org/spec/#metrics
    # total-files-size
    project, catalog, namespace, table = table_id.split(".")
    session = google.auth.transport.requests.AuthorizedSession(credentials=credentials)
    path = _TABLE_METADATA_PATH.format(
        project=project,
        catalog=catalog,
        namespace=namespace,
        table=table,
    )
    return session.get(
        f"{_ICEBERG_REST_CATALOG_URI}.{path}",
        headers={
            "x-goog-user-project": billing_project_id,
            "Content-Type": "application/json; charset=utf-8",
            # TODO(tswast): parameter for this option (or get from catalog metadata?)
            # /iceberg/{$api_version}/restcatalog/extensions/{name=projects/*/catalogs/*}
            "X-Iceberg-Access-Delegation": "vended-credentials",
        },
    ).json()
