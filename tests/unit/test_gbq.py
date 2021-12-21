# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# -*- coding: utf-8 -*-

from unittest import mock

import numpy
import pandas
from pandas import DataFrame
import pytest

from pandas_gbq import gbq
from pandas_gbq.features import FEATURES


pytestmark = pytest.mark.filterwarnings("ignore:credentials from Google Cloud SDK")


def _make_connector(project_id="some-project", **kwargs):
    return gbq.GbqConnector(project_id, **kwargs)


def mock_get_credentials_no_project(*args, **kwargs):
    import google.auth.credentials

    mock_credentials = mock.create_autospec(google.auth.credentials.Credentials)
    return mock_credentials, None


@pytest.mark.parametrize(
    ("type_", "expected"),
    [
        ("SOME_NEW_UNKNOWN_TYPE", None),
        ("INTEGER", "Int64"),
        ("FLOAT", numpy.dtype(float)),
        # TIMESTAMP will be localized after DataFrame construction.
        ("TIMESTAMP", "datetime64[ns]"),
        ("DATETIME", "datetime64[ns]"),
    ],
)
def test__bqschema_to_nullsafe_dtypes(type_, expected):
    result = gbq._bqschema_to_nullsafe_dtypes(
        [dict(name="x", type=type_, mode="NULLABLE")]
    )
    if not expected:
        assert result == {}
    else:
        assert result == {"x": expected}


@pytest.mark.parametrize(
    ["query_or_table", "expected"],
    [
        ("SELECT 1", True),
        ("SELECT\n1", True),
        ("SELECT\t1", True),
        ("dataset.table", False),
        (" dataset.table ", False),
        ("\r\ndataset.table\r\n", False),
        ("project-id.dataset.table", False),
        (" project-id.dataset.table ", False),
        ("\r\nproject-id.dataset.table\r\n", False),
    ],
)
def test__is_query(query_or_table, expected):
    result = gbq._is_query(query_or_table)
    assert result == expected


def test_GbqConnector_get_client_w_old_bq(monkeypatch, mock_bigquery_client):
    gbq._test_google_api_imports()
    connector = _make_connector()
    monkeypatch.setattr(
        type(FEATURES),
        "bigquery_has_client_info",
        mock.PropertyMock(return_value=False),
    )

    connector.get_client()

    # No client_info argument.
    mock_bigquery_client.assert_called_with(credentials=mock.ANY, project=mock.ANY)


def test_GbqConnector_get_client_w_new_bq(mock_bigquery_client):
    gbq._test_google_api_imports()
    if not FEATURES.bigquery_has_client_info:
        pytest.skip("google-cloud-bigquery missing client_info feature")
    pytest.importorskip("google.api_core.client_info")

    connector = _make_connector()
    connector.get_client()

    _, kwargs = mock_bigquery_client.call_args
    assert kwargs["client_info"].user_agent == "pandas-{}".format(pandas.__version__)


def test_generate_bq_schema_deprecated():
    # 11121 Deprecation of generate_bq_schema
    with pytest.warns(FutureWarning):
        df = DataFrame([[1, "two"], [3, "four"]])
        gbq.generate_bq_schema(df)
