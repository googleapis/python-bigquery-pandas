# Copyright (c) 2021 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from unittest import mock

from pandas import DataFrame
import pytest

from pandas_gbq import gbq


def mock_get_credentials_no_project(*args, **kwargs):
    import google.auth.credentials

    mock_credentials = mock.create_autospec(google.auth.credentials.Credentials)
    return mock_credentials, None


def test_to_gbq_create_dataset_with_location(mock_bigquery_client):
    import google.api_core.exceptions

    mock_bigquery_client.get_table.side_effect = google.api_core.exceptions.NotFound(
        "my_table"
    )
    mock_bigquery_client.get_dataset.side_effect = google.api_core.exceptions.NotFound(
        "my_dataset"
    )
    gbq.to_gbq(
        DataFrame([[1]]), "my_dataset.my_table", project_id="1234", location="us-west1"
    )
    assert mock_bigquery_client.create_dataset.called
    args, _ = mock_bigquery_client.create_dataset.call_args
    sent_dataset = args[0]
    assert sent_dataset.location == "us-west1"


def test_to_gbq_create_dataset_translates_exception(mock_bigquery_client):
    import google.api_core.exceptions

    mock_bigquery_client.get_table.side_effect = google.api_core.exceptions.NotFound(
        "my_table"
    )
    mock_bigquery_client.get_dataset.side_effect = google.api_core.exceptions.NotFound(
        "my_dataset"
    )
    mock_bigquery_client.create_dataset.side_effect = google.api_core.exceptions.InternalServerError(
        "something went wrong"
    )

    with pytest.raises(gbq.GenericGBQException):
        gbq.to_gbq(DataFrame([[1]]), "my_dataset.my_table", project_id="1234")
