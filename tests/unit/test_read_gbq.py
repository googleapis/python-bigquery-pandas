# Copyright (c) 2021 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from unittest import mock

import google.api_core.exceptions
import pytest

from pandas_gbq import gbq
from pandas_gbq.features import FEATURES


def mock_get_credentials_no_project(*args, **kwargs):
    import google.auth.credentials

    mock_credentials = mock.create_autospec(google.auth.credentials.Credentials)
    return mock_credentials, None


def test_read_gbq_with_no_project_id_given_should_fail(monkeypatch):
    import pydata_google_auth

    monkeypatch.setattr(pydata_google_auth, "default", mock_get_credentials_no_project)

    with pytest.raises(ValueError, match="Could not determine project ID"):
        gbq.read_gbq("SELECT 1", dialect="standard")


def test_read_gbq_with_inferred_project_id(mock_bigquery_client):
    df = gbq.read_gbq("SELECT 1", dialect="standard")
    assert df is not None
    mock_bigquery_client.query.assert_called_once()


def test_read_gbq_with_inferred_project_id_from_service_account_credentials(
    mock_bigquery_client, mock_service_account_credentials
):
    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "SELECT 1", dialect="standard", credentials=mock_service_account_credentials,
    )
    assert df is not None
    mock_bigquery_client.query.assert_called_once_with(
        "SELECT 1",
        job_config=mock.ANY,
        location=None,
        project="service_account_project_id",
    )


def test_read_gbq_without_inferred_project_id_from_compute_engine_credentials(
    mock_compute_engine_credentials,
):
    with pytest.raises(ValueError, match="Could not determine project ID"):
        gbq.read_gbq(
            "SELECT 1", dialect="standard", credentials=mock_compute_engine_credentials,
        )


def test_read_gbq_with_max_results_zero(monkeypatch):
    df = gbq.read_gbq("SELECT 1", dialect="standard", max_results=0)
    assert df is None


def test_read_gbq_with_max_results_ten(monkeypatch, mock_bigquery_client):
    df = gbq.read_gbq("SELECT 1", dialect="standard", max_results=10)
    assert df is not None
    mock_bigquery_client.list_rows.assert_called_with(mock.ANY, max_results=10)


@pytest.mark.parametrize(["verbose"], [(True,), (False,)])
def test_read_gbq_with_verbose_new_pandas_warns_deprecation(monkeypatch, verbose):
    monkeypatch.setattr(
        type(FEATURES),
        "pandas_has_deprecated_verbose",
        mock.PropertyMock(return_value=True),
    )
    with pytest.warns(FutureWarning, match="verbose is deprecated"):
        gbq.read_gbq("SELECT 1", project_id="my-project", verbose=verbose)


def test_read_gbq_wo_verbose_w_new_pandas_no_warnings(monkeypatch, recwarn):
    monkeypatch.setattr(
        type(FEATURES),
        "pandas_has_deprecated_verbose",
        mock.PropertyMock(return_value=False),
    )
    gbq.read_gbq("SELECT 1", project_id="my-project", dialect="standard")
    assert len(recwarn) == 0


def test_read_gbq_with_old_bq_raises_importerror(monkeypatch):
    import google.cloud.bigquery

    monkeypatch.setattr(google.cloud.bigquery, "__version__", "0.27.0")
    monkeypatch.setattr(FEATURES, "_bigquery_installed_version", None)
    with pytest.raises(ImportError, match="google-cloud-bigquery"):
        gbq.read_gbq(
            "SELECT 1", project_id="my-project",
        )


def test_read_gbq_with_verbose_old_pandas_no_warnings(monkeypatch, recwarn):
    monkeypatch.setattr(
        type(FEATURES),
        "pandas_has_deprecated_verbose",
        mock.PropertyMock(return_value=False),
    )
    gbq.read_gbq(
        "SELECT 1", project_id="my-project", dialect="standard", verbose=True,
    )
    assert len(recwarn) == 0


def test_read_gbq_with_private_raises_notimplmentederror():
    with pytest.raises(NotImplementedError, match="private_key"):
        gbq.read_gbq(
            "SELECT 1", project_id="my-project", private_key="path/to/key.json"
        )


def test_read_gbq_with_invalid_dialect():
    with pytest.raises(ValueError, match="is not valid for dialect"):
        gbq.read_gbq("SELECT 1", dialect="invalid")


def test_read_gbq_with_configuration_query():
    df = gbq.read_gbq(None, configuration={"query": {"query": "SELECT 2"}})
    assert df is not None


def test_read_gbq_with_configuration_duplicate_query_raises_error():
    with pytest.raises(
        ValueError, match="Query statement can't be specified inside config"
    ):
        gbq.read_gbq("SELECT 1", configuration={"query": {"query": "SELECT 2"}})


def test_read_gbq_passes_dtypes(mock_bigquery_client, mock_service_account_credentials):
    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "SELECT 1 AS int_col",
        dialect="standard",
        credentials=mock_service_account_credentials,
        dtypes={"int_col": "my-custom-dtype"},
    )
    assert df is not None

    mock_list_rows = mock_bigquery_client.list_rows("dest", max_results=100)

    _, to_dataframe_kwargs = mock_list_rows.to_dataframe.call_args
    assert to_dataframe_kwargs["dtypes"] == {"int_col": "my-custom-dtype"}


def test_read_gbq_use_bqstorage_api(
    mock_bigquery_client, mock_service_account_credentials
):
    if not FEATURES.bigquery_has_bqstorage:  # pragma: NO COVER
        pytest.skip("requires BigQuery Storage API")

    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "SELECT 1 AS int_col",
        dialect="standard",
        credentials=mock_service_account_credentials,
        use_bqstorage_api=True,
    )
    assert df is not None

    mock_list_rows = mock_bigquery_client.list_rows("dest", max_results=100)
    mock_list_rows.to_dataframe.assert_called_once_with(
        create_bqstorage_client=True, dtypes=mock.ANY, progress_bar_type=mock.ANY,
    )


def test_read_gbq_calls_tqdm(mock_bigquery_client, mock_service_account_credentials):
    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "SELECT 1",
        dialect="standard",
        credentials=mock_service_account_credentials,
        progress_bar_type="foobar",
    )
    assert df is not None

    mock_list_rows = mock_bigquery_client.list_rows("dest", max_results=100)

    _, to_dataframe_kwargs = mock_list_rows.to_dataframe.call_args
    assert to_dataframe_kwargs["progress_bar_type"] == "foobar"


def test_read_gbq_with_full_table_id(
    mock_bigquery_client, mock_service_account_credentials
):
    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "my-project.my_dataset.read_gbq_table",
        credentials=mock_service_account_credentials,
        project_id="param-project",
    )
    assert df is not None

    mock_bigquery_client.query.assert_not_called()
    sent_table = mock_bigquery_client.list_rows.call_args[0][0]
    assert sent_table.project == "my-project"
    assert sent_table.dataset_id == "my_dataset"
    assert sent_table.table_id == "read_gbq_table"


def test_read_gbq_with_partial_table_id(
    mock_bigquery_client, mock_service_account_credentials
):
    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "my_dataset.read_gbq_table",
        credentials=mock_service_account_credentials,
        project_id="param-project",
    )
    assert df is not None

    mock_bigquery_client.query.assert_not_called()
    sent_table = mock_bigquery_client.list_rows.call_args[0][0]
    assert sent_table.project == "param-project"
    assert sent_table.dataset_id == "my_dataset"
    assert sent_table.table_id == "read_gbq_table"


def test_read_gbq_bypasses_query_with_table_id_and_max_results(
    mock_bigquery_client, mock_service_account_credentials
):
    mock_service_account_credentials.project_id = "service_account_project_id"
    df = gbq.read_gbq(
        "my-project.my_dataset.read_gbq_table",
        credentials=mock_service_account_credentials,
        max_results=11,
    )
    assert df is not None

    mock_bigquery_client.query.assert_not_called()
    sent_table = mock_bigquery_client.list_rows.call_args[0][0]
    assert sent_table.project == "my-project"
    assert sent_table.dataset_id == "my_dataset"
    assert sent_table.table_id == "read_gbq_table"
    sent_max_results = mock_bigquery_client.list_rows.call_args[1]["max_results"]
    assert sent_max_results == 11


def test_read_gbq_with_list_rows_error_translates_exception(
    mock_bigquery_client, mock_service_account_credentials
):
    mock_bigquery_client.list_rows.side_effect = (
        google.api_core.exceptions.NotFound("table not found"),
    )

    with pytest.raises(gbq.GenericGBQException, match="table not found"):
        gbq.read_gbq(
            "my-project.my_dataset.read_gbq_table",
            credentials=mock_service_account_credentials,
        )
