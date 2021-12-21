# Copyright (c) 2021 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import copy
import datetime
from unittest import mock

from pandas import DataFrame
import pytest

from pandas_gbq import gbq
from pandas_gbq.features import FEATURES


def mock_get_credentials_no_project(*args, **kwargs):
    import google.auth.credentials

    mock_credentials = mock.create_autospec(google.auth.credentials.Credentials)
    return mock_credentials, None


def test_to_gbq_should_fail_if_invalid_table_name_passed():
    with pytest.raises(gbq.NotFoundException):
        gbq.to_gbq(DataFrame([[1]]), "invalid_table_name", project_id="1234")


def test_to_gbq_with_no_project_id_given_should_fail(monkeypatch):
    import pydata_google_auth

    monkeypatch.setattr(pydata_google_auth, "default", mock_get_credentials_no_project)

    with pytest.raises(ValueError, match="Could not determine project ID"):
        gbq.to_gbq(DataFrame([[1]]), "dataset.tablename")


@pytest.mark.parametrize(
    ["api_method", "warning_message", "warning_type"],
    [
        ("load_parquet", "chunksize is ignored", DeprecationWarning),
        ("load_csv", "chunksize will be ignored", PendingDeprecationWarning),
    ],
)
def test_to_gbq_with_chunksize_warns_deprecation(
    api_method, warning_message, warning_type
):
    with pytest.warns(warning_type, match=warning_message):
        try:
            gbq.to_gbq(
                DataFrame([[1]]),
                "dataset.tablename",
                project_id="my-project",
                api_method=api_method,
                chunksize=100,
            )
        except gbq.TableCreationError:
            pass


@pytest.mark.parametrize(["verbose"], [(True,), (False,)])
def test_to_gbq_with_verbose_new_pandas_warns_deprecation(monkeypatch, verbose):
    monkeypatch.setattr(
        type(FEATURES),
        "pandas_has_deprecated_verbose",
        mock.PropertyMock(return_value=True),
    )
    with pytest.warns(FutureWarning, match="verbose is deprecated"):
        try:
            gbq.to_gbq(
                DataFrame([[1]]),
                "dataset.tablename",
                project_id="my-project",
                verbose=verbose,
            )
        except gbq.TableCreationError:
            pass


def test_to_gbq_wo_verbose_w_new_pandas_no_warnings(monkeypatch, recwarn):
    monkeypatch.setattr(
        type(FEATURES),
        "pandas_has_deprecated_verbose",
        mock.PropertyMock(return_value=True),
    )
    try:
        gbq.to_gbq(DataFrame([[1]]), "dataset.tablename", project_id="my-project")
    except gbq.TableCreationError:
        pass
    assert len(recwarn) == 0


def test_to_gbq_with_verbose_old_pandas_no_warnings(monkeypatch, recwarn):
    monkeypatch.setattr(
        type(FEATURES),
        "pandas_has_deprecated_verbose",
        mock.PropertyMock(return_value=False),
    )
    try:
        gbq.to_gbq(
            DataFrame([[1]]),
            "dataset.tablename",
            project_id="my-project",
            verbose=True,
        )
    except gbq.TableCreationError:
        pass
    assert len(recwarn) == 0


def test_to_gbq_with_private_key_raises_notimplementederror():
    with pytest.raises(NotImplementedError, match="private_key"):
        gbq.to_gbq(
            DataFrame([[1]]),
            "dataset.tablename",
            project_id="my-project",
            private_key="path/to/key.json",
        )


def test_to_gbq_doesnt_run_query(mock_bigquery_client):
    try:
        gbq.to_gbq(DataFrame([[1]]), "dataset.tablename", project_id="my-project")
    except gbq.TableCreationError:
        pass

    mock_bigquery_client.query.assert_not_called()


def test_to_gbq_w_empty_df(mock_bigquery_client):
    import google.api_core.exceptions

    mock_bigquery_client.get_table.side_effect = google.api_core.exceptions.NotFound(
        "my_table"
    )
    gbq.to_gbq(DataFrame(), "my_dataset.my_table", project_id="1234")
    mock_bigquery_client.create_table.assert_called_with(mock.ANY)
    mock_bigquery_client.load_table_from_dataframe.assert_not_called()
    mock_bigquery_client.load_table_from_file.assert_not_called()


def test_to_gbq_w_default_project(mock_bigquery_client):
    """If no project is specified, we should be able to use project from
    default credentials.
    """
    import google.api_core.exceptions
    from google.cloud.bigquery.table import TableReference

    mock_bigquery_client.get_table.side_effect = google.api_core.exceptions.NotFound(
        "my_table"
    )
    gbq.to_gbq(DataFrame(), "my_dataset.my_table")

    mock_bigquery_client.get_table.assert_called_with(
        TableReference.from_string("default-project.my_dataset.my_table")
    )
    mock_bigquery_client.create_table.assert_called_with(mock.ANY)
    table = mock_bigquery_client.create_table.call_args[0][0]
    assert table.project == "default-project"


def test_to_gbq_w_project_table(mock_bigquery_client):
    """If a project is included in the table ID, use that instead of the client
    project. See: https://github.com/pydata/pandas-gbq/issues/321
    """
    import google.api_core.exceptions
    from google.cloud.bigquery.table import TableReference

    mock_bigquery_client.get_table.side_effect = google.api_core.exceptions.NotFound(
        "my_table"
    )
    gbq.to_gbq(
        DataFrame(), "project_table.my_dataset.my_table", project_id="project_client",
    )

    mock_bigquery_client.get_table.assert_called_with(
        TableReference.from_string("project_table.my_dataset.my_table")
    )
    mock_bigquery_client.create_table.assert_called_with(mock.ANY)
    table = mock_bigquery_client.create_table.call_args[0][0]
    assert table.project == "project_table"


def test_to_gbq_create_dataset(mock_bigquery_client):
    import google.api_core.exceptions

    mock_bigquery_client.get_table.side_effect = google.api_core.exceptions.NotFound(
        "my_table"
    )
    mock_bigquery_client.get_dataset.side_effect = google.api_core.exceptions.NotFound(
        "my_dataset"
    )
    gbq.to_gbq(DataFrame([[1]]), "my_dataset.my_table", project_id="1234")
    mock_bigquery_client.create_dataset.assert_called_with(mock.ANY)


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


def test_to_gbq_does_not_modify_schema_arg(mock_bigquery_client):
    """Test of Issue # 277."""
    from google.api_core.exceptions import NotFound

    # Create table with new schema.
    mock_bigquery_client.get_table.side_effect = NotFound("nope")
    df = DataFrame(
        {
            "field1": ["a", "b"],
            "field2": [1, 2],
            "field3": [datetime.date(2019, 1, 1), datetime.date(2019, 5, 1)],
        }
    )
    original_schema = [
        {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        {"name": "field2", "type": "INTEGER"},
        {"name": "field3", "type": "DATE"},
    ]
    original_schema_cp = copy.deepcopy(original_schema)
    gbq.to_gbq(
        df,
        "dataset.schematest",
        project_id="my-project",
        table_schema=original_schema,
        if_exists="fail",
    )
    assert original_schema == original_schema_cp

    # Test again now that table exists - behavior will differ internally
    # branch at if table.exists(table_id)
    original_schema = [
        {"name": "field1", "type": "STRING", "mode": "REQUIRED"},
        {"name": "field2", "type": "INTEGER"},
        {"name": "field3", "type": "DATE"},
    ]
    original_schema_cp = copy.deepcopy(original_schema)
    gbq.to_gbq(
        df,
        "dataset.schematest",
        project_id="my-project",
        table_schema=original_schema,
        if_exists="append",
    )
    assert original_schema == original_schema_cp
