"""Shared pytest fixtures for system tests."""

import os
import os.path
import uuid

import pytest


@pytest.fixture(scope="session")
def project_id():
    return os.environ.get("GBQ_PROJECT_ID") or os.environ.get(
        "GOOGLE_CLOUD_PROJECT"
    )  # noqa


@pytest.fixture(scope="session")
def private_key_path():
    path = None
    if "GBQ_GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        path = os.environ["GBQ_GOOGLE_APPLICATION_CREDENTIALS"]
    elif "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    if path is None:
        pytest.skip(
            "Cannot run integration tests without a "
            "private key json file path"
        )
        return None
    if not os.path.isfile(path):
        pytest.skip(
            "Cannot run integration tests when there is "
            "no file at the private key json file path"
        )
        return None

    return path


@pytest.fixture(scope="module")
def bigquery_client(project_id, private_key_path):
    from google.cloud import bigquery

    return bigquery.Client.from_service_account_json(
        private_key_path, project=project_id
    )


@pytest.fixture()
def random_dataset_id(bigquery_client):
    import google.api_core.exceptions

    dataset_id = "".join(["pandas_gbq_", str(uuid.uuid4()).replace("-", "_")])
    dataset_ref = bigquery_client.dataset(dataset_id)
    yield dataset_id
    try:
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)
    except google.api_core.exceptions.NotFound:
        pass  # Not all tests actually create a dataset
