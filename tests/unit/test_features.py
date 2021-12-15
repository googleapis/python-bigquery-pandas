# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import pytest

from pandas_gbq.features import FEATURES


@pytest.fixture(autouse=True)
def fresh_bigquery_version(monkeypatch):
    monkeypatch.setattr(FEATURES, "_bigquery_installed_version", None)


@pytest.mark.parametrize(
    ["bigquery_version", "expected"],
    [
        ("1.11.1", False),
        ("1.26.0", False),
        ("2.9.999", False),
        ("2.10.0", True),
        ("2.12.0", True),
        ("3.0.0", True),
    ],
)
def test_bigquery_has_bignumeric(monkeypatch, bigquery_version, expected):
    import google.cloud.bigquery

    monkeypatch.setattr(google.cloud.bigquery, "__version__", bigquery_version)
    assert FEATURES.bigquery_has_bignumeric == expected


@pytest.mark.parametrize(
    ["bigquery_version", "expected"],
    [
        ("1.11.1", False),
        ("1.26.0", False),
        ("2.5.4", False),
        ("2.6.0", True),
        ("2.6.1", True),
        ("2.12.0", True),
    ],
)
def test_bigquery_has_from_dataframe_with_csv(monkeypatch, bigquery_version, expected):
    import google.cloud.bigquery

    monkeypatch.setattr(google.cloud.bigquery, "__version__", bigquery_version)
    assert FEATURES.bigquery_has_from_dataframe_with_csv == expected


@pytest.mark.parametrize(
    ["bigquery_version", "expected"],
    [("1.26.0", True), ("2.12.0", True), ("3.0.0", False), ("3.1.0", False)],
)
def test_bigquery_needs_date_as_object(monkeypatch, bigquery_version, expected):
    import google.cloud.bigquery

    monkeypatch.setattr(google.cloud.bigquery, "__version__", bigquery_version)
    assert FEATURES.bigquery_needs_date_as_object == expected
