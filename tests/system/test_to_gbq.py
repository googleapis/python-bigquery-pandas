# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import datetime
import decimal
import functools
import random

import db_dtypes
import pandas
import pandas.testing
import pytest


pytest.importorskip("google.cloud.bigquery", minversion="1.24.0")


@pytest.fixture(params=["default", "load_parquet", "load_csv"])
def api_method(request):
    return request.param


@pytest.fixture
def method_under_test(credentials, project_id):
    import pandas_gbq

    return functools.partial(
        pandas_gbq.to_gbq, project_id=project_id, credentials=credentials
    )


@pytest.mark.parametrize(
    ["input_series", "skip_csv"],
    [
        # Ensure that 64-bit floating point numbers are unchanged.
        # See: https://github.com/pydata/pandas-gbq/issues/326
        (
            pandas.Series(
                [
                    0.14285714285714285,
                    0.4406779661016949,
                    1.05148,
                    1.05153,
                    1.8571428571428572,
                    2.718281828459045,
                    3.141592653589793,
                    2.0988936657440586e43,
                ],
                name="test_col",
            ),
            False,
        ),
        (
            pandas.Series(
                [
                    "abc",
                    "defg",
                    # Ensure that unicode characters are encoded. See:
                    # https://github.com/googleapis/python-bigquery-pandas/issues/106
                    "信用卡",
                    "Skywalker™",
                    "hülle",
                ],
                name="test_col",
            ),
            False,
        ),
        (
            pandas.Series(
                [
                    "abc",
                    "defg",
                    # Ensure that empty strings are written as empty string,
                    # not NULL. See:
                    # https://github.com/googleapis/python-bigquery-pandas/issues/366
                    "",
                    None,
                ],
                name="empty_strings",
            ),
            True,
        ),
    ],
)
def test_series_round_trip(
    method_under_test,
    random_dataset_id,
    bigquery_client,
    input_series,
    api_method,
    skip_csv,
):
    if api_method == "load_csv" and skip_csv:
        pytest.skip("Loading with CSV not supported.")
    table_id = f"{random_dataset_id}.round_trip_{random.randrange(1_000_000)}"
    input_series = input_series.sort_values().reset_index(drop=True)
    df = pandas.DataFrame(
        # Some errors only occur in multi-column dataframes. See:
        # https://github.com/googleapis/python-bigquery-pandas/issues/366
        {"test_col": input_series, "test_col2": input_series}
    )
    method_under_test(df, table_id, api_method=api_method)

    round_trip = bigquery_client.list_rows(table_id).to_dataframe()
    round_trip_series = round_trip["test_col"].sort_values().reset_index(drop=True)
    pandas.testing.assert_series_equal(
        round_trip_series, input_series, check_exact=True, check_names=False,
    )


DATAFRAME_ROUND_TRIPS = [
    # Ensure that a DATE column can be written with datetime64[ns] dtype
    # data. See:
    # https://github.com/googleapis/python-bigquery-pandas/issues/362
    (
        pandas.DataFrame(
            {
                "row_num": [0, 1, 2],
                "date_col": pandas.Series(
                    ["2021-04-17", "1999-12-31", "2038-01-19"], dtype="datetime64[ns]",
                ),
            }
        ),
        None,
        [{"name": "date_col", "type": "DATE"}],
        True,
    ),
    (
        (
            pandas.DataFrame(
                {
                    "row_num": [0, 1, 2],
                    "date_col": pandas.Series(
                        ["2021-04-17", "1999-12-31", "2038-01-19"],
                        dtype=db_dtypes.DateDtype(),
                    ),
                }
            ),
            None,
            [{"name": "date_col", "type": "DATE"}],
            False,
        )
    ),
    # Loading a DATE column should work for string objects. See:
    # https://github.com/googleapis/python-bigquery-pandas/issues/421
    (
        pandas.DataFrame(
            {"row_num": [123], "date_col": ["2021-12-12"]},
            columns=["row_num", "date_col"],
        ),
        pandas.DataFrame(
            {"row_num": [123], "date_col": [datetime.date(2021, 12, 12)]},
            columns=["row_num", "date_col"],
        ),
        [{"name": "row_num", "type": "INTEGER"}, {"name": "date_col", "type": "DATE"}],
        False,
    ),
    # Loading a NUMERIC column should work for floating point objects. See:
    # https://github.com/googleapis/python-bigquery-pandas/issues/421
    (
        pandas.DataFrame(
            {"row_num": [123], "num_col": [1.25]}, columns=["row_num", "num_col"],
        ),
        pandas.DataFrame(
            {"row_num": [123], "num_col": [decimal.Decimal("1.25")]},
            columns=["row_num", "num_col"],
        ),
        [
            {"name": "row_num", "type": "INTEGER"},
            {"name": "num_col", "type": "NUMERIC"},
        ],
        False,
    ),
]


@pytest.mark.parametrize(
    ["input_df", "expected_df", "table_schema", "skip_csv"], DATAFRAME_ROUND_TRIPS
)
def test_dataframe_round_trip_with_table_schema(
    method_under_test,
    random_dataset_id,
    bigquery_client,
    input_df,
    expected_df,
    table_schema,
    api_method,
    skip_csv,
):
    if api_method == "load_csv" and skip_csv:
        pytest.skip("Loading with CSV not supported.")
    if expected_df is None:
        expected_df = input_df
    table_id = f"{random_dataset_id}.round_trip_w_schema_{random.randrange(1_000_000)}"
    method_under_test(
        input_df, table_id, table_schema=table_schema, api_method=api_method
    )
    round_trip = bigquery_client.list_rows(table_id).to_dataframe(
        dtypes=dict(zip(expected_df.columns, expected_df.dtypes))
    )
    round_trip.sort_values("row_num", inplace=True)
    pandas.testing.assert_frame_equal(expected_df, round_trip)
