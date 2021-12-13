# Copyright (c) 2021 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import datetime

import pandas
import pandas.testing
import pytest

# from pandas_gbq.features import FEATURES


@pytest.mark.parametrize(["use_bqstorage_api"], [(True,), (False,)])
@pytest.mark.parametrize(
    ["query", "expected"],
    [
        pytest.param(
            """
            SELECT
                date_col
            FROM
                UNNEST([
                    STRUCT(DATE(1998, 9, 4) AS date_col),
                    STRUCT(DATE(2011, 10, 1) AS date_col),
                    STRUCT(DATE(2018, 4, 11) AS date_col)
                ])
            """,
            pandas.DataFrame(
                {
                    "date_col": [
                        datetime.date(1998, 9, 4),
                        datetime.date(2011, 10, 1),
                        datetime.date(2018, 4, 11),
                    ]
                }
            ),
            id="alltypes-nonnull-normal-range",
        )
    ],
)
def test_default_dtypes(read_gbq, query, use_bqstorage_api, expected):
    result = read_gbq(query, use_bqstorage_api=use_bqstorage_api)
    pandas.testing.assert_frame_equal(result, expected)


#     @pytest.mark.parametrize(
#         "expression, is_expected_dtype",
#         [
#             ("current_date()", pandas.api.types.is_datetime64_ns_dtype),
#             ("current_timestamp()", pandas.api.types.is_datetime64tz_dtype),
#             ("current_datetime()", pandas.api.types.is_datetime64_ns_dtype),
#             ("TRUE", pandas.api.types.is_bool_dtype),
#             ("FALSE", pandas.api.types.is_bool_dtype),
#         ],
#     )
#    def test_return_correct_types(self, project_id, expression, is_expected_dtype):
#        """
#        All type checks can be added to this function using additional
#        parameters, rather than creating additional functions.
#        We can consolidate the existing functions here in time
#
#        TODO: time doesn't currently parse
#        ("time(12,30,00)", "<M8[ns]"),
#        """
#        query = "SELECT {} AS _".format(expression)
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#        )
#        assert is_expected_dtype(df["_"].dtype)
#
#    def test_should_properly_handle_empty_strings(self, project_id):
#        query = 'SELECT "" AS empty_string'
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(df, DataFrame({"empty_string": [""]}))
#
#    def test_should_properly_handle_null_strings(self, project_id):
#        query = "SELECT STRING(NULL) AS null_string"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(df, DataFrame({"null_string": [None]}))
#
#    def test_should_properly_handle_valid_integers(self, project_id):
#        query = "SELECT CAST(3 AS INT64) AS valid_integer"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#        )
#        tm.assert_frame_equal(df, DataFrame({"valid_integer": [3]}, dtype="Int64"))
#
#    def test_should_properly_handle_nullable_integers(self, project_id):
#        query = """SELECT * FROM
#                    UNNEST([1, NULL]) AS nullable_integer
#                """
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#            dtypes={"nullable_integer": "Int64"},
#        )
#        tm.assert_frame_equal(
#            df,
#            DataFrame({"nullable_integer": pandas.Series([1, None], dtype="Int64")}),
#        )
#
#    def test_should_properly_handle_valid_longs(self, project_id):
#        query = "SELECT 1 << 62 AS valid_long"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#        )
#        tm.assert_frame_equal(df, DataFrame({"valid_long": [1 << 62]}, dtype="Int64"))
#
#    def test_should_properly_handle_nullable_longs(self, project_id):
#        query = """SELECT * FROM
#                    UNNEST([1 << 62, NULL]) AS nullable_long
#                """
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#            dtypes={"nullable_long": "Int64"},
#        )
#        tm.assert_frame_equal(
#            df,
#            DataFrame({"nullable_long": pandas.Series([1 << 62, None], dtype="Int64")}),
#        )
#
#    def test_should_properly_handle_null_integers(self, project_id):
#        query = "SELECT CAST(NULL AS INT64) AS null_integer"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#            dtypes={"null_integer": "Int64"},
#        )
#        tm.assert_frame_equal(
#            df, DataFrame({"null_integer": pandas.Series([None], dtype="Int64")}),
#        )
#
#    def test_should_properly_handle_valid_floats(self, project_id):
#        from math import pi
#
#        query = "SELECT PI() AS valid_float"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(df, DataFrame({"valid_float": [pi]}))
#
#    def test_should_properly_handle_nullable_floats(self, project_id):
#        from math import pi
#
#        query = """SELECT * FROM
#                    (SELECT PI() AS nullable_float),
#                    (SELECT NULL AS nullable_float)"""
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(df, DataFrame({"nullable_float": [pi, None]}))
#
#    def test_should_properly_handle_valid_doubles(self, project_id):
#        from math import pi
#
#        query = "SELECT PI() * POW(10, 307) AS valid_double"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(df, DataFrame({"valid_double": [pi * 10 ** 307]}))
#
#    def test_should_properly_handle_nullable_doubles(self, project_id):
#        from math import pi
#
#        query = """SELECT * FROM
#                    (SELECT PI() * POW(10, 307) AS nullable_double),
#                    (SELECT NULL AS nullable_double)"""
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(
#            df, DataFrame({"nullable_double": [pi * 10 ** 307, None]})
#        )
#
#    def test_should_properly_handle_null_floats(self, project_id):
#        query = """SELECT null_float
#        FROM UNNEST(ARRAY<FLOAT64>[NULL, 1.0]) AS null_float
#        """
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#        )
#        tm.assert_frame_equal(df, DataFrame({"null_float": [np.nan, 1.0]}))
#
#    def test_should_properly_handle_date(self, project_id):
#        query = "SELECT DATE(2003, 1, 4) AS date_col"
#        df = gbq.read_gbq(query, project_id=project_id, credentials=self.credentials,)
#        expected = DataFrame(
#            {
#                "date_col": pandas.Series(
#                    [datetime.date(2003, 1, 4)], dtype="datetime64[ns]"
#                )
#            },
#        )
#        tm.assert_frame_equal(df, expected)
#
#    def test_should_properly_handle_time(self, project_id):
#        query = (
#            "SELECT TIME_ADD(TIME(3, 14, 15), INTERVAL 926589 MICROSECOND) AS time_col"
#        )
#        df = gbq.read_gbq(query, project_id=project_id, credentials=self.credentials,)
#        expected = DataFrame(
#            {
#                "time_col": pandas.Series(
#                    [datetime.time(3, 14, 15, 926589)], dtype="object"
#                )
#            },
#        )
#        tm.assert_frame_equal(df, expected)
#
#    def test_should_properly_handle_timestamp_unix_epoch(self, project_id):
#        query = 'SELECT TIMESTAMP("1970-01-01 00:00:00") AS unix_epoch'
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        expected = DataFrame(
#            {"unix_epoch": ["1970-01-01T00:00:00.000000Z"]}, dtype="datetime64[ns]",
#        )
#        if expected["unix_epoch"].dt.tz is None:
#            expected["unix_epoch"] = expected["unix_epoch"].dt.tz_localize("UTC")
#        tm.assert_frame_equal(df, expected)
#
#    def test_should_properly_handle_arbitrary_timestamp(self, project_id):
#        query = 'SELECT TIMESTAMP("2004-09-15 05:00:00") AS valid_timestamp'
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        expected = DataFrame(
#            {"valid_timestamp": ["2004-09-15T05:00:00.000000Z"]},
#            dtype="datetime64[ns]",
#        )
#        if expected["valid_timestamp"].dt.tz is None:
#            expected["valid_timestamp"] = expected["valid_timestamp"].dt.tz_localize(
#                "UTC"
#            )
#        tm.assert_frame_equal(df, expected)
#
#    def test_should_properly_handle_datetime_unix_epoch(self, project_id):
#        query = 'SELECT DATETIME("1970-01-01 00:00:00") AS unix_epoch'
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(
#            df,
#            DataFrame({"unix_epoch": ["1970-01-01T00:00:00"]}, dtype="datetime64[ns]"),
#        )
#
#    def test_should_properly_handle_arbitrary_datetime(self, project_id):
#        query = 'SELECT DATETIME("2004-09-15 05:00:00") AS valid_timestamp'
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        tm.assert_frame_equal(
#            df, DataFrame({"valid_timestamp": [np.datetime64("2004-09-15T05:00:00")]}),
#        )
#
#    def test_should_properly_handle_null_timestamp(self, project_id):
#        query = "SELECT TIMESTAMP(NULL) AS null_timestamp"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        expected = DataFrame({"null_timestamp": [NaT]}, dtype="datetime64[ns]")
#        expected["null_timestamp"] = expected["null_timestamp"].dt.tz_localize("UTC")
#        tm.assert_frame_equal(df, expected)
#
#    def test_should_properly_handle_null_datetime(self, project_id):
#        query = "SELECT CAST(NULL AS DATETIME) AS null_datetime"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="standard",
#        )
#        tm.assert_frame_equal(df, DataFrame({"null_datetime": [NaT]}))
#
#    def test_should_properly_handle_null_boolean(self, project_id):
#        query = "SELECT BOOLEAN(NULL) AS null_boolean"
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        expected_dtype = "boolean" if FEATURES.pandas_has_boolean_dtype else None
#        tm.assert_frame_equal(
#            df, DataFrame({"null_boolean": [None]}, dtype=expected_dtype)
#        )
#
#    def test_should_properly_handle_nullable_booleans(self, project_id):
#        query = """SELECT * FROM
#                    (SELECT BOOLEAN(TRUE) AS nullable_boolean),
#                    (SELECT NULL AS nullable_boolean)"""
#        df = gbq.read_gbq(
#            query,
#            project_id=project_id,
#            credentials=self.credentials,
#            dialect="legacy",
#        )
#        expected_dtype = "boolean" if FEATURES.pandas_has_boolean_dtype else None
#        tm.assert_frame_equal(
#            df, DataFrame({"nullable_boolean": [True, None]}, dtype=expected_dtype)
#        )
#
