import functools
import pandas
import pandas.testing
import pytest

from pandas_gbq import gbq

pytest.importorskip("google.cloud.bigquery", minversion="1.24.0")


@pytest.fixture
def method_under_test(credentials):
    import pandas_gbq

    return functools.partial(pandas_gbq.to_gbq, credentials=credentials)


def test_float_round_trip(
    method_under_test, random_dataset_id, bigquery_client
):
    """Ensure that 64-bit floating point numbers are unchanged.

    See: https://github.com/pydata/pandas-gbq/issues/326
    """

    table_id = "{}.float_round_trip".format(random_dataset_id)
    input_floats = pandas.Series(
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
        name="float_col",
    )
    df = pandas.DataFrame({"float_col": input_floats})
    method_under_test(df, table_id)

    round_trip = bigquery_client.list_rows(table_id).to_dataframe()
    round_trip_floats = round_trip["float_col"].sort_values()
    pandas.testing.assert_series_equal(
        round_trip_floats, input_floats, check_exact=True
    )


def test_include_project_name(
    method_under_test, random_dataset_id, bigquery_client
):
    """Ensure that we can pass in a table identifier that includes a project.
    """

    table_id = "{}.{}.int_round_trip".format(
        bigquery_client.project_id, random_dataset_id
    )
    input_series = pandas.Series([1, 2], name="int_col")
    df = pandas.DataFrame({"int_col": input_series})
    method_under_test(df, table_id)

    round_trip = bigquery_client.list_rows(table_id).to_dataframe()
    round_trip_data = round_trip["int_col"].sort_values()
    pandas.testing.assert_series_equal(
        round_trip_data, input_series, check_exact=True
    )


def test_include_project_name_failure(
    method_under_test, random_dataset_id, bigquery_client
):
    """Ensure that we can pass in a table identifier that includes a project.
    """
    with pytest.raises(gbq.GenericGBQException):
        table_id = "{}.{}.int_round_trip".format(
            "this_project_does_not_exist", random_dataset_id
        )
        input_series = pandas.Series([1, 2], name="int_col")
        df = pandas.DataFrame({"int_col": input_series})
        method_under_test(df, table_id)
