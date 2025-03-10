# Copyright (c) 2025 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import warnings

import pyarrow
import pytest
from google.cloud import bigquery
from google.cloud.bigquery import schema


@pytest.fixture
def module_under_test():
    from pandas_gbq.schema import bigquery_to_pyarrow

    return bigquery_to_pyarrow


def is_none(value):
    return value is None


def is_datetime(type_):
    # See: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime-type
    return all_(
        pyarrow.types.is_timestamp,
        lambda type_: type_.unit == "us",
        lambda type_: type_.tz is None,
    )(type_)


def is_numeric(type_):
    # See: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
    return all_(
        pyarrow.types.is_decimal,
        lambda type_: type_.precision == 38,
        lambda type_: type_.scale == 9,
    )(type_)


def is_bignumeric(type_):
    # See: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
    return all_(
        pyarrow.types.is_decimal,
        lambda type_: type_.precision == 76,
        lambda type_: type_.scale == 38,
    )(type_)


def is_timestamp(type_):
    # See: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type
    return all_(
        pyarrow.types.is_timestamp,
        lambda type_: type_.unit == "us",
        lambda type_: type_.tz == "UTC",
    )(type_)


def do_all(functions, value):
    return all((func(value) for func in functions))


def all_(*functions):
    return functools.partial(do_all, functions)


def test_is_datetime():
    assert is_datetime(pyarrow.timestamp("us", tz=None))
    assert not is_datetime(pyarrow.timestamp("ms", tz=None))
    assert not is_datetime(pyarrow.timestamp("us", tz="UTC"))
    assert not is_datetime(pyarrow.timestamp("ns", tz="UTC"))
    assert not is_datetime(pyarrow.string())


def test_do_all():
    assert do_all((lambda _: True, lambda _: True), None)
    assert not do_all((lambda _: True, lambda _: False), None)
    assert not do_all((lambda _: False,), None)


def test_all_():
    assert all_(lambda _: True, lambda _: True)(None)
    assert not all_(lambda _: True, lambda _: False)(None)


@pytest.mark.parametrize(
    "bq_type,bq_mode,is_correct_type",
    [
        ("STRING", "NULLABLE", pyarrow.types.is_string),
        ("STRING", None, pyarrow.types.is_string),
        ("string", "NULLABLE", pyarrow.types.is_string),
        ("StRiNg", "NULLABLE", pyarrow.types.is_string),
        ("BYTES", "NULLABLE", pyarrow.types.is_binary),
        ("INTEGER", "NULLABLE", pyarrow.types.is_int64),
        ("INT64", "NULLABLE", pyarrow.types.is_int64),
        ("FLOAT", "NULLABLE", pyarrow.types.is_float64),
        ("FLOAT64", "NULLABLE", pyarrow.types.is_float64),
        ("NUMERIC", "NULLABLE", is_numeric),
        (
            "BIGNUMERIC",
            "NULLABLE",
            is_bignumeric,
        ),
        ("BOOLEAN", "NULLABLE", pyarrow.types.is_boolean),
        ("BOOL", "NULLABLE", pyarrow.types.is_boolean),
        ("TIMESTAMP", "NULLABLE", is_timestamp),
        ("DATE", "NULLABLE", pyarrow.types.is_date32),
        ("TIME", "NULLABLE", pyarrow.types.is_time64),
        ("DATETIME", "NULLABLE", is_datetime),
        ("GEOGRAPHY", "NULLABLE", pyarrow.types.is_string),
        ("UNKNOWN_TYPE", "NULLABLE", is_none),
        # Use pyarrow.list_(item_type) for repeated (array) fields.
        (
            "STRING",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_string(type_.value_type),
            ),
        ),
        (
            "STRING",
            "repeated",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_string(type_.value_type),
            ),
        ),
        (
            "STRING",
            "RePeAtEd",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_string(type_.value_type),
            ),
        ),
        (
            "BYTES",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_binary(type_.value_type),
            ),
        ),
        (
            "INTEGER",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_int64(type_.value_type),
            ),
        ),
        (
            "INT64",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_int64(type_.value_type),
            ),
        ),
        (
            "FLOAT",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_float64(type_.value_type),
            ),
        ),
        (
            "FLOAT64",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_float64(type_.value_type),
            ),
        ),
        (
            "NUMERIC",
            "REPEATED",
            all_(pyarrow.types.is_list, lambda type_: is_numeric(type_.value_type)),
        ),
        (
            "BIGNUMERIC",
            "REPEATED",
            all_(pyarrow.types.is_list, lambda type_: is_bignumeric(type_.value_type)),
        ),
        (
            "BOOLEAN",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_boolean(type_.value_type),
            ),
        ),
        (
            "BOOL",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_boolean(type_.value_type),
            ),
        ),
        (
            "TIMESTAMP",
            "REPEATED",
            all_(pyarrow.types.is_list, lambda type_: is_timestamp(type_.value_type)),
        ),
        (
            "DATE",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_date32(type_.value_type),
            ),
        ),
        (
            "TIME",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_time64(type_.value_type),
            ),
        ),
        (
            "DATETIME",
            "REPEATED",
            all_(pyarrow.types.is_list, lambda type_: is_datetime(type_.value_type)),
        ),
        (
            "GEOGRAPHY",
            "REPEATED",
            all_(
                pyarrow.types.is_list,
                lambda type_: pyarrow.types.is_string(type_.value_type),
            ),
        ),
        ("RECORD", "REPEATED", is_none),
        ("UNKNOWN_TYPE", "REPEATED", is_none),
    ],
)
def test_bq_to_arrow_data_type(module_under_test, bq_type, bq_mode, is_correct_type):
    field = bigquery.SchemaField("ignored_name", bq_type, mode=bq_mode)
    actual = module_under_test.bq_to_arrow_data_type(field)
    assert is_correct_type(actual)


@pytest.mark.parametrize("bq_type", ["RECORD", "record", "STRUCT", "struct"])
def test_bq_to_arrow_data_type_w_struct(module_under_test, bq_type):
    fields = (
        bigquery.SchemaField("field01", "STRING"),
        bigquery.SchemaField("field02", "BYTES"),
        bigquery.SchemaField("field03", "INTEGER"),
        bigquery.SchemaField("field04", "INT64"),
        bigquery.SchemaField("field05", "FLOAT"),
        bigquery.SchemaField("field06", "FLOAT64"),
        bigquery.SchemaField("field07", "NUMERIC"),
        bigquery.SchemaField("field08", "BIGNUMERIC"),
        bigquery.SchemaField("field09", "BOOLEAN"),
        bigquery.SchemaField("field10", "BOOL"),
        bigquery.SchemaField("field11", "TIMESTAMP"),
        bigquery.SchemaField("field12", "DATE"),
        bigquery.SchemaField("field13", "TIME"),
        bigquery.SchemaField("field14", "DATETIME"),
        bigquery.SchemaField("field15", "GEOGRAPHY"),
    )

    field = bigquery.SchemaField(
        "ignored_name", bq_type, mode="NULLABLE", fields=fields
    )
    actual = module_under_test.bq_to_arrow_data_type(field)

    expected = (
        pyarrow.field("field01", pyarrow.string()),
        pyarrow.field("field02", pyarrow.binary()),
        pyarrow.field("field03", pyarrow.int64()),
        pyarrow.field("field04", pyarrow.int64()),
        pyarrow.field("field05", pyarrow.float64()),
        pyarrow.field("field06", pyarrow.float64()),
        pyarrow.field("field07", module_under_test.pyarrow_numeric()),
        pyarrow.field("field08", module_under_test.pyarrow_bignumeric()),
        pyarrow.field("field09", pyarrow.bool_()),
        pyarrow.field("field10", pyarrow.bool_()),
        pyarrow.field("field11", module_under_test.pyarrow_timestamp()),
        pyarrow.field("field12", pyarrow.date32()),
        pyarrow.field("field13", module_under_test.pyarrow_time()),
        pyarrow.field("field14", module_under_test.pyarrow_datetime()),
        pyarrow.field("field15", pyarrow.string()),
    )
    expected = pyarrow.struct(expected)

    assert pyarrow.types.is_struct(actual)
    assert actual.num_fields == len(fields)
    assert actual.equals(expected)


@pytest.mark.parametrize("bq_type", ["RECORD", "record", "STRUCT", "struct"])
def test_bq_to_arrow_data_type_w_array_struct(module_under_test, bq_type):
    fields = (
        bigquery.SchemaField("field01", "STRING"),
        bigquery.SchemaField("field02", "BYTES"),
        bigquery.SchemaField("field03", "INTEGER"),
        bigquery.SchemaField("field04", "INT64"),
        bigquery.SchemaField("field05", "FLOAT"),
        bigquery.SchemaField("field06", "FLOAT64"),
        bigquery.SchemaField("field07", "NUMERIC"),
        bigquery.SchemaField("field08", "BIGNUMERIC"),
        bigquery.SchemaField("field09", "BOOLEAN"),
        bigquery.SchemaField("field10", "BOOL"),
        bigquery.SchemaField("field11", "TIMESTAMP"),
        bigquery.SchemaField("field12", "DATE"),
        bigquery.SchemaField("field13", "TIME"),
        bigquery.SchemaField("field14", "DATETIME"),
        bigquery.SchemaField("field15", "GEOGRAPHY"),
    )

    field = bigquery.SchemaField(
        "ignored_name", bq_type, mode="REPEATED", fields=fields
    )
    actual = module_under_test.bq_to_arrow_data_type(field)

    expected = (
        pyarrow.field("field01", pyarrow.string()),
        pyarrow.field("field02", pyarrow.binary()),
        pyarrow.field("field03", pyarrow.int64()),
        pyarrow.field("field04", pyarrow.int64()),
        pyarrow.field("field05", pyarrow.float64()),
        pyarrow.field("field06", pyarrow.float64()),
        pyarrow.field("field07", module_under_test.pyarrow_numeric()),
        pyarrow.field("field08", module_under_test.pyarrow_bignumeric()),
        pyarrow.field("field09", pyarrow.bool_()),
        pyarrow.field("field10", pyarrow.bool_()),
        pyarrow.field("field11", module_under_test.pyarrow_timestamp()),
        pyarrow.field("field12", pyarrow.date32()),
        pyarrow.field("field13", module_under_test.pyarrow_time()),
        pyarrow.field("field14", module_under_test.pyarrow_datetime()),
        pyarrow.field("field15", pyarrow.string()),
    )
    expected_value_type = pyarrow.struct(expected)

    assert pyarrow.types.is_list(actual)
    assert pyarrow.types.is_struct(actual.value_type)
    assert actual.value_type.num_fields == len(fields)
    assert actual.value_type.equals(expected_value_type)


def test_bq_to_arrow_data_type_w_struct_unknown_subfield(module_under_test):
    fields = (
        bigquery.SchemaField("field1", "STRING"),
        bigquery.SchemaField("field2", "INTEGER"),
        # Don't know what to convert UNKNOWN_TYPE to, let type inference work,
        # instead.
        bigquery.SchemaField("field3", "UNKNOWN_TYPE"),
    )
    field = bigquery.SchemaField(
        "ignored_name", "RECORD", mode="NULLABLE", fields=fields
    )

    with warnings.catch_warnings(record=True) as warned:
        actual = module_under_test.bq_to_arrow_data_type(field)

    assert actual is None
    assert len(warned) == 1
    warning = warned[0]
    assert "field3" in str(warning)


@pytest.mark.parametrize(
    "bq_schema,expected",
    [
        (
            bigquery.SchemaField(
                "field1",
                "RANGE",
                range_element_type=schema.FieldElementType("DATE"),
                mode="NULLABLE",
            ),
            pyarrow.struct(
                [
                    ("start", pyarrow.date32()),
                    ("end", pyarrow.date32()),
                ]
            ),
        ),
        (
            bigquery.SchemaField(
                "field2",
                "RANGE",
                range_element_type=schema.FieldElementType("DATETIME"),
                mode="NULLABLE",
            ),
            pyarrow.struct(
                [
                    ("start", pyarrow.timestamp("us", tz=None)),
                    ("end", pyarrow.timestamp("us", tz=None)),
                ]
            ),
        ),
        (
            bigquery.SchemaField(
                "field3",
                "RANGE",
                range_element_type=schema.FieldElementType("TIMESTAMP"),
                mode="NULLABLE",
            ),
            pyarrow.struct(
                [
                    ("start", pyarrow.timestamp("us", tz="UTC")),
                    ("end", pyarrow.timestamp("us", tz="UTC")),
                ]
            ),
        ),
    ],
)
def test_bq_to_arrow_data_type_w_range(module_under_test, bq_schema, expected):
    actual = module_under_test.bq_to_arrow_data_type(bq_schema)
    assert actual.equals(expected)


def test_bq_to_arrow_data_type_w_range_no_element(module_under_test):
    field = bigquery.SchemaField("field1", "RANGE", mode="NULLABLE")
    with pytest.raises(ValueError, match="Range element type cannot be None"):
        module_under_test.bq_to_arrow_data_type(field)


def test_bq_to_arrow_schema_w_unknown_type(module_under_test):
    fields = (
        bigquery.SchemaField("field1", "STRING"),
        bigquery.SchemaField("field2", "INTEGER"),
        # Don't know what to convert UNKNOWN_TYPE to, let type inference work,
        # instead.
        bigquery.SchemaField("field3", "UNKNOWN_TYPE"),
    )
    with warnings.catch_warnings(record=True) as warned:
        actual = module_under_test.bq_to_arrow_schema(fields)
    assert actual is None

    assert len(warned) == 1
    warning = warned[0]
    assert "field3" in str(warning)


def test_bq_to_arrow_field_type_override(module_under_test):
    # When loading pandas data, we may need to override the type
    # decision based on data contents, because GEOGRAPHY data can be
    # stored as either text or binary.

    assert (
        module_under_test.bq_to_arrow_field(bigquery.SchemaField("g", "GEOGRAPHY")).type
        == pyarrow.string()
    )

    assert (
        module_under_test.bq_to_arrow_field(
            bigquery.SchemaField("g", "GEOGRAPHY"),
            pyarrow.binary(),
        ).type
        == pyarrow.binary()
    )


def test_bq_to_arrow_field_set_repeated_nullable_false(module_under_test):
    assert (
        module_under_test.bq_to_arrow_field(
            bigquery.SchemaField("name", "STRING", mode="REPEATED")
        ).nullable
        is False
    )

    assert (
        module_under_test.bq_to_arrow_field(
            bigquery.SchemaField("name", "STRING", mode="NULLABLE")
        ).nullable
        is True
    )


@pytest.mark.parametrize(
    "field_type, metadata",
    [
        ("datetime", {b"ARROW:extension:name": b"google:sqlType:datetime"}),
        (
            "geography",
            {
                b"ARROW:extension:name": b"google:sqlType:geography",
                b"ARROW:extension:metadata": b'{"encoding": "WKT"}',
            },
        ),
    ],
)
def test_bq_to_arrow_field_metadata(module_under_test, field_type, metadata):
    assert (
        module_under_test.bq_to_arrow_field(
            bigquery.SchemaField("g", field_type)
        ).metadata
        == metadata
    )


def test_bq_to_arrow_scalars(module_under_test):
    assert (
        module_under_test.bq_to_arrow_scalars("BIGNUMERIC")
        == module_under_test.pyarrow_bignumeric
    )
    assert module_under_test.bq_to_arrow_scalars("UNKNOWN_TYPE") is None
