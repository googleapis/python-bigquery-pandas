# Copyright (c) 2025 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import warnings
from typing import Any, Union

import db_dtypes
import pyarrow
from google.cloud import bigquery


def pyarrow_datetime():
    return pyarrow.timestamp("us", tz=None)


def pyarrow_numeric():
    return pyarrow.decimal128(38, 9)


def pyarrow_bignumeric():
    # 77th digit is partial.
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
    return pyarrow.decimal256(76, 38)


def pyarrow_time():
    return pyarrow.time64("us")


def pyarrow_timestamp():
    return pyarrow.timestamp("us", tz="UTC")


# Prefer JSON type built-in to pyarrow (adding in 19.0.0), if available.
# Otherwise, fallback to db-dtypes, where the JSONArrowType was added in 1.4.0,
# but since they might have an older db-dtypes, have string as a fallback for that.
if hasattr(pyarrow, "json_"):
    json_arrow_type = pyarrow.json_(pyarrow.string())
elif hasattr(db_dtypes, "JSONArrowType"):
    json_arrow_type = db_dtypes.JSONArrowType()
else:
    json_arrow_type = pyarrow.string()


# This dictionary is duplicated in bigquery_storage/test/unite/test_reader.py
# When modifying it be sure to update it there as well.
# Note(todo!!): type "BIGNUMERIC"'s matching pyarrow type is added in _pandas_helpers.py
_BQ_TO_ARROW_SCALARS = {
    "BIGNUMERIC": pyarrow_bignumeric,
    "BOOL": pyarrow.bool_,
    "BOOLEAN": pyarrow.bool_,
    "BYTES": pyarrow.binary,
    "DATE": pyarrow.date32,
    "DATETIME": pyarrow_datetime,
    "FLOAT": pyarrow.float64,
    "FLOAT64": pyarrow.float64,
    "GEOGRAPHY": pyarrow.string,
    "INT64": pyarrow.int64,
    "INTEGER": pyarrow.int64,
    "JSON": json_arrow_type,
    "NUMERIC": pyarrow_numeric,
    "STRING": pyarrow.string,
    "TIME": pyarrow_time,
    "TIMESTAMP": pyarrow_timestamp,
}

_STRUCT_TYPES = ("RECORD", "STRUCT")


def bq_to_arrow_scalars(bq_scalar: str):
    """
    Returns:
        The Arrow scalar type that the input BigQuery scalar type maps to.
        If it cannot find the BigQuery scalar, return None.
    """
    return _BQ_TO_ARROW_SCALARS.get(bq_scalar)


BQ_FIELD_TYPE_TO_ARROW_FIELD_METADATA = {
    "GEOGRAPHY": {
        b"ARROW:extension:name": b"google:sqlType:geography",
        b"ARROW:extension:metadata": b'{"encoding": "WKT"}',
    },
    "DATETIME": {b"ARROW:extension:name": b"google:sqlType:datetime"},
    "JSON": {b"ARROW:extension:name": b"google:sqlType:json"},
}


def bq_to_arrow_struct_data_type(field):
    arrow_fields = []
    for subfield in field.fields:
        arrow_subfield = bq_to_arrow_field(subfield)
        if arrow_subfield:
            arrow_fields.append(arrow_subfield)
        else:
            # Could not determine a subfield type. Fallback to type
            # inference.
            return None
    return pyarrow.struct(arrow_fields)


def bq_to_arrow_range_data_type(field):
    if field is None:
        raise ValueError(
            "Range element type cannot be None, must be one of "
            "DATE, DATETIME, or TIMESTAMP"
        )
    element_type = field.element_type.upper()
    arrow_element_type = bq_to_arrow_scalars(element_type)()
    return pyarrow.struct([("start", arrow_element_type), ("end", arrow_element_type)])


def bq_to_arrow_data_type(field):
    """Return the Arrow data type, corresponding to a given BigQuery column.

    Returns:
        None: if default Arrow type inspection should be used.
    """
    if field.mode is not None and field.mode.upper() == "REPEATED":
        inner_type = bq_to_arrow_data_type(
            bigquery.SchemaField(field.name, field.field_type, fields=field.fields)
        )
        if inner_type:
            return pyarrow.list_(inner_type)
        return None

    field_type_upper = field.field_type.upper() if field.field_type else ""
    if field_type_upper in _STRUCT_TYPES:
        return bq_to_arrow_struct_data_type(field)

    if field_type_upper == "RANGE":
        return bq_to_arrow_range_data_type(field.range_element_type)

    data_type_constructor = bq_to_arrow_scalars(field_type_upper)
    if data_type_constructor is None:
        return None
    return data_type_constructor()


def bq_to_arrow_field(bq_field, array_type=None):
    """Return the Arrow field, corresponding to a given BigQuery column.

    Returns:
        None: if the Arrow type cannot be determined.
    """
    arrow_type = bq_to_arrow_data_type(bq_field)
    if arrow_type is not None:
        if array_type is not None:
            arrow_type = array_type  # For GEOGRAPHY, at least initially
        metadata = BQ_FIELD_TYPE_TO_ARROW_FIELD_METADATA.get(
            bq_field.field_type.upper() if bq_field.field_type else ""
        )
        return pyarrow.field(
            bq_field.name,
            arrow_type,
            # Even if the remote schema is REQUIRED, there's a chance there's
            # local NULL values. Arrow will gladly interpret these NULL values
            # as non-NULL and give you an arbitrary value. See:
            # https://github.com/googleapis/python-bigquery/issues/1692
            nullable=False if bq_field.mode.upper() == "REPEATED" else True,
            metadata=metadata,
        )

    warnings.warn(
        "Unable to determine Arrow type for field '{}'.".format(bq_field.name)
    )
    return None


def bq_to_arrow_schema(bq_schema):
    """Return the Arrow schema, corresponding to a given BigQuery schema.

    Returns:
        None: if any Arrow type cannot be determined.
    """
    arrow_fields = []
    for bq_field in bq_schema:
        arrow_field = bq_to_arrow_field(bq_field)
        if arrow_field is None:
            # Auto-detect the schema if there is an unknown field type.
            return None
        arrow_fields.append(arrow_field)
    return pyarrow.schema(arrow_fields)


def default_types_mapper(
    date_as_object: bool = False,
    bool_dtype: Union[Any, None] = None,
    int_dtype: Union[Any, None] = None,
    float_dtype: Union[Any, None] = None,
    string_dtype: Union[Any, None] = None,
    date_dtype: Union[Any, None] = None,
    datetime_dtype: Union[Any, None] = None,
    time_dtype: Union[Any, None] = None,
    timestamp_dtype: Union[Any, None] = None,
    range_date_dtype: Union[Any, None] = None,
    range_datetime_dtype: Union[Any, None] = None,
    range_timestamp_dtype: Union[Any, None] = None,
):
    """Create a mapping from pyarrow types to pandas types.

    This overrides the pandas defaults to use null-safe extension types where
    available.

    See: https://arrow.apache.org/docs/python/api/datatypes.html for a list of
    data types. See:
    tests/unit/test__pandas_helpers.py::test_bq_to_arrow_data_type for
    BigQuery to Arrow type mapping.

    Note to google-cloud-bigquery developers: If you update the default dtypes,
    also update the docs at docs/usage/pandas.rst.
    """

    def types_mapper(arrow_data_type):
        if bool_dtype is not None and pyarrow.types.is_boolean(arrow_data_type):
            return bool_dtype

        elif int_dtype is not None and pyarrow.types.is_integer(arrow_data_type):
            return int_dtype

        elif float_dtype is not None and pyarrow.types.is_floating(arrow_data_type):
            return float_dtype

        elif string_dtype is not None and pyarrow.types.is_string(arrow_data_type):
            return string_dtype

        elif (
            # If date_as_object is True, we know some DATE columns are
            # out-of-bounds of what is supported by pandas.
            date_dtype is not None
            and not date_as_object
            and pyarrow.types.is_date(arrow_data_type)
        ):
            return date_dtype

        elif (
            datetime_dtype is not None
            and pyarrow.types.is_timestamp(arrow_data_type)
            and arrow_data_type.tz is None
        ):
            return datetime_dtype

        elif (
            timestamp_dtype is not None
            and pyarrow.types.is_timestamp(arrow_data_type)
            and arrow_data_type.tz is not None
        ):
            return timestamp_dtype

        elif time_dtype is not None and pyarrow.types.is_time(arrow_data_type):
            return time_dtype

        elif pyarrow.types.is_struct(arrow_data_type):
            if range_datetime_dtype is not None and arrow_data_type.equals(
                range_datetime_dtype.pyarrow_dtype
            ):
                return range_datetime_dtype

            elif range_date_dtype is not None and arrow_data_type.equals(
                range_date_dtype.pyarrow_dtype
            ):
                return range_date_dtype

            # TODO: this section does not have a test yet OR at least not one that is
            # recognized by coverage, hence the pragma. See Issue: #2132
            elif (
                range_timestamp_dtype is not None
                and arrow_data_type.equals(  # pragma: NO COVER
                    range_timestamp_dtype.pyarrow_dtype
                )
            ):
                return range_timestamp_dtype

    return types_mapper
