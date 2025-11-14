# Copyright (c) 2025 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from typing import Sequence

import google.cloud.bigquery
import pytest

import pandas_gbq.constants
import pandas_gbq.core.sample

test_cases = [
    pytest.param(
        [
            google.cloud.bigquery.SchemaField("id", "INT64"),  # 8
            google.cloud.bigquery.SchemaField("is_valid", "BOOL"),  # 1
            google.cloud.bigquery.SchemaField("price", "NUMERIC"),  # 16
            google.cloud.bigquery.SchemaField("big_value", "BIGNUMERIC"),  # 32
        ],
        8 + 1 + 16 + 32,  # 57
        id="Fixed_Size_Types",
    ),
    pytest.param(
        [
            google.cloud.bigquery.SchemaField(
                "coords",
                "RECORD",
                fields=[
                    google.cloud.bigquery.SchemaField("lat", "FLOAT64"),  # 8
                    google.cloud.bigquery.SchemaField("lon", "FLOAT64"),  # 8
                ],
            ),
        ],
        16,  # 8 + 8
        id="Simple_Struct",
    ),
    pytest.param(
        [
            google.cloud.bigquery.SchemaField(
                "history", "TIMESTAMP", mode="REPEATED"
            ),  # 5 * 8
        ],
        pandas_gbq.core.sample._ARRAY_LENGTH_ESTIMATE * 8,  # 40
        id="Simple_Array",
    ),
    pytest.param(
        [
            google.cloud.bigquery.SchemaField(
                "addresses",
                "RECORD",
                mode="REPEATED",
                fields=[
                    google.cloud.bigquery.SchemaField("street", "STRING"),  # 1KIB
                    google.cloud.bigquery.SchemaField("zip", "INT64"),  # 8
                ],
            ),
        ],
        pandas_gbq.core.sample._ARRAY_LENGTH_ESTIMATE
        * (pandas_gbq.constants.BYTES_IN_KIB + 8),
        id="Repeated_Struct",
    ),
    pytest.param(
        [
            google.cloud.bigquery.SchemaField("empty_struct", "RECORD", fields=[]),  # 0
            google.cloud.bigquery.SchemaField("simple_int", "INT64"),  # 8
        ],
        8,  # 0 + 8
        id="empty-struct",
    ),
    pytest.param(
        [
            google.cloud.bigquery.SchemaField("bytes", "BYTES"),
        ]
        * 9_999,
        pandas_gbq.core.sample._MAX_ROW_BYTES,
        id="many-bytes",
    ),
    # Case 8: Complex Mix (Combining multiple cases)
    pytest.param(
        [
            google.cloud.bigquery.SchemaField("key", "INT64"),  # 8
            google.cloud.bigquery.SchemaField("notes", "STRING"),  # 1KIB
            google.cloud.bigquery.SchemaField(
                "history", "TIMESTAMP", mode="REPEATED"
            ),  # 40
            google.cloud.bigquery.SchemaField(
                "details",
                "RECORD",
                fields=[
                    google.cloud.bigquery.SchemaField("d1", "NUMERIC"),  # 16
                    google.cloud.bigquery.SchemaField("d2", "BYTES"),  # 1MB
                ],
            ),
        ],
        8
        + pandas_gbq.constants.BYTES_IN_KIB
        + 40
        + (16 + pandas_gbq.constants.BYTES_IN_MIB),
        id="Complex_Mix",
    ),
]


@pytest.mark.parametrize("schema, expected_size", test_cases)
def test_estimate_row_size_parametrized(
    schema: Sequence[google.cloud.bigquery.SchemaField], expected_size: int
):
    actual_size = pandas_gbq.core.sample._estimate_row_bytes(schema)
    assert actual_size == expected_size
