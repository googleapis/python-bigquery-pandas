# Copyright (c) 2025 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from __future__ import annotations

import typing
from typing import Optional, Sequence, cast

import google.cloud.bigquery
import google.oauth2.credentials
import psutil

import pandas_gbq.constants
import pandas_gbq.core.read
import pandas_gbq.gbq_connector

# Only import at module-level at type checking time to avoid circular
# dependencies in the pandas package, which has an optional dependency on
# pandas-gbq.
if typing.TYPE_CHECKING:  # pragma: NO COVER
    import pandas


_TABLESAMPLE_ELIGIBLE_TYPES = ("TABLE", "EXTERNAL")


def _calculate_target_bytes(target_mb: Optional[int]) -> int:
    if target_mb is not None:
        return target_mb * pandas_gbq.constants.BYTES_IN_MIB

    mem = psutil.virtual_memory()
    return max(100 * pandas_gbq.constants.BYTES_IN_MIB, mem.available // 4)


def _estimate_limit(target_bytes : int, num_rows: Optional[int]):
    pass


def _estimate_row_bytes(fields: Sequence[google.cloud.bigquery.SchemaField]) -> int:
    pass


def _sample_with_tablesample(
    table: google.cloud.bigquery.Table,
    bqclient: google.cloud.bigquery.Client,
    proportion: float,
    row_count: int,
    progress_bar_type: Optional[str] = None,
    use_bqstorage_api: bool = True,
) -> Optional[pandas.DataFrame]:
    query = f"""
    SELECT *
    FROM `{table.project}.{table.dataset_id}.{table.table_id}`
    TABLESAMPLE SYSTEM ({float(proportion) * 100.0} PERCENT)
    ORDER BY RAND() DESC
    LIMIT {int(row_count)};
    """
    rows = bqclient.query_and_wait(query)
    return pandas_gbq.core.read.download_results(
        rows,
        bqclient=bqclient,
        progress_bar_type=progress_bar_type,
        warn_on_large_results=False,
        max_results=None,
        user_dtypes=None,
        use_bqstorage_api=use_bqstorage_api,
    )


def _sample_with_limit(
    bqclient: google.cloud.bigquery.Client, limit: int
) -> google.cloud.bigquery.TableReference:
    pass


def sample(
    table_id: str,
    *,
    target_mb: Optional[int] = None,
    credentials: Optional[google.oauth2.credentials.Credentials] = None,
    billing_project_id: Optional[str] = None,
    progress_bar_type: Optional[str] = None,
    use_bqstorage_api: bool = True,
) -> Optional[pandas.DataFrame]:
    target_bytes = _calculate_target_bytes(target_mb)
    connector = pandas_gbq.gbq_connector.GbqConnector(
        project_id=billing_project_id, credentials=credentials
    )
    credentials = cast(google.oauth2.credentials.Credentials, connector.credentials)
    bqclient = connector.get_client()
    table = bqclient.get_table(table_id)
    num_rows = table.num_rows

    # Table is small enough to download the whole thing.
    if (num_bytes := table.num_bytes) is not None and num_bytes <= target_bytes:
        rows_iter = bqclient.list_rows(table)
        return pandas_gbq.core.read.download_results(
            rows_iter,
            bqclient=bqclient,
            progress_bar_type=progress_bar_type,
            warn_on_large_results=False,
            max_results=None,
            user_dtypes=None,
            use_bqstorage_api=use_bqstorage_api,
        )

    # Table is eligible for TABLESAMPLE.
    if num_bytes is not None and table.table_type in _TABLESAMPLE_ELIGIBLE_TYPES:
        proportion = target_bytes / num_bytes
        row_count = max(1, int(num_rows * proportion)) if num_rows is not None else 
        return _sample_with_tablesample(
            table, bqclient=bqclient, proportion=proportion
        )
    table.num_rows

    # TODO: check table type to see if tablesample would be compatible.
    table.table_type
