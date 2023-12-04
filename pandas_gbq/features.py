# Copyright (c) 2017 pandas-gbq Authors All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

"""Module for checking dependency versions and supported features."""

# https://github.com/googleapis/python-bigquery/blob/master/CHANGELOG.md
BIGQUERY_MINIMUM_VERSION = "1.27.2"
BIGQUERY_ACCURATE_TIMESTAMP_VERSION = "2.6.0"
BIGQUERY_FROM_DATAFRAME_CSV_VERSION = "2.6.0"
BIGQUERY_SUPPORTS_BIGNUMERIC_VERSION = "2.10.0"
BIGQUERY_NO_DATE_AS_OBJECT_VERSION = "3.0.0"
PANDAS_VERBOSITY_DEPRECATION_VERSION = "0.23.0"
PANDAS_BOOLEAN_DTYPE_VERSION = "1.0.0"
PANDAS_PARQUET_LOSSLESS_TIMESTAMP_VERSION = "1.1.0"


class Features:
    def __init__(self):
        self._bigquery_installed_version = None
        self._pandas_installed_version = None

    def compareVersion(self, version1, version2):
        def cmp(a, b):
            return (a > b) - (a < b)

        v1, v2 = (list(map(int, v.split("."))) for v in (version1, version2))
        d = len(v2) - len(v1)
        result = cmp(v1 + [0] * d, v2 + [0] * -d)

        if result == 0:
            return 0  # version1 == version 2
        elif result < 0:
            return -1  # version1 < version2
        else:
            return 1  # version1 > version2

    @property
    def bigquery_installed_version(self):
        import google.cloud.bigquery

        try:
            import importlib.metadata as metadata
        except ImportError:
            import importlib_metadata as metadata

        if self._bigquery_installed_version is not None:
            return self._bigquery_installed_version

        self._bigquery_installed_version = metadata.version("google-cloud-bigquery")
        bigquery_minimum_version = BIGQUERY_MINIMUM_VERSION

        if self._bigquery_installed_version < bigquery_minimum_version:
            raise ImportError(
                "pandas-gbq requires google-cloud-bigquery >= {0}, "
                "current version {1}".format(
                    bigquery_minimum_version, self._bigquery_installed_version
                )
            )

        return self._bigquery_installed_version

    @property
    def bigquery_has_accurate_timestamp(self):
        min_version = BIGQUERY_ACCURATE_TIMESTAMP_VERSION
        return self.compareVersion(self.bigquery_installed_version, min_version) in [
            0,
            1,
        ]

    @property
    def bigquery_has_bignumeric(self):
        min_version = BIGQUERY_SUPPORTS_BIGNUMERIC_VERSION
        return self.compareVersion(self.bigquery_installed_version, min_version) in [
            0,
            1,
        ]

    @property
    def bigquery_has_from_dataframe_with_csv(self):
        bigquery_from_dataframe_version = BIGQUERY_FROM_DATAFRAME_CSV_VERSION
        return self.compareVersion(
            self.bigquery_installed_version, bigquery_from_dataframe_version
        ) in [0, 1]

    @property
    def bigquery_needs_date_as_object(self):
        max_version = BIGQUERY_NO_DATE_AS_OBJECT_VERSION
        return self.compareVersion(self.bigquery_installed_version, max_version) == -1

    @property
    def pandas_installed_version(self):
        import pandas

        try:
            import importlib.metadata as metadata
        except ImportError:
            import importlib_metadata as metadata

        if self._pandas_installed_version is not None:
            return self._pandas_installed_version

        self._pandas_installed_version = metadata.version("pandas")
        return self._pandas_installed_version

    @property
    def pandas_has_deprecated_verbose(self):
        # Add check for Pandas version before showing deprecation warning.
        # https://github.com/pydata/pandas-gbq/issues/157
        pandas_verbosity_deprecation = PANDAS_VERBOSITY_DEPRECATION_VERSION
        return self.compareVersion(
            self.pandas_installed_version, pandas_verbosity_deprecation
        ) in [0, 1]

    @property
    def pandas_has_boolean_dtype(self):
        desired_version = PANDAS_BOOLEAN_DTYPE_VERSION
        return self.compareVersion(self.pandas_installed_version, desired_version) in [
            0,
            1,
        ]

    @property
    def pandas_has_parquet_with_lossless_timestamp(self):
        desired_version = PANDAS_PARQUET_LOSSLESS_TIMESTAMP_VERSION
        return self.compareVersion(self.pandas_installed_version, desired_version) in [
            0,
            1,
        ]


FEATURES = Features()
