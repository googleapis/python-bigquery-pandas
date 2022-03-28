# pandas-gbq Roadmap

The purpose of this package is to provide a small subset of BigQuery
functionality that maps well to
[pandas.read_gbq](https://pandas.pydata.org/docs/reference/api/pandas.read_gbq.html#pandas.read_gbq)
and
[pandas.DataFrame.to_gbq](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_gbq.html#pandas.DataFrame.to_gbq).

A note on pandas.read_sql: we'd like to be compatible with this too, for folks
that need better performance compared to the SQLAlchemy connector.
