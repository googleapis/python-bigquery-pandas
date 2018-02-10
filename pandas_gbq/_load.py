"""Helper methods for loading data into BigQuery"""

from google.cloud import bigquery
import six


def encode_chunk(dataframe):
    """Return a file-like object of CSV-encoded rows.

    Args:
      dataframe (pandas.DataFrame): A chunk of a dataframe to encode
    """
    csv_buffer = six.StringIO()
    dataframe.to_csv(
        csv_buffer, index=False, header=False, encoding='utf-8',
        date_format='%Y-%m-%d %H:%M')

    # Convert to a BytesIO buffer so that unicode text is properly handled.
    # See: https://github.com/pydata/pandas-gbq/issues/106
    body = csv_buffer.getvalue()
    if isinstance(body, bytes):
        body = body.decode('utf-8')
    body = body.encode('utf-8')
    return six.BytesIO(body)


def encode_chunks(dataframe, chunksize):
    dataframe = dataframe.reset_index(drop=True)
    remaining_rows = len(dataframe)
    total_rows = remaining_rows
    start_index = 0
    while start_index < total_rows:
        chunk_buffer = encode_chunk(
            dataframe[start_index:start_index+chunksize])
        start_index += chunksize
        remaining_rows = max(0, remaining_rows - chunksize)
        yield remaining_rows, chunk_buffer


def load_chunks(client, dataframe, dataset_id, table_id, chunksize):
    destination_table = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.source_format = 'CSV'

    for remaining_rows, chunk_buffer in encode_chunks(dataframe, chunksize):
        yield remaining_rows
        client.load_table_from_file(
            chunk_buffer,
            destination_table,
            job_config=job_config).result()