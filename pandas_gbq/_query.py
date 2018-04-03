
import pkg_resources
from google.cloud import bigquery


# Version with query config breaking change.
BIGQUERY_CONFIG_VERSION = pkg_resources.parse_version('0.32.0.dev1')


def query_config(resource, installed_version):
    if installed_version < BIGQUERY_CONFIG_VERSION:
        return bigquery.QueryJobConfig.from_api_repr(resource.get('query', {}))
    return bigquery.QueryJobConfig.from_api_repr(resource)
