Installation
============

You can install pandas-gbq with ``conda``, ``pip``, or by installing from source.

Conda
-----

.. code-block:: shell

   $ conda install pandas-gbq --channel conda-forge

This installs pandas-gbq and all common dependencies, including ``pandas``.

Pip
---

To install the latest version of pandas-gbq: from the

.. code-block:: shell

    $ pip install pandas-gbq -U

This installs pandas-gbq and all common dependencies, including ``pandas``.


Install from Source
-------------------

.. code-block:: shell

    $ pip install git+https://github.com/pydata/pandas-gbq.git


Dependencies
------------

This module requires following additional dependencies:

- `google-auth <https://github.com/GoogleCloudPlatform/google-auth-library-python>`__: authentication and authorization for Google's API
- `google-auth-oauthlib <https://github.com/GoogleCloudPlatform/google-auth-library-python-oauthlib>`__: integration with `oauthlib <https://github.com/idan/oauthlib>`__ for end-user authentication
- `google-cloud-bigquery <http://github.com/GoogleCloudPlatform/google-cloud-python>`__: Google Cloud client library for BigQuery

.. note::

   The dependency on `google-cloud-bigquery <http://github.com/GoogleCloudPlatform/google-cloud-python>`__ is new in version 0.3.0 of ``pandas-gbq``.