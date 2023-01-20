Authentication
==============

.. contents:: Table of Contents
    :local:
    :depth: 1

Before you begin, you must create a Google Cloud Platform project. Use the
`BigQuery sandbox <https://cloud.google.com/bigquery/docs/sandbox>`__ to try
the service for free.

pandas-gbq `authenticates with the Google BigQuery service
<https://cloud.google.com/bigquery/docs/authentication/>`_ via OAuth 2.0. Use
the ``credentials`` argument to explicitly pass in Google
:class:`~google.auth.credentials.Credentials`.

.. _authentication:

Default Authentication Methods
------------------------------

If the ``credentials`` parameter is not set, pandas-gbq tries the following
authentication methods:

1. In-memory, cached credentials at ``pandas_gbq.context.credentials``. See
   :attr:`pandas_gbq.Context.credentials` for details.

   .. code:: python

       import pandas_gbq

       credentials = ...  # From google-auth or pydata-google-auth library.

       # Update the in-memory credentials cache (added in pandas-gbq 0.7.0).
       pandas_gbq.context.credentials = credentials
       pandas_gbq.context.project = "your-project-id"

       # The credentials and project_id arguments can be omitted.
       df = pandas_gbq.read_gbq("SELECT my_col FROM `my_dataset.my_table`")

2. Application Default Credentials via the :func:`google.auth.default`
   function.

   .. note::

       If pandas-gbq can obtain default credentials but those credentials
       cannot be used to query BigQuery, pandas-gbq will also try obtaining
       user account credentials.

       A common problem with default credentials when running on Google
       Compute Engine is that the VM does not have sufficient scopes to query
       BigQuery.

3. User account credentials.

   pandas-gbq loads cached credentials from a hidden user folder on the
   operating system.

   Windows
       ``%APPDATA%\pandas_gbq\bigquery_credentials.dat``

   Linux/Mac/Unix
       ``~/.config/pandas_gbq/bigquery_credentials.dat``

   If pandas-gbq does not find cached credentials, it prompts you to open a
   web browser, where you can grant pandas-gbq permissions to access your
   cloud resources. These credentials are only used locally. See the
   :doc:`privacy policy <../privacy>` for details.


Authenticating with a Service Account
--------------------------------------

Using service account credentials is particularly useful when working on
remote servers without access to user input.

Create a service account key via the `service account key creation page
<https://console.cloud.google.com/apis/credentials/serviceaccountkey>`_ in
the Google Cloud Platform Console. Select the JSON key type and download the
key file.

To use service account credentials, set the ``credentials`` parameter to the result of a call to:

* :func:`google.oauth2.service_account.Credentials.from_service_account_file`,
    which accepts a file path to the JSON file.

    .. code:: python

        from google.oauth2 import service_account
        import pandas_gbq

        credentials = service_account.Credentials.from_service_account_file(
            'path/to/key.json',
        )
        df = pandas_gbq.read_gbq(sql, project_id="YOUR-PROJECT-ID", credentials=credentials)

* :func:`google.oauth2.service_account.Credentials.from_service_account_info`,
    which accepts a dictionary corresponding to the JSON file contents.

    .. code:: python

        from google.oauth2 import service_account
        import pandas_gbq

        credentials = service_account.Credentials.from_service_account_info(
            {
                "type": "service_account",
                "project_id": "YOUR-PROJECT-ID",
                "private_key_id": "6747200734a1f2b9d8d62fc0b9414c5f2461db0e",
                "private_key": "-----BEGIN PRIVATE KEY-----\nM...I==\n-----END PRIVATE KEY-----\n",
                "client_email": "service-account@YOUR-PROJECT-ID.iam.gserviceaccount.com",
                "client_id": "12345678900001",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/...iam.gserviceaccount.com"
            },
        )
        df = pandas_gbq.read_gbq(sql, project_id="YOUR-PROJECT-ID", credentials=credentials)

Use the :func:`~google.oauth2.service_account.Credentials.with_scopes` method
to use authorize with specific OAuth2 scopes, which may be required in
queries to federated data sources such as Google Sheets.

.. code:: python

   credentials = ...
   credentials = credentials.with_scopes(
       [
           'https://www.googleapis.com/auth/drive',
           'https://www.googleapis.com/auth/cloud-platform',
       ],
   )
   df = pandas_gbq.read_gbq(..., credentials=credentials)

See the `Getting started with authentication on Google Cloud Platform
<https://cloud.google.com/docs/authentication/getting-started>`_ guide for
more information on service accounts.

.. _authentication-user:

Authenticating with a User Account
----------------------------------

Use the `pydata-google-auth <https://pydata-google-auth.readthedocs.io/>`__
library to authenticate with a user account (i.e. a G Suite or Gmail
account). The :func:`pydata_google_auth.get_user_credentials` function loads
credentials from a cache on disk or initiates an OAuth 2.0 flow if cached
credentials are not found.

.. code:: python

   import pandas_gbq
   import pydata_google_auth

   SCOPES = [
       'https://www.googleapis.com/auth/cloud-platform',
       'https://www.googleapis.com/auth/drive',
   ]

   credentials = pydata_google_auth.get_user_credentials(
       SCOPES,
       # Note, this doesn't work if you're running from a notebook on a
       # remote sever, such as over SSH or with Google Colab. In those cases,
       # install the gcloud command line interface and authenticate with the
       # `gcloud auth application-default login` command and the `--no-browser`
       # option.
       auth_local_webserver=True,
   )

   df = pandas_gbq.read_gbq(
       "SELECT my_col FROM `my_dataset.my_table`",
       project_id='YOUR-PROJECT-ID',
       credentials=credentials,
   )

.. warning::

   Do not store credentials on disk when using shared computing resources
   such as a GCE VM or Colab notebook. Use the
   :data:`pydata_google_auth.cache.NOOP` cache to avoid writing credentials
   to disk.

   .. code:: python

      import pydata_google_auth.cache

      credentials = pydata_google_auth.get_user_credentials(
          SCOPES,
          # Use the NOOP cache to avoid writing credentials to disk.
          cache=pydata_google_auth.cache.NOOP,
      )

Additional information on the user credentials authentication mechanism
can be found in the `Google Cloud authentication guide
<https://cloud.google.com/docs/authentication/end-user>`__.

Authenticating from Highly Constrained Development Environments
---------------------------------------------------------------

These instructions are primarily for users who are working in a *highly
constrained development environment*:

Highly constrained development environments typically prevent users from using
the `Default Authentication Methods` and are generally characterized by one or
more of the following circumstances:

* There are limitations on what you can install on the development environment
  (i.e. you can't install ``gcloud``).
* You don't have access to a graphical user interface (i.e. you are remotely
  SSH'ed into a headless server and don't have access to a browser to complete
  the authentication process used in the default login workflow) .
* The code is being executed in a typical data science context: using a Jupyter
  (or similar) notebook.

When dealing with highly constrained environments, there are two primary options
that one can choose from: Testing Mode OR an institution-specific authentication
page.

#. Testing Mode: This approach requires that you enable Testing Mode on your
   Cloud Project and that you have fewer than 100 users.
#. Institution-specific authentication page: In cases where the Testing Mode
   option is not possible and/or there are specific institutional needs,
   you/your institution can create and host an institution-specific OAuth
   authentication page and associate a redirect URI to that Cloud Project.

OPTION 1 - Testing Mode
^^^^^^^^^^^^^^^^^^^^^^^

This approach is for limited use, such as when testing your product. It is not
intended for production use. If you have fewer than 100 users, it is possible to
configure User Type as External and the Publishing Status of your Project as
Testing Mode to enable OAuth Out-of-Band (OOB) Authentication. NOTE: general
purpose `OOB Authentication was deprecated <https://developers.googleblog.com/2022/02/making-oauth-flows-safer.html>`_ for all use cases except Testing Mode.

.. note:: Projects configured with a Publishing Status of Testing are **limited to
   up to 100 test users** who must be individually listed in the OAuth consent
   screen. A test user consumes a Project's test user quota once added to the
   Project.

   Authentications by a test user **will expire seven days from the time of consent.** If your OAuth client requests an offline access type and receives a refresh token, that token will also expire.

   To move a project from Testing Mode to In Production requires app verification
   and requires your institution to switch to using an alternate authentication
   method, such as an institution-specific authentication page.

The test users must be manually and individually added to your Cloud Project (i.e. you can not provide a group email alias for your development team because the system does not support alias expansion).

Google displays a warning message before allowing a specified test user to authenticate scopes requested by your Project's OAuth clients. The warning message confirms the user has test access to your Project and reminds them that they should consider the risks associated with granting access to their data to an unverified app.

For additional limitations and details about Testing Mode, see: `Setting up your OAuth consent screen <https://support.google.com/cloud/answer/10311615?hl=en#zippy=%2Ctesting>`_.

To enable Testing Mode and add users to your Cloud Project, in your Project dashboard:

#. Click on **APIs & Services > OAuth consent screen**.
#. Select **External** to enable Testing Mode.
#. Click **Create**.
#. Fill in the necessary details related to the following:

   #. App information
   #. App domain, including Authorized domains
   #. Developer contact information

#. Click **Save and Continue**.
#. Click **Add or Remove Scopes** to choose appropriate Scopes for your Project. An *Update selected scopes* dialogue will open.
#. Click **Update**.
#. Click **Save and Continue**.
#. Click **Add Users** to add users to your Project. An *Add Users* dialogue will open.
#. Enter the user's name in the text field.
#. Click **Add**.
#. Click **Save and Continue**.
#. A summary screen will display with all the information you entered.

To access BigQuery programmatically, you will need your Client ID and your Client Secret, which can be generated as follows:

#. Click **APIs and Services > Credentials**.
#. Click **+ Create Credentials > OAuth client ID**.
#. Select Desktop app In the **Application Type** field.
#. Fill in the name of your OAuth 2.0 client in the **Name** field.
#. Click **Create**.

Your Client ID and Client Secret are displayed in the pop-up. There is also a reminder that only test users that are listed on the Oauth consent screen can access the application. The client ID and Client Secret can also be found here, if they have already been generated:

#. Click on **APIs and Services > Credentials**.
#. Click on the name of your OAuth 2.0 Client under **OAuth 2.0 Client IDs**.
#. The Client ID and Client Secret will be displayed.

With the Client ID and Client Secret, you are ready to create an OAuth workflow using code similar to the following:

To run this code sample, you will need to have ``python-bigquery-pandas`` installed. The following dependencies will be installed by ``python-bigquery-pandas``:

* pydata-google-auth
* google-auth
* google-auth-oauthlib
* pandas
* google-cloud-bigquery
* tqdm

**Sample code:** ``oauth-read-from-bq-testing-mode.py``

.. code:: python

    import pandas_gbq

    projectid = "your-project-name here"

    CLIENT_ID = "your-client-id here"

    # WARNING: for the purposes of this demo code, the Client Secret is
    # included here. In your script, take precautions to ensure
    # that your Client Secret does not get pushed to a public
    # repository or otherwise get compromised
    CLIENT_SECRET = "your-client-secret here"

    df = pandas_gbq.read_gbq(
        "SELECT SESSION_USER() as user_id, CURRENT_TIMESTAMP() as time",
        project_id=projectid,
        auth_local_webserver=False,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    print(df)

OPTION 2 - Institution-specific authentication page
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To access Bigquery programmatically, you will need your Client ID and your Client Secret, an OAuth authorization page, and an assigned redirect URI.

To add a Client ID, Client Secret, and Redirect URI to your Cloud Project, in your Project dashboard:

#. Click on **APIs & Services > OAuth consent screen**.
#. Select **Internal**.
#. Click **Create**.
#. Fill in the necessary details related to the following:

   #. App information
   #. App domain, including Authorized domains
   #. Developer contact information

#. Click **Save and Continue**.
#. Click **Add or Remove Scopes** to choose appropriate Scopes for your Project. An Update selected scopes dialogue will open.
#. Click **Update**.
#. Click **Save and Continue**.
#. Click on **APIs and Services > Credentials**.
#. Click on **+ Create Credentials > OAuth client ID**.
#. Select Web application in the **Application Type** field.
#. Fill in the name of your OAuth 2.0 client in the **Name** field.
#. Click **Add Uri** under the Authorized Redirect URIs section.
#. Add a URI for your application (i.e. the path to where you are hosting a file such as the ``oauth.html`` file shown below).
#. Click **Create**.

Your Client ID and Client Secret will be displayed in the pop-up. The client ID and Client Secret can also be found here:

#. Click on **APIs and Services > Credentials**.
#. Click on the name of your OAuth 2.0 Client under **OAuth 2.0 Client IDs**.
#. The Client ID and Client Secret and the Authorized Redirect URIs will be displayed.

You will need to host a webpage (such as ``oauth.html``) with some associated javascript (such as shown below in ``authcodescripts.js``) to parse the results of the OAuth workflow.

**Code Sample**: ``oauth.html``

.. code:: html

    <!DOCTYPE html>
    <html>
    <head>
        <script src="_static/js/authcodescripts.js"></script>
    </head>
    <body>
        <h1>Sign in to BigQuery</h1>
        <p>You are seeing this page because you are attempting to access BigQuery via one
    of several possible methods, including:</p>
        <blockquote>
        <div>
            <ul>
                <li><p>the <code><span>pandas_gbq</span></code> library (<a href="https://github.com/googleapis/python-bigquery-pandas">https://github.com/googleapis/python-bigquery-pandas</a>)</p></li>
            </ul>
            <p>OR a <code><span>pandas</span></code> library helper function such as:</p>
            <ul>
                <li><p><code><span>pandas.DataFrame.to_gbq()</span></code></p></li>
                <li><p><code><span>pandas.read_gbq()</span></code></p></li>
            </ul>
        </div>
        </blockquote>
        <p>from this or another machine. If this is not the case, close this tab.</p>
        <p>Enter the following verification code in the CommandLine Interface (CLI) on the
    machine you want to log into. This is a credential <strong>similar to your password</strong>
    and should not be shared with others.</p>
        <script type="text/javascript">
        window.addEventListener( "load", onloadoauthcode )
        </script>
        <div>
        <code class="auth-code"></code>
        </div>
        <br>
        <button class="copy" aria-live="assertive">Copy</button>
    </body>
    </html>

**Code Sample**: ``authcodescripts.js``

.. code:: javascript

    function onloadoauthcode() {
        const PARAMS = new Proxy(new URLSearchParams(window.location.search), {
            get: (searchParams, prop) => searchParams.get(prop),
        });
        const AUTH_CODE = PARAMS.code;
        document.querySelector('.auth-code').textContent = AUTH_CODE;
        setupCopyButton(document.querySelector('.copy'), AUTH_CODE);
    }

    function setupCopyButton(button, text) {
        button.addEventListener('click', () => {
            navigator.clipboard.writeText(text);
            button.textContent = "Verification Code Copied";
            setTimeout(() => {
                // Remove the aria-live label so that when the
                // button text changes back to "Copy", it is
                // not read out.
                button.removeAttribute("aria-live");
                button.textContent = "Copy";
            }, 1000);

            // Re-Add the aria-live attribute to enable speech for
            // when button text changes next time.
            setTimeout(() => {
                button.setAttribute("aria-live", "assertive");
            }, 2000);
        });
    }

With these items in place:

* Client ID
* Client Secret
* redirect URI
* authentication page

you are ready to create an OAuth workflow using code similar to the following:

To run this code sample, you will need to have ``python-bigquery-pandas`` installed. The following dependencies will be installed by ``python-bigquery-pandas``:

* pydata-google-auth
* google-auth
* google-auth-oauthlib
* pandas
* google-cloud-bigquery
* tqdm

**Sample Code**: ``oauth-read-from-bq-org-specific.py``

.. code:: python

    import pandas_gbq

    projectid = "your-project-name-here"

    REDIRECT_URI = "your-redirect-uri here/oauth.html"
    CLIENT_ID = "your-client-id here"

    # WARNING: for the purposes of this demo code, the Client Secret is
    # included here. In your script, take precautions to ensure
    # that your Client Secret does not get pushed to a public
    # repository or otherwise compromised
    CLIENT_SECRET = "your-client-secret here"

    df = pandas_gbq.read_gbq(
        "SELECT SESSION_USER() as user_id, CURRENT_TIMESTAMP() as time",
        project_id=projectid,
        auth_local_webserver=False,
        auth_redirect_uri=REDIRECT_URI,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    print(df)
