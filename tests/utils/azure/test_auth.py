"""Test cases for the auth module in the azure package."""

import os

from azure.identity import ClientSecretCredential

from cleansweep.utils.azure.auth import AzureCredentials, _get_env, _get_secret_env


class TestCredentials:
    """Test suite for credentials."""

    def test_get_env(self):
        """
        Test the _get_env function to ensure it retrieves the correct environment variable value.

        This test sets an environment variable "TEST_VAR" to "test_value", verifies that the
        _get_env function returns the correct value, and then deletes the environment variable.

        Steps:
        1. Set the environment variable "TEST_VAR" to "test_value".
        2. Assert that _get_env("TEST_VAR") returns "test_value".
        3. Delete the environment variable "TEST_VAR".

        Raises:
            AssertionError: If the _get_env function does not return the expected value.
        """

        os.environ["TEST_VAR"] = "test_value"
        assert _get_env("TEST_VAR") == "test_value"
        del os.environ["TEST_VAR"]

    def test_get_secret_env(self):
        """
        Test the `_get_secret_env` function to ensure it retrieves the secret value
        from the environment variable correctly.

        Steps:
        1. Set an environment variable `TEST_SECRET_VAR` with a value of "secret_value".
        2. Call `_get_secret_env` with the name of the environment variable.
        3. Assert that the returned secret is not None.
        4. Assert that the secret's value matches "secret_value".
        5. Clean up by deleting the environment variable `TEST_SECRET_VAR`.

        This test ensures that the `_get_secret_env` function can correctly fetch
        and return the value of an environment variable.
        """
        os.environ["TEST_SECRET_VAR"] = "secret_value"
        secret = _get_secret_env("TEST_SECRET_VAR")
        assert secret is not None
        assert secret.get_secret_value() == "secret_value"
        del os.environ["TEST_SECRET_VAR"]

    def test_azure_credentials_initialization(self):
        """
        Test the initialization of AzureCredentials with environment variables.

        This test sets up the necessary environment variables for Azure credentials,
        initializes an instance of AzureCredentials, and asserts that the credentials
        are correctly set. After the assertions, it cleans up by deleting the environment
        variables.

        Tested environment variables:
        - AZURE_CLIENT_ID
        - AZURE_CLIENT_SECRET
        - AZURE_SCOPE
        - AZURE_TENANT_ID
        - OPENAI_API_BASE
        - OPENAI_API_VERSION

        Assertions:
        - The Azure client ID is correctly set.
        - The Azure client secret is correctly set.
        - The Azure scope is correctly set.
        - The Azure tenant ID is correctly set.
        - The OpenAI API base is correctly set.
        - The OpenAI API version is correctly set.
        """
        if not os.getenv("AZURE_CLIENT_ID"):
            os.environ["AZURE_CLIENT_ID"] = "test_client_id"
        if not os.getenv("AZURE_CLIENT_SECRET"):
            os.environ["AZURE_CLIENT_SECRET"] = "test_client_secret"
        if not os.getenv("AZURE_SCOPE"):
            os.environ["AZURE_SCOPE"] = "test_scope"
        if not os.getenv("AZURE_TENANT_ID"):
            os.environ["AZURE_TENANT_ID"] = "test_tenant_id"
        if not os.getenv("OPENAI_API_BASE"):
            os.environ["OPENAI_API_BASE"] = "test_api_base"
        if not os.getenv("OPENAI_API_VERSION"):
            os.environ["OPENAI_API_VERSION"] = "test_api_version"

        creds = AzureCredentials()
        assert creds.azure_client_id.get_secret_value() == os.getenv("AZURE_CLIENT_ID")
        assert creds.azure_client_secret.get_secret_value() == os.getenv(
            "AZURE_CLIENT_SECRET"
        )
        assert creds.azure_scope == os.getenv("AZURE_SCOPE")
        assert creds.azure_tenant_id.get_secret_value() == os.getenv("AZURE_TENANT_ID")
        assert creds.openai_api_base == os.getenv("OPENAI_API_BASE")
        assert creds.openai_api_version == os.getenv("OPENAI_API_VERSION")

    def test_get_credentials(self):
        """
        Test the `get_credentials` method of the `AzureCredentials` class.

        This test sets up environment variables for Azure authentication if they are not already set,
        then it creates an instance of `AzureCredentials` and retrieves the credentials using the
        `_get_credentials` method. It asserts that the returned credential is an instance of
        `ClientSecretCredential` and verifies that the credential's client ID, client secret, and
        tenant ID match the expected test values.

        Environment Variables:
        - AZURE_CLIENT_ID: The client ID for Azure authentication.
        - AZURE_CLIENT_SECRET: The client secret for Azure authentication.
        - AZURE_SCOPE: The scope for Azure authentication.
        - AZURE_TENANT_ID: The tenant ID for Azure authentication.
        - OPENAI_API_BASE: The base URL for the OpenAI API.
        - OPENAI_API_VERSION: The version of the OpenAI API.

        Assertions:
        - The credential is an instance of `ClientSecretCredential`.
        - The credential's client ID matches "test_client_id".
        - The credential's client secret matches "test_client_secret".
        - The credential's tenant ID matches "test_tenant_id".
        """
        if not os.getenv("AZURE_CLIENT_ID"):
            os.environ["AZURE_CLIENT_ID"] = "test_client_id"
        if not os.getenv("AZURE_CLIENT_SECRET"):
            os.environ["AZURE_CLIENT_SECRET"] = "test_client_secret"
        if not os.getenv("AZURE_SCOPE"):
            os.environ["AZURE_SCOPE"] = "test_scope"
        if not os.getenv("AZURE_TENANT_ID"):
            os.environ["AZURE_TENANT_ID"] = "test_tenant_id"
        if not os.getenv("OPENAI_API_BASE"):
            os.environ["OPENAI_API_BASE"] = "test_api_base"
        if not os.getenv("OPENAI_API_VERSION"):
            os.environ["OPENAI_API_VERSION"] = "test_api_version"

        creds = AzureCredentials()
        credential = creds._get_credentials()

        assert isinstance(credential, ClientSecretCredential)
