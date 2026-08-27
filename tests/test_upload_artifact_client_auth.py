from unittest.mock import patch

import pytest

from app.infrastructure.upload_artifact_client import (
    UploadArtifactClient,
    UploadArtifactClientError,
)


OAUTH_ENV = {
    "OAUTH_CLIENT_ID": "etl-client",
    "OAUTH_CLIENT_SECRET": "etl-secret",
    "OAUTH_TOKEN_URL": "http://identity/oauth/token",
    "OAUTH_AUDIENCE": "upload-api",
    "OAUTH_SCOPES": "upload.artifacts.read upload.artifacts.lease",
    "UPLOAD_API_SERVICE_TOKEN": "",
    "UPLOAD_API_CALLER_TOKEN": "",
}


def test_oauth_credentials_configure_async_and_sync_callers():
    with patch.dict("os.environ", OAUTH_ENV, clear=False), patch(
        "app.infrastructure.upload_artifact_client.OAuthServiceClient"
    ) as async_client, patch(
        "app.infrastructure.upload_artifact_client.SyncOAuthServiceClient"
    ) as sync_client:
        sync_client.return_value.authorization_header.return_value = {
            "Authorization": "Bearer generated"
        }
        caller = UploadArtifactClient()

    expected = (
        "http://identity/oauth/token",
        "etl-client",
        "etl-secret",
        "upload-api",
        ("upload.artifacts.read", "upload.artifacts.lease"),
    )
    async_client.assert_called_once_with(*expected)
    sync_client.assert_called_once_with(*expected)
    assert caller.headers == {"Authorization": "Bearer generated"}


def test_partial_oauth_credentials_are_rejected_even_with_legacy_fallback():
    env = {
        **OAUTH_ENV,
        "OAUTH_CLIENT_SECRET": "",
        "UPLOAD_API_SERVICE_TOKEN": "legacy-token",
    }
    with patch.dict("os.environ", env, clear=False):
        with pytest.raises(UploadArtifactClientError, match="configured together"):
            UploadArtifactClient()


def test_legacy_token_remains_available_during_migration():
    env = {
        **OAUTH_ENV,
        "OAUTH_CLIENT_ID": "",
        "OAUTH_CLIENT_SECRET": "",
        "UPLOAD_API_SERVICE_TOKEN": "legacy-token",
    }
    with patch.dict("os.environ", env, clear=False), patch(
        "app.infrastructure.upload_artifact_client.record_auth_event"
    ) as auth_event:
        caller = UploadArtifactClient()
        assert caller.headers == {"Authorization": "Bearer legacy-token"}

    assert auth_event.call_args_list[-1].kwargs == {
        "outcome": "used",
        "caller_service": "etl-api",
        "resource_service": "upload-api",
    }
