from __future__ import annotations

import os
import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import aiohttp
import requests
from service_auth import OAuthServiceClient, SyncOAuthServiceClient, record_auth_event


class UploadArtifactClientError(RuntimeError):
    pass


class UploadArtifactClient:
    def __init__(self):
        self.base_url = os.getenv("UPLOAD_API_URL", "http://localhost:8010/api/v1").rstrip("/")
        self.legacy_token = os.getenv("UPLOAD_API_SERVICE_TOKEN") or os.getenv("UPLOAD_API_CALLER_TOKEN", "")
        oauth_client_id = os.getenv("OAUTH_CLIENT_ID", "")
        oauth_client_secret = os.getenv("OAUTH_CLIENT_SECRET", "")
        token_url = os.getenv("OAUTH_TOKEN_URL", "http://localhost:8000/oauth/token")
        oauth_audience = os.getenv("OAUTH_AUDIENCE", "upload-api")
        oauth_scopes = tuple(
            os.getenv(
                "OAUTH_SCOPES",
                "upload.artifacts.read upload.artifacts.lease",
            ).split()
        )
        self._async_oauth = None
        self._sync_oauth = None
        if bool(oauth_client_id) != bool(oauth_client_secret):
            raise UploadArtifactClientError(
                "OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET must be configured together"
            )
        if oauth_client_id and (not oauth_audience or not oauth_scopes):
            raise UploadArtifactClientError(
                "OAUTH_AUDIENCE and OAUTH_SCOPES must be configured for OAuth calls"
            )
        if oauth_client_id and oauth_client_secret:
            arguments = (
                token_url,
                oauth_client_id,
                oauth_client_secret,
                oauth_audience,
                oauth_scopes,
            )
            self._async_oauth = OAuthServiceClient(*arguments)
            self._sync_oauth = SyncOAuthServiceClient(*arguments)
        elif self.legacy_token:
            logging.getLogger(__name__).warning(
                "DEPRECATED UPLOAD_API_SERVICE_TOKEN is in use; configure OAuth client credentials"
            )
            record_auth_event(
                "legacy_static_token",
                outcome="configured",
                caller_service="etl-api",
                resource_service="upload-api",
            )
        else:
            raise UploadArtifactClientError(
                "OAuth client credentials are not configured and no deprecated Upload token is available"
            )

    @property
    def headers(self) -> dict[str, str]:
        if self._sync_oauth:
            return self._sync_oauth.authorization_header()
        return self._legacy_headers()

    async def async_headers(self) -> dict[str, str]:
        if self._async_oauth:
            return await self._async_oauth.authorization_header()
        return self._legacy_headers()

    def _legacy_headers(self) -> dict[str, str]:
        record_auth_event(
            "legacy_static_token",
            outcome="used",
            caller_service="etl-api",
            resource_service="upload-api",
        )
        return {"Authorization": f"Bearer {self.legacy_token}"}

    async def acquire_lease(self, artifact_id: str, grant_id: str, reference: str) -> dict:
        async with aiohttp.ClientSession(headers=await self.async_headers()) as client:
            async with client.put(
                f"{self.base_url}/artifacts/{artifact_id}/leases",
                json={"grant_id": grant_id, "consumer_reference": reference},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                payload = await response.json()
                if response.status >= 400:
                    raise UploadArtifactClientError(payload.get("message") or str(payload))
                return payload

    async def metadata(self, artifact_id: str) -> dict:
        async with aiohttp.ClientSession(headers=await self.async_headers()) as client:
            async with client.get(
                f"{self.base_url}/artifacts/{artifact_id}",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                payload = await response.json()
                if response.status >= 400:
                    raise UploadArtifactClientError(payload.get("message") or str(payload))
                return payload

    async def release_lease(self, artifact_id: str, lease_id: str) -> None:
        async with aiohttp.ClientSession(headers=await self.async_headers()) as client:
            async with client.delete(
                f"{self.base_url}/artifacts/{artifact_id}/leases/{lease_id}",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as response:
                if response.status >= 400:
                    payload = await response.text()
                    raise UploadArtifactClientError(payload)

    @contextmanager
    def materialize(self, artifact_id: str, filename: str) -> Iterator[str]:
        suffix = Path(filename).suffix
        fd, temporary_path = tempfile.mkstemp(prefix="etl-artifact-", suffix=suffix)
        os.close(fd)
        try:
            with requests.get(
                f"{self.base_url}/artifacts/{artifact_id}/content",
                headers=self.headers,
                stream=True,
                allow_redirects=True,
                timeout=(10, 300),
            ) as response:
                if response.status_code >= 400:
                    raise UploadArtifactClientError(
                        f"Artifact download failed with HTTP {response.status_code}: {response.text[:500]}"
                    )
                with open(temporary_path, "wb") as destination:
                    for block in response.iter_content(chunk_size=1024 * 1024):
                        if block:
                            destination.write(block)
            yield temporary_path
        finally:
            Path(temporary_path).unlink(missing_ok=True)
