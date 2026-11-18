"""application-manager E2E: the section schema and combined schema reads.

Both reads are on the platform plane and decided by the allowed callers. A
caller with no platform credential is turned away, and the shared-secret
identity, which no allowed-callers entry can match, gets a 403 problem.
Permitted reads are covered by the gear's in-process route tests.
"""
import httpx
import pytest

PROBLEM_TYPE_PREFIX = "gts://gts.cf.core.errors.err.v1~cf.core.err."

SCHEMA_READS = [
    "/section-keys/cf.core.noop.section/schema",
    "/manifest-schema",
]


def _is_problem(body: dict, status: int) -> bool:
    """A canonical problem document: `type` as a `gts://` URI, `title`, `status`."""
    return (
        isinstance(body, dict)
        and str(body.get("type", "")).startswith(PROBLEM_TYPE_PREFIX)
        and isinstance(body.get("title"), str)
        and body.get("status") == status
    )


@pytest.mark.smoke
@pytest.mark.asyncio
@pytest.mark.parametrize("path", SCHEMA_READS)
async def test_schema_read_without_platform_credential_is_refused(am_url, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}")
    assert resp.status_code == 401, f"{resp.status_code} {resp.text}"
    assert _is_problem(resp.json(), 401), resp.text


@pytest.mark.smoke
@pytest.mark.asyncio
@pytest.mark.parametrize("path", SCHEMA_READS)
async def test_schema_read_by_the_shared_identity_is_not_permitted(am_url, platform_headers, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}", headers=platform_headers)
    assert resp.status_code == 403, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert _is_problem(body, 403), resp.text
    assert body["type"].endswith("cf.core.err.permission_denied.v1~"), resp.text
    assert "read_schema" in body["context"]["reason"], resp.text
