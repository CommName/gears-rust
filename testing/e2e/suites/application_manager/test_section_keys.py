"""application-manager E2E: the startup plugin set and the section-keys read.

- The gear started: its start phase listed the section plugins, read the
  no-op plugin's key, and published the set, because the read answers 200.
- The read is authenticated and decided by platform access control: an
  anonymous caller is turned away, a denied caller gets a 403 problem.
"""
import httpx
import pytest

PROBLEM_TYPE_PREFIX = "gts://gts.cf.core.errors.err.v1~cf.core.err."


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
async def test_section_keys_list_the_noop_plugin(am_url, tenant_a_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}/section-keys", headers=tenant_a_headers)
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
    assert resp.json() == {"items": ["noop"]}


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_anonymous_caller_is_refused(am_url):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}/section-keys")
    assert resp.status_code == 401, f"{resp.status_code} {resp.text}"
    assert _is_problem(resp.json(), 401), resp.text


@pytest.mark.asyncio
async def test_denied_caller_gets_a_permission_denied_problem(am_url, denied_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}/section-keys", headers=denied_headers)
    assert resp.status_code == 403, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert _is_problem(body, 403), resp.text
    assert body["type"].endswith("cf.core.err.permission_denied.v1~"), resp.text
    assert "read_schema" in body["context"]["reason"], resp.text


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_health_reports_application_manager_ready(base_url):
    """Readiness follows startup: healthy once the plugin set is published."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        health = await client.get(f"{base_url}/health")
        ready = await client.get(f"{base_url}/readyz")
    assert health.status_code == 200, f"{health.status_code} {health.text}"
    components = [
        c for c in health.json().get("components", []) if c.get("gear") == "application-manager"
    ]
    assert len(components) == 1, health.text
    assert components[0]["status"] == "healthy", components[0]
    assert ready.status_code == 200, f"{ready.status_code} {ready.text}"
