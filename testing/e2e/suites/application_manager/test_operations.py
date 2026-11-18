"""application-manager E2E: seed, uninstall, and the operation read.

All four routes are on the platform plane and decided by the allowed callers.
Local e2e has only the gateway's shared-secret platform identity, which no
allowed-callers entry can match, so the suite checks that a caller with no
platform credential is turned away (401) and that the shared identity is not
permitted (403). Accepted requests, dispatch, and the operation read are
covered by the gear's in-process orchestrator and route tests.
"""
import httpx
import pytest

PROBLEM_TYPE_PREFIX = "gts://gts.cf.core.errors.err.v1~cf.core.err."

MANIFEST = {
    "application": {"id": "e2e.app", "version": "1.0.0"},
    "sections": {"cf.core.noop.section": {}},
}

# (method, path, JSON body, action named in the refusal)
ROUTES = [
    ("POST", "/applications", MANIFEST, "seed"),
    ("DELETE", "/applications/e2e.app", None, "uninstall"),
    ("POST", "/applications:uninstall", {"application_id": "e2e.app"}, "uninstall"),
    ("GET", "/operations/00000000-0000-7000-8000-000000000000", None, "read_operation"),
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
@pytest.mark.parametrize("method,path,body,action", ROUTES)
async def test_operation_routes_refuse_a_caller_without_platform_credential(
    am_url, method, path, body, action
):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.request(method, f"{am_url}{path}", json=body)
    assert resp.status_code == 401, f"{resp.status_code} {resp.text}"
    assert _is_problem(resp.json(), 401), resp.text


@pytest.mark.smoke
@pytest.mark.asyncio
@pytest.mark.parametrize("method,path,body,action", ROUTES)
async def test_operation_routes_do_not_permit_the_shared_identity(
    am_url, platform_headers, method, path, body, action
):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.request(
            method, f"{am_url}{path}", json=body, headers=platform_headers
        )
    assert resp.status_code == 403, f"{resp.status_code} {resp.text}"
    problem = resp.json()
    assert _is_problem(problem, 403), resp.text
    assert problem["type"].endswith("cf.core.err.permission_denied.v1~"), resp.text
    assert action in problem["context"]["reason"], resp.text
