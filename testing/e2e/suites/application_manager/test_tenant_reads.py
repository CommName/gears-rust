"""application-manager E2E: the tenant-plane reads.

`GET /operation-history` and `GET /applications` take a tenant token and are
decided by the PDP as platform-level permissions with no tenant and no
constraints (ADR 0004). The e2e PDP (static-authz) permits any action on a
resource with no properties and adds no constraint, so a tenant token is
permitted. Local e2e cannot seed (no allowed caller), so the history may hold
only refused events and the application list is empty unless another suite
seeded.
"""
import httpx
import pytest

PROBLEM_TYPE_PREFIX = "gts://gts.cf.core.errors.err.v1~cf.core.err."

READS = ["/operation-history", "/applications"]


def _violation(body: dict, field: str, reason: str) -> bool:
    violations = (body.get("context") or {}).get("field_violations") or []
    return any(v.get("field") == field and v.get("reason") == reason for v in violations)


@pytest.mark.smoke
@pytest.mark.asyncio
@pytest.mark.parametrize("path", READS)
async def test_a_tenant_token_reads_a_page(am_url, auth_headers, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}", headers=auth_headers)
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert isinstance(body["items"], list), body
    assert body["page_info"]["limit"] == 100, body


@pytest.mark.smoke
@pytest.mark.asyncio
@pytest.mark.parametrize("path", READS)
async def test_a_caller_without_a_token_is_refused(am_url, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}")
    assert resp.status_code == 401, f"{resp.status_code} {resp.text}"
    assert str(resp.json().get("type", "")).startswith(PROBLEM_TYPE_PREFIX), resp.text


@pytest.mark.asyncio
@pytest.mark.parametrize("path", READS)
async def test_limit_is_capped_at_the_page_maximum(am_url, auth_headers, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{am_url}{path}", params={"limit": "1000"}, headers=auth_headers
        )
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
    assert resp.json()["page_info"]["limit"] == 100, resp.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,params,field,reason",
    [
        ("/operation-history", {"$orderby": "event_id desc"}, "$orderby", "INVALID_ORDERBY_FIELD"),
        ("/applications", {"$orderby": "application_id"}, "$orderby", "INVALID_ORDERBY_FIELD"),
        ("/operation-history", {"$select": "kind"}, "$select", "INVALID_SELECT"),
        ("/operation-history", {"$filter": "kind eq 'refused'"}, "$filter", "INVALID_FILTER"),
        ("/applications", {"$filter": "version eq '1'"}, "$filter", "INVALID_FILTER"),
    ],
)
async def test_query_options_the_reads_do_not_serve_are_400(
    am_url, auth_headers, path, params, field, reason
):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}", params=params, headers=auth_headers)
    assert resp.status_code == 400, f"{resp.status_code} {resp.text}"
    assert _violation(resp.json(), field, reason), resp.text


@pytest.mark.asyncio
async def test_a_refused_seed_shows_in_the_history_by_its_request_id(
    am_url, auth_headers, platform_headers
):
    async with httpx.AsyncClient(timeout=10.0) as client:
        refused = await client.post(
            f"{am_url}/applications",
            json={"application": {"id": "e2e.app", "version": "1"}, "sections": {}},
            headers=platform_headers,
        )
        assert refused.status_code == 403, refused.text
        violations = refused.json().get("context", {}).get("reason", "")
        request_id = violations.split()[1].rstrip(":")
        resp = await client.get(
            f"{am_url}/operation-history",
            params={"$filter": f"operation_id eq {request_id}"},
            headers=auth_headers,
        )
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
    items = resp.json()["items"]
    assert [e["kind"] for e in items] == ["refused"], items
    assert items[0]["correlation_id"] == request_id
    assert items[0]["content"]["result"]["category"] == "not_permitted", items
