"""application-manager E2E: live section schemas and the combined schema.

- One section schema is served live by its section plugin, composed under
  `urn:cf:application-manager:section:<key>`, with its schema version.
- The combined schema of a whole manifest requires exactly the envelope and
  one section per section key, and lists every schema version.
- The reads are authenticated and decided by platform access control; an
  unknown section key is not found.
"""
import httpx
import pytest

PROBLEM_TYPE_PREFIX = "gts://gts.cf.core.errors.err.v1~cf.core.err."
NOOP_SCHEMA_ID = "urn:cf:application-manager:section:noop"


def _problem_kind(body: dict, status: int) -> str:
    """The canonical error kind of a problem document."""
    assert isinstance(body, dict), body
    assert body.get("status") == status, body
    problem_type = str(body.get("type", ""))
    assert problem_type.startswith(PROBLEM_TYPE_PREFIX), body
    return problem_type.removeprefix(PROBLEM_TYPE_PREFIX).removesuffix(".v1~")


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_noop_section_schema_is_served_live(am_url, tenant_a_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{am_url}/section-keys/noop/schema", headers=tenant_a_headers
        )
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert body["key"] == "noop"
    assert body["schema_version"] == "1"
    assert body["schema"]["$id"] == NOOP_SCHEMA_ID
    assert body["schema"]["type"] == "object"


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_manifest_schema_combines_every_section(am_url, tenant_a_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}/manifest-schema", headers=tenant_a_headers)
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert body["sections"] == [{"key": "noop", "schema_version": "1"}]

    schema = body["schema"]
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["required"] == ["application", "sections"]
    assert schema["additionalProperties"] is False

    envelope = schema["properties"]["application"]
    assert envelope["required"] == ["id", "version"]
    assert envelope["additionalProperties"] is False
    for field in ("id", "version"):
        assert envelope["properties"][field]["type"] == "string"
        assert envelope["properties"][field]["minLength"] == 1

    sections = schema["properties"]["sections"]
    assert sections["required"] == ["noop"]
    assert sections["additionalProperties"] is False
    assert sections["properties"]["noop"]["$id"] == NOOP_SCHEMA_ID


@pytest.mark.asyncio
async def test_unknown_section_key_is_not_found(am_url, tenant_a_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{am_url}/section-keys/no-such-key/schema", headers=tenant_a_headers
        )
    assert resp.status_code == 404, f"{resp.status_code} {resp.text}"
    assert _problem_kind(resp.json(), 404) == "not_found", resp.text


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/section-keys/noop/schema", "/manifest-schema"])
async def test_schema_reads_are_authenticated(am_url, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}")
    assert resp.status_code == 401, f"{resp.status_code} {resp.text}"
    _problem_kind(resp.json(), 401)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/section-keys/noop/schema", "/manifest-schema"])
async def test_denied_caller_gets_permission_denied(am_url, denied_headers, path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{am_url}{path}", headers=denied_headers)
    assert resp.status_code == 403, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert _problem_kind(body, 403) == "permission_denied", resp.text
    assert "read_schema" in body["context"]["reason"], resp.text
