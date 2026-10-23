"""application-manager E2E: seed, uninstall and operation reads.

- A seed is accepted with 202 and a Location, then runs to a final state that
  the operation read reports: ``completed`` with category ``succeeded`` for
  the no-op plugin.
- An uninstall by id (DELETE) and by body (POST ``applications:uninstall``)
  is accepted and completes the same way.
- A malformed envelope is an invalid manifest (400); the routes are
  authenticated and decided by platform access control; an unknown operation
  is not found.

A conflict is not exercised here: the no-op plugin finishes at once, so a
second request cannot reliably meet an open operation. The orchestrator and
REST unit tests cover it with a plugin that hangs.
"""
import asyncio
import time
import uuid
from urllib.parse import quote

import httpx
import pytest

PROBLEM_TYPE_PREFIX = "gts://gts.cf.core.errors.err.v1~cf.core.err."
POLL_DEADLINE_SECONDS = 15.0
POLL_INTERVAL_SECONDS = 0.1


def _problem_kind(body: dict, status: int) -> str:
    """The canonical error kind of a problem document."""
    assert isinstance(body, dict), body
    assert body.get("status") == status, body
    problem_type = str(body.get("type", ""))
    assert problem_type.startswith(PROBLEM_TYPE_PREFIX), body
    return problem_type.removeprefix(PROBLEM_TYPE_PREFIX).removesuffix(".v1~")


def _unique_application() -> str:
    return f"e2e-app-{uuid.uuid4().hex}"


def _manifest(application_id: str) -> dict:
    return {
        "application": {"id": application_id, "version": "1"},
        "sections": {"noop": {}},
    }


async def _wait_until_final(client: httpx.AsyncClient, url: str, headers: dict) -> dict:
    """Poll the operation until it is final, within a bounded time."""
    deadline = time.monotonic() + POLL_DEADLINE_SECONDS
    last = None
    while time.monotonic() < deadline:
        resp = await client.get(url, headers=headers)
        assert resp.status_code == 200, f"{resp.status_code} {resp.text}"
        last = resp.json()
        if last["status"] in ("completed", "interrupted"):
            return last
        assert last["status"] in ("accepted", "running"), last
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    pytest.fail(f"operation did not become final within the poll limit: {last}")


def _assert_accepted(resp: httpx.Response, am_url: str) -> tuple[str, str]:
    """The 202 body and Location of an accepted request: (operation id, URL)."""
    assert resp.status_code == 202, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert body["status"] == "accepted", body
    operation_id = body["operation_id"]
    uuid.UUID(operation_id)
    location = resp.headers["location"]
    assert location == f"/application-manager/v1/operations/{operation_id}", location
    return operation_id, f"{am_url}/operations/{operation_id}"


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_seed_runs_to_completed_succeeded(am_url, tenant_a_headers):
    application_id = _unique_application()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{am_url}/applications",
            json=_manifest(application_id),
            headers=tenant_a_headers,
        )
        operation_id, url = _assert_accepted(resp, am_url)
        operation = await _wait_until_final(client, url, tenant_a_headers)

    assert operation["operation_id"] == operation_id
    assert operation["application_id"] == application_id
    assert operation["action"] == "seed"
    assert operation["status"] == "completed", operation
    assert operation["category"] == "succeeded", operation
    report = operation["report"]
    assert report["category"] == "succeeded", report
    assert report["interrupted"] is False, report
    assert [(s["key"], s["outcome"]) for s in report["sections"]] == [
        ("noop", "succeeded")
    ]


@pytest.mark.asyncio
async def test_uninstall_by_delete_runs_to_completed_succeeded(am_url, tenant_a_headers):
    application_id = _unique_application()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.delete(
            f"{am_url}/applications/{quote(application_id, safe='')}",
            headers=tenant_a_headers,
        )
        _, url = _assert_accepted(resp, am_url)
        operation = await _wait_until_final(client, url, tenant_a_headers)

    assert operation["application_id"] == application_id
    assert operation["action"] == "uninstall"
    assert operation["status"] == "completed", operation
    assert operation["category"] == "succeeded", operation
    assert [(s["key"], s["outcome"]) for s in operation["report"]["sections"]] == [
        ("noop", "succeeded")
    ]


@pytest.mark.asyncio
async def test_uninstall_by_body_runs_to_completed_succeeded(am_url, tenant_a_headers):
    application_id = _unique_application()
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{am_url}/applications:uninstall",
            json={"application_id": application_id},
            headers=tenant_a_headers,
        )
        _, url = _assert_accepted(resp, am_url)
        operation = await _wait_until_final(client, url, tenant_a_headers)

    assert operation["application_id"] == application_id
    assert operation["action"] == "uninstall"
    assert operation["status"] == "completed", operation
    assert operation["category"] == "succeeded", operation


@pytest.mark.asyncio
async def test_seed_with_a_bad_envelope_is_invalid_argument(am_url, tenant_a_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{am_url}/applications",
            json={"application": {"id": ""}, "sections": {}},
            headers=tenant_a_headers,
        )
    assert resp.status_code == 400, f"{resp.status_code} {resp.text}"
    body = resp.json()
    assert _problem_kind(body, 400) == "invalid_argument", resp.text
    violations = body["context"]["field_violations"]
    assert violations, resp.text
    assert all({"field", "description", "reason"} <= set(v) for v in violations)


@pytest.mark.asyncio
async def test_seed_with_a_body_that_is_not_json_is_invalid_argument(am_url, tenant_a_headers):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            f"{am_url}/applications",
            content=b"{not json",
            headers={**tenant_a_headers, "Content-Type": "application/json"},
        )
    assert resp.status_code == 400, f"{resp.status_code} {resp.text}"
    assert _problem_kind(resp.json(), 400) == "invalid_argument", resp.text


@pytest.mark.asyncio
async def test_operation_reads_and_writes_are_authenticated(am_url):
    unknown = uuid.uuid4()
    async with httpx.AsyncClient(timeout=10.0) as client:
        responses = [
            await client.post(f"{am_url}/applications", json=_manifest("anonymous")),
            await client.delete(f"{am_url}/applications/anonymous"),
            await client.post(
                f"{am_url}/applications:uninstall", json={"application_id": "anonymous"}
            ),
            await client.get(f"{am_url}/operations/{unknown}"),
        ]
    for resp in responses:
        assert resp.status_code == 401, f"{resp.status_code} {resp.text}"
        _problem_kind(resp.json(), 401)


@pytest.mark.asyncio
async def test_denied_caller_gets_permission_denied(am_url, denied_headers):
    unknown = uuid.uuid4()
    cases = [
        ("POST", f"{am_url}/applications", {"json": _manifest("denied")}, "seed"),
        ("DELETE", f"{am_url}/applications/denied", {}, "uninstall"),
        (
            "POST",
            f"{am_url}/applications:uninstall",
            {"json": {"application_id": "denied"}},
            "uninstall",
        ),
        ("GET", f"{am_url}/operations/{unknown}", {}, "read_operation"),
    ]
    async with httpx.AsyncClient(timeout=10.0) as client:
        for method, url, kwargs, action in cases:
            resp = await client.request(method, url, headers=denied_headers, **kwargs)
            assert resp.status_code == 403, f"{method} {url}: {resp.status_code} {resp.text}"
            body = resp.json()
            assert _problem_kind(body, 403) == "permission_denied", resp.text
            assert action in body["context"]["reason"], resp.text


@pytest.mark.asyncio
@pytest.mark.parametrize("operation_id", [str(uuid.uuid4()), "not-a-uuid"])
async def test_unknown_or_malformed_operation_is_not_found(
    am_url, tenant_a_headers, operation_id
):
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"{am_url}/operations/{operation_id}", headers=tenant_a_headers
        )
    assert resp.status_code == 404, f"{resp.status_code} {resp.text}"
    assert _problem_kind(resp.json(), 404) == "not_found", resp.text
