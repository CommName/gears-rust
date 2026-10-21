"""Pytest fixtures for the application-manager E2E suite.

The suite runs against the standard e2e server (``config/e2e-local.yaml``)
with the no-op section plugin as the whole section plugin set. Tokens map to
static identities (static-authn-plugin); static-authz permits a caller with a
resolvable tenant and denies ``e2e-token-nil-tenant``.
"""
from __future__ import annotations

import os

import pytest

APPLICATION_MANAGER = "/application-manager/v1"


@pytest.fixture
def base_url():
    """API Gateway base URL."""
    return os.getenv("E2E_BASE_URL", "http://localhost:8086")


@pytest.fixture
def am_url(base_url):
    """The gear's versioned prefix."""
    return f"{base_url}{APPLICATION_MANAGER}"


@pytest.fixture
def tenant_a_headers():
    """Headers for a caller static-authz permits."""
    token = os.getenv("E2E_AUTH_TOKEN", "e2e-token-tenant-a")
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def denied_headers():
    """Headers for a caller static-authz denies (nil tenant)."""
    token = os.getenv("E2E_AUTH_TOKEN_NIL_TENANT", "e2e-token-nil-tenant")
    return {"Authorization": f"Bearer {token}"}
