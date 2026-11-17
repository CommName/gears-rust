"""Pytest fixtures for the application-manager E2E suite.

The suite runs against the standard e2e server (``config/e2e-local.yaml``)
with the no-op section plugin as the whole section plugin set. The gear's
platform-plane routes admit only allowed callers, a Kubernetes service account
or a SPIFFE identity. Local e2e has only the gateway's shared-secret platform
auth (``platform_headers`` from the root conftest), which yields a shared
identity that never matches, so the suite checks 401 and 403 only for them;
permitted paths are covered by the gear's in-process route tests. The two
tenant-plane reads take the tenant token (``auth_headers``) and are decided by
the e2e PDP, which permits them.
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
