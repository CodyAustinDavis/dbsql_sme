"""
Tests for auth.py. Runs with pytest, or standalone:  python test_auth.py

Covers the OAuth M2M token cache/refresh behavior, per-host token isolation, and the
static token getter. Per-request header injection lives in the monitor and is covered by
test_warehouse_monitor.py.
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from auth import OAuthM2MTokenProvider, static_token_getter  # noqa: E402


class _FakeResp:
    def __init__(self, payload, status_ok=True):
        self._payload = payload
        self._ok = status_ok

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError("HTTP error")

    def json(self):
        return self._payload


class _FakeSession:
    """Records POSTs and returns queued responses (or a default token response)."""

    def __init__(self, responses=None):
        self.calls = []
        self._responses = list(responses or [])
        self._counter = 0

    def post(self, url, auth=None, data=None, headers=None, timeout=None):
        self.calls.append(SimpleNamespace(url=url, auth=auth, data=data, timeout=timeout))
        if self._responses:
            return self._responses.pop(0)
        self._counter += 1
        return _FakeResp({"access_token": f"tok-{self._counter}", "expires_in": 3600})


class _Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def test_missing_credentials_raises():
    for cid, secret in [("", "s"), ("c", ""), ("", "")]:
        try:
            OAuthM2MTokenProvider(cid, secret)
        except ValueError:
            continue
        raise AssertionError("expected ValueError for missing credentials")


def test_token_is_cached_until_near_expiry():
    session = _FakeSession()
    clock = _Clock()
    p = OAuthM2MTokenProvider("cid", "secret", refresh_skew_seconds=60, session=session, now=clock)

    first = p.token_for("host.example.com")
    again = p.token_for("host.example.com")
    assert first == again == "tok-1"
    assert len(session.calls) == 1  # second call served from cache

    # still within (expiry - skew): 1000 + 3600 - 60 = 4540
    clock.t = 4000.0
    assert p.token_for("host.example.com") == "tok-1"
    assert len(session.calls) == 1

    # cross the refresh threshold -> refetch
    clock.t = 4550.0
    assert p.token_for("host.example.com") == "tok-2"
    assert len(session.calls) == 2


def test_tokens_are_isolated_per_host():
    session = _FakeSession()
    p = OAuthM2MTokenProvider("cid", "secret", session=session, now=_Clock())
    a = p.token_for("a.example.com")
    b = p.token_for("b.example.com")
    assert a != b
    assert len(session.calls) == 2
    assert session.calls[0].url == "https://a.example.com/oidc/v1/token"
    assert session.calls[1].url == "https://b.example.com/oidc/v1/token"
    # client-credentials payload is what the token endpoint expects
    assert session.calls[0].data["grant_type"] == "client_credentials"
    assert session.calls[0].data["scope"] == "all-apis"
    assert session.calls[0].auth == ("cid", "secret")


def test_missing_access_token_raises():
    session = _FakeSession(responses=[_FakeResp({"token_type": "Bearer"})])
    p = OAuthM2MTokenProvider("cid", "secret", session=session, now=_Clock())
    try:
        p.token_for("host.example.com")
    except RuntimeError:
        return
    raise AssertionError("expected RuntimeError when access_token is absent")


def test_static_token_getter():
    get = static_token_getter("pat-123")
    assert get("any.host") == "pat-123"
    assert get("other.host") == "pat-123"
    try:
        static_token_getter("")
    except ValueError:
        return
    raise AssertionError("expected ValueError for empty static token")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"ok  {t.__name__}")
    print(f"\n{len(tests)} passed")
