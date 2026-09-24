"""Authentication for the exporter.

Two modes:

- **Static token** — a PAT or a short-lived bearer, passed as ``DATABRICKS_TOKEN``.
  Convenient for local testing; the token is used verbatim on every request.
- **OAuth M2M (service principal)** — a client id and secret, passed as
  ``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET``. This is the mode for a
  long-running deployment. OAuth access tokens are short lived (about an hour), so
  ``OAuthM2MTokenProvider`` caches a token per workspace host and refreshes it
  before it expires.

Both modes are wired into the monitor through ``BearerAuth``, a ``requests`` auth
adapter that stamps a fresh ``Authorization`` header on every request. That matters
because the monitor builds its ``requests.Session`` once and would otherwise cache a
single bearer header for the life of the process.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Dict, Optional, Tuple
from urllib.parse import urlsplit

import requests

TokenGetter = Callable[[str], str]


class OAuthM2MTokenProvider:
    """Fetches and caches per-host workspace OAuth tokens via client credentials.

    A single service principal (one client id / secret) can serve several workspaces
    in the same account; the token endpoint is per workspace host, so tokens are
    cached keyed by host. Refreshes happen ``refresh_skew_seconds`` before the token's
    stated expiry so a request never rides an about-to-expire token.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        scope: str = "all-apis",
        refresh_skew_seconds: int = 60,
        timeout_seconds: int = 30,
        session: Optional[requests.Session] = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required for OAuth M2M")
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._skew = max(0, refresh_skew_seconds)
        self._timeout = timeout_seconds
        # A dedicated session for token calls, separate from the monitor's data session.
        self._session = session or requests.Session()
        self._now = now
        # _cache_lock guards the cache and the per-host lock table only. It is a fast,
        # in-memory lock and is never held across the token HTTP call.
        self._cache_lock = threading.Lock()
        self._cache: Dict[str, Tuple[str, float]] = {}  # host -> (token, expires_at_epoch)
        self._host_locks: Dict[str, threading.Lock] = {}  # host -> refresh lock

    def _valid_cached(self, host: str) -> Optional[str]:
        cached = self._cache.get(host)
        if cached and self._now() < cached[1] - self._skew:
            return cached[0]
        return None

    def _lock_for(self, host: str) -> threading.Lock:
        with self._cache_lock:
            lock = self._host_locks.get(host)
            if lock is None:
                lock = threading.Lock()
                self._host_locks[host] = lock
            return lock

    def token_for(self, host: str) -> str:
        """Return a valid token for ``host``, fetching or refreshing if needed.

        A cache hit takes only the fast in-memory ``_cache_lock`` and returns
        immediately, so a slow token endpoint for one host never blocks cached reads
        for another. On a miss, callers serialize on a per-host refresh lock (not a
        single global one) and double-check the cache inside it, so concurrent callers
        for the same host still collapse to a single refresh while other hosts proceed.
        """
        with self._cache_lock:
            hit = self._valid_cached(host)
        if hit is not None:
            return hit

        with self._lock_for(host):
            # Another thread may have refreshed this host while we waited.
            with self._cache_lock:
                hit = self._valid_cached(host)
            if hit is not None:
                return hit
            # Fetch outside _cache_lock so other hosts' cached reads are never blocked.
            token, ttl = self._request_token(host)
            with self._cache_lock:
                self._cache[host] = (token, self._now() + ttl)
            return token

    def _request_token(self, host: str) -> Tuple[str, float]:
        url = f"https://{host}/oidc/v1/token"
        resp = self._session.post(
            url,
            auth=(self._client_id, self._client_secret),
            data={"grant_type": "client_credentials", "scope": self._scope},
            headers={"Accept": "application/json"},
            timeout=self._timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError(f"Token endpoint {url} returned no access_token")
        # expires_in is seconds; default to a conservative hour if the field is absent.
        return token, float(payload.get("expires_in", 3600))


class BearerAuth(requests.auth.AuthBase):
    """A ``requests`` auth adapter that sets a fresh bearer token on every request.

    ``token_getter`` is called with the request host (netloc), so one session can
    talk to multiple workspaces, each getting its own token.
    """

    def __init__(self, token_getter: TokenGetter) -> None:
        self._token_getter = token_getter

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        host = urlsplit(request.url).netloc
        request.headers["Authorization"] = f"Bearer {self._token_getter(host)}"
        return request


def static_token_getter(token: str) -> TokenGetter:
    """A token getter that returns the same static token for every host."""
    if not token:
        raise ValueError("token is required for static-token auth")

    def _get(_host: str) -> str:
        return token

    return _get
