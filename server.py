#!/usr/bin/env python3
"""agent-search — unified search MCP service.

One endpoint over Firecrawl / Exa / Tavily with round-robin + failover.
Exposed as an MCP server (StreamableHTTP) at /mcp, plus REST /health and
/providers. Keys come from env (k8s Secret agent-search-keys):
FIRECRAWL_API_KEY, EXA_API_KEY, TAVILY_API_KEY. A provider is enabled iff
its key is set and non-empty; with no keys the service runs but reports
"no providers configured" on query.

Modes:
  rr  (default)  round-robin across enabled providers; on error/rate-limit
                 the provider enters an exponential cooldown and the query
                 fails over to the next provider.
  all            fan out to every enabled provider in parallel and merge
                 results deduped by URL.
  provider pin   query() with provider= overrides mode.

Env knobs: PORT (8651), SEARCH_HTTP_TIMEOUT (20s), SEARCH_COOLDOWN_BASE
(30s, doubles per consecutive failure), SEARCH_COOLDOWN_MAX (600s).
"""

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

PORT = int(os.environ.get("PORT", "8651"))
HTTP_TIMEOUT = float(os.environ.get("SEARCH_HTTP_TIMEOUT", "20"))
COOLDOWN_BASE = float(os.environ.get("SEARCH_COOLDOWN_BASE", "30"))
COOLDOWN_MAX = float(os.environ.get("SEARCH_COOLDOWN_MAX", "600"))
MAX_LIMIT = 20


# --------------------------------------------------------------------------
# Providers
# --------------------------------------------------------------------------

async def _firecrawl_search(client: httpx.AsyncClient, query: str, limit: int, key: str):
    r = await client.post(
        "https://api.firecrawl.dev/v1/search",
        headers={"Authorization": f"Bearer {key}"},
        json={"query": query, "limit": limit},
    )
    r.raise_for_status()
    out = []
    for d in r.json().get("data", []):
        url = d.get("url", "")
        if not url:
            continue
        out.append({
            "title": d.get("title") or url,
            "url": url,
            "snippet": (d.get("description") or "").strip(),
            "published": d.get("date") or None,
        })
    return out


async def _exa_search(client: httpx.AsyncClient, query: str, limit: int, key: str):
    r = await client.post(
        "https://api.exa.ai/search",
        headers={"x-api-key": key},
        json={
            "query": query,
            "numResults": limit,
            "type": "auto",
            "contents": {"text": {"maxCharacters": 240}},
        },
    )
    r.raise_for_status()
    out = []
    for d in r.json().get("results", []):
        url = d.get("url", "")
        if not url:
            continue
        out.append({
            "title": d.get("title") or url,
            "url": url,
            "snippet": (d.get("text") or "").strip()[:400],
            "published": d.get("publishedDate") or None,
        })
    return out


async def _tavily_search(client: httpx.AsyncClient, query: str, limit: int, key: str):
    r = await client.post(
        "https://api.tavily.com/search",
        headers={"Authorization": f"Bearer {key}"},
        json={"query": query, "max_results": limit, "search_depth": "basic"},
    )
    r.raise_for_status()
    out = []
    for d in r.json().get("results", []):
        url = d.get("url", "")
        if not url:
            continue
        out.append({
            "title": d.get("title") or url,
            "url": url,
            "snippet": (d.get("content") or "").strip(),
            "published": d.get("published_date") or None,
        })
    return out


PROVIDERS = {
    "firecrawl": {"env": "FIRECRAWL_API_KEY", "fn": _firecrawl_search},
    "exa": {"env": "EXA_API_KEY", "fn": _exa_search},
    "tavily": {"env": "TAVILY_API_KEY", "fn": _tavily_search},
}


class ProviderState:
    __slots__ = ("failures", "cooldown_until", "last_status", "last_error",
                 "last_took_ms", "calls")

    def __init__(self):
        self.failures = 0
        self.cooldown_until = 0.0
        self.last_status = "idle"
        self.last_error = None
        self.last_took_ms = None
        self.calls = 0


POOL = {name: ProviderState() for name in PROVIDERS}
_rr_index = 0


def _key(name: str) -> str:
    return os.environ.get(PROVIDERS[name]["env"], "").strip()


def _enabled() -> list:
    return [n for n in PROVIDERS if _key(n)]


def _in_cooldown(name: str) -> bool:
    return time.monotonic() < POOL[name].cooldown_until


def _brief(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.HTTPError):
        return "http error"
    msg = str(exc).strip()
    return (msg[:160]) if msg else exc.__class__.__name__


async def _call(name: str, query: str, limit: int):
    """Run one provider. Returns (results, error). Updates pool state."""
    st = POOL[name]
    key = _key(name)
    t0 = time.monotonic()
    st.calls += 1
    if not key:
        st.last_status = "error"
        st.last_error = f"{PROVIDERS[name]['env']} not set"
        st.last_took_ms = 0
        return None, st.last_error
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            results = await PROVIDERS[name]["fn"](client, query, limit, key)
        st.last_status = "ok"
        st.last_took_ms = int((time.monotonic() - t0) * 1000)
        st.failures = 0
        st.last_error = None
        return results, None
    except Exception as exc:  # noqa: BLE001 — any provider error feeds cooldown
        st.failures += 1
        st.last_status = "error"
        st.last_took_ms = int((time.monotonic() - t0) * 1000)
        st.last_error = _brief(exc)
        st.cooldown_until = time.monotonic() + min(
            COOLDOWN_BASE * (2 ** (st.failures - 1)), COOLDOWN_MAX)
        return None, st.last_error


def _normalize_url(url: str) -> str:
    try:
        p = urlparse(url)
        keep = [kv for kv in p.query.split("&") if kv and not kv.split("=")[0].startswith("utm_")]
        q = "&".join(keep)
        base = f"{p.scheme}://{p.netloc}{p.path.rstrip('/')}"
        return f"{base}?{q}" if q else base
    except Exception:
        return url.rstrip("/")


def _dedupe(rows: list) -> list:
    seen, out = set(), []
    for row in rows:
        norm = _normalize_url(row.get("url", ""))
        if norm in seen:
            continue
        seen.add(norm)
        out.append(row)
    return out


def _envelope(query, mode, provider, results, t0, extra=None):
    e = {
        "ok": True,
        "query": query,
        "mode": mode,
        "provider": provider,
        "took_ms": int((time.monotonic() - t0) * 1000),
        "count": len(results),
        "results": results,
    }
    if extra:
        e.update(extra)
    return e


async def _search_rr(query: str, limit: int, t0: float):
    global _rr_index
    candidates = _enabled()
    if not candidates:
        return {"ok": False, "error": "no search providers configured",
                "hint": "set FIRECRAWL_API_KEY / EXA_API_KEY / TAVILY_API_KEY"}
    healthy = [n for n in candidates if not _in_cooldown(n)]
    if not healthy:
        # everything cooling — fail open rather than dead-end the query
        healthy = candidates
        for n in healthy:
            POOL[n].cooldown_until = 0.0
    order = healthy[_rr_index % len(healthy):] + healthy[:_rr_index % len(healthy)]
    errors = {}
    for name in order:
        results, err = await _call(name, query, limit)
        if err:
            errors[name] = err
            continue
        _rr_index = (healthy.index(name) + 1) % len(healthy)
        return _envelope(query, "rr", name, results, t0)
    return {"ok": False, "mode": "rr", "error": "all providers failed",
            "provider_errors": errors}


async def _search_all(query: str, limit: int, t0: float):
    enabled = _enabled()
    if not enabled:
        return {"ok": False, "error": "no search providers configured",
                "hint": "set FIRECRAWL_API_KEY / EXA_API_KEY / TAVILY_API_KEY"}
    results, providers_used, errors = [], [], {}
    for name, outcome in zip(enabled, await asyncio.gather(
            *[_call(n, query, limit) for n in enabled])):
        rows, err = outcome
        if err:
            errors[name] = err
            continue
        providers_used.append(name)
        results.extend(rows)
    if not providers_used:
        return {"ok": False, "mode": "all", "error": "all providers failed",
                "provider_errors": errors}
    merged = _dedupe(results)[:limit]
    return _envelope(query, "all", ",".join(providers_used), merged, t0,
                     extra={"providers": providers_used,
                            "provider_errors": errors or None})


async def _search(query: str, limit: int, mode: str, provider: str | None):
    t0 = time.monotonic()
    if not query or not query.strip():
        return {"ok": False, "error": "empty query"}
    limit = max(1, min(int(limit), MAX_LIMIT))
    mode = (mode or "rr").lower()
    if provider:
        if provider not in PROVIDERS:
            return {"ok": False, "error": f"unknown provider '{provider}'",
                    "known": sorted(PROVIDERS)}
        if not _key(provider):
            return {"ok": False, "error": f"provider '{provider}' not configured",
                    "hint": f"set {PROVIDERS[provider]['env']}"}
        results, err = await _call(provider, query, limit)
        if err:
            return {"ok": False, "mode": "pin", "provider": provider, "error": err}
        return _envelope(query, "pin", provider, results, t0)
    if mode == "all":
        return await _search_all(query, limit, t0)
    return await _search_rr(query, limit, t0)


def _pool_status():
    now = time.monotonic()
    return {
        name: {
            "configured": bool(_key(name)),
            "enabled": bool(_key(name)),
            "in_cooldown": _in_cooldown(name),
            "cooldown_left_s": max(0, int(POOL[name].cooldown_until - now)),
            "failures": POOL[name].failures,
            "last_status": POOL[name].last_status,
            "last_error": POOL[name].last_error,
            "last_took_ms": POOL[name].last_took_ms,
            "calls": POOL[name].calls,
        }
        for name in PROVIDERS
    }


# --------------------------------------------------------------------------
# MCP + FastAPI wiring
# --------------------------------------------------------------------------

mcp = FastMCP(
    "agent-search",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            "agent-search.ffilt.us", "agent-search.ffilt.us:*",
            "192.168.1.2", "192.168.1.2:*",
            "127.0.0.1:*", "localhost:*", "[::1]:*",
        ],
        allowed_origins=[
            "http://agent-search.ffilt.us", "http://agent-search.ffilt.us:*",
            "http://127.0.0.1:*", "http://localhost:*", "http://192.168.1.2:*",
        ],
    ),
)


@mcp.tool()
async def query(query: str, limit: int = 5, mode: str = "rr",
                provider: str | None = None) -> str:
    """Web search across the agent-search provider pool (firecrawl, exa, tavily).
    mode: "rr" = round-robin, one provider per call with failover + cooldown
    (default); "all" = query every enabled provider in parallel, merge results
    deduped by URL. provider: pin one provider name, overrides mode. Returns
    JSON {"ok":true,"provider","mode","took_ms","count","results":[{title,url,
    snippet,published}]} or {"ok":false,"error","provider_errors"?}."""
    return json.dumps(await _search(query, limit, mode, provider))


@mcp.tool()
def providers() -> str:
    """Search pool status: per provider (firecrawl/exa/tavily) — configured,
    enabled, in_cooldown, cooldown_left_s, failures, last_status, last_error,
    last_took_ms, calls."""
    return json.dumps(_pool_status())


mcp_starlette = mcp.streamable_http_app()
mcp_session_manager = mcp._session_manager  # noqa: SLF001 — created by streamable_http_app()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    async with mcp_session_manager.run():
        yield


app = FastAPI(title="agent-search", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "agent-search",
            "providers_configured": len(_enabled())}


@app.get("/providers")
async def providers_rest():
    return _pool_status()


app.mount("/", mcp_starlette)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
