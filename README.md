# agent-search

Unified web-search MCP service — Firecrawl / Exa / Tavily behind one endpoint
with **round-robin + failover**. Deployed at `agent-search.ffilt.us` (k3s,
ns `agent-search`), registered in Hermes as MCP server `agent-search`
→ tools `mcp_agent_search_query` / `mcp_agent_search_providers`.

Like llm-gateway: the service holds its own keys (k8s Secret
`agent-search-keys`), consumers never see them.

## Tools (MCP)

- `query(query, limit=5, mode="rr", provider=None)` — web search.
  - `mode="rr"` (default): round-robin across enabled providers; a failing
    provider enters exponential cooldown (30s → 600s cap) and the query
    fails over to the next provider.
  - `mode="all"`: fan out to every enabled provider in parallel, merge,
    dedupe by URL.
  - `provider="firecrawl"|"exa"|"tavily"`: pin one provider (overrides mode).
  - Returns `{"ok":true,"provider","mode","took_ms","count","results":
    [{title,url,snippet,published}]}` or `{"ok":false,"error","provider_errors"?}`.
- `providers()` — pool status: configured/enabled/cooldown/failures/last call
  per provider.

## REST

- `GET /health` — liveness/readiness (probes).
- `GET /providers` — same as `providers()` tool.
- `POST /mcp` — StreamableHTTP MCP endpoint (session-managed).

## Providers

| name      | env key             | endpoint                   |
|-----------|---------------------|----------------------------|
| firecrawl | `FIRECRAWL_API_KEY` | `https://api.firecrawl.dev/v1/search` |
| exa       | `EXA_API_KEY`       | `https://api.exa.ai/search` |
| tavily    | `TAVILY_API_KEY`    | `https://api.tavily.com/search` |

A provider is enabled iff its key is set and non-empty. No keys → service
runs, `query` returns a structured "no providers configured" error.

Env knobs: `PORT` (8651), `SEARCH_HTTP_TIMEOUT` (20s),
`SEARCH_COOLDOWN_BASE` (30s), `SEARCH_COOLDOWN_MAX` (600s).

## Ops

```bash
# update keys (empty value = provider disabled)
kubectl create secret generic agent-search-keys -n agent-search \
  --from-literal=FIRECRAWL_API_KEY=... \
  --from-literal=EXA_API_KEY=... \
  --from-literal=TAVILY_API_KEY=... \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deploy/agent-search -n agent-search

# verify
curl -s http://agent-search.ffilt.us/health
curl -s http://agent-search.ffilt.us/providers
```

Image bumps: push to `main` → Actions builds `:latest` + `:<sha8>` → update
`apps/agent-search/deployment.yaml` in `reeds/sutliff-cluster` → Flux applies.

## Hermes registration

```yaml
# ~/.hermes/config.yaml
mcp_servers:
  agent-search:
    url: http://agent-search.ffilt.us/mcp
    enabled: true
    timeout: 90
```

MCP tools load at Hermes startup — `/reset` (or gateway restart) to see
`mcp_agent_search_*`.
