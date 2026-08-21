#!/usr/bin/env python3
"""Local SDK probe for agent-search MCP endpoint (before/after deploy)."""
import asyncio, json, sys
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8652/mcp"

async def main():
    async with streamablehttp_client(URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            names = [t.name for t in tools.tools]
            print("TOOLS:", names)
            # providers tool
            r = await session.call_tool("providers", {})
            print("PROVIDERS:", r.content[0].text)
            # query with no keys -> structured error
            r = await session.call_tool("query", {"query": "k3s homelab"})
            print("QUERY(no keys):", r.content[0].text[:200])
            # query with a pinned fake key -> provider error + cooldown
            import os
            os.environ["EXA_API_KEY"] = "fake-key-for-test"
            r = await session.call_tool("query", {"query": "test", "provider": "exa"})
            print("QUERY(fake exa):", r.content[0].text[:200])
            r = await session.call_tool("providers", {})
            print("PROVIDERS(after fail):", r.content[0].text)

asyncio.run(main())
