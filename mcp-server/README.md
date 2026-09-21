# MGIMO trade — MCP server (local, stdio)

An [MCP](https://modelcontextprotocol.io) server that exposes the MGIMO
foreign-trade API (`https://nts.mgimo.ru/api`) as tools, so any MCP client
(Claude Desktop, IDEs, agents) can answer questions over the data in natural
language. It is a thin wrapper over the HTTP API — authentication, quotas and
audit all stay in the API.

## Tools

| Tool | What it does |
|---|---|
| `meta` | latest available period + your plan and remaining quota |
| `reference` | code dictionaries: `countries` (ISO-2 + Russian names) or `tnved` (HS-code names, by `level`) |
| `trade` | trade rows or server-side aggregates (filters: country, direction, HS code, period, `group_by`, `metrics`) |
| `fizob` | physical-volume indices (real volumes, price effect removed — a project speciality) |

## Prerequisites

- Python 3.10+.
- A personal API key. Get one from the Superset cabinet at
  <https://nts.mgimo.ru/superset/apikey/> (role `API`, granted by an admin).

## Install

With [uv](https://docs.astral.sh/uv/) (recommended — no manual venv):

```bash
cd mcp-server
uv sync
```

Or plain pip in a virtualenv:

```bash
cd mcp-server
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configure your MCP client

Point the client at `server.py` and pass the token in `env`. The token never
leaves the Authorization header.

**Claude Desktop** — edit `claude_desktop_config.json`
(macOS: `~/Library/Application Support/Claude/`, Windows:
`%APPDATA%\Claude\`), then restart Claude Desktop:

```json
{
  "mcpServers": {
    "mgimo-trade": {
      "command": "uv",
      "args": ["run", "--directory", "/ABSOLUTE/PATH/TO/mcp-server", "mgimo-trade-mcp"],
      "env": { "MGIMO_API_TOKEN": "mgt_YOUR_TOKEN" }
    }
  }
}
```

Without uv, run the script directly with a Python that has the deps installed:

```json
{
  "mcpServers": {
    "mgimo-trade": {
      "command": "/ABSOLUTE/PATH/TO/mcp-server/.venv/bin/python",
      "args": ["/ABSOLUTE/PATH/TO/mcp-server/server.py"],
      "env": { "MGIMO_API_TOKEN": "mgt_YOUR_TOKEN" }
    }
  }
}
```

On Windows use the full path to `python.exe` and backslashes (or forward
slashes) in the paths, e.g. `C:\\...\\.venv\\Scripts\\python.exe`.

Other MCP clients (Cursor, Continue, etc.) use the same idea: a stdio server
started with that command + `MGIMO_API_TOKEN` in the environment.

## Remote (hosted) server

A hosted instance runs on the VPS at **`https://nts.mgimo.ru/mcp`** (Streamable
HTTP transport, TLS). Nothing to install — each request must carry the client's
own API key in `Authorization: Bearer <key>`, which the server forwards to the
API, so quotas and audit stay per-user. The server stores no token.

Clients that support a remote MCP URL with a custom header can use it directly.
For stdio-only clients (e.g. Claude Desktop today), bridge with
[`mcp-remote`](https://www.npmjs.com/package/mcp-remote):

```json
{
  "mcpServers": {
    "mgimo-trade": {
      "command": "npx",
      "args": [
        "-y", "mcp-remote", "https://nts.mgimo.ru/mcp",
        "--header", "Authorization: Bearer mgt_YOUR_TOKEN"
      ]
    }
  }
}
```

(Needs Node.js for `npx`. This is the only setup a user needs — no Python.)

### Deploy / update the hosted server (VPS)

The server runs as the `trade-mcp` container next to `trade-api`, on Superset's
Docker network, proxied by the landing nginx at `/mcp` (see
`deploy/landing-nginx.conf`). It calls the API internally (`trade-api:8000`) and
holds no token.

```bash
ssh mgimo
cd ~/mgimo-foreign-trade && git pull
cd mcp-server && docker compose up -d --build     # build + (re)start trade-mcp
```

nginx already has the `/mcp` location; after editing it:
`scp deploy/landing-nginx.conf mgimo:/home/marcel/landing-nginx.conf && \
 ssh mgimo "docker exec landing nginx -t && docker exec landing nginx -s reload"`.

Check: `curl -sN -H "Authorization: Bearer mgt_..." https://nts.mgimo.ru/mcp`
should speak MCP (405/JSON on a bare GET is fine; use a real client to call tools).

## Configuration (env)

| Variable | Default | Notes |
|---|---|---|
| `MGIMO_API_TOKEN` | — | required; personal API key |
| `MGIMO_API_BASE` | `https://nts.mgimo.ru/api` | override for a local/dev API |

As a fallback the server also reads a `MGIMO_API_TOKEN=` line from a `.env` next
to `server.py` or in the working directory — handy for local testing. Do not
commit that `.env`.

## Quick check

Run the server standalone; it should start and wait on stdio (Ctrl+C to stop):

```bash
MGIMO_API_TOKEN=mgt_... uv run mgimo-trade-mcp
```

To exercise the API path without a client, use the sibling
[`.claude/skills/trade-data/query.py`](../.claude/skills/trade-data/query.py)
(same endpoints), e.g. `python query.py meta`.

## Notes

- Read-only: the tools only issue GET requests to `/v1/*`.
- The same key powers the OData feed for Excel/Power BI and the trade-data skill.
- Reference: [`docs/api-reference.md`](../docs/api-reference.md).
