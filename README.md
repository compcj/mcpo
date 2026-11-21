# ⚡️ mcpo

Expose any MCP tool as an OpenAPI-compatible HTTP server—instantly.

mcpo is a dead-simple proxy that takes an MCP server command and makes it accessible via standard RESTful OpenAPI, so your tools "just work" with LLM agents and apps expecting OpenAPI servers.

No custom protocol. No glue code. No hassle.

## 🤔 Why Use mcpo Instead of Native MCP?

MCP servers usually speak over raw stdio, which is:

- 🔓 Inherently insecure
- ❌ Incompatible with most tools
- 🧩 Missing standard features like docs, auth, error handling, etc.

mcpo solves all of that—without extra effort:

- ✅ Works instantly with OpenAPI tools, SDKs, and UIs
- 🛡 Adds security, stability, and scalability using trusted web standards
- 🧠 Auto-generates interactive docs for every tool, no config needed
- 🔌 Uses pure HTTP—no sockets, no glue code, no surprises

What feels like "one more step" is really fewer steps with better outcomes.

mcpo makes your AI tools usable, secure, and interoperable—right now, with zero hassle.

## 🚀 Quick Usage

To install this fork's `0.0.20f1` release (Python package version `0.0.20+f1`):

```bash
pip install "git+https://github.com/compcj/mcpo.git@v0.0.20f1"
```

To run the upstream package with uv:

```bash
uvx mcpo --port 8000 --api-key "top-secret" -- your_mcp_server_command
```

Or, if you’re using Python:

```bash
pip install mcpo
mcpo --port 8000 --api-key "top-secret" -- your_mcp_server_command
```

To use an SSE-compatible MCP server, simply specify the server type and endpoint:

```bash
mcpo --port 8000 --api-key "top-secret" --server-type "sse" -- http://127.0.0.1:8001/sse
```

You can also provide headers for the SSE connection:

```bash
mcpo --port 8000 --api-key "top-secret" --server-type "sse" --header '{"Authorization": "Bearer token", "X-Custom-Header": "value"}' -- http://127.0.0.1:8001/sse
```

To use a Streamable HTTP-compatible MCP server, specify the server type and endpoint:

```bash
mcpo --port 8000 --api-key "top-secret" --server-type "streamable-http" -- http://127.0.0.1:8002/mcp
```

You can also run mcpo via Docker with no installation:

```bash
docker run -p 8000:8000 ghcr.io/compcj/mcpo:v0.0.20f1 --api-key "top-secret" -- your_mcp_server_command
```

Example:

```bash
uvx mcpo --port 8000 --api-key "top-secret" -- uvx mcp-server-time --local-timezone=America/New_York
```

That’s it. Your MCP tool is now available at http://localhost:8000 with a generated OpenAPI schema — test it live at [http://localhost:8000/docs](http://localhost:8000/docs).

🤝 **To integrate with Open WebUI after launching the server, check our [docs](https://docs.openwebui.com/openapi-servers/open-webui/).**


### 🌐 Serving Under a Subpath (`--root-path`)

If you need to serve mcpo behind a reverse proxy or under a subpath (e.g., `/api/mcpo`), use the `--root-path` argument:

```bash
mcpo --port 8000 --root-path "/api/mcpo" --api-key "top-secret" -- your_mcp_server_command
```

All routes will be served under the specified root path, e.g. `http://localhost:8000/api/mcpo/memory`.


### 🔄 Using a Config File

You can serve multiple MCP tools via a single config file that follows the [Claude Desktop](https://modelcontextprotocol.io/quickstart/user) format.

Enable hot-reload mode with `--hot-reload` to automatically watch your config file for changes and reload servers without downtime:

Start via:

```bash
mcpo --config /path/to/config.json
```

Or with hot-reload enabled:

```bash
mcpo --config /path/to/config.json --hot-reload
```

Example config.json:

```json
{
  "mcpServers": {
    "memory": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-memory"]
    },
    "time": {
      "command": "uvx",
      "args": ["mcp-server-time", "--local-timezone=America/New_York"],
      "disabledTools": ["convert_time"] // Disable specific tools if needed
    },
    "mcp_sse": {
      "type": "sse", // Explicitly define type
      "url": "http://127.0.0.1:8001/sse",
      "headers": {
        "Authorization": "Bearer token",
        "X-Custom-Header": "value"
      }
    },
    "mcp_streamable_http": {
      "type": "streamable-http",
      "url": "http://127.0.0.1:8002/mcp"
    } // Streamable HTTP MCP Server
  }
}
```

Each tool will be accessible under its own unique route, e.g.:
- http://localhost:8000/memory
- http://localhost:8000/time

Each with a dedicated OpenAPI schema and proxy handler. Access full schema UI at: `http://localhost:8000/<tool>/docs`  (e.g. /memory/docs, /time/docs)

### 📈 Example: [@pmxt/mcp](https://github.com/pmxt-dev/pmxt-mcp) (prediction markets)

mcpo works with any MCP server. [@pmxt/mcp](https://github.com/pmxt-dev/pmxt-mcp) exposes the [PMXT](https://pmxt.dev) unified prediction-market API (Polymarket, Kalshi, Limitless, and more) as tools.

Get an API key at [pmxt.dev/dashboard](https://pmxt.dev/dashboard), put it in the example config, then:

```bash
# stdio (local MCP process via npx)
mcpo --config examples/pmxt-stdio.json --port 8000

# hosted Streamable HTTP
mcpo --config examples/pmxt-http.json --port 8000
```

Docker (this fork's image):

```bash
docker run -p 8000:8000 -v ${PWD}/examples/pmxt-stdio.json:/config.json \
  ghcr.io/compcj/mcpo:v0.0.20f1 --config /config.json
```

Tools are available under `/pmxt/...`, for example:

- Interactive docs: http://localhost:8000/pmxt/docs
- Discovery: `POST /pmxt/fetchEvents`
- Quotes: `POST /pmxt/fetchOrderBook`

Every tool requires an `exchange` parameter (enum values such as `polymarket`, `kalshi`, `limitless`). OpenAPI schemas preserve those enums so clients and LLMs see allowed venues.

**Trading tools** (`createOrder`, `submitOrder`, `cancelOrder`) are marked destructive in OpenAPI (`x-mcp-annotations` / `destructive` tag). They can spend real money—confirm with the user before calling.

### 🔐 OAuth 2.1 Authentication

mcpo supports OAuth 2.1 authentication for MCP servers that require it. The implementation defaults to **dynamic client registration**, so most servers only need minimal configuration:

```json
{
  "mcpServers": {
    "oauth-protected-server": {
      "type": "streamable-http",
      "url": "http://localhost:8000/mcp",
      "oauth": {
        "server_url": "http://localhost:8000"
      }
    }
  }
}
```

#### OAuth Configuration Options

**Basic Options:**
- `server_url` (required): OAuth server base URL
- `storage_type`: "file" (persistent) or "memory" (session-only, default: "file")
- `callback_port`: Local port for OAuth callback (default: 3030)
- `use_loopback`: Auto-open browser for auth (default: true)

**Advanced Options (rarely needed):**
For servers that don't support dynamic client registration, you can specify static client metadata:

```json
{
  "mcpServers": {
    "legacy-oauth-server": {
      "type": "streamable-http", 
      "url": "http://api.example.com/mcp",
      "oauth": {
        "server_url": "http://api.example.com",
        "client_metadata": {
          "client_name": "My MCPO Client",
          "redirect_uris": ["http://localhost:3030/callback"]
        }
      }
    }
  }
}
```

> **Note**: Avoid setting `scope`, `authorization_endpoint`, or `token_endpoint` in the config. These are automatically discovered from the server's OAuth metadata during the dynamic registration flow.

On first connection, mcpo will:
1. Perform dynamic client registration (if supported)
2. Open your browser for authorization
3. Capture the OAuth callback automatically  
4. Store tokens securely (in `~/.mcpo/tokens/` for file storage)
5. Use tokens for all subsequent requests

OAuth is supported for `streamable-http` server types. See [OAUTH_GUIDE.md](OAUTH_GUIDE.md) for detailed documentation.

## Connection recovery and safe retries

MCPO requires AnyIO 4.14.2 or newer (below 5), which includes the fix for
cancellation-related CPU spinning. The Docker image installs the checked-in
`uv.lock`; rebuild and recreate the container to apply this update. MCP SDK 1.x
remains required by the current transport integration.

Each MCP connection has a dedicated task that opens and closes its transport and
session. A failed connection is cleaned up, and a subsequent request can establish
a new one. Hot reload prepares replacement connections before switching routes;
if preparation fails, existing connections remain available. Removed connections
and connections added by hot reload are also closed during cleanup and shutdown.

`MCP_REQUEST_TIMEOUT` sets a positive, finite deadline in seconds for connection
setup and each MCP operation (default: `900`). For example, set
`MCP_REQUEST_TIMEOUT=60` in the Docker container environment to limit each MCP wait
to one minute. This also bounds waiting when an upstream transport loses a response
without closing its connection. Choose a value that accommodates your longest
expected tool execution. A timeout retires the shared connection and can interrupt
other calls using it; it does not guarantee cancellation of upstream work.
Remote HTTP session termination gets an additional deadline of five seconds or
`MCP_REQUEST_TIMEOUT`, whichever is shorter. MCPO closes the local connection even
if that termination request times out; the remote server may retain its session
until its own cleanup runs.

MCPO can retry initialization before dispatching a tool. **Once a tool call has
started, MCPO does not automatically send it again.** A lost connection or request
timeout returns HTTP 502 with `detail.code` set to `tool_execution_unknown`: the operation may
have completed upstream even though its result was lost. Check the upstream state
before retrying writes, especially orders, payments or deletions. HTTP clients and
reverse proxies must not blindly retry these responses either.

MCPO's tool-dispatch and connection-error logs contain endpoint/server names and
error types, not argument values or upstream exception bodies. Upstream servers
and MCP SDK diagnostics are separate: SDK error logs as well as protocol-level
DEBUG logs can contain sensitive response fragments. Protect log access and avoid
protocol-level DEBUG logging when sending credentials.

## 🔧 Requirements

- Python 3.11+
- uv (optional, but highly recommended for performance + packaging)

## 🛠️ Development & Testing

To contribute or run tests locally:

1.  **Set up the environment:**
    ```bash
    # Clone the repository
    git clone https://github.com/open-webui/mcpo.git
    cd mcpo

    # Install dependencies (including dev dependencies)
    uv sync --locked --dev
    ```

2.  **Run tests:**
    ```bash
    uv run pytest
    ```

3.  **Running Locally with Active Changes:**

    To run `mcpo` with your local modifications from a specific branch (e.g., `my-feature-branch`):

    ```bash
    # Ensure you are on your development branch
    git checkout my-feature-branch

    # Make your code changes in the src/mcpo directory or elsewhere

    # Run mcpo using uv, which will use your local, modified code
    # This command starts mcpo on port 8000 and proxies your_mcp_server_command
    uv run mcpo --port 8000 -- your_mcp_server_command

    # Example with a test MCP server (like mcp-server-time):
    # uv run mcpo --port 8000 -- uvx mcp-server-time --local-timezone=America/New_York
    ```
    This allows you to test your changes interactively before committing or creating a pull request. Access your locally running `mcpo` instance at `http://localhost:8000` and the auto-generated docs at `http://localhost:8000/docs`.


## 🪪 License

MIT

## 🤝 Contributing

We welcome and strongly encourage contributions from the community!

Whether you're fixing a bug, adding features, improving documentation, or just sharing ideas—your input is incredibly valuable and helps make mcpo better for everyone.

Getting started is easy:

- Fork the repo
- Create a new branch
- Make your changes
- Open a pull request

Not sure where to start? Feel free to open an issue or ask a question—we’re happy to help you find a good first task.

## ✨ Star History

<a href="https://star-history.com/#open-webui/mcpo&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=open-webui/mcpo&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=open-webui/mcpo&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=open-webui/mcpo&type=Date" />
  </picture>
</a>

---

✨ Let's build the future of interoperable AI tooling together!
