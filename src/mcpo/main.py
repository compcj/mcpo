import asyncio
import json
import logging
import math
import os
import signal
import socket
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx
import uvicorn
from anyio import ClosedResourceError
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.routing import Mount

from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from mcpo.utils.auth import APIKeyMiddleware, get_verify_api_key
from mcpo.utils.main import (
    get_model_fields,
    get_tool_handler,
    normalize_server_type,
    collect_schema_defs,
)
from mcpo.utils.config_watcher import ConfigWatcher
from mcpo.utils.headers import validate_client_header_forwarding_config
from mcpo.utils.oauth import create_oauth_provider


logger = logging.getLogger(__name__)

CONNECTION_TIMEOUT = os.getenv("CONNECTION_TIMEOUT", None)


class GracefulShutdown:
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.tasks = set()

    def handle_signal(self, sig, frame=None):
        """Handle shutdown signals gracefully"""
        logger.info(
            f"\nReceived {signal.Signals(sig).name}, initiating graceful shutdown..."
        )
        self.shutdown_event.set()

    def track_task(self, task):
        """Track tasks for cleanup"""
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)


class _MCPHTTPClient(httpx.AsyncClient):
    """Bound best-effort remote session termination inside the HTTP call."""

    def __init__(self, *, session_close_timeout: float, **kwargs):
        super().__init__(**kwargs)
        self._session_close_timeout = session_close_timeout

    async def delete(self, url, **kwargs):
        try:
            async with asyncio.timeout(self._session_close_timeout):
                return await super().delete(url, **kwargs)
        except TimeoutError:
            # Catch at the HTTP layer so the SDK can finish closing its AnyIO
            # contexts normally, even when the upstream never acknowledges DELETE.
            raise TimeoutError("MCP session termination timed out") from None


class MCPConnectionManager:
    """
    Own the SDK's transport and session contexts in one dedicated task.

    AnyIO cancel scopes must be entered and exited by the same task. HTTP
    requests can share a session, but must never unwind its contexts themselves.
    """

    def __init__(
        self,
        *,
        server_type: str,
        command: Optional[str],
        args: List[str],
        env: Dict[str, str],
        headers: Optional[Dict[str, str]],
        connection_timeout: Optional[int],
        auth_provider: Optional[Any] = None,
        request_timeout: Optional[float] = None,
    ):
        self.server_type = normalize_server_type(server_type)
        self.command = command
        self.args = args
        self.env = env
        self.headers = headers
        self.connection_timeout = connection_timeout
        self.auth_provider = auth_provider
        self.request_timeout = float(
            request_timeout
            if request_timeout is not None
            else os.getenv("MCP_REQUEST_TIMEOUT", "900")
        )
        if not math.isfinite(self.request_timeout) or self.request_timeout <= 0:
            raise ValueError("MCP_REQUEST_TIMEOUT must be a positive finite number")

        self._session: Optional[ClientSession] = None
        self._lock = asyncio.Lock()
        self._session_task: Optional[asyncio.Task] = None
        self._ready: Optional[asyncio.Future] = None
        self._stopping = False
        self._closed = False

    @property
    def current_session(self) -> Optional[ClientSession]:
        return self._session

    async def get_session(self) -> ClientSession:
        session, _ = await self.ensure_initialized()
        return session

    async def ensure_initialized(self):
        async with self._lock:
            if self._closed:
                raise RuntimeError("MCP connection manager is closed")
            if self._session_task is not None and (
                self._stopping
                or self._session_task.done()
                or (self._ready.done() and self._session is None)
            ):
                await self._close_session_locked()
            if self._session_task is None:
                self._ready = asyncio.get_running_loop().create_future()
                # Retrieve failures even when every waiting HTTP request was
                # cancelled; awaiting the future still raises the same error.
                self._ready.add_done_callback(
                    lambda future: None if future.cancelled() else future.exception()
                )
                self._session_task = asyncio.create_task(
                    self._run_session(self._ready), name="mcpo-session"
                )

                # The owner can be cancelled before executing its first line.
                # Wake initialization waiters even if close() is also cancelled.
                def owner_done(_, ready=self._ready):
                    if not ready.done():
                        ready.set_exception(
                            ClosedResourceError("MCP connection closed")
                        )

                self._session_task.add_done_callback(owner_done)
            ready = self._ready

        # Do not hold the lifecycle lock during network I/O. Shutdown must be
        # able to interrupt an unfinished handshake. One cancelled waiter must
        # not cancel setup shared with another request.
        session, result = await asyncio.shield(ready)
        if self._closed or self._stopping or session is not self._session:
            raise ClosedResourceError("MCP connection closed during initialization")
        return session, result

    async def reconnect(self):
        async with self._lock:
            if self._closed:
                raise RuntimeError("MCP connection manager is closed")
            await self._close_session_locked()
        return await self.ensure_initialized()

    async def run_with_session(self, session, operation, *args, **kwargs):
        """Wake a request even if the SDK cannot publish its connection error."""
        owner = self._session_task
        if (
            self._closed
            or session is not self._session
            or self._stopping
            or owner is None
            or owner.done()
        ):
            raise ClosedResourceError("MCP connection closed")
        request_task = asyncio.create_task(operation(*args, **kwargs))
        try:
            done, _ = await asyncio.wait(
                (request_task, owner),
                timeout=self.request_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            # A received result is authoritative even if the transport ended
            # simultaneously. Never replace success with an ambiguous error.
            if request_task in done:
                return await request_task
            if not done:
                raise TimeoutError("MCP request timed out")
            raise ClosedResourceError("MCP connection closed")
        finally:
            if not request_task.done():
                request_task.cancel()
            await asyncio.shield(asyncio.gather(request_task, return_exceptions=True))

    async def invalidate(self, session: ClientSession):
        """Retire a failed session without replaying calls or closing a newer one."""
        async with self._lock:
            if self._session is session:
                await self._close_session_locked()

    async def close(self):
        self._closed = True
        async with self._lock:
            await self._close_session_locked()

    async def _run_session(self, ready: asyncio.Future):
        try:
            async with AsyncExitStack() as stack:
                try:
                    async with asyncio.timeout(self.request_timeout):
                        connection = await stack.enter_async_context(
                            self._create_client_context()
                        )
                        reader, writer, *_ = connection
                        session = await stack.enter_async_context(
                            ClientSession(
                                reader,
                                writer,
                                read_timeout_seconds=timedelta(
                                    seconds=self.request_timeout
                                ),
                            )
                        )
                        result = await session.initialize()
                    self._session = session
                    ready.set_result((session, result))
                    await asyncio.Event().wait()
                finally:
                    # A transport child may have already cancelled this owner.
                    # Mark teardown before the SDK contexts unwind so another
                    # failing request cannot interrupt cleanup with a second cancel.
                    self._stopping = True
                    self._session = None
        except asyncio.CancelledError:
            if not ready.done():
                ready.set_exception(ClosedResourceError("MCP connection closed"))
        except Exception as error:
            # Upstream exception text can contain URLs, credentials or arguments.
            logger.warning("MCP connection ended (%s)", type(error).__name__)
            if not ready.done():
                ready.set_exception(error)
        finally:
            self._session = None
            if not ready.done():
                ready.set_exception(ClosedResourceError("MCP connection closed"))

    async def _close_session_locked(self):
        task = self._session_task
        if task is None:
            return
        if not task.done() and not self._stopping:
            self._stopping = True
            task.cancel()
        # Shield the owner, including its context-manager cleanup, from repeated
        # request cancellation. Gather also handles cancellation before it starts.
        await asyncio.shield(asyncio.gather(task, return_exceptions=True))
        if not self._ready.done():
            self._ready.set_exception(ClosedResourceError("MCP connection closed"))
        self._session_task = None
        self._ready = None
        self._stopping = False

    def _create_http_client(self, **kwargs):
        return _MCPHTTPClient(
            session_close_timeout=min(self.request_timeout, 5.0),
            follow_redirects=True,
            **kwargs,
        )

    def _create_client_context(self):
        if self.server_type == "stdio":
            server_params = StdioServerParameters(
                command=self.command,
                args=self.args,
                env={**os.environ, **self.env},
            )
            return stdio_client(server_params)
        if self.server_type == "sse":
            timeout = self.connection_timeout or 900
            return sse_client(
                url=self.args[0],
                sse_read_timeout=timeout,
                headers=self.headers,
            )
        if self.server_type == "streamable-http":
            return streamablehttp_client(
                url=self.args[0],
                headers=self.headers,
                auth=self.auth_provider,
                httpx_client_factory=self._create_http_client,
            )
        raise ValueError(f"Unsupported server type: {self.server_type}")


def validate_server_config(server_name: str, server_cfg: Dict[str, Any]) -> None:
    """Validate individual server configuration."""
    server_type = server_cfg.get("type")

    if normalize_server_type(server_type) in ("sse", "streamable-http"):
        if not server_cfg.get("url"):
            raise ValueError(
                f"Server '{server_name}' of type '{server_type}' requires a 'url' field"
            )
    elif server_cfg.get("command"):
        # stdio server
        if not isinstance(server_cfg["command"], str):
            raise ValueError(f"Server '{server_name}' 'command' must be a string")
        if server_cfg.get("args") and not isinstance(server_cfg["args"], list):
            raise ValueError(f"Server '{server_name}' 'args' must be a list")
    elif server_cfg.get("url") and not server_type:
        # Fallback for old SSE config without explicit type
        pass
    else:
        raise ValueError(f"Server '{server_name}' must have either 'command' for stdio or 'type' and 'url' for remote servers")
    
    # Validate disabledTools (supports camelCase & snake_case for backwards compatibility)
    disabled_tools = server_cfg.get("disabledTools")
    if disabled_tools is None:
        disabled_tools = server_cfg.get("disabled_tools")
    if disabled_tools is not None:
        if not isinstance(disabled_tools, list):
            raise ValueError(f"Server '{server_name}' 'disabledTools' must be a list")
        for tool_name in disabled_tools:
            if not isinstance(tool_name, str):
                raise ValueError(f"Server '{server_name}' 'disabledTools' must contain only strings")


def validate_config_data(config_data: Dict[str, Any]) -> None:
    """Apply the same validation to startup files and hot-reload snapshots."""
    if not isinstance(config_data, dict):
        raise ValueError("MCP configuration must be an object")
    mcp_servers = config_data.get("mcpServers")
    if not isinstance(mcp_servers, dict) or not mcp_servers:
        raise ValueError("No 'mcpServers' found in config file.")
    for server_name, server_cfg in mcp_servers.items():
        if (
            not isinstance(server_name, str)
            or not server_name
            or not isinstance(server_cfg, dict)
        ):
            raise ValueError(
                "Each MCP server must have a name and an object configuration"
            )
        validate_server_config(server_name, server_cfg)
        header_config = server_cfg.get("client_header_forwarding", {})
        if header_config:
            validate_client_header_forwarding_config(server_name, header_config)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and validate config from file."""
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)

        validate_config_data(config_data)
        return config_data
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file {config_path}: {e}")
        raise
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise
    except ValueError as e:
        logger.error(f"Invalid configuration: {e}")
        raise


def create_sub_app(
    server_name: str,
    server_cfg: Dict[str, Any],
    cors_allow_origins,
    api_key: Optional[str],
    strict_auth: bool,
    api_dependency,
    connection_timeout,
    lifespan,
) -> FastAPI:
    """Create a sub-application for an MCP server."""
    sub_app = FastAPI(
        title=f"{server_name}",
        description=f"{server_name} MCP Server\n\n- [back to tool list](/docs)",
        version="1.0",
        lifespan=lifespan,
    )

    sub_app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Configure server type and connection parameters
    if server_cfg.get("command"):
        # stdio
        sub_app.state.server_type = "stdio"
        sub_app.state.command = server_cfg["command"]
        sub_app.state.args = server_cfg.get("args", [])
        sub_app.state.env = {**os.environ, **server_cfg.get("env", {})}

    server_config_type = server_cfg.get("type")
    if server_config_type == "sse" and server_cfg.get("url"):
        sub_app.state.server_type = "sse"
        sub_app.state.args = [server_cfg["url"]]
        sub_app.state.headers = server_cfg.get("headers")
    elif normalize_server_type(
        server_config_type
    ) == "streamable-http" and server_cfg.get("url"):
        url = server_cfg["url"]
        sub_app.state.server_type = "streamablehttp"
        sub_app.state.args = [url]
        sub_app.state.headers = server_cfg.get("headers")
    elif not server_config_type and server_cfg.get(
        "url"
    ):  # Fallback for old SSE config
        sub_app.state.server_type = "sse"
        sub_app.state.args = [server_cfg["url"]]
        sub_app.state.headers = server_cfg.get("headers")

    if api_key and strict_auth:
        sub_app.add_middleware(APIKeyMiddleware, api_key=api_key)

    sub_app.state.api_dependency = api_dependency
    sub_app.state.connection_timeout = connection_timeout

    # Store list of tools to be disabled, if present (accept both key styles)
    disabled_tools = server_cfg.get("disabledTools")
    if disabled_tools is None:
        disabled_tools = server_cfg.get("disabled_tools", [])
    sub_app.state.disabled_tools = disabled_tools

    
    # Store client header forwarding configuration
    sub_app.state.client_header_forwarding = server_cfg.get(
        "client_header_forwarding", {"enabled": False}
    )

    # Store OAuth configuration if present
    sub_app.state.oauth_config = server_cfg.get("oauth")


    return sub_app


def mount_config_servers(
    main_app: FastAPI,
    config_data: Dict[str, Any],
    cors_allow_origins,
    api_key: Optional[str],
    strict_auth: bool,
    api_dependency,
    connection_timeout,
    lifespan,
    path_prefix: str,
):
    """Mount MCP servers from config data."""
    mcp_servers = config_data.get("mcpServers", {})

    logger.info("Configuring MCP Servers:")
    for server_name, server_cfg in mcp_servers.items():
        sub_app = create_sub_app(
            server_name,
            server_cfg,
            cors_allow_origins,
            api_key,
            strict_auth,
            api_dependency,
            connection_timeout,
            lifespan,
        )
        main_app.mount(f"{path_prefix}{server_name}", sub_app)


def _get_reload_lock(app: FastAPI):
    lock = getattr(app.state, "reload_lock", None)
    if lock is None:
        lock = app.state.reload_lock = asyncio.Lock()
    return lock


def _retire_lifespans(app: FastAPI, contexts):
    """Track cleanup independently of the reload request that initiated it."""

    async def cleanup():
        for context in reversed(contexts):
            try:
                await context.__aexit__(None, None, None)
            except Exception as error:
                logger.warning("MCP lifespan cleanup failed (%s)", type(error).__name__)

    pending = getattr(app.state, "retiring_lifespans", None)
    if pending is None:
        pending = app.state.retiring_lifespans = set()
    task = asyncio.create_task(cleanup(), name="mcpo-lifespan-cleanup")
    pending.add(task)
    task.add_done_callback(pending.discard)
    return task


async def reload_config_handler(main_app: FastAPI, new_config_data: Dict[str, Any]):
    """Stage replacements before swapping routes, then await old session cleanup."""
    async with _get_reload_lock(main_app):
        if getattr(main_app.state, "shutting_down", False):
            return
        try:
            validate_config_data(new_config_data)
        except ValueError as error:
            logger.error(
                "Invalid reload configuration; keeping current connections (%s)",
                type(error).__name__,
            )
            return
        old_config = getattr(main_app.state, "config_data", {}).get("mcpServers", {})
        new_config = new_config_data.get("mcpServers", {})
        changed = {
            name
            for name in new_config
            if name not in old_config or old_config[name] != new_config[name]
        }
        removed = set(old_config) - set(new_config)
        path_prefix = getattr(main_app.state, "path_prefix", "/")
        staged_routes = []
        staged_contexts = {}
        main_app.state.staging_reload_task = asyncio.current_task()
        try:
            for name in sorted(changed):
                sub_app = create_sub_app(
                    name,
                    new_config[name],
                    getattr(main_app.state, "cors_allow_origins", ["*"]),
                    getattr(main_app.state, "api_key", None),
                    getattr(main_app.state, "strict_auth", False),
                    getattr(main_app.state, "api_dependency", None),
                    getattr(main_app.state, "connection_timeout", None),
                    getattr(main_app.state, "lifespan", None),
                )
                route = Mount(f"{path_prefix}{name}", app=sub_app)
                context = sub_app.router.lifespan_context(sub_app)
                await context.__aenter__()
                staged_contexts[name] = context
                staged_routes.append(route)
        except BaseException as error:
            await asyncio.shield(
                _retire_lifespans(main_app, list(staged_contexts.values()))
            )
            if not isinstance(error, Exception):
                raise
            logger.error(
                "Config reload failed; previous connections retained (%s)",
                type(error).__name__,
            )
            return
        finally:
            main_app.state.staging_reload_task = None

        active = getattr(main_app.state, "active_lifespans", None)
        if active is None:
            active = main_app.state.active_lifespans = {}
        retired = [active.pop(name) for name in removed | changed if name in active]
        removed_paths = {f"{path_prefix}{name}" for name in removed | changed}
        # There is no await between publishing routes, lifespans and configuration.
        main_app.router.routes = [
            route
            for route in main_app.router.routes
            if not (isinstance(route, Mount) and route.path in removed_paths)
        ] + staged_routes
        active.update(staged_contexts)
        main_app.state.config_data = new_config_data
        await asyncio.shield(_retire_lifespans(main_app, retired))
        logger.info("Config reload completed successfully")


async def create_dynamic_endpoints(app: FastAPI, api_dependency=None):
    session_manager: Optional[MCPConnectionManager] = getattr(
        app.state, "session_manager", None
    )

    if session_manager:
        session, result = await session_manager.ensure_initialized()
        app.state.session = session
    else:
        session: Optional[ClientSession] = getattr(app.state, "session", None)
        if not session:
            raise ValueError("Session is not initialized in the app state.")
        result = await session.initialize()
    server_info = getattr(result, "serverInfo", None)
    if server_info:
        app.title = server_info.name or app.title
        app.description = (
            f"{server_info.name} MCP Server" if server_info.name else app.description
        )
        app.version = server_info.version or app.version

    instructions = getattr(result, "instructions", None)
    if instructions:
        app.description = instructions

    tools_result = (
        await session_manager.run_with_session(session, session.list_tools)
        if session_manager
        else await session.list_tools()
    )
    tools = tools_result.tools
    
    # Filter out disabled tools
    disabled_tools = getattr(app.state, "disabled_tools", [])
    if disabled_tools:
        original_count = len(tools)
        tools = [tool for tool in tools if tool.name not in disabled_tools]
        filtered_count = original_count - len(tools)
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} tool(s) for server '{app.title}': {disabled_tools}")

    for tool in tools:
        endpoint_name = tool.name
        try:
            endpoint_description = tool.description or ""

            inputSchema = tool.inputSchema or {}
            outputSchema = getattr(tool, "outputSchema", None)

            form_model_fields = get_model_fields(
                f"{endpoint_name}_form_model",
                inputSchema.get("properties", {}),
                inputSchema.get("required", []),
                collect_schema_defs(inputSchema),
            )

            response_model_fields = None
            if outputSchema:
                response_model_fields = get_model_fields(
                    f"{endpoint_name}_response_model",
                    outputSchema.get("properties", {}),
                    outputSchema.get("required", []),
                    collect_schema_defs(outputSchema),
                )

            # Get client header forwarding configuration from app state
            client_header_forwarding_config = getattr(
                app.state, "client_header_forwarding", {"enabled": False}
            )

            tool_handler = get_tool_handler(
                endpoint_name,
                form_model_fields,
                response_model_fields,
                client_header_forwarding_config,
            )

            annotations = getattr(tool, "annotations", None)
            openapi_extra = None
            tags = None
            if annotations is not None:
                # MCP ToolAnnotations may be a pydantic model; normalize to a dict
                if hasattr(annotations, "model_dump"):
                    annotations_dict = annotations.model_dump(exclude_none=True)
                elif isinstance(annotations, dict):
                    annotations_dict = {
                        k: v for k, v in annotations.items() if v is not None
                    }
                else:
                    annotations_dict = {}

                if annotations_dict:
                    openapi_extra = {"x-mcp-annotations": annotations_dict}

                    if annotations_dict.get("destructiveHint"):
                        warning = (
                            "\n\n**Warning:** This tool is marked destructive "
                            "(destructiveHint). It may have irreversible side effects "
                            "(for example, spending money or modifying remote state). "
                            "Confirm with the user before calling."
                        )
                        endpoint_description = f"{endpoint_description}{warning}"
                        tags = ["destructive"]
                    elif annotations_dict.get("readOnlyHint"):
                        tags = ["read-only"]

            post_kwargs = {
                "summary": endpoint_name.replace("_", " ").title(),
                "description": endpoint_description,
                "response_model_exclude_none": True,
                "dependencies": [Depends(api_dependency)] if api_dependency else [],
            }
            if openapi_extra is not None:
                post_kwargs["openapi_extra"] = openapi_extra
            if tags is not None:
                post_kwargs["tags"] = tags

            app.post(f"/{endpoint_name}", **post_kwargs)(tool_handler)
        except Exception:
            logger.exception(
                "Failed to register tool '%s'; skipping this tool",
                endpoint_name,
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    server_type = normalize_server_type(getattr(app.state, "server_type", "stdio"))
    command = getattr(app.state, "command", None)
    args = getattr(app.state, "args", [])
    args = args if isinstance(args, list) else [args]
    env = getattr(app.state, "env", {})
    connection_timeout = getattr(app.state, "connection_timeout", CONNECTION_TIMEOUT)
    api_dependency = getattr(app.state, "api_dependency", None)
    path_prefix = getattr(app.state, "path_prefix", "/")

    is_main_app = not command and not (
        server_type in ["sse", "streamable-http"] and args
    )

    if is_main_app:
        startup_lock = _get_reload_lock(app)
        await startup_lock.acquire()
        startup_locked = True
        app.state.active_lifespans = {}
        app.state.shutting_down = False
        config_watcher = getattr(app.state, "config_watcher", None)
        try:
            # Observe edits during slow initial handshakes. Reload callbacks wait
            # for this startup lock before they can replace any connections.
            if config_watcher is not None:
                config_watcher.start()
            successful_servers = []
            failed_servers = []

            sub_lifespans = [
                (
                    route.path[len(path_prefix) :],
                    route.app,
                    route.app.router.lifespan_context(route.app),
                )
                for route in app.routes
                if isinstance(route, Mount) and isinstance(route.app, FastAPI)
            ]

            for server_id, sub_app, lifespan_context in sub_lifespans:
                server_name = sub_app.title
                logger.info(f"Initiating connection for server: '{server_name}'...")
                try:
                    await lifespan_context.__aenter__()
                    app.state.active_lifespans[server_id] = lifespan_context
                    is_connected = getattr(sub_app.state, "is_connected", False)
                    if is_connected:
                        logger.info(f"Successfully connected to '{server_name}'.")
                        successful_servers.append(server_name)
                    else:
                        logger.warning(
                            f"Connection attempt for '{server_name}' finished, but status is not 'connected'."
                        )
                        failed_servers.append(server_name)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    logger.error(
                        "Failed to establish connection for server '%s' (%s)",
                        server_name,
                        type(error).__name__,
                    )
                    failed_servers.append(server_name)

            logger.info("\n--- Server Startup Summary ---")
            if successful_servers:
                logger.info("Successfully connected to:")
                for name in successful_servers:
                    logger.info(f"  - {name}")
                app.description += "\n\n- **available tools**："
                for name in successful_servers:
                    docs_path = urljoin(path_prefix, f"{name}/docs")
                    app.description += f"\n    - [{name}]({docs_path})"
            if failed_servers:
                logger.warning("Failed to connect to:")
                for name in failed_servers:
                    logger.warning(f"  - {name}")
            logger.info("--------------------------\n")

            if not successful_servers:
                logger.error("No MCP servers could be reached.")

            startup_locked = False
            startup_lock.release()
            yield
        finally:
            app.state.shutting_down = True
            staging_reload = getattr(app.state, "staging_reload_task", None)
            if (
                staging_reload is not None
                and staging_reload is not asyncio.current_task()
            ):
                # A replacement's handshake must not block shutdown of healthy
                # connections. Its cancellation path rolls back staged lifespans.
                staging_reload.cancel()
            if startup_locked:
                startup_lock.release()
            if config_watcher is not None:
                config_watcher.stop()
            async with _get_reload_lock(app):
                contexts = list(app.state.active_lifespans.values())
                app.state.active_lifespans.clear()
                _retire_lifespans(app, contexts)
                pending = list(app.state.retiring_lifespans)
            await asyncio.shield(asyncio.gather(*pending))
    else:
        # This is a sub-app's lifespan
        app.state.is_connected = False
        try:
            # Check for OAuth configuration
            oauth_config = getattr(app.state, "oauth_config", None)
            auth_provider = None

            if oauth_config:
                server_name = app.title
                logger.info(f"OAuth configuration detected for server: {server_name}")
                try:
                    auth_provider = await create_oauth_provider(
                        server_name=server_name,
                        oauth_config=oauth_config,
                        storage_type=oauth_config.get("storage_type", "file"),
                    )
                    logger.info(f"OAuth provider created for server: {server_name}")
                except Exception as error:
                    logger.error(
                        "Failed to create OAuth provider for '%s' (%s)",
                        server_name,
                        type(error).__name__,
                    )
                    raise

            if oauth_config and server_type == "stdio":
                logger.warning("OAuth not supported for stdio server type")
            if oauth_config and server_type == "sse":
                logger.warning("OAuth not supported for SSE server type")

            headers = getattr(app.state, "headers", None)
            session_manager = MCPConnectionManager(
                server_type=server_type,
                command=command,
                args=args,
                env=env,
                headers=headers,
                connection_timeout=connection_timeout,
                auth_provider=auth_provider,
            )
            app.state.session_manager = session_manager

            session = await session_manager.get_session()
            app.state.session = session
            await create_dynamic_endpoints(app, api_dependency=api_dependency)
            app.state.is_connected = True
            yield
        except Exception as error:
            logger.error(
                "Failed to connect to MCP server '%s' (%s)",
                app.title,
                type(error).__name__,
            )
            app.state.is_connected = False
            # Re-raise the exception so it propagates to the main app's lifespan
            raise
        finally:
            session_manager = getattr(app.state, "session_manager", None)
            if session_manager:
                await session_manager.close()
                app.state.session_manager = None
            app.state.session = None


async def run(
    host: str = "127.0.0.1",
    port: int = 8000,
    api_key: Optional[str] = "",
    cors_allow_origins=["*"],
    **kwargs,
):
    hot_reload = kwargs.get("hot_reload", False)
    # Server API Key
    api_dependency = get_verify_api_key(api_key) if api_key else None
    connection_timeout = kwargs.get("connection_timeout", None)
    strict_auth = kwargs.get("strict_auth", False)

    # MCP Server
    server_type = normalize_server_type(kwargs.get("server_type"))
    server_command = kwargs.get("server_command")

    # MCP Config
    config_path = kwargs.get("config_path")

    # mcpo server
    name = kwargs.get("name") or "MCP OpenAPI Proxy"
    description = (
        kwargs.get("description") or "Automatically generated API from MCP Tool Schemas"
    )
    version = kwargs.get("version") or "1.0"

    ssl_certfile = kwargs.get("ssl_certfile")
    ssl_keyfile = kwargs.get("ssl_keyfile")
    path_prefix = kwargs.get("path_prefix") or "/"
    root_path = kwargs.get("root_path") or ""

    # Configure logging based on LOG_LEVEL environment variable
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    try:
        numeric_level = getattr(logging, log_level, None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {log_level}")
    except (ValueError, AttributeError):
        logger.warning(f"Invalid LOG_LEVEL '{log_level}', defaulting to INFO")
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Suppress HTTP request logs
    class HTTPRequestFilter(logging.Filter):
        def filter(self, record):
            return not (
                record.levelname == "INFO" and "HTTP Request:" in record.getMessage()
            )

    # Apply filter to suppress HTTP request logs
    logging.getLogger("uvicorn.access").addFilter(HTTPRequestFilter())
    logging.getLogger("httpx.access").addFilter(HTTPRequestFilter())
    logger.info("Starting MCPO Server...")
    logger.info(f"  Name: {name}")
    logger.info(f"  Version: {version}")
    logger.info(f"  Description: {description}")
    logger.info(f"  Hostname: {socket.gethostname()}")
    logger.info(f"  Port: {port}")
    logger.info(f"  API Key: {'Provided' if api_key else 'Not Provided'}")
    logger.info(f"  CORS Allowed Origins: {cors_allow_origins}")
    if ssl_certfile:
        logger.info(f"  SSL Certificate File: {ssl_certfile}")
    if ssl_keyfile:
        logger.info(f"  SSL Key File: {ssl_keyfile}")
    logger.info(f"  Path Prefix: {path_prefix}")
    logger.info(f"  Root Path: {root_path}")

    # Create shutdown handler
    shutdown_handler = GracefulShutdown()

    main_app = FastAPI(
        title=name,
        description=description,
        version=version,
        root_path=root_path,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
        lifespan=lifespan,
    )

    # Pass shutdown handler to app state
    main_app.state.shutdown_handler = shutdown_handler
    main_app.state.path_prefix = path_prefix

    main_app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_allow_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add middleware to protect also documentation and spec
    if api_key and strict_auth:
        main_app.add_middleware(APIKeyMiddleware, api_key=api_key)

    headers = kwargs.get("headers")
    if headers and isinstance(headers, str):
        try:
            headers = json.loads(headers)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON format for headers. Headers will be ignored.")
            headers = None

    if server_type == "sse":
        logger.info(
            f"Configuring for a single SSE MCP Server with URL {server_command[0]}"
        )
        main_app.state.server_type = "sse"
        main_app.state.args = server_command[0]  # Expects URL as the first element
        main_app.state.api_dependency = api_dependency
        main_app.state.headers = headers
    elif server_type == "streamable-http":
        logger.info(
            f"Configuring for a single StreamableHTTP MCP Server with URL {server_command[0]}"
        )
        main_app.state.server_type = "streamable-http"
        main_app.state.args = server_command[0]  # Expects URL as the first element
        main_app.state.api_dependency = api_dependency
        main_app.state.headers = headers
    elif server_command:  # This handles stdio
        logger.info(
            f"Configuring for a single Stdio MCP Server with command: {' '.join(server_command)}"
        )
        main_app.state.server_type = "stdio"  # Explicitly set type
        main_app.state.command = server_command[0]
        main_app.state.args = server_command[1:]
        main_app.state.env = os.environ.copy()
        main_app.state.api_dependency = api_dependency
    elif config_path:
        logger.info(f"Loading MCP server configurations from: {config_path}")
        config_data = load_config(config_path)
        mount_config_servers(
            main_app,
            config_data,
            cors_allow_origins,
            api_key,
            strict_auth,
            api_dependency,
            connection_timeout,
            lifespan,
            path_prefix,
        )

        # Store config info and app state for hot reload
        main_app.state.config_path = config_path
        main_app.state.config_data = config_data
        main_app.state.cors_allow_origins = cors_allow_origins
        main_app.state.api_key = api_key
        main_app.state.strict_auth = strict_auth
        main_app.state.api_dependency = api_dependency
        main_app.state.connection_timeout = connection_timeout
        main_app.state.lifespan = lifespan
        main_app.state.path_prefix = path_prefix
    else:
        logger.error("MCPO server_command or config_path must be provided.")
        raise ValueError("You must provide either server_command or config.")

    # Setup hot reload if enabled and config_path is provided
    config_watcher = None
    if hot_reload and config_path:
        logger.info(f"Enabling hot reload for config file: {config_path}")

        async def reload_callback(new_config):
            await reload_config_handler(main_app, new_config)

        config_watcher = ConfigWatcher(config_path, reload_callback)
        main_app.state.config_watcher = config_watcher

    logger.info("Uvicorn server starting...")
    uvicorn_log_level = log_level.lower()

    config = uvicorn.Config(
        app=main_app,
        host=host,
        port=port,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
        log_level=uvicorn_log_level,
    )
    server = uvicorn.Server(config)

    # Setup signal handlers
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda s=sig: shutdown_handler.handle_signal(s)
            )
    except NotImplementedError:
        logger.warning(
            "loop.add_signal_handler is not available on this platform. Using signal.signal()."
        )
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda s, f: shutdown_handler.handle_signal(s))

    # Modified server startup
    try:
        # Create server task
        server_task = asyncio.create_task(server.serve())
        shutdown_handler.track_task(server_task)

        # Wait for either the server to fail or a shutdown signal
        shutdown_wait_task = asyncio.create_task(shutdown_handler.shutdown_event.wait())
        done, pending = await asyncio.wait(
            [server_task, shutdown_wait_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if server_task in done:
            # Check if the server task raised an exception
            try:
                server_task.result()  # This will raise the exception if there was one
                logger.warning("Server task exited unexpectedly. Initiating shutdown.")
            except SystemExit as e:
                logger.error(f"Server failed to start: {e}")
                raise  # Re-raise SystemExit to maintain proper exit behavior
            except Exception as e:
                logger.error(f"Server task failed with exception: {e}")
                raise
            shutdown_handler.shutdown_event.set()

        # Cancel the other task
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Graceful shutdown if server didn't fail with SystemExit
        logger.info("Initiating server shutdown...")
        server.should_exit = True

        # Cancel all tracked tasks
        for task in list(shutdown_handler.tasks):
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete
        if shutdown_handler.tasks:
            await asyncio.gather(*shutdown_handler.tasks, return_exceptions=True)

    except SystemExit:
        # Re-raise SystemExit to allow proper program termination
        logger.info("Server startup failed, exiting...")
        raise
    except Exception as e:
        logger.error(f"Error during server execution: {e}")
        raise
    finally:
        # Stop config watcher if it was started
        if config_watcher:
            config_watcher.stop()
        logger.info("Server shutdown complete")
