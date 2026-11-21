"""Exercise connection ownership across independent HTTP/lifespan tasks."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import anyio
from mcp.server.lowlevel import Server
from fastapi import FastAPI

import mcpo.main as main


def make_manager(**kwargs):
    return main.MCPConnectionManager(
        server_type="streamable-http",
        command=None,
        args=["https://mcp.example.test/mcp"],
        env={},
        headers=None,
        connection_timeout=None,
        **kwargs,
    )


@pytest.fixture
def task_bound_transport(monkeypatch):
    """Record SDK context ownership without leaving broken AnyIO scopes behind."""
    state = SimpleNamespace(
        contexts=[],
        sessions=[],
        initialize_gate=None,
        exit_gate=None,
        initializing=asyncio.Event(),
        exiting=asyncio.Event(),
    )

    @asynccontextmanager
    async def transport():
        record = SimpleNamespace(entered=asyncio.current_task(), exited=None)
        state.contexts.append(record)
        try:
            yield None, None
        finally:
            record.exited = asyncio.current_task()

    class Session:
        def __init__(self, *_, **kwargs):
            self.entered = None
            self.exited = None
            self.initialize_calls = 0
            state.sessions.append(self)

        async def __aenter__(self):
            self.entered = asyncio.current_task()
            return self

        async def __aexit__(self, *_):
            state.exiting.set()
            if state.exit_gate is not None:
                await state.exit_gate.wait()
            self.exited = asyncio.current_task()

        async def initialize(self):
            self.initialize_calls += 1
            state.initializing.set()
            if state.initialize_gate is not None:
                await state.initialize_gate.wait()
            return SimpleNamespace(serverInfo=SimpleNamespace(name="fixture"))

    monkeypatch.setattr(main, "ClientSession", Session)
    monkeypatch.setattr(
        main.MCPConnectionManager, "_create_client_context", lambda _: transport()
    )
    return state


@pytest.mark.asyncio
async def test_reconnect_and_close_exit_in_the_task_that_opened_the_context(
    task_bound_transport,
):
    manager = make_manager()
    first, _ = await asyncio.create_task(manager.ensure_initialized())
    second, _ = await asyncio.create_task(manager.reconnect())
    await asyncio.create_task(manager.close())

    assert second is not first
    for record in [*task_bound_transport.contexts, *task_bound_transport.sessions]:
        assert record.exited is record.entered


@pytest.mark.asyncio
async def test_cancelled_request_does_not_cancel_shared_initialization(
    task_bound_transport,
):
    state = task_bound_transport
    state.initialize_gate = asyncio.Event()
    manager = make_manager()
    first = asyncio.create_task(manager.ensure_initialized())
    await asyncio.wait_for(state.initializing.wait(), 2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    second = asyncio.create_task(manager.ensure_initialized())
    state.initialize_gate.set()
    try:
        session, _ = await asyncio.wait_for(second, 2)
        assert session.initialize_calls == 1
        assert len(state.sessions) == 1
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_concurrent_requests_share_one_initialized_session(task_bound_transport):
    manager = make_manager()
    try:
        results = await asyncio.gather(
            *(manager.ensure_initialized() for _ in range(12))
        )
        assert all(result[0] is results[0][0] for result in results)
        assert results[0][0].initialize_calls == 1
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_old_failure_cannot_invalidate_replacement_session(task_bound_transport):
    manager = make_manager()
    old, _ = await manager.ensure_initialized()
    new, _ = await manager.reconnect()
    try:
        await manager.invalidate(old)
        assert (await manager.ensure_initialized())[0] is new
        await manager.invalidate(new)
        assert (await manager.ensure_initialized())[0] is not new
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_repeated_close_and_caller_cancellation_do_not_interrupt_cleanup(
    task_bound_transport,
):
    state = task_bound_transport
    state.exit_gate = asyncio.Event()
    manager = make_manager()
    session, _ = await manager.ensure_initialized()
    closer = asyncio.create_task(manager.close())
    await asyncio.wait_for(state.exiting.wait(), 2)
    closer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closer
    second = asyncio.create_task(manager.close())
    state.exit_gate.set()
    await asyncio.wait_for(second, 2)
    assert session.exited is session.entered
    with pytest.raises(RuntimeError, match="closed"):
        await manager.ensure_initialized()


@pytest.mark.asyncio
async def test_close_interrupts_pending_initialization(task_bound_transport):
    state = task_bound_transport
    state.initialize_gate = asyncio.Event()
    manager = make_manager()
    waiter = asyncio.create_task(manager.ensure_initialized())
    await asyncio.wait_for(state.initializing.wait(), 2)
    await asyncio.wait_for(manager.close(), 2)
    with pytest.raises(anyio.ClosedResourceError):
        await waiter
    assert state.sessions[0].exited is state.sessions[0].entered


@pytest.mark.asyncio
async def test_cancelled_close_wakes_initialization_even_before_owner_starts(
    task_bound_transport,
):
    manager = make_manager()
    waiter = asyncio.create_task(manager.ensure_initialized())
    closer = asyncio.create_task(manager.close())
    asyncio.get_running_loop().call_soon(closer.cancel)
    try:
        with pytest.raises(asyncio.CancelledError):
            await closer
        with pytest.raises(anyio.ClosedResourceError):
            await asyncio.wait_for(waiter, 0.2)
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_request_deadline_cancels_a_lost_response(memory_mcp):
    manager = make_manager(request_timeout=0.1)
    finished = asyncio.Event()

    async def lost_response():
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    try:
        session, _ = await manager.ensure_initialized()
        with pytest.raises(TimeoutError, match="MCP request timed out"):
            await asyncio.wait_for(manager.run_with_session(session, lost_response), 1)
        assert finished.is_set()
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_environment_deadline_bounds_initialization(
    task_bound_transport, monkeypatch
):
    monkeypatch.setenv("MCP_REQUEST_TIMEOUT", "0.1")
    task_bound_transport.initialize_gate = asyncio.Event()
    manager = make_manager()
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(manager.ensure_initialized(), 1)
        assert task_bound_transport.sessions[0].exited is not None
    finally:
        await manager.close()


@pytest.fixture
def memory_mcp(monkeypatch):
    """Real SDK protocol and AnyIO task groups; only the network is in memory."""
    connections = []
    server = Server("lifecycle-fixture")

    @server.list_tools()
    async def list_tools():
        return []

    @asynccontextmanager
    async def transport():
        server_send, client_receive = anyio.create_memory_object_stream(0)
        client_send, server_receive = anyio.create_memory_object_stream(0)
        connection = SimpleNamespace(
            fail=asyncio.Event(),
            closed=asyncio.Event(),
            entered=asyncio.current_task(),
            exited=None,
        )
        connections.append(connection)

        async def fail_connection():
            await connection.fail.wait()
            raise RuntimeError("synthetic upstream failure")

        try:
            async with server_send, client_receive, client_send, server_receive:
                async with anyio.create_task_group() as group:
                    group.start_soon(
                        server.run,
                        server_receive,
                        server_send,
                        server.create_initialization_options(),
                    )
                    group.start_soon(fail_connection)
                    try:
                        yield client_receive, client_send
                    finally:
                        group.cancel_scope.cancel()
        finally:
            connection.exited = asyncio.current_task()
            connection.closed.set()

    monkeypatch.setattr(
        main.MCPConnectionManager, "_create_client_context", lambda _: transport()
    )
    return connections


@pytest.mark.asyncio
async def test_real_sdk_recovers_from_background_failure_without_cancelling_other_sessions(
    memory_mcp, caplog
):
    broken = make_manager()
    healthy = make_manager()
    try:
        first, _ = await asyncio.wait_for(broken.ensure_initialized(), 2)
        other, _ = await asyncio.wait_for(healthy.ensure_initialized(), 2)
        assert (await first.list_tools()).tools == []
        memory_mcp[0].fail.set()
        await asyncio.wait_for(memory_mcp[0].closed.wait(), 2)
        # The unrelated manager and the request task remain usable.
        assert (await other.list_tools()).tools == []
        replacement, _ = await asyncio.wait_for(broken.ensure_initialized(), 2)
        assert replacement is not first
        assert (await replacement.list_tools()).tools == []
    finally:
        await asyncio.wait_for(asyncio.gather(broken.close(), healthy.close()), 2)
    assert len(memory_mcp) == 3
    assert all(c.closed.is_set() and c.entered is c.exited for c in memory_mcp)
    assert "cancel scope" not in caplog.text


@pytest.mark.asyncio
async def test_real_sdk_failed_initialization_cleans_up_before_next_connection(
    memory_mcp, monkeypatch, caplog
):
    initialize = main.ClientSession.initialize
    attempts = 0

    async def fail_first(session):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("synthetic-secret-in-handshake-error")
        return await initialize(session)

    monkeypatch.setattr(main.ClientSession, "initialize", fail_first)
    manager = make_manager()
    try:
        with pytest.raises(ExceptionGroup):
            await asyncio.wait_for(manager.ensure_initialized(), 2)
        assert memory_mcp[0].closed.is_set()
        session, _ = await asyncio.wait_for(manager.ensure_initialized(), 2)
        assert (await session.list_tools()).tools == []
    finally:
        await asyncio.wait_for(manager.close(), 2)
    assert all(c.closed.is_set() and c.entered is c.exited for c in memory_mcp)
    assert "synthetic-secret-in-handshake-error" not in caplog.text


def configured_app(config):
    app = FastAPI()
    app.state.config_data = config
    app.state.lifespan = main.lifespan
    app.state.path_prefix = "/"
    for name, server in config["mcpServers"].items():
        child = main.create_sub_app(
            name, server, ["*"], None, False, None, None, main.lifespan
        )
        app.mount(f"/{name}", child)
    return app


@pytest.mark.asyncio
async def test_hot_reload_retires_initial_and_added_connections(memory_mcp):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    app = configured_app({"mcpServers": {"initial": server}})
    try:
        async with main.lifespan(app):
            await main.reload_config_handler(app, {"mcpServers": {"added": server}})
            assert memory_mcp[0].closed.is_set()
            assert not any(
                getattr(route, "path", None) == "/initial" for route in app.routes
            )
            assert any(getattr(route, "path", None) == "/added" for route in app.routes)
        assert all(c.closed.is_set() for c in memory_mcp)
    finally:
        # Keep a regression failure from leaving live owners in the test process.
        for task in list(asyncio.all_tasks()):
            if task.get_name() == "mcpo-session":
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_failed_reload_keeps_working_routes_and_closes_staged_connections(
    memory_mcp, monkeypatch, caplog
):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    original_config = {"mcpServers": {"initial": server}}
    app = configured_app(original_config)
    create_endpoints = main.create_dynamic_endpoints

    async def fail_last(child, api_dependency=None):
        if child.title == "z-fail":
            raise ValueError("synthetic-secret-in-discovery-error")
        await create_endpoints(child, api_dependency)

    async with main.lifespan(app):
        old_route = next(
            route for route in app.routes if getattr(route, "path", None) == "/initial"
        )
        monkeypatch.setattr(main, "create_dynamic_endpoints", fail_last)
        await main.reload_config_handler(
            app,
            {"mcpServers": {"initial": server, "a-staged": server, "z-fail": server}},
        )
        assert app.state.config_data == original_config
        assert old_route in app.routes
        assert not any(
            getattr(route, "path", None) == "/a-staged" for route in app.routes
        )
        assert not memory_mcp[0].closed.is_set()
        assert all(c.closed.is_set() for c in memory_mcp[1:])
        session = await old_route.app.state.session_manager.get_session()
        assert (await session.list_tools()).tools == []
    assert all(c.closed.is_set() for c in memory_mcp)
    assert "synthetic-secret-in-discovery-error" not in caplog.text


@pytest.mark.asyncio
async def test_cancelling_one_operation_preserves_the_shared_connection(memory_mcp):
    manager = make_manager()
    session, _ = await manager.ensure_initialized()
    started = asyncio.Event()
    finished = asyncio.Event()

    async def wait_for_result():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            finished.set()

    request = asyncio.create_task(manager.run_with_session(session, wait_for_result))
    await asyncio.wait_for(started.wait(), 2)
    request.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await request
        assert finished.is_set()
        assert (await manager.run_with_session(session, session.list_tools)).tools == []
        assert not memory_mcp[0].closed.is_set()
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_connection_failure_wakes_all_waiting_operations(memory_mcp):
    manager = make_manager()
    session, _ = await manager.ensure_initialized()
    waiting = asyncio.Event()
    requests_started = 0
    requests_finished = 0

    async def lost_result():
        nonlocal requests_started, requests_finished
        requests_started += 1
        if requests_started == 3:
            waiting.set()
        try:
            await asyncio.Event().wait()
        finally:
            requests_finished += 1

    requests = [
        asyncio.create_task(manager.run_with_session(session, lost_result))
        for _ in range(3)
    ]
    try:
        await asyncio.wait_for(waiting.wait(), 2)
        memory_mcp[0].fail.set()
        results = await asyncio.wait_for(
            asyncio.gather(*requests, return_exceptions=True), 2
        )
        assert all(isinstance(result, anyio.ClosedResourceError) for result in results)
        assert requests_finished == 3
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_shutdown_cancellation_during_startup_is_not_swallowed(
    memory_mcp, monkeypatch
):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    app = configured_app({"mcpServers": {"first": server, "second": server}})
    initialize = main.ClientSession.initialize
    second_starting = asyncio.Event()
    attempts = 0

    async def block_second(session):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            second_starting.set()
            await asyncio.Event().wait()
        return await initialize(session)

    monkeypatch.setattr(main.ClientSession, "initialize", block_second)

    async def start_app():
        async with main.lifespan(app):
            pass

    startup = asyncio.create_task(start_app())
    await asyncio.wait_for(second_starting.wait(), 2)
    startup.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(startup, 2)
    assert all(c.closed.is_set() for c in memory_mcp)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        {},
        {"mcpServers": {}},
        {"mcpServers": {"initial": {}}},
        {"mcpServers": {"initial": {"type": "streamable-http"}}},
        {
            "mcpServers": {
                "initial": {
                    "type": "streamable-http",
                    "url": "https://mcp.example.test/mcp",
                    "client_header_forwarding": {"enabled": "not-a-boolean"},
                }
            }
        },
    ],
)
async def test_invalid_reload_preserves_live_configuration(memory_mcp, invalid):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    original = {"mcpServers": {"initial": server}}
    app = configured_app(original)
    async with main.lifespan(app):
        routes = list(app.routes)
        await main.reload_config_handler(app, invalid)
        assert app.state.config_data == original
        assert app.routes == routes
        assert not memory_mcp[0].closed.is_set()
    assert all(c.closed.is_set() for c in memory_mcp)


@pytest.mark.asyncio
async def test_reload_waits_for_initial_startup_and_shutdown_closes_replacement(
    memory_mcp, monkeypatch
):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    app = configured_app({"mcpServers": {"initial": server}})
    initialize = main.ClientSession.initialize
    starting = asyncio.Event()
    release_startup = asyncio.Event()
    ready = asyncio.Event()
    shutdown = asyncio.Event()
    attempts = 0

    async def slow_first(session):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            starting.set()
            await release_startup.wait()
        return await initialize(session)

    monkeypatch.setattr(main.ClientSession, "initialize", slow_first)

    async def serve():
        async with main.lifespan(app):
            ready.set()
            await shutdown.wait()

    serving = asyncio.create_task(serve())
    await asyncio.wait_for(starting.wait(), 2)
    reloading = asyncio.create_task(
        main.reload_config_handler(
            app,
            {
                "mcpServers": {
                    "initial": {**server, "headers": {"X-Test": "replacement"}}
                }
            },
        )
    )
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(reloading), 0.05)
        release_startup.set()
        await asyncio.wait_for(ready.wait(), 2)
        await asyncio.wait_for(reloading, 2)
        shutdown.set()
        await asyncio.wait_for(serving, 2)
        assert len(memory_mcp) == 2
        assert all(c.closed.is_set() for c in memory_mcp)
    finally:
        release_startup.set()
        shutdown.set()
        await asyncio.gather(serving, reloading, return_exceptions=True)
        for task in list(asyncio.all_tasks()):
            if task.get_name() == "mcpo-session":
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_shutdown_cancels_stalled_reload_and_skips_queued_reload(
    memory_mcp, monkeypatch
):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    app = configured_app({"mcpServers": {"initial": server}})
    initialize = main.ClientSession.initialize
    replacement_starting = asyncio.Event()
    ready = asyncio.Event()
    shutdown = asyncio.Event()
    attempts = 0

    async def stall_replacement(session):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            replacement_starting.set()
            await asyncio.Event().wait()
        return await initialize(session)

    monkeypatch.setattr(main.ClientSession, "initialize", stall_replacement)

    async def serve():
        async with main.lifespan(app):
            ready.set()
            await shutdown.wait()

    serving = asyncio.create_task(serve())
    await asyncio.wait_for(ready.wait(), 2)
    replacement = {**server, "headers": {"X-Test": "replacement"}}
    reloading = asyncio.create_task(
        main.reload_config_handler(app, {"mcpServers": {"initial": replacement}})
    )
    await asyncio.wait_for(replacement_starting.wait(), 2)
    queued = asyncio.create_task(
        main.reload_config_handler(
            app, {"mcpServers": {"initial": replacement, "extra": server}}
        )
    )
    try:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(serving), 0.5)
        await asyncio.wait_for(queued, 1)
        assert reloading.cancelled()
        assert app.state.shutting_down
        assert len(memory_mcp) == 2
        assert all(c.closed.is_set() for c in memory_mcp)
    finally:
        reloading.cancel()
        queued.cancel()
        shutdown.set()
        await asyncio.gather(reloading, queued, serving, return_exceptions=True)


@pytest.mark.asyncio
async def test_watcher_observes_config_changes_during_initial_handshake(
    memory_mcp, monkeypatch
):
    server = {"type": "streamable-http", "url": "https://mcp.example.test/mcp"}
    app = configured_app({"mcpServers": {"initial": server}})
    initialize = main.ClientSession.initialize
    starting = asyncio.Event()
    release = asyncio.Event()
    ready = asyncio.Event()
    shutdown = asyncio.Event()
    watcher = SimpleNamespace(started=False, stopped=False, reload=None)

    def start_watcher():
        watcher.started = True
        watcher.reload = asyncio.create_task(
            main.reload_config_handler(app, {"mcpServers": {"replacement": server}})
        )

    def stop_watcher():
        watcher.stopped = True

    watcher.start = start_watcher
    watcher.stop = stop_watcher
    app.state.config_watcher = watcher

    async def block_first(session):
        if not starting.is_set():
            starting.set()
            await release.wait()
        return await initialize(session)

    monkeypatch.setattr(main.ClientSession, "initialize", block_first)

    async def serve():
        async with main.lifespan(app):
            ready.set()
            await shutdown.wait()

    serving = asyncio.create_task(serve())
    try:
        await asyncio.wait_for(starting.wait(), 2)
        assert watcher.started
        assert not watcher.reload.done()
        release.set()
        await asyncio.wait_for(ready.wait(), 2)
        await asyncio.wait_for(watcher.reload, 2)
        assert any(
            getattr(route, "path", None) == "/replacement" for route in app.routes
        )
        assert not any(
            getattr(route, "path", None) == "/initial" for route in app.routes
        )
    finally:
        release.set()
        shutdown.set()
        await serving
        if watcher.reload is not None:
            await asyncio.gather(watcher.reload, return_exceptions=True)
    assert watcher.stopped
    assert all(c.closed.is_set() for c in memory_mcp)
