import asyncio
import logging

import pytest
from anyio import BrokenResourceError, ClosedResourceError, EndOfStream
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from mcp.shared.exceptions import McpError
from mcp.types import (
    CONNECTION_CLOSED,
    INVALID_PARAMS,
    CallToolResult,
    ErrorData,
    TextContent,
)

from mcpo.utils.main import get_tool_handler


TOOL_NAME = "write_record"
SECRET = "synthetic-tool-argument-secret"


class ToolSession:
    def __init__(self, execute):
        self.execute = execute

    async def call_tool(self, name, *, arguments):
        assert name == TOOL_NAME
        return await self.execute(arguments)


class SessionManager:
    def __init__(self, session, replacement=None, *, initialization_errors=()):
        self.session = session
        self.replacement = replacement
        self.initialization_errors = iter(initialization_errors)
        self.initializations = 0

    async def ensure_initialized(self):
        self.initializations += 1
        error = next(self.initialization_errors, None)
        if error is not None:
            raise error
        if self.session is None:
            self.session = self.replacement
        return self.session, None

    async def reconnect(self):
        # Exercise the previous handler's replay path during regression testing.
        self.session = None
        return await self.ensure_initialized()

    async def invalidate(self, session):
        assert session is self.session
        self.session = None

    async def run_with_session(self, session, operation, *args, **kwargs):
        assert session is self.session
        return await operation(*args, **kwargs)


def tool_app(manager, has_args):
    app = FastAPI()
    app.state.session_manager = manager
    fields = {"value": (str, ...)} if has_args else {}
    app.post(f"/{TOOL_NAME}")(get_tool_handler(TOOL_NAME, fields))
    return app


def tool_client(app):
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def tool_result(text, *, is_error=False):
    return CallToolResult(
        content=[TextContent(type="text", text=text)], isError=is_error
    )


def sdk_connection_closed():
    return McpError(ErrorData(code=CONNECTION_CLOSED, message="Connection closed"))


def sdk_session_terminated():
    return McpError(ErrorData(code=32600, message="Session terminated"))


def sdk_request_timeout():
    error = McpError(
        ErrorData(
            code=408,
            message=(
                "Timed out while waiting for response to ClientRequest. Waited 0.5 seconds."
            ),
        )
    )
    error.__context__ = TimeoutError()
    return error


@pytest.mark.asyncio
@pytest.mark.parametrize("has_args", [True, False], ids=["with-args", "no-args"])
@pytest.mark.parametrize(
    "lost_response",
    [
        ClosedResourceError,
        BrokenResourceError,
        EndOfStream,
        sdk_connection_closed,
        sdk_session_terminated,
        TimeoutError,
        sdk_request_timeout,
    ],
)
async def test_lost_tool_response_is_never_replayed(has_args, lost_response):
    records = []

    async def execute(arguments):
        records.append(arguments)
        return tool_result('{"stored": true}')

    async def drop_result(arguments):
        await execute(arguments)
        raise lost_response()

    arguments = {"value": SECRET} if has_args else {}
    broken_session = ToolSession(drop_result)
    replacement = ToolSession(execute)
    manager = SessionManager(broken_session, replacement)
    app = tool_app(manager, has_args)

    async with tool_client(app) as client:
        response = await client.post(f"/{TOOL_NAME}", json=arguments)

        assert records == [arguments]
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "tool_execution_unknown"
        message = response.json()["detail"]["message"].lower()
        assert "may have completed" in message
        assert "check upstream before retrying" in message
        assert manager.session is None
        assert manager.initializations == 1

        response = await client.post(f"/{TOOL_NAME}", json=arguments)

    assert response.status_code == 200
    assert response.json() == {"stored": True}
    assert records == [arguments, arguments]
    assert manager.session is replacement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ErrorData(code=32600, message="Another upstream error"),
        ErrorData(
            code=32600, message="Session terminated", data={"reason": "upstream"}
        ),
        sdk_request_timeout().error,
    ],
    ids=["code-only", "with-upstream-data", "remote-408"],
)
async def test_similar_upstream_errors_keep_their_response_and_session(error):
    async def execute(arguments):
        raise McpError(error)

    session = ToolSession(execute)
    manager = SessionManager(session)
    app = tool_app(manager, has_args=False)

    async with tool_client(app) as client:
        response = await client.post(f"/{TOOL_NAME}")

    assert response.status_code == 500
    assert response.json()["detail"] == error.model_dump(
        exclude={"code"}, exclude_none=True
    )
    assert manager.session is session


@pytest.mark.asyncio
async def test_initialization_timeout_returns_without_dispatch_or_immediate_retry():
    async def unexpected_call(arguments):
        pytest.fail("Tool was dispatched after initialization timed out")

    manager = SessionManager(
        ToolSession(unexpected_call), initialization_errors=[TimeoutError()]
    )
    app = tool_app(manager, has_args=False)

    async with tool_client(app) as client:
        response = await client.post(f"/{TOOL_NAME}")

    assert response.status_code == 500
    assert manager.initializations == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_count", [1, 2], ids=["recovers", "retry-also-fails"])
@pytest.mark.parametrize("wrapped", [False, True], ids=["direct", "task-group"])
async def test_initialization_retries_once_before_dispatch(failure_count, wrapped):
    records = []

    async def execute(arguments):
        records.append(arguments)
        return tool_result("ok")

    failures = [BrokenResourceError() for _ in range(failure_count)]
    if wrapped:
        failures = [
            ExceptionGroup("transport", [ExceptionGroup("session", [error])])
            for error in failures
        ]
    manager = SessionManager(ToolSession(execute), initialization_errors=failures)
    app = tool_app(manager, has_args=False)

    async with tool_client(app) as client:
        response = await client.post(f"/{TOOL_NAME}")

    assert manager.initializations == 2
    if failure_count == 2:
        assert response.status_code == 500
        assert records == []
    else:
        assert response.status_code == 200
        assert response.json() == "ok"
        assert records == [{}]


@pytest.mark.asyncio
@pytest.mark.parametrize("has_args", [True, False], ids=["with-args", "no-args"])
async def test_cancelled_invocation_propagates_without_replay(has_args):
    called = asyncio.Event()
    records = []

    async def execute(arguments):
        records.append(arguments)
        called.set()
        await asyncio.Future()

    session = ToolSession(execute)
    manager = SessionManager(session)
    app = tool_app(manager, has_args)
    arguments = {"value": SECRET} if has_args else {}

    async with tool_client(app) as client:
        pending = asyncio.create_task(client.post(f"/{TOOL_NAME}", json=arguments))
        try:
            await asyncio.wait_for(called.wait(), timeout=2)
        finally:
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending

    assert records == [arguments]
    assert manager.session is session
    assert manager.initializations == 1


@pytest.mark.asyncio
async def test_success_logs_only_endpoint_name(caplog):
    async def execute(arguments):
        assert arguments == {"value": SECRET}
        return tool_result("ok")

    app = tool_app(SessionManager(ToolSession(execute)), has_args=True)
    caplog.set_level(logging.INFO, logger="mcpo.utils.main")

    async with tool_client(app) as client:
        response = await client.post(f"/{TOOL_NAME}", json={"value": SECRET})

    assert response.status_code == 200
    assert response.json() == "ok"
    assert TOOL_NAME in caplog.text
    assert SECRET not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("has_args", [True, False], ids=["with-args", "no-args"])
@pytest.mark.parametrize(
    "failure", ["tool-result", "mcp-error", "unexpected", "server-error"]
)
async def test_tool_error_response_and_log_redaction(has_args, failure, caplog):
    message = f"Tool rejected a synthetic secret: {SECRET}"
    details = {"value": SECRET}

    async def execute(arguments):
        if failure == "tool-result":
            return tool_result(message, is_error=True)
        if failure == "mcp-error":
            raise McpError(
                ErrorData(code=INVALID_PARAMS, message=message, data=details)
            )
        if failure == "server-error":
            # Some upstreams use -32000 for generic application errors.
            raise McpError(ErrorData(code=CONNECTION_CLOSED, message=message))
        raise ValueError(message)

    manager = SessionManager(ToolSession(execute))
    app = tool_app(manager, has_args)
    caplog.set_level(logging.INFO, logger="mcpo.utils.main")
    arguments = {"value": SECRET} if has_args else {}

    async with tool_client(app) as client:
        response = await client.post(f"/{TOOL_NAME}", json=arguments)

    assert TOOL_NAME in caplog.text
    assert SECRET not in caplog.text
    assert manager.session is not None
    if failure == "mcp-error":
        assert response.status_code == 422
        assert response.json()["detail"] == {"message": message, "data": details}
    elif failure == "unexpected":
        assert response.status_code == 500
        assert response.json()["detail"]["message"] == "Unexpected error"
    else:
        assert response.status_code == 500
        assert response.json()["detail"] == {"message": message}
