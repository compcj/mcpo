import asyncio
import json
import logging

import httpx
import pytest
from fastapi import FastAPI
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamablehttp_client
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

import mcpo.main as main


async def assert_recovery(
    server_type,
    url,
    records,
    sdk_clients,
    caplog,
    *,
    request_timeout=0.5,
    release_delete=None,
):
    manager = main.MCPConnectionManager(
        server_type=server_type,
        command=None,
        args=[url],
        env={},
        headers=None,
        connection_timeout=None,
        request_timeout=request_timeout,
    )
    bridge = FastAPI()
    bridge.state.session_manager = manager

    @bridge.get("/health")
    def health():
        return {"status": "ok"}

    caplog.set_level(logging.INFO)
    background_tasks = asyncio.all_tasks()
    try:
        async with asyncio.timeout(5):
            await main.create_dynamic_endpoints(bridge)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=bridge), base_url="http://bridge.test"
            ) as client:
                response = await client.post("/write_record", json={"value": "first"})

                assert records == ["first"]
                assert response.status_code == 502
                assert response.json()["detail"]["code"] == "tool_execution_unknown"
                assert manager.current_session is None
                assert len(sdk_clients) == 1
                assert sdk_clients[0].is_closed
                assert (await client.get("/health")).json() == {"status": "ok"}

                response = await client.post("/write_record", json={"value": "second"})

                assert response.status_code == 200
                assert response.json() == "recorded"
                assert records == ["first", "second"]
                assert len(sdk_clients) == 2
            await manager.close()
    finally:
        if release_delete is not None:
            release_delete.set()
        await manager.close()

    assert all(client.is_closed for client in sdk_clients)
    assert all(
        task.get_name() != "mcpo-session"
        for task in asyncio.all_tasks() - background_tasks
    )
    assert "cancel scope" not in caplog.text.lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "upstream_status, slow_delete",
    [
        pytest.param(502, False, id="bad-gateway"),
        pytest.param(404, False, id="expired-session"),
        pytest.param(200, False, id="malformed-json"),
        pytest.param(404, True, id="expired-session-slow-delete"),
    ],
)
async def test_streamable_http_recovers_without_replaying_a_completed_tool(
    monkeypatch, caplog, upstream_status, slow_delete
):
    records = []
    session_ids = []
    deleted_session_ids = []
    release_delete = asyncio.Event() if slow_delete else None
    server = FastMCP(
        "Recovery test", json_response=True, stateless_http=upstream_status != 404
    )

    @server.tool()
    def write_record(value: str) -> str:
        records.append(value)
        return "recorded"

    upstream = server.streamable_http_app()

    async def drop_first_tool_response(request, call_next):
        if request.method == "DELETE" and release_delete is not None:
            deleted_session_ids.append(request.headers.get("mcp-session-id"))
            await release_delete.wait()
        if request.method == "GET":
            # This fixture exchanges messages over JSON POSTs. ASGITransport
            # buffers responses, so leave the optional SSE GET stream disabled.
            return Response(status_code=405)
        method = (
            (await request.json()).get("method") if request.method == "POST" else None
        )
        response = await call_next(request)
        if method == "tools/call":
            session_ids.append(request.headers.get("mcp-session-id"))
            if len(session_ids) == 1:
                # Drain the completed result before simulating response loss.
                async for _ in response.body_iterator:
                    pass
                if upstream_status == 200:
                    return Response(b"{", media_type="application/json")
                return Response(status_code=upstream_status)
        return response

    upstream.add_middleware(BaseHTTPMiddleware, dispatch=drop_first_tool_response)
    sdk_clients = []

    def local_streamable_http(*args, **kwargs):
        factory = kwargs.pop("httpx_client_factory", httpx.AsyncClient)

        def client_factory(**options):
            client = factory(transport=httpx.ASGITransport(app=upstream), **options)
            sdk_clients.append(client)
            return client

        return streamablehttp_client(
            *args, httpx_client_factory=client_factory, **kwargs
        )

    monkeypatch.setattr(main, "streamablehttp_client", local_streamable_http)
    async with upstream.router.lifespan_context(upstream):
        await assert_recovery(
            "streamable-http",
            "http://127.0.0.1:8000/mcp",
            records,
            sdk_clients,
            caplog,
            # Force the HTTP error cases to recover before the request deadline.
            request_timeout=0.5 if upstream_status == 200 or slow_delete else 10,
            release_delete=release_delete,
        )

    if upstream_status == 404:
        assert all(session_ids)
        assert session_ids[0] != session_ids[1]
    if slow_delete:
        assert deleted_session_ids == session_ids


class LegacySSEStream(httpx.AsyncByteStream):
    def __init__(self):
        self.messages = asyncio.Queue()
        self.closed = False

    async def __aiter__(self):
        yield b"event: endpoint\ndata: /messages\n\n"
        while True:
            yield await self.messages.get()

    async def aclose(self):
        self.closed = True

    def send_result(self, request_id, result):
        payload = json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result})
        self.messages.put_nowait(f"event: message\ndata: {payload}\n\n".encode())


@pytest.mark.asyncio
async def test_legacy_sse_failed_tool_post_recovers_while_get_stream_stays_open(
    monkeypatch, caplog
):
    records = []
    sdk_clients = []
    streams = []

    def client_factory(**kwargs):
        stream = LegacySSEStream()
        streams.append(stream)

        async def handle_request(request):
            if request.method == "GET":
                return httpx.Response(
                    200, headers={"content-type": "text/event-stream"}, stream=stream
                )

            assert request.method == "POST" and request.url.path == "/messages"
            message = json.loads(request.content)
            method = message["method"]
            if method == "notifications/initialized":
                return httpx.Response(202)
            if method == "initialize":
                result = {
                    "protocolVersion": message["params"]["protocolVersion"],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "SSE test", "version": "1"},
                }
            elif method == "tools/list":
                result = {
                    "tools": [
                        {
                            "name": "write_record",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"value": {"type": "string"}},
                                "required": ["value"],
                            },
                        }
                    ]
                }
            else:
                assert method == "tools/call"
                records.append(message["params"]["arguments"]["value"])
                if len(records) == 1:
                    # Fail the write POST after execution without ending the GET.
                    assert not stream.closed
                    return httpx.Response(502)
                result = {"content": [{"type": "text", "text": "recorded"}]}

            stream.send_result(message["id"], result)
            return httpx.Response(202)

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle_request), **kwargs
        )
        sdk_clients.append(client)
        return client

    def local_sse(*args, **kwargs):
        return sse_client(*args, httpx_client_factory=client_factory, **kwargs)

    monkeypatch.setattr(main, "sse_client", local_sse)
    await assert_recovery("sse", "http://sse.test/sse", records, sdk_clients, caplog)

    assert all(stream.closed for stream in streams)
