import logging
import json
from typing import Any, Dict, ForwardRef, List, Literal, Optional, Type, Union

from anyio import BrokenResourceError, ClosedResourceError, EndOfStream
from fastapi import HTTPException, Request
from httpx import codes

from mcp import types
from mcp.types import (
    CallToolResult,
    PARSE_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    INVALID_PARAMS,
    INTERNAL_ERROR,
)

from mcp.shared.exceptions import McpError

from pydantic import Field, create_model
from pydantic.fields import FieldInfo

from mcpo.utils.headers import process_headers_for_server

MCP_ERROR_TO_HTTP_STATUS = {
    PARSE_ERROR: 400,
    INVALID_REQUEST: 400,
    METHOD_NOT_FOUND: 404,
    INVALID_PARAMS: 422,
    INTERNAL_ERROR: 500,
}

logger = logging.getLogger(__name__)


def normalize_server_type(server_type: str) -> str:
    """Normalize server_type to a standard value."""
    if server_type in ["streamable_http", "streamablehttp", "streamable-http"]:
        return "streamable-http"
    return server_type


def process_tool_response(result: CallToolResult) -> list:
    """Universal response processor for all tool endpoints"""
    response = []
    for content in result.content:
        if isinstance(content, types.TextContent):
            text = content.text
            if isinstance(text, str):
                try:
                    text = json.loads(text)
                except json.JSONDecodeError:
                    pass
            response.append(text)
        elif isinstance(content, types.ImageContent):
            image_data = f"data:{content.mimeType};base64,{content.data}"
            response.append(image_data)
        elif isinstance(content, types.EmbeddedResource):
            # TODO: Handle embedded resources
            response.append("Embedded resource not supported yet.")
    return response


def name_needs_alias(name: str) -> bool:
    """Check if a field name needs aliasing (if it starts with '_')."""
    return name.startswith("_")


def generate_alias_name(original_name: str, existing_names: set) -> str:
    """
    Generate an alias field name by stripping unwanted chars, and avoiding conflicts with existing names.

    Args:
        original_name: The original field name (should start with '_')
        existing_names: Set of existing names to avoid conflicts with

    Returns:
        An alias name that doesn't conflict with existing names
    """
    alias_name = original_name.lstrip("_")
    # Handle potential naming conflicts
    original_alias_name = alias_name
    suffix_counter = 1
    while alias_name in existing_names:
        alias_name = f"{original_alias_name}_{suffix_counter}"
        suffix_counter += 1
    return alias_name


def _literal_type_from_enum(enum_values: Any) -> Optional[Any]:
    if not isinstance(enum_values, list) or not enum_values:
        return None

    try:
        return Literal[tuple(enum_values)]
    except TypeError:
        return None


def _field_for_schema(
    default_value: Any, description: str, prop_schema: Dict[str, Any]
) -> FieldInfo:
    field_args = {"default": default_value, "description": description}
    if "enum" in prop_schema:
        field_args["json_schema_extra"] = {"enum": prop_schema["enum"]}
    return Field(**field_args)


def collect_schema_defs(schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Gather named schemas from JSON Schema and OpenAPI locations."""
    if not schema:
        return {}
    defs: Dict[str, Any] = {}
    for key in ("$defs", "definitions"):
        extra = schema.get(key)
        if isinstance(extra, dict):
            defs.update(extra)
    components = schema.get("components")
    if isinstance(components, dict):
        schemas = components.get("schemas")
        if isinstance(schemas, dict):
            defs.update(schemas)
    return defs


def _process_schema_property(
    _model_cache: Dict[str, Type],
    prop_schema: Dict[str, Any],
    model_name_prefix: str,
    prop_name: str,
    is_required: bool,
    schema_defs: Optional[Dict] = None,
) -> tuple[Union[Type, List, ForwardRef, Any], FieldInfo]:
    """
    Recursively processes a schema property to determine its Python type hint
    and Pydantic Field definition.

    Returns:
        A tuple containing (python_type_hint, pydantic_field).
        The pydantic_field contains default value and description.
    """
    schema_defs = schema_defs or {}
    if "$ref" in prop_schema:
        ref = prop_schema["$ref"]
        if isinstance(ref, str) and ref.startswith("#/properties/"):
            # Remove common prefix in pathes.
            prefix_path = model_name_prefix.split("_form_model_")[-1]
            ref_path = ref.split("#/properties/")[-1]
            # Translate $ref path to model_name_prefix style.
            ref_path = ref_path.replace("/properties/", "_model_")
            ref_path = ref_path.replace("/items", "_item")
            # If $ref path is a prefix substring of model_name_prefix path,
            # there exists a circular reference.
            # The loop should be broke with a return to avoid exception.
            if prefix_path.startswith(ref_path):
                # TODO: Find the exact type hint for the $ref.
                return Any, Field(default=None, description="")
        ref_name = ref.split("/")[-1] if isinstance(ref, str) else None
        resolved = schema_defs.get(ref_name) if ref_name else None
        if not isinstance(resolved, dict):
            logger.warning(
                "Unresolved $ref '%s' for %s.%s; treating as a free-form object",
                ref,
                model_name_prefix,
                prop_name,
            )
            desc = prop_schema.get("description", "")
            default_value = ... if is_required else None
            return Dict[str, Any], Field(default=default_value, description=desc)
        # Overlay sibling keywords (e.g. description) onto the resolved schema
        prop_schema = {
            **resolved,
            **{k: v for k, v in prop_schema.items() if k != "$ref"},
        }

    if "allOf" in prop_schema:
        merged: Dict[str, Any] = {}
        for option in prop_schema.get("allOf") or []:
            if isinstance(option, dict):
                merged.update(option)
        if "description" in prop_schema:
            merged.setdefault("description", prop_schema["description"])
        return _process_schema_property(
            _model_cache,
            merged,
            model_name_prefix,
            prop_name,
            is_required,
            schema_defs=schema_defs,
        )

    prop_type = prop_schema.get("type")
    prop_desc = prop_schema.get("description", "")

    default_value = ... if is_required else prop_schema.get("default", None)
    pydantic_field = _field_for_schema(default_value, prop_desc, prop_schema)

    # Handle the case where prop_type is missing but 'anyOf' key exists
    # In this case, use data type from 'anyOf' to determine the type hint
    if "anyOf" in prop_schema:
        type_hints = []
        for i, schema_option in enumerate(prop_schema["anyOf"]):
            type_hint, _ = _process_schema_property(
                _model_cache,
                schema_option,
                f"{model_name_prefix}_{prop_name}",
                f"choice_{i}",
                False,
                schema_defs=schema_defs,
            )
            type_hints.append(type_hint)
        return Union[tuple(type_hints)], pydantic_field

    # Handle the case where prop_type is a list of types, e.g. ['string', 'number']
    if isinstance(prop_type, list):
        # Create a Union of all the types
        type_hints = []
        for type_option in prop_type:
            # Create a temporary schema with the single type and process it
            temp_schema = dict(prop_schema)
            temp_schema["type"] = type_option
            type_hint, _ = _process_schema_property(
                _model_cache,
                temp_schema,
                model_name_prefix,
                prop_name,
                False,
                schema_defs=schema_defs,
            )
            type_hints.append(type_hint)

        # Return a Union of all possible types
        return Union[tuple(type_hints)], pydantic_field

    enum_type_hint = _literal_type_from_enum(prop_schema.get("enum"))
    if enum_type_hint is not None:
        return enum_type_hint, pydantic_field

    if prop_type == "object":
        nested_properties = prop_schema.get("properties", {})
        nested_required = prop_schema.get("required", [])
        nested_fields = {}

        nested_model_name = f"{model_name_prefix}_{prop_name}_model".replace(
            "__", "_"
        ).rstrip("_")

        if nested_model_name in _model_cache:
            return _model_cache[nested_model_name], pydantic_field

        for name, schema in nested_properties.items():
            is_nested_required = name in nested_required
            nested_type_hint, nested_pydantic_field = _process_schema_property(
                _model_cache,
                schema,
                nested_model_name,
                name,
                is_nested_required,
                schema_defs,
            )

            if name_needs_alias(name):
                other_names = set().union(
                    nested_properties, nested_fields, _model_cache
                )
                alias_name = generate_alias_name(name, other_names)
                aliased_field = Field(
                    default=nested_pydantic_field.default,
                    description=nested_pydantic_field.description,
                    alias=name,
                    json_schema_extra=nested_pydantic_field.json_schema_extra,
                )
                nested_fields[alias_name] = (nested_type_hint, aliased_field)
            else:
                nested_fields[name] = (nested_type_hint, nested_pydantic_field)

        if not nested_fields:
            return Dict[str, Any], pydantic_field

        NestedModel = create_model(nested_model_name, **nested_fields)
        _model_cache[nested_model_name] = NestedModel

        return NestedModel, pydantic_field

    elif prop_type == "array":
        items_schema = prop_schema.get("items")
        if not items_schema:
            # Default to list of anything if items schema is missing
            return List[Any], pydantic_field

        # Recursively determine the type of items in the array
        item_type_hint, _ = _process_schema_property(
            _model_cache,
            items_schema,
            f"{model_name_prefix}_{prop_name}",
            "item",
            False,  # Items aren't required at this level,
            schema_defs,
        )
        list_type_hint = List[item_type_hint]
        return list_type_hint, pydantic_field

    elif prop_type == "string":
        return str, pydantic_field
    elif prop_type == "integer":
        return int, pydantic_field
    elif prop_type == "boolean":
        return bool, pydantic_field
    elif prop_type == "number":
        return float, pydantic_field
    elif prop_type == "null":
        return None, pydantic_field
    else:
        return Any, pydantic_field


def get_model_fields(form_model_name, properties, required_fields, schema_defs=None):
    model_fields = {}

    _model_cache: Dict[str, Type] = {}

    for param_name, param_schema in properties.items():
        is_required = param_name in required_fields
        python_type_hint, pydantic_field_info = _process_schema_property(
            _model_cache,
            param_schema,
            form_model_name,
            param_name,
            is_required,
            schema_defs,
        )

        # Handle parameter names with leading underscores (e.g., __top, __filter) which Pydantic v2 does not allow
        if name_needs_alias(param_name):
            other_names = set().union(properties, model_fields, _model_cache)
            alias_name = generate_alias_name(param_name, other_names)
            aliased_field = Field(
                default=pydantic_field_info.default,
                description=pydantic_field_info.description,
                alias=param_name,
                json_schema_extra=pydantic_field_info.json_schema_extra,
            )
            # Use the generated type hint and Field info
            model_fields[alias_name] = (python_type_hint, aliased_field)
        else:
            model_fields[param_name] = (python_type_hint, pydantic_field_info)

    return model_fields


def _is_connection_closed(error: Exception) -> bool:
    if isinstance(error, (BrokenResourceError, ClosedResourceError, EndOfStream)):
        return True
    if isinstance(error, ExceptionGroup):
        return all(_is_connection_closed(cause) for cause in error.exceptions)

    # Match the SDK's receive-loop and Streamable HTTP 404 errors. Upstream
    # JSON-RPC errors can carry the same codes with their own messages or data.
    return (
        isinstance(error, McpError)
        and error.error.data is None
        and (error.error.code, error.error.message)
        in (
            (types.CONNECTION_CLOSED, "Connection closed"),
            (32600, "Session terminated"),
        )
    )


def _is_request_timeout(error: Exception) -> bool:
    if isinstance(error, TimeoutError):
        return True
    # The SDK raises this McpError while handling its local read TimeoutError.
    # A JSON-RPC error sent by the server has no such exception context.
    return (
        isinstance(error, McpError)
        and isinstance(error.__context__, TimeoutError)
        and error.error.code == codes.REQUEST_TIMEOUT
        and error.error.data is None
        and error.error.message.startswith(
            "Timed out while waiting for response to ClientRequest. Waited "
        )
        and error.error.message.endswith(" seconds.")
    )


def get_tool_handler(
    endpoint_name,
    form_model_fields,
    response_model_fields=None,
    client_header_forwarding_config=None,
):
    async def call_tool_once(
        request: Request, arguments: Dict[str, Any]
    ) -> CallToolResult:
        session_manager = getattr(request.app.state, "session_manager", None)

        if session_manager is not None:
            try:
                session, _ = await session_manager.ensure_initialized()
            except Exception as error:
                if not _is_connection_closed(error):
                    raise
                logger.warning(
                    "Session closed while initializing '%s' (%s); attempting reconnect",
                    endpoint_name,
                    type(error).__name__,
                )
                session, _ = await session_manager.ensure_initialized()
        else:
            session = getattr(request.app.state, "session", None)

        if session is None:
            raise HTTPException(
                status_code=500,
                detail={"message": "MCP session is not available"},
            )

        try:
            if session_manager is not None:
                return await session_manager.run_with_session(
                    session, session.call_tool, endpoint_name, arguments=arguments
                )
            return await session.call_tool(endpoint_name, arguments=arguments)
        except Exception as error:
            if not (_is_connection_closed(error) or _is_request_timeout(error)):
                raise
            logger.warning(
                "Session interrupted during call to '%s' (%s)",
                endpoint_name,
                type(error).__name__,
            )
            if session_manager is not None:
                await session_manager.invalidate(session)
            raise HTTPException(
                status_code=502,
                detail={
                    "code": "tool_execution_unknown",
                    "message": (
                        "Tool execution may have completed. Check upstream before retrying."
                    ),
                },
            ) from None

    async def execute_tool(request: Request, arguments: Dict[str, Any]) -> Any:
        if client_header_forwarding_config and client_header_forwarding_config.get(
            "enabled", False
        ):
            process_headers_for_server(request, client_header_forwarding_config)

        logger.info("Calling endpoint: %s", endpoint_name)
        try:
            result = await call_tool_once(request, arguments)

            if result.isError:
                error_message = "Unknown tool execution error"
                if result.content and isinstance(result.content[0], types.TextContent):
                    error_message = result.content[0].text
                detail = {"message": error_message}
                raise HTTPException(status_code=500, detail=detail)

            response_data = process_tool_response(result)
            final_response = (
                response_data[0] if len(response_data) == 1 else response_data
            )
            return final_response

        except HTTPException:
            raise
        except McpError as e:
            logger.info("MCP error calling %s (%s)", endpoint_name, type(e).__name__)
            status_code = MCP_ERROR_TO_HTTP_STATUS.get(e.error.code, 500)
            raise HTTPException(
                status_code=status_code,
                detail=(
                    {"message": e.error.message, "data": e.error.data}
                    if e.error.data is not None
                    else {"message": e.error.message}
                ),
            )
        except Exception as e:
            logger.info(
                "Unexpected error calling %s (%s)", endpoint_name, type(e).__name__
            )
            raise HTTPException(
                status_code=500,
                detail={"message": "Unexpected error", "error": str(e)},
            )

    if form_model_fields:
        FormModel = create_model(f"{endpoint_name}_form_model", **form_model_fields)
        ResponseModel = (
            create_model(f"{endpoint_name}_response_model", **response_model_fields)
            if response_model_fields
            else Any
        )

        async def tool(
            form_data: FormModel, request: Request
        ) -> Union[ResponseModel, Any]:
            arguments = form_data.model_dump(exclude_none=True, by_alias=True)
            return await execute_tool(request, arguments)

        return tool

    async def tool(request: Request):
        return await execute_tool(request, {})

    return tool
