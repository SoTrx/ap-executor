"""Transparent reverse proxy: forwards every request verbatim to the real
operator backend, streaming both the request and response bodies so large
payloads are never buffered in memory.
"""
import logging
from typing import Iterable, Tuple

import httpx
from starlette.requests import Request
from starlette.responses import StreamingResponse

logger = logging.getLogger(__name__)

# RFC 7230 §6.1 hop-by-hop headers, plus Host/Content-Length which httpx
# recomputes itself for the outgoing request/response.
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def _filter_headers(items: Iterable[Tuple[str, str]]) -> list[tuple[str, str]]:
    return [(k, v) for k, v in items if k.lower() not in _HOP_BY_HOP]


async def proxy_request(request: Request, http: httpx.AsyncClient, upstream_base_url: str) -> StreamingResponse:
    """Forward `request` to `upstream_base_url` unchanged and stream the response back."""
    url = f"{upstream_base_url}{request.url.path}"
    upstream_req = http.build_request(
        method=request.method,
        url=url,
        params=request.query_params,
        headers=_filter_headers(request.headers.items()),
        content=request.stream(),
    )
    upstream_resp = await http.send(upstream_req, stream=True)

    async def _relay():
        try:
            async for chunk in upstream_resp.aiter_raw():
                yield chunk
        finally:
            await upstream_resp.aclose()

    return StreamingResponse(
        _relay(),
        status_code=upstream_resp.status_code,
        headers=dict(_filter_headers(upstream_resp.headers.items())),
    )
