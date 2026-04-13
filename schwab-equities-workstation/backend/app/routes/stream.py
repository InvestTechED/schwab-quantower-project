from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.models import StreamStatus
from app.services.stream import streaming_service

router = APIRouter(tags=["stream"])


@router.get("/stream/status", response_model=StreamStatus)
async def stream_status() -> StreamStatus:
    return await streaming_service.get_status()


@router.websocket("/stream/equities/{symbol}")
async def equities_stream(websocket: WebSocket, symbol: str) -> None:
    await websocket.accept()
    queue = await streaming_service.register(symbol)

    try:
        while True:
            event = await queue.get()
            await websocket.send_json(event)
    except WebSocketDisconnect:
        pass
    finally:
        await streaming_service.unregister(symbol, queue)
