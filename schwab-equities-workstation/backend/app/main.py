import logging

from fastapi import FastAPI

from app.routes.auth import router as auth_router
from app.routes.broker import router as broker_router
from app.routes.market import router as market_router
from app.routes.stream import router as stream_router
from app.services.stream import streaming_service


def create_app() -> FastAPI:
    logging.getLogger("websockets.server").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)

    app = FastAPI(
        title="Schwab Equities Workstation API",
        version="0.1.0",
        description="Equities-first analysis backend with Schwab-ready adapters."
    )
    app.include_router(auth_router, prefix="/api")
    app.include_router(broker_router, prefix="/api")
    app.include_router(market_router, prefix="/api")
    app.include_router(stream_router, prefix="/api")

    @app.on_event("shutdown")
    async def shutdown_streaming() -> None:
        await streaming_service.shutdown()

    return app


app = create_app()
