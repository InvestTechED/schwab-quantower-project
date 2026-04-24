import logging
from datetime import date, datetime

from fastapi import APIRouter, HTTPException

from app.services.analysis import PriceActionAnalyzer
from app.services.schwab import SchwabMarketDataService

router = APIRouter(tags=["market"])
logger = logging.getLogger(__name__)

market_data_service = SchwabMarketDataService()
analyzer = PriceActionAnalyzer()


def _raise_market_error(exc: Exception) -> None:
    response = getattr(exc, "response", None)
    detail = response.text if response is not None else str(exc)
    status_code = response.status_code if response is not None else 502
    raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "mode": "scaffold"}


@router.get("/market/snapshot/{symbol}")
def market_snapshot(symbol: str):
    try:
        return market_data_service.get_snapshot(symbol)
    except Exception as exc:
        _raise_market_error(exc)


@router.get("/market/symbol/{symbol}")
def market_symbol(symbol: str):
    try:
        return market_data_service.get_symbol_profile(symbol)
    except Exception as exc:
        _raise_market_error(exc)


@router.get("/market/search")
def market_search(q: str, limit: int = 50):
    try:
        return market_data_service.search_symbols(q, limit=limit)
    except Exception as exc:
        _raise_market_error(exc)


@router.get("/market/bars/{symbol}")
def market_bars(
    symbol: str,
    limit: int = 500,
    timeframe: str = "5m",
    start: datetime | None = None,
    end: datetime | None = None,
):
    try:
        return market_data_service.get_bars(symbol, limit=limit, timeframe=timeframe, start=start, end=end)
    except Exception:
        logger.exception(
            "market_bars failed symbol=%s timeframe=%s limit=%s start=%s end=%s",
            symbol,
            timeframe,
            limit,
            start,
            end,
        )
        return []


@router.get("/market/options/{symbol}/series")
def market_option_series(symbol: str):
    try:
        return market_data_service.get_option_series(symbol)
    except Exception as exc:
        _raise_market_error(exc)


@router.get("/market/options/{symbol}/chain")
def market_option_chain(symbol: str, expiration: date | None = None):
    try:
        return market_data_service.get_option_chain(symbol, expiration=expiration)
    except Exception as exc:
        _raise_market_error(exc)


@router.get("/analysis/price-action/{symbol}")
def price_action_report(symbol: str):
    try:
        snapshot = market_data_service.get_snapshot(symbol)
        return analyzer.build_report(snapshot)
    except Exception as exc:
        _raise_market_error(exc)
