import logging
from datetime import date, datetime

from fastapi import APIRouter

from app.services.analysis import PriceActionAnalyzer
from app.services.schwab import SchwabMarketDataService

router = APIRouter(tags=["market"])
logger = logging.getLogger(__name__)

market_data_service = SchwabMarketDataService()
analyzer = PriceActionAnalyzer()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "mode": "scaffold"}


@router.get("/market/snapshot/{symbol}")
def market_snapshot(symbol: str):
    return market_data_service.get_snapshot(symbol)


@router.get("/market/symbol/{symbol}")
def market_symbol(symbol: str):
    return market_data_service.get_symbol_profile(symbol)


@router.get("/market/search")
def market_search(q: str, limit: int = 10):
    return market_data_service.search_symbols(q, limit=limit)


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
    return market_data_service.get_option_series(symbol)


@router.get("/market/options/{symbol}/chain")
def market_option_chain(symbol: str, expiration: date | None = None):
    return market_data_service.get_option_chain(symbol, expiration=expiration)


@router.get("/analysis/price-action/{symbol}")
def price_action_report(symbol: str):
    snapshot = market_data_service.get_snapshot(symbol)
    return analyzer.build_report(snapshot)
