from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class MarketSnapshot(BaseModel):
    symbol: str
    as_of: datetime
    last: float
    bid: float
    ask: float
    bid_size: int
    ask_size: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    asset_type: str | None = None
    description: str | None = None
    exchange: str | None = None
    relative_volume: float = Field(ge=0)
    trend_state: Literal["bullish", "bearish", "neutral"]
    vwap_bias: Literal["above", "below", "flat"]


class Signal(BaseModel):
    name: str
    score: float = Field(ge=0, le=100)
    summary: str


class Bar(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class SymbolProfile(BaseModel):
    symbol: str
    normalized_symbol: str
    asset_type: str | None = None
    instrument_type: str | None = None
    description: str | None = None
    exchange: str | None = None
    options_available: bool = False


class OptionSeries(BaseModel):
    id: str
    underlier_symbol: str
    expiration_date: datetime
    days_to_expiration: int | None = None
    series_type: Literal["daily", "week", "month", "unknown"] = "unknown"
    name: str
    exchange: str | None = None


class OptionContract(BaseModel):
    symbol: str
    underlier_symbol: str
    description: str
    exchange: str | None = None
    option_type: Literal["CALL", "PUT"]
    strike_price: float
    expiration_date: datetime
    days_to_expiration: int | None = None
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    mark: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    last_size: int | None = None
    open_interest: int | None = None
    volume: int | None = None
    volatility: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None


class OptionChain(BaseModel):
    underlier_symbol: str
    underlier_last: float | None = None
    underlier_bid: float | None = None
    underlier_ask: float | None = None
    series: list[OptionSeries] = Field(default_factory=list)
    contracts: list[OptionContract] = Field(default_factory=list)


class StreamStatus(BaseModel):
    started: bool
    stream_task_running: bool
    has_client: bool
    quote_initialized: bool
    chart_initialized: bool
    nasdaq_book_initialized: bool
    nyse_book_initialized: bool
    registered_display_count: int
    active_display_symbol_count: int
    active_actual_symbol_count: int
    subscribed_symbol_count: int
    published_event_count: int
    dropped_event_count: int
    restart_count: int
    start_count: int
    shutdown_count: int
    queue_max_size: int
    last_event_at: datetime | None = None
    last_started_at: datetime | None = None
    last_restarted_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error_message: str | None = None
    subscribed_symbols: list[str] = Field(default_factory=list)


class PriceActionReport(BaseModel):
    symbol: str
    snapshot: MarketSnapshot
    signals: list[Signal]
    conclusion: str


class BrokerAccount(BaseModel):
    account_number: str
    account_hash: str
    account_type: str | None = None
    liquidation_value: float | None = None
    cash_balance: float | None = None
    buying_power: float | None = None
    cash_available_for_trading: float | None = None
    cash_available_for_withdrawal: float | None = None
    total_cash: float | None = None
    unsettled_cash: float | None = None
    long_market_value: float | None = None


class BrokerPosition(BaseModel):
    account_hash: str
    symbol: str
    quantity: float
    average_price: float | None = None
    market_value: float | None = None
    market_price: float | None = None
    asset_type: str | None = None
    instrument_type: str | None = None
    description: str | None = None
    day_profit_loss: float | None = None
    day_profit_loss_percent: float | None = None
    unrealized_profit_loss: float | None = None


class BrokerOrder(BaseModel):
    account_hash: str
    order_id: int | str
    symbol: str | None = None
    instruction: str | None = None
    order_type: str | None = None
    status: str | None = None
    duration: str | None = None
    session: str | None = None
    entered_time: datetime | None = None
    close_time: datetime | None = None
    quantity: float | None = None
    filled_quantity: float | None = None
    remaining_quantity: float | None = None
    average_fill_price: float | None = None
    price: float | None = None


class BrokerExecution(BaseModel):
    account_hash: str
    execution_id: str
    order_id: int | str
    symbol: str | None = None
    instruction: str | None = None
    execution_type: str | None = None
    position_effect: str | None = None
    executed_time: datetime | None = None
    quantity: float | None = None
    price: float | None = None
    gross_amount: float | None = None
    fees: float | None = None


class EquityOrderRequest(BaseModel):
    account_hash: str
    symbol: str
    quantity: float = Field(gt=0)
    instruction: Literal["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
    order_type: Literal["MARKET", "LIMIT"]
    limit_price: float | None = Field(default=None, gt=0)
    time_in_force: Literal["DAY", "GTC"] | None = None


class ModifyEquityOrderRequest(BaseModel):
    account_hash: str
    order_id: str
    symbol: str
    quantity: float = Field(gt=0)
    instruction: Literal["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
    order_type: Literal["LIMIT"]
    limit_price: float = Field(gt=0)
    time_in_force: Literal["DAY", "GTC"] | None = None
