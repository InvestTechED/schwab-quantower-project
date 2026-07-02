from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


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
    order_strategy_type: str | None = None
    status: str | None = None
    original_status: str | None = None
    duration: str | None = None
    session: str | None = None
    entered_time: datetime | None = None
    close_time: datetime | None = None
    expiration_time: datetime | None = None
    quantity: float | None = None
    filled_quantity: float | None = None
    remaining_quantity: float | None = None
    average_fill_price: float | None = None
    price: float | None = None
    stop_price: float | None = None
    trail_offset: float | None = None
    trigger_price: float | None = None
    position_id: str | None = None
    group_id: str | None = None


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


class BrokerTrade(BaseModel):
    account_hash: str
    trade_id: str
    order_id: str | None = None
    symbol: str
    instruction: str | None = None
    executed_time: datetime | None = None
    quantity: float
    price: float
    gross_amount: float | None = None
    fees: float | None = None
    net_amount: float | None = None
    position_id: str | None = None


class EquityOrderRequest(BaseModel):
    account_hash: str
    symbol: str
    quantity: float = Field(gt=0)
    instruction: Literal["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
    order_type: Literal["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
    limit_price: float | None = Field(default=None, gt=0)
    stop_price: float | None = Field(default=None, gt=0)
    time_in_force: Literal["DAY", "GTC"] | None = None
    stop_loss_price: float | None = Field(default=None, gt=0)
    take_profit_price: float | None = Field(default=None, gt=0)
    trailing_stop_offset: float | None = Field(default=None, gt=0)


class ModifyEquityOrderRequest(BaseModel):
    account_hash: str
    order_id: str
    symbol: str
    quantity: float = Field(gt=0)
    instruction: Literal["BUY", "SELL", "SELL_SHORT", "BUY_TO_COVER"]
    order_type: Literal["MARKET", "LIMIT", "STOP", "STOP_LIMIT"]
    limit_price: float | None = Field(default=None, gt=0)
    stop_price: float | None = Field(default=None, gt=0)
    time_in_force: Literal["DAY", "GTC"] | None = None
