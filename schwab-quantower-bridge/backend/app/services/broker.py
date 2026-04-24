from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

from schwab.orders.common import Duration, Session
from schwab.orders.common import (
    EquityInstruction,
    OrderStrategyType,
    OrderType,
    StopPriceLinkBasis,
    StopPriceLinkType,
    StopType,
    first_triggers_second,
    one_cancels_other,
)
from schwab.orders.equities import (
    equity_buy_limit,
    equity_buy_market,
    equity_buy_to_cover_limit,
    equity_buy_to_cover_market,
    equity_sell_limit,
    equity_sell_market,
    equity_sell_short_limit,
    equity_sell_short_market,
)
from schwab.orders.generic import OrderBuilder
from schwab.utils import Utils

from app.models import BrokerAccount, BrokerExecution, BrokerOrder, BrokerPosition, BrokerTrade, EquityOrderRequest, ModifyEquityOrderRequest
from app.config import settings
from app.services.auth import SchwabAuthService


class SchwabBrokerService:
    def __init__(self) -> None:
        self.auth_service = SchwabAuthService()
        self._recent_order_fingerprints: dict[tuple[str, str, str, float, str, float | None], datetime] = {}
        self.audit_path = Path(__file__).resolve().parents[2] / "logs" / "schwab_trading_audit.jsonl"

    def _account_mappings(self, client) -> tuple[dict[str, str], dict[str, str]]:
        account_numbers_response = client.get_account_numbers()
        account_numbers_response.raise_for_status()
        number_payload = account_numbers_response.json()
        hash_to_number = {
            str(item.get("hashValue")): str(item.get("accountNumber"))
            for item in number_payload
            if item.get("hashValue") and item.get("accountNumber")
        }
        number_to_hash = {number: hash_value for hash_value, number in hash_to_number.items()}
        return hash_to_number, number_to_hash

    def _resolve_account_hash(self, client, account_hash_or_number: str) -> str:
        hash_to_number, number_to_hash = self._account_mappings(client)
        normalized = str(account_hash_or_number)
        if normalized in hash_to_number:
            return normalized
        if normalized in number_to_hash:
            return number_to_hash[normalized]
        raise ValueError(f"Unknown Schwab account reference: {account_hash_or_number}")

    def get_accounts(self) -> list[BrokerAccount]:
        client = self.auth_service.create_client()
        _, number_to_hash = self._account_mappings(client)

        accounts_response = client.get_accounts(
            fields=[client.Account.Fields.POSITIONS]
        )
        accounts_response.raise_for_status()
        payload = accounts_response.json()

        accounts: list[BrokerAccount] = []
        for account_entry in payload:
            securities_account = account_entry.get("securitiesAccount", {})
            account_hash = securities_account.get("accountNumber")
            initial_balances = securities_account.get("initialBalances", {})
            current_balances = securities_account.get("currentBalances", {})

            accounts.append(
                BrokerAccount(
                    account_number=str(account_hash),
                    account_hash=number_to_hash.get(str(account_hash), "unknown"),
                    account_type=securities_account.get("type"),
                    liquidation_value=_as_float(
                        current_balances.get("liquidationValue")
                        or initial_balances.get("liquidationValue")
                    ),
                    cash_balance=_as_float(
                        current_balances.get("cashBalance")
                        or initial_balances.get("cashBalance")
                    ),
                    buying_power=_as_float(
                        current_balances.get("buyingPower")
                        or initial_balances.get("buyingPower")
                    ),
                    cash_available_for_trading=_as_float(
                        current_balances.get("cashAvailableForTrading")
                        or current_balances.get("availableFunds")
                    ),
                    cash_available_for_withdrawal=_as_float(
                        current_balances.get("cashAvailableForWithdrawal")
                    ),
                    total_cash=_as_float(
                        current_balances.get("totalCash")
                        or current_balances.get("cashBalance")
                    ),
                    unsettled_cash=_as_float(current_balances.get("unsettledCash")),
                    long_market_value=_as_float(
                        current_balances.get("longMarketValue")
                        or current_balances.get("longNonMarginableMarketValue")
                    ),
                )
            )

        return accounts

    def get_positions(self) -> list[BrokerPosition]:
        client = self.auth_service.create_client()
        _, number_to_hash = self._account_mappings(client)
        response = client.get_accounts(fields=[client.Account.Fields.POSITIONS])
        response.raise_for_status()
        payload = response.json()

        positions: list[BrokerPosition] = []
        for account_entry in payload:
            securities_account = account_entry.get("securitiesAccount", {})
            account_hash = securities_account.get("accountNumber")
            for position in securities_account.get("positions", []):
                instrument = position.get("instrument", {})
                positions.append(
                    BrokerPosition(
                        account_hash=number_to_hash.get(str(account_hash), str(account_hash)),
                        symbol=instrument.get("symbol", "UNKNOWN"),
                        quantity=_as_float(position.get("longQuantity"))
                        - _as_float(position.get("shortQuantity")),
                        average_price=_as_float(position.get("averagePrice")),
                        market_value=_as_float(position.get("marketValue")),
                        market_price=_resolve_market_price(position),
                        asset_type=instrument.get("assetType"),
                        instrument_type=instrument.get("type"),
                        description=instrument.get("description"),
                        day_profit_loss=_as_float(position.get("currentDayProfitLoss")),
                        day_profit_loss_percent=_as_float(position.get("currentDayProfitLossPercentage")),
                        unrealized_profit_loss=_as_float(position.get("longOpenProfitLoss")),
                    )
                )

        return positions

    def get_orders(
        self,
        lookback_days: int = 7,
        from_entered_datetime: datetime | None = None,
        to_entered_datetime: datetime | None = None,
    ) -> list[BrokerOrder]:
        client = self.auth_service.create_client()
        _, number_to_hash = self._account_mappings(client)
        end_time = _normalize_datetime(to_entered_datetime) or datetime.now(timezone.utc)
        start_time = _normalize_datetime(from_entered_datetime) or (end_time - timedelta(days=lookback_days))
        response = client.get_orders_for_all_linked_accounts(
            from_entered_datetime=start_time,
            to_entered_datetime=end_time,
        )
        response.raise_for_status()
        payload = response.json()

        orders: list[BrokerOrder] = []
        for order in payload:
            account_hash = number_to_hash.get(
                str(order.get("accountNumber", "unknown")),
                str(order.get("accountNumber", "unknown")),
            )
            orders.extend(_flatten_orders(order, account_hash))

        return orders

    def get_executions(
        self,
        lookback_days: int = 7,
        from_entered_datetime: datetime | None = None,
        to_entered_datetime: datetime | None = None,
    ) -> list[BrokerExecution]:
        client = self.auth_service.create_client()
        _, number_to_hash = self._account_mappings(client)
        end_time = _normalize_datetime(to_entered_datetime) or datetime.now(timezone.utc)
        start_time = _normalize_datetime(from_entered_datetime) or (end_time - timedelta(days=lookback_days))
        response = client.get_orders_for_all_linked_accounts(
            from_entered_datetime=start_time,
            to_entered_datetime=end_time,
        )
        response.raise_for_status()
        payload = response.json()

        executions: list[BrokerExecution] = []
        for order in payload:
            account_hash = number_to_hash.get(
                str(order.get("accountNumber", "unknown")),
                str(order.get("accountNumber", "unknown")),
            )
            leg = (order.get("orderLegCollection") or [{}])[0]
            instrument = leg.get("instrument", {})
            base_symbol = instrument.get("symbol")
            instruction = leg.get("instruction")
            position_effect = leg.get("positionEffect")

            for activity in order.get("orderActivityCollection") or []:
                if str(activity.get("activityType") or "").upper() != "EXECUTION":
                    continue

                execution_type = str(activity.get("executionType") or "").upper() or None
                activity_id = str(activity.get("activityId") or order.get("orderId") or "execution")
                execution_legs = activity.get("executionLegs") or [activity]

                for index, execution_leg in enumerate(execution_legs, start=1):
                    quantity = _resolve_execution_quantity(execution_leg, activity, order)
                    price = _resolve_execution_price(execution_leg, activity, order)
                    executed_time = _parse_datetime(execution_leg.get("time")) or _parse_datetime(order.get("closeTime")) or _parse_datetime(order.get("enteredTime"))
                    gross_amount = quantity * price if quantity is not None and price is not None and quantity > 0 and price > 0 else None

                    executions.append(
                        BrokerExecution(
                            account_hash=account_hash,
                            execution_id=f"{order.get('orderId', 'unknown')}:{activity_id}:{index}",
                            order_id=str(order.get("orderId", "unknown")),
                            symbol=base_symbol,
                            instruction=instruction,
                            execution_type=execution_type,
                            position_effect=position_effect,
                            executed_time=executed_time,
                            quantity=quantity,
                            price=price,
                            gross_amount=gross_amount,
                            fees=None,
                        )
                    )

        executions.sort(key=lambda item: item.executed_time or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return executions

    def get_trades(
        self,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        lookback_days: int = 7,
    ) -> list[BrokerTrade]:
        executions = self.get_executions(
            lookback_days=lookback_days,
            from_entered_datetime=from_time,
            to_entered_datetime=to_time,
        )

        trades: list[BrokerTrade] = []
        for execution in executions:
            execution_type = str(execution.execution_type or "").upper()
            if execution_type and "FILL" not in execution_type:
                continue

            if not execution.symbol or execution.quantity is None or execution.price is None:
                continue

            trades.append(
                BrokerTrade(
                    account_hash=execution.account_hash,
                    trade_id=execution.execution_id,
                    order_id=execution.order_id,
                    symbol=execution.symbol,
                    instruction=execution.instruction,
                    executed_time=execution.executed_time,
                    quantity=execution.quantity,
                    price=execution.price,
                    gross_amount=execution.gross_amount,
                    fees=execution.fees,
                    net_amount=(execution.gross_amount - execution.fees) if execution.gross_amount is not None and execution.fees is not None else execution.gross_amount,
                    position_id=f"{execution.account_hash}:{execution.symbol}",
                )
            )

        return trades

    def preview_order(self, request: EquityOrderRequest) -> dict[str, object]:
        client = self.auth_service.create_client()
        account_hash = self._resolve_account_hash(client, request.account_hash)
        self._validate_order_request(client, account_hash, request, enforce_duplicate=False)
        order_spec = _build_equity_order(
            request,
            duration=_resolve_duration(request.time_in_force, None),
            session=_resolve_session_for_request(request.time_in_force),
        )
        response = client.preview_order(account_hash, order_spec)
        response.raise_for_status()
        result = {
            "status_code": response.status_code,
            "account_hash": account_hash,
            "preview": response.json(),
        }
        self._audit("preview", account_hash, request, result)
        return result

    def place_order(self, request: EquityOrderRequest) -> dict[str, object]:
        client = self.auth_service.create_client()
        account_hash = self._resolve_account_hash(client, request.account_hash)
        self._validate_order_request(client, account_hash, request, enforce_duplicate=True)
        order_spec = _build_equity_order(
            request,
            duration=_resolve_duration(request.time_in_force, None),
            session=_resolve_session_for_request(request.time_in_force),
        )
        preview_response = client.preview_order(account_hash, order_spec)
        preview_response.raise_for_status()
        preview_payload = preview_response.json()
        rejects = preview_payload.get("orderValidationResult", {}).get("rejects", [])
        if rejects:
            raise ValueError(f"Schwab preview rejected order: {rejects}")

        response = client.place_order(account_hash, order_spec)
        response.raise_for_status()
        order_id = Utils(client, account_hash).extract_order_id(response)
        result = {
            "status_code": response.status_code,
            "account_hash": account_hash,
            "order_id": str(order_id) if order_id is not None else None,
        }
        self._remember_order(account_hash, request)
        self._audit("place", account_hash, request, {**result, "preview": preview_payload})
        return result

    def modify_order(self, request: ModifyEquityOrderRequest) -> dict[str, object]:
        client = self.auth_service.create_client()
        account_hash = self._resolve_account_hash(client, request.account_hash)
        current_order = self._get_order_details(client, account_hash, request.order_id)
        self._validate_modify_request(current_order, request)

        order_spec = _build_equity_order(
            request,
            duration=_resolve_duration(request.time_in_force, current_order.get("duration")),
            session=_resolve_session(current_order, request.time_in_force),
        )

        response = client.replace_order(account_hash, request.order_id, order_spec)
        response.raise_for_status()
        replacement_order_id = Utils(client, account_hash).extract_order_id(response)
        result = {
            "status_code": response.status_code,
            "account_hash": account_hash,
            "order_id": str(replacement_order_id) if replacement_order_id is not None else str(request.order_id),
            "replaced": True,
        }
        self._audit("modify", account_hash, None, {"request": request.model_dump(), **result})
        return result

    def cancel_order(self, account_hash: str, order_id: str) -> dict[str, object]:
        client = self.auth_service.create_client()
        resolved_hash = self._resolve_account_hash(client, account_hash)
        active_order_ids = {order.order_id for order in self.get_orders() if order.account_hash == resolved_hash and _is_active_order_status(order.status)}
        if str(order_id) not in active_order_ids:
            raise ValueError(f"Order {order_id} is not active/cancelable in the current Schwab order set")

        response = client.cancel_order(order_id, resolved_hash)
        response.raise_for_status()
        result = {
            "status_code": response.status_code,
            "account_hash": resolved_hash,
            "order_id": str(order_id),
            "canceled": True,
        }
        self._audit("cancel", resolved_hash, None, result)
        return result

    def _get_order_details(self, client, account_hash: str, order_id: str) -> dict:
        response = client.get_order(order_id, account_hash)
        response.raise_for_status()
        order = response.json()
        if not order:
            raise ValueError(f"Order {order_id} was not found at Schwab")
        return order

    def _validate_order_request(self, client, account_hash: str, request: EquityOrderRequest, enforce_duplicate: bool) -> None:
        if not settings.schwab_trading_enabled:
            raise ValueError("Schwab trading kill switch is OFF. Set SCHWAB_TRADING_ENABLED=true and restart the backend to allow orders.")

        if settings.schwab_max_order_shares > 0 and request.quantity > settings.schwab_max_order_shares:
            raise ValueError(f"Order quantity {request.quantity} exceeds max {settings.schwab_max_order_shares} share(s)")

        if request.quantity != int(request.quantity):
            raise ValueError("Fractional share orders are disabled for Schwab bridge trading")

        if request.order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("Only LIMIT and MARKET orders are enabled for Schwab bridge trading")

        if request.order_type == "LIMIT" and request.limit_price is None:
            raise ValueError("limit_price is required")

        if request.stop_loss_price is not None and request.trailing_stop_offset is not None:
            raise ValueError("Use either stop_loss_price or trailing_stop_offset, not both")

        if (
            request.stop_loss_price is not None
            or request.take_profit_price is not None
            or request.trailing_stop_offset is not None
        ) and request.instruction not in {"BUY", "SELL_SHORT"}:
            raise ValueError("Attached SL/TP/trailing protections are supported only for opening BUY or SELL_SHORT orders")

        notional = request.quantity * (request.limit_price or 0)
        if request.order_type == "LIMIT" and settings.schwab_max_order_notional > 0 and notional > settings.schwab_max_order_notional:
            raise ValueError(f"Order notional ${notional:.2f} exceeds max ${settings.schwab_max_order_notional:.2f}")

        quote_response = client.get_quote(request.symbol.upper())
        quote_response.raise_for_status()
        quote_payload = quote_response.json().get(request.symbol.upper(), {})
        quote = quote_payload.get("quote", {})
        regular = quote_payload.get("regular", {})
        last = float(regular.get("regularMarketLastPrice") or quote.get("lastPrice") or quote.get("mark") or 0)
        if request.order_type == "LIMIT" and last > 0:
            deviation_pct = abs(request.limit_price - last) / last * 100
            if deviation_pct > settings.schwab_limit_price_max_deviation_pct:
                raise ValueError(
                    f"Limit price deviation {deviation_pct:.2f}% exceeds max {settings.schwab_limit_price_max_deviation_pct:.2f}% from last price {last:.2f}"
                )

        if enforce_duplicate and self._is_duplicate_order(account_hash, request):
            raise ValueError("Duplicate order blocked by Schwab bridge duplicate-protection window")

    def _validate_modify_request(self, current_order: dict, request: ModifyEquityOrderRequest) -> None:
        status = str(current_order.get("status") or "")
        if not _is_active_order_status(status):
            raise ValueError(f"Order {request.order_id} is not active/cancelable and cannot be modified")

        if str(current_order.get("orderType") or "").upper() != "LIMIT":
            raise ValueError("Only LIMIT order modifications are supported in the Schwab bridge")

        if request.quantity != int(request.quantity):
            raise ValueError("Fractional share modifications are disabled for Schwab bridge trading")

        current_quantity = _as_float(current_order.get("quantity")) or 0.0
        if (
            settings.schwab_max_order_shares > 0
            and request.quantity > current_quantity
            and request.quantity > settings.schwab_max_order_shares
        ):
            raise ValueError(
                f"Schwab bridge can only increase quantity up to {settings.schwab_max_order_shares:.0f} share(s) during QT-side modification"
            )

    def _fingerprint(self, account_hash: str, request: EquityOrderRequest) -> tuple[str, str, str, float, str, float | None]:
        limit_price = round(request.limit_price, 4) if request.limit_price is not None else None
        return (account_hash, request.symbol.upper(), request.instruction, request.quantity, request.order_type, limit_price)

    def _is_duplicate_order(self, account_hash: str, request: EquityOrderRequest) -> bool:
        now = datetime.now(timezone.utc)
        fingerprint = self._fingerprint(account_hash, request)
        self._recent_order_fingerprints = {
            key: timestamp
            for key, timestamp in self._recent_order_fingerprints.items()
            if (now - timestamp).total_seconds() <= settings.schwab_duplicate_window_seconds
        }
        last_seen = self._recent_order_fingerprints.get(fingerprint)
        return last_seen is not None and (now - last_seen).total_seconds() <= settings.schwab_duplicate_window_seconds

    def _remember_order(self, account_hash: str, request: EquityOrderRequest) -> None:
        self._recent_order_fingerprints[self._fingerprint(account_hash, request)] = datetime.now(timezone.utc)

    def _audit(self, action: str, account_hash: str, request: EquityOrderRequest | None, result: dict[str, object]) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "account_hash": account_hash,
            "request": request.model_dump() if request else None,
            "result": result,
        }
        with self.audit_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")


def _as_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def _extract_order_price(order: dict) -> float | None:
    value = _as_float(order.get("price"))
    if value is not None and value > 0:
        return value

    for activity in order.get("orderActivityCollection") or []:
        for execution_leg in activity.get("executionLegs") or []:
            value = _as_float(execution_leg.get("price"))
            if value is not None and value > 0:
                return value

    return None


def _extract_stop_price(order: dict) -> float | None:
    value = _as_float(order.get("stopPrice"))
    if value is not None and value > 0:
        return value

    return None


def _extract_trail_offset(order: dict) -> float | None:
    value = _as_float(order.get("stopPriceOffset"))
    if value is not None and value > 0:
        return value

    return None


def _resolve_execution_quantity(execution_leg: dict, activity: dict, order: dict) -> float | None:
    for source in (execution_leg, activity, order):
        quantity = _as_float(source.get("quantity"))
        if quantity is not None and quantity > 0:
            return quantity

    filled_quantity = _as_float(order.get("filledQuantity"))
    if filled_quantity is not None and filled_quantity > 0:
        return filled_quantity

    return None


def _resolve_execution_price(execution_leg: dict, activity: dict, order: dict) -> float | None:
    for source in (execution_leg, activity, order):
        price = _as_float(source.get("price"))
        if price is not None and price > 0:
            return price

    return _resolve_average_fill_price(order)


def _resolve_average_fill_price(order: dict) -> float | None:
    total_quantity = 0.0
    total_notional = 0.0
    for activity in order.get("orderActivityCollection") or []:
        for execution_leg in activity.get("executionLegs") or []:
            quantity = _as_float(execution_leg.get("quantity"))
            price = _as_float(execution_leg.get("price"))
            if quantity is None or price is None or quantity <= 0 or price <= 0:
                continue
            total_quantity += quantity
            total_notional += quantity * price

    if total_quantity > 0:
        return total_notional / total_quantity

    return _extract_order_price(order)


def _flatten_orders(order: dict, account_hash: str) -> list[BrokerOrder]:
    orders: list[BrokerOrder] = []
    leg = (order.get("orderLegCollection") or [{}])[0]
    instrument = leg.get("instrument", {})
    symbol = instrument.get("symbol")

    if symbol:
        orders.append(
            BrokerOrder(
                account_hash=account_hash,
                order_id=str(order.get("orderId", "unknown")),
                symbol=symbol,
                instruction=leg.get("instruction"),
                order_type=order.get("orderType"),
                order_strategy_type=order.get("orderStrategyType"),
                status=order.get("status"),
                original_status=order.get("status"),
                duration=order.get("duration"),
                session=order.get("session"),
                entered_time=_parse_datetime(order.get("enteredTime")),
                close_time=_parse_datetime(order.get("closeTime")),
                expiration_time=_parse_datetime(order.get("cancelTime")),
                quantity=_as_float(leg.get("quantity") or order.get("quantity")),
                filled_quantity=_as_float(order.get("filledQuantity")),
                remaining_quantity=_as_float(order.get("remainingQuantity")),
                average_fill_price=_resolve_average_fill_price(order),
                price=_extract_order_price(order),
                stop_price=_extract_stop_price(order),
                trail_offset=_extract_trail_offset(order),
                trigger_price=_extract_stop_price(order),
                position_id=f"{account_hash}:{symbol}",
                group_id=str(order.get("orderStrategyType") or ""),
            )
        )

    for child in order.get("childOrderStrategies") or []:
        orders.extend(_flatten_orders(child, account_hash))

    return orders


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def _resolve_market_price(position: dict) -> float | None:
    quantity = _as_float(position.get("longQuantity")) - _as_float(position.get("shortQuantity"))
    market_value = _as_float(position.get("marketValue"))
    if not quantity or not market_value:
        return None

    return abs(market_value / quantity)


def _build_equity_order(
    request: EquityOrderRequest | ModifyEquityOrderRequest,
    *,
    duration: Duration | None = None,
    session: Session | None = None,
):
    if isinstance(request, EquityOrderRequest) and (
        request.stop_loss_price is not None
        or request.take_profit_price is not None
        or request.trailing_stop_offset is not None
    ):
        return _build_equity_order_with_protection(
            request,
            duration=duration,
            session=session,
        )

    symbol = request.symbol.upper()
    quantity = request.quantity

    if request.order_type == "LIMIT" and request.limit_price is None:
        raise ValueError("limit_price is required for LIMIT orders")

    limit_price = None
    if request.limit_price is not None:
        limit_price = format(Decimal(str(request.limit_price)).quantize(Decimal("0.01")), "f")

    if request.instruction == "BUY":
        order = equity_buy_limit(symbol, quantity, limit_price) if request.order_type == "LIMIT" else equity_buy_market(symbol, quantity)
    elif request.instruction == "SELL":
        order = equity_sell_limit(symbol, quantity, limit_price) if request.order_type == "LIMIT" else equity_sell_market(symbol, quantity)
    elif request.instruction == "SELL_SHORT":
        order = equity_sell_short_limit(symbol, quantity, limit_price) if request.order_type == "LIMIT" else equity_sell_short_market(symbol, quantity)
    else:
        order = equity_buy_to_cover_limit(symbol, quantity, limit_price) if request.order_type == "LIMIT" else equity_buy_to_cover_market(symbol, quantity)

    resolved_duration = duration or Duration.DAY
    resolved_session = session or (Session.SEAMLESS if settings.schwab_extended_hours_enabled else Session.NORMAL)
    order.set_duration(resolved_duration)
    order.set_session(resolved_session)

    return order


def _build_equity_order_with_protection(
    request: EquityOrderRequest,
    *,
    duration: Duration | None = None,
    session: Session | None = None,
):
    resolved_duration = duration or Duration.DAY
    resolved_session = session or (Session.SEAMLESS if settings.schwab_extended_hours_enabled else Session.NORMAL)
    entry_order = _build_base_equity_order(
        symbol=request.symbol.upper(),
        quantity=request.quantity,
        instruction=request.instruction,
        order_type=request.order_type,
        limit_price=request.limit_price,
        duration=resolved_duration,
        session=resolved_session,
    )

    exit_instruction = _resolve_exit_instruction(request.instruction)
    exit_orders: list[OrderBuilder] = []

    if request.take_profit_price is not None:
        exit_orders.append(_build_limit_exit_order(
            symbol=request.symbol.upper(),
            quantity=request.quantity,
            instruction=exit_instruction,
            price=request.take_profit_price,
            duration=resolved_duration,
            session=Session.NORMAL if resolved_duration == Duration.GOOD_TILL_CANCEL else resolved_session,
        ))

    if request.trailing_stop_offset is not None:
        exit_orders.append(_build_trailing_stop_exit_order(
            symbol=request.symbol.upper(),
            quantity=request.quantity,
            instruction=exit_instruction,
            trail_offset=request.trailing_stop_offset,
            duration=resolved_duration,
            session=Session.NORMAL if resolved_duration == Duration.GOOD_TILL_CANCEL else resolved_session,
        ))
    elif request.stop_loss_price is not None:
        exit_orders.append(_build_stop_exit_order(
            symbol=request.symbol.upper(),
            quantity=request.quantity,
            instruction=exit_instruction,
            stop_price=request.stop_loss_price,
            duration=resolved_duration,
            session=Session.NORMAL if resolved_duration == Duration.GOOD_TILL_CANCEL else resolved_session,
        ))

    if not exit_orders:
        return entry_order

    if len(exit_orders) == 1:
        return first_triggers_second(entry_order, exit_orders[0])

    return first_triggers_second(entry_order, one_cancels_other(exit_orders[0], exit_orders[1]))


def _build_base_equity_order(
    *,
    symbol: str,
    quantity: float,
    instruction: str,
    order_type: str,
    limit_price: float | None,
    duration: Duration,
    session: Session,
) -> OrderBuilder:
    order = OrderBuilder()
    order.set_session(session)
    order.set_duration(duration)
    order.set_order_strategy_type(OrderStrategyType.SINGLE)
    order.add_equity_leg(_resolve_equity_instruction(instruction), symbol, quantity)

    if order_type == "MARKET":
        order.set_order_type(OrderType.MARKET)
    else:
        order.set_order_type(OrderType.LIMIT)
        order.set_price(format(Decimal(str(limit_price)).quantize(Decimal("0.01")), "f"))

    return order


def _build_limit_exit_order(
    *,
    symbol: str,
    quantity: float,
    instruction: str,
    price: float,
    duration: Duration,
    session: Session,
) -> OrderBuilder:
    return _build_base_equity_order(
        symbol=symbol,
        quantity=quantity,
        instruction=instruction,
        order_type="LIMIT",
        limit_price=price,
        duration=duration,
        session=session,
    )


def _build_stop_exit_order(
    *,
    symbol: str,
    quantity: float,
    instruction: str,
    stop_price: float,
    duration: Duration,
    session: Session,
) -> OrderBuilder:
    return (OrderBuilder()
        .set_session(session)
        .set_duration(duration)
        .set_order_strategy_type(OrderStrategyType.SINGLE)
        .set_order_type(OrderType.STOP)
        .set_stop_type(StopType.STANDARD)
        .set_stop_price(format(Decimal(str(stop_price)).quantize(Decimal("0.01")), "f"))
        .add_equity_leg(_resolve_equity_instruction(instruction), symbol, quantity))


def _build_trailing_stop_exit_order(
    *,
    symbol: str,
    quantity: float,
    instruction: str,
    trail_offset: float,
    duration: Duration,
    session: Session,
) -> OrderBuilder:
    return (OrderBuilder()
        .set_session(session)
        .set_duration(duration)
        .set_order_strategy_type(OrderStrategyType.SINGLE)
        .set_order_type(OrderType.TRAILING_STOP)
        .set_stop_type(StopType.STANDARD)
        .set_stop_price_link_basis(StopPriceLinkBasis.LAST)
        .set_stop_price_link_type(StopPriceLinkType.VALUE)
        .set_stop_price_offset(float(Decimal(str(trail_offset)).quantize(Decimal("0.01"))))
        .add_equity_leg(_resolve_equity_instruction(instruction), symbol, quantity))


def _resolve_equity_instruction(instruction: str) -> EquityInstruction:
    return {
        "BUY": EquityInstruction.BUY,
        "SELL": EquityInstruction.SELL,
        "SELL_SHORT": EquityInstruction.SELL_SHORT,
        "BUY_TO_COVER": EquityInstruction.BUY_TO_COVER,
    }[instruction.upper()]


def _resolve_exit_instruction(instruction: str) -> str:
    return {
        "BUY": "SELL",
        "SELL_SHORT": "BUY_TO_COVER",
    }[instruction.upper()]


def _is_active_order_status(status: str | None) -> bool:
    return status in {
        "ACCEPTED",
        "AWAITING_PARENT_ORDER",
        "AWAITING_CONDITION",
        "AWAITING_STOP_CONDITION",
        "AWAITING_MANUAL_REVIEW",
        "PENDING_ACTIVATION",
        "QUEUED",
        "WORKING",
        "NEW",
        "PARTIAL_FILL",
        "PARTIALLY_FILLED",
    }


def _resolve_duration(time_in_force: str | None, current_duration: str | None) -> Duration:
    normalized = (time_in_force or current_duration or "DAY").upper()
    return Duration.GOOD_TILL_CANCEL if normalized in {"GTC", "GOOD_TILL_CANCEL"} else Duration.DAY


def _resolve_session_for_request(time_in_force: str | None) -> Session:
    if (time_in_force or "").upper() == "GTC":
        return Session.NORMAL

    return Session.SEAMLESS if settings.schwab_extended_hours_enabled else Session.NORMAL


def _resolve_session(current_order: dict, time_in_force: str | None) -> Session:
    if (time_in_force or "").upper() == "GTC":
        return Session.NORMAL

    normalized = str(current_order.get("session") or "").upper()
    return {
        "AM": Session.AM,
        "PM": Session.PM,
        "SEAMLESS": Session.SEAMLESS,
        "NORMAL": Session.NORMAL,
    }.get(normalized, Session.NORMAL)
