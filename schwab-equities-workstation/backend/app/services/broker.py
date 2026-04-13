from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

from schwab.orders.common import Duration, Session
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
from schwab.utils import Utils

from app.models import BrokerAccount, BrokerExecution, BrokerOrder, BrokerPosition, EquityOrderRequest, ModifyEquityOrderRequest
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
            leg = (order.get("orderLegCollection") or [{}])[0]
            instrument = leg.get("instrument", {})
            entered_time = _parse_datetime(order.get("enteredTime"))
            orders.append(
                BrokerOrder(
                    account_hash=number_to_hash.get(
                        str(order.get("accountNumber", "unknown")),
                        str(order.get("accountNumber", "unknown")),
                    ),
                    order_id=str(order.get("orderId", "unknown")),
                    symbol=instrument.get("symbol"),
                    instruction=leg.get("instruction"),
                    order_type=order.get("orderType"),
                    status=order.get("status"),
                    duration=order.get("duration"),
                    session=order.get("session"),
                    entered_time=entered_time,
                    close_time=_parse_datetime(order.get("closeTime")),
                    quantity=_as_float(leg.get("quantity") or order.get("quantity")),
                    filled_quantity=_as_float(order.get("filledQuantity")),
                    remaining_quantity=_as_float(order.get("remainingQuantity")),
                    average_fill_price=_resolve_average_fill_price(order),
                    price=_extract_order_price(order),
                )
            )

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

                execution_type = activity.get("executionType")
                activity_id = str(activity.get("activityId") or order.get("orderId") or "execution")
                execution_legs = activity.get("executionLegs") or []

                for index, execution_leg in enumerate(execution_legs, start=1):
                    quantity = _as_float(execution_leg.get("quantity") or activity.get("quantity"))
                    price = _as_float(execution_leg.get("price"))
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

        if request.quantity > settings.schwab_max_order_shares:
            raise ValueError(f"Order quantity {request.quantity} exceeds max {settings.schwab_max_order_shares} share(s)")

        if request.quantity != int(request.quantity):
            raise ValueError("Fractional share orders are disabled for Schwab bridge trading")

        if request.order_type != "LIMIT":
            raise ValueError("Only LIMIT orders are enabled for Schwab bridge trading")

        if request.limit_price is None:
            raise ValueError("limit_price is required")

        notional = request.quantity * request.limit_price
        if notional > settings.schwab_max_order_notional:
            raise ValueError(f"Order notional ${notional:.2f} exceeds max ${settings.schwab_max_order_notional:.2f}")

        quote_response = client.get_quote(request.symbol.upper())
        quote_response.raise_for_status()
        quote_payload = quote_response.json().get(request.symbol.upper(), {})
        quote = quote_payload.get("quote", {})
        regular = quote_payload.get("regular", {})
        last = float(regular.get("regularMarketLastPrice") or quote.get("lastPrice") or quote.get("mark") or 0)
        if last > 0:
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
        if request.quantity > current_quantity and request.quantity > settings.schwab_max_order_shares:
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
    for key in ("price", "stopPrice"):
        value = _as_float(order.get(key))
        if value is not None and value > 0:
            return value

    for activity in order.get("orderActivityCollection") or []:
        for execution_leg in activity.get("executionLegs") or []:
            value = _as_float(execution_leg.get("price"))
            if value is not None and value > 0:
                return value

    return None


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
