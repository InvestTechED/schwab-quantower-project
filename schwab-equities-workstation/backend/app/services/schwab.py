import logging
from datetime import date, datetime, timedelta, timezone
from time import sleep

import httpx

from app.config import settings
from app.models import Bar, MarketSnapshot, OptionChain, OptionContract, OptionSeries, SymbolProfile
from app.services.auth import SchwabAuthService
from app.services.symbols import denormalize_symbol, display_symbol, normalize_symbol

logger = logging.getLogger(__name__)


class SchwabMarketDataService:
    def __init__(self) -> None:
        self.auth_service = SchwabAuthService()
        self._snapshot_cache: dict[str, tuple[datetime, MarketSnapshot]] = {}
        self._bars_cache: dict[tuple[str, str, int, str | None, str | None], tuple[datetime, list[Bar]]] = {}
        self._instrument_cache: dict[str, tuple[datetime, dict]] = {}
        self._options_series_cache: dict[str, tuple[datetime, list[OptionSeries]]] = {}
        self._options_chain_cache: dict[tuple[str, str | None], tuple[datetime, OptionChain]] = {}
        self._snapshot_ttl = timedelta(seconds=2)
        self._bars_ttl = timedelta(seconds=5)
        self._instrument_ttl = timedelta(hours=4)
        self._options_series_ttl = timedelta(minutes=15)
        self._options_chain_ttl = timedelta(seconds=15)
        self._market_timezone = datetime.now().astimezone().tzinfo or timezone.utc

    def get_snapshot(self, symbol: str) -> MarketSnapshot:
        requested_symbol = display_symbol(symbol)
        schwab_symbol = normalize_symbol(requested_symbol)
        now = datetime.now(timezone.utc)
        cached_snapshot = self._snapshot_cache.get(requested_symbol)
        if cached_snapshot and now - cached_snapshot[0] <= self._snapshot_ttl:
            return cached_snapshot[1]

        try:
            response = self._request_with_retry(lambda client: client.get_quote(schwab_symbol))
            response.raise_for_status()

            payload = _extract_quote_payload(response.json(), schwab_symbol)
            quote = payload.get("quote", {})
            fundamental = payload.get("fundamental", {})
            regular = payload.get("regular", {})
            instrument = self._get_instrument(None, schwab_symbol)

            last = _as_float(
                regular.get("regularMarketLastPrice")
                or quote.get("lastPrice")
                or quote.get("mark")
                or quote.get("bidPrice")
                or quote.get("askPrice")
            )
            bid = _as_float(quote.get("bidPrice")) or last
            ask = _as_float(quote.get("askPrice")) or last
            bid_size = int(_as_float(quote.get("bidSize")) or 0)
            ask_size = int(_as_float(quote.get("askSize")) or 0)
            open_price = _as_float(quote.get("openPrice")) or last
            high = _as_float(quote.get("highPrice")) or last
            low = _as_float(quote.get("lowPrice")) or last
            close = _as_float(quote.get("closePrice")) or last
            volume = int(_as_float(quote.get("totalVolume")) or 0)
            avg_10d_volume = _as_float(fundamental.get("avg10DaysVolume")) or 0.0
            relative_volume = (volume / avg_10d_volume) if avg_10d_volume > 0 else 0.0

            if last > open_price:
                trend_state = "bullish"
            elif last < open_price:
                trend_state = "bearish"
            else:
                trend_state = "neutral"

            session_mid = (high + low) / 2 if high and low else last
            if last > session_mid:
                vwap_bias = "above"
            elif last < session_mid:
                vwap_bias = "below"
            else:
                vwap_bias = "flat"

            quote_time_ms = (
                regular.get("regularMarketTradeTime")
                or quote.get("tradeTime")
                or quote.get("quoteTime")
            )
            as_of = (
                datetime.fromtimestamp(int(quote_time_ms) / 1000, tz=timezone.utc)
                if quote_time_ms
                else datetime.now(timezone.utc)
            )

            snapshot = MarketSnapshot(
                symbol=requested_symbol,
                as_of=as_of,
                last=last,
                bid=bid,
                ask=ask,
                bid_size=bid_size,
                ask_size=ask_size,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
                asset_type=instrument.get("assetType") or quote.get("quoteType") or quote.get("assetMainType"),
                description=instrument.get("description") or quote.get("description") or requested_symbol,
                exchange=instrument.get("exchange") or quote.get("exchangeName") or "US",
                relative_volume=relative_volume,
                trend_state=trend_state,
                vwap_bias=vwap_bias,
            )
            self._snapshot_cache[requested_symbol] = (now, snapshot)
            return snapshot
        except Exception:
            logger.exception("get_snapshot failed for symbol=%s", requested_symbol)
            if cached_snapshot:
                return cached_snapshot[1]
            raise

    def get_symbol_profile(self, symbol: str) -> SymbolProfile:
        requested_symbol = display_symbol(symbol)
        schwab_symbol = normalize_symbol(requested_symbol)
        instrument = self._get_instrument(None, schwab_symbol)

        asset_type = instrument.get("assetType")
        instrument_type = instrument.get("type")
        description = instrument.get("description") or requested_symbol
        exchange = instrument.get("exchange") or "US"

        if not asset_type or not instrument_type:
            try:
                response = self._request_with_retry(lambda client: client.get_quote(schwab_symbol))
                response.raise_for_status()
                payload = _extract_quote_payload(response.json(), schwab_symbol)
                quote = payload.get("quote", {})
                asset_type = asset_type or quote.get("quoteType") or quote.get("assetMainType")
                exchange = exchange or quote.get("exchangeName")
                description = description or quote.get("description")
            except Exception:
                logger.warning("get_symbol_profile quote fallback failed for symbol=%s", requested_symbol)

        return SymbolProfile(
            symbol=requested_symbol,
            normalized_symbol=schwab_symbol,
            asset_type=asset_type,
            instrument_type=instrument_type,
            description=description,
            exchange=exchange,
            options_available=asset_type in {"EQUITY", "ETF", "INDEX", "INDX", "COLLECTIVE_INVESTMENT"},
        )

    def search_symbols(self, query: str, limit: int = 10) -> list[SymbolProfile]:
        requested_query = display_symbol(query)
        if not requested_query:
            return []

        client = self.auth_service.create_client()
        results: dict[str, SymbolProfile] = {}

        def add_profile(profile: SymbolProfile | None) -> None:
            if profile is None or not profile.symbol:
                return

            key = profile.symbol.upper()
            if key not in results:
                results[key] = profile

        try:
            add_profile(self.get_symbol_profile(requested_query))
        except Exception:
            logger.warning("search_symbols exact profile lookup failed for query=%s", requested_query)

        try:
            response = self._request_with_retry(
                lambda client: client.get_instruments(
                    normalize_symbol(requested_query),
                    client.Instrument.Projection.SYMBOL_SEARCH,
                )
            )
            response.raise_for_status()
            payload = response.json()
            for instrument in _extract_instruments(payload):
                add_profile(_symbol_profile_from_instrument(instrument))
        except Exception:
            logger.warning("search_symbols symbol search failed for query=%s", requested_query)

        ordered = list(results.values())
        if len(ordered) > limit:
            ordered = ordered[:limit]

        return ordered

    def get_option_series(self, symbol: str) -> list[OptionSeries]:
        requested_symbol = display_symbol(symbol)
        now = datetime.now(timezone.utc)
        cached_series = self._options_series_cache.get(requested_symbol)
        if cached_series and now - cached_series[0] <= self._options_series_ttl:
            return cached_series[1]

        response = self._request_with_retry(
            lambda client: client.get_option_expiration_chain(normalize_symbol(requested_symbol))
        )
        response.raise_for_status()
        payload = response.json()

        series: list[OptionSeries] = []
        for item in payload.get("expirationList", []):
            expiration_date = _parse_option_datetime(item.get("expirationDate"))
            if expiration_date is None:
                continue

            series.append(
                OptionSeries(
                    id=_option_series_id(requested_symbol, expiration_date.date()),
                    underlier_symbol=requested_symbol,
                    expiration_date=expiration_date,
                    days_to_expiration=_as_int(item.get("daysToExpiration")),
                    series_type=_map_series_type(item.get("expirationType")),
                    name=f"{requested_symbol} {expiration_date.strftime('%Y-%m-%d')}",
                    exchange="OPR",
                )
            )

        self._options_series_cache[requested_symbol] = (now, series)
        return series

    def get_option_chain(self, symbol: str, expiration: date | None = None) -> OptionChain:
        requested_symbol = display_symbol(symbol)
        expiration_key = expiration.isoformat() if expiration else None
        cache_key = (requested_symbol, expiration_key)
        now = datetime.now(timezone.utc)
        cached_chain = self._options_chain_cache.get(cache_key)
        if cached_chain and now - cached_chain[0] <= self._options_chain_ttl:
            return cached_chain[1]

        response = self._request_with_retry(
            lambda client: client.get_option_chain(
                normalize_symbol(requested_symbol),
                include_underlying_quote=True,
                from_date=expiration,
                to_date=expiration,
            )
        )
        response.raise_for_status()
        payload = response.json()

        contracts: list[OptionContract] = []
        series_by_id: dict[str, OptionSeries] = {}

        for option_type, exp_map_key in (("CALL", "callExpDateMap"), ("PUT", "putExpDateMap")):
            exp_map = payload.get(exp_map_key, {})
            if not isinstance(exp_map, dict):
                continue

            for exp_key, strikes in exp_map.items():
                expiration_date = _parse_exp_map_expiration(exp_key)
                if expiration_date is None:
                    continue
                if expiration and expiration_date.date() != expiration:
                    continue

                series_id = _option_series_id(requested_symbol, expiration_date.date())
                series_by_id.setdefault(
                    series_id,
                    OptionSeries(
                        id=series_id,
                        underlier_symbol=requested_symbol,
                        expiration_date=expiration_date,
                        days_to_expiration=_parse_days_from_exp_key(exp_key),
                        series_type=_infer_series_type_from_exp_key(exp_key),
                        name=f"{requested_symbol} {expiration_date.strftime('%Y-%m-%d')}",
                        exchange="OPR",
                    ),
                )

                if not isinstance(strikes, dict):
                    continue

                for strike_key, entries in strikes.items():
                    if not isinstance(entries, list):
                        continue

                    strike_price = _as_float(strike_key)
                    if strike_price is None:
                        continue

                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        contracts.append(
                            OptionContract(
                                symbol=str(entry.get("symbol") or "").strip(),
                                underlier_symbol=requested_symbol,
                                description=str(entry.get("description") or "").strip() or f"{requested_symbol} {expiration_date:%m/%d/%Y} {strike_price:.2f} {option_type[0]}",
                                exchange=str(entry.get("exchangeName") or "OPR"),
                                option_type=option_type,
                                strike_price=strike_price,
                                expiration_date=expiration_date,
                                days_to_expiration=_as_int(entry.get("daysToExpiration")) or _parse_days_from_exp_key(exp_key),
                                bid=_as_float(entry.get("bid")),
                                ask=_as_float(entry.get("ask")),
                                last=_as_float(entry.get("last")),
                                mark=_as_float(entry.get("mark")),
                                bid_size=_as_int(entry.get("bidSize")),
                                ask_size=_as_int(entry.get("askSize")),
                                last_size=_as_int(entry.get("lastSize")),
                                open_interest=_as_int(entry.get("openInterest")),
                                volume=_as_int(entry.get("totalVolume")),
                                volatility=_as_float(entry.get("volatility")),
                                delta=_as_float(entry.get("delta")),
                                gamma=_as_float(entry.get("gamma")),
                                theta=_as_float(entry.get("theta")),
                                vega=_as_float(entry.get("vega")),
                                rho=_as_float(entry.get("rho")),
                            )
                        )

        underlying = payload.get("underlying", {}) if isinstance(payload.get("underlying"), dict) else {}
        chain = OptionChain(
            underlier_symbol=requested_symbol,
            underlier_last=_as_float(underlying.get("last")),
            underlier_bid=_as_float(underlying.get("bid")),
            underlier_ask=_as_float(underlying.get("ask")),
            series=sorted(series_by_id.values(), key=lambda item: item.expiration_date),
            contracts=sorted(
                contracts,
                key=lambda item: (item.expiration_date, item.strike_price, 0 if item.option_type == "CALL" else 1),
            ),
        )
        self._options_chain_cache[cache_key] = (now, chain)
        return chain

    def get_bars(
        self,
        symbol: str,
        limit: int = 500,
        timeframe: str = "5m",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Bar]:
        requested_symbol = display_symbol(symbol)
        schwab_symbol = normalize_symbol(requested_symbol)
        timeframe_key = timeframe.lower()
        normalized_start, normalized_end = self._normalize_history_window(timeframe_key, start, end)
        start_key = normalized_start.isoformat() if normalized_start else None
        end_key = normalized_end.isoformat() if normalized_end else None
        cache_key = (requested_symbol, timeframe_key, limit, start_key, end_key)
        now = datetime.now(timezone.utc)
        cached_bars = self._bars_cache.get(cache_key)
        if cached_bars and now - cached_bars[0] <= self._bars_ttl:
            return cached_bars[1]

        try:
            response = self._request_with_retry(
                lambda client: self._get_price_history_response(client, schwab_symbol, timeframe_key, normalized_start, normalized_end)
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                fallback_response = self._retry_price_history_without_range(schwab_symbol, timeframe_key, exc)
                if fallback_response is None:
                    raise

                response = fallback_response
                response.raise_for_status()
            payload = response.json()

            bars: list[Bar] = []
            for candle in payload.get("candles", []):
                timestamp_ms = candle.get("datetime")
                if timestamp_ms is None:
                    continue

                bars.append(
                    Bar(
                        timestamp=datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
                        open=_as_float(candle.get("open")) or 0.0,
                        high=_as_float(candle.get("high")) or 0.0,
                        low=_as_float(candle.get("low")) or 0.0,
                        close=_as_float(candle.get("close")) or 0.0,
                        volume=int(_as_float(candle.get("volume")) or 0),
                    )
                )

            if timeframe_key in {"1h", "4h"}:
                bars = self._aggregate_bars(bars, timeframe_key)

            if limit > 0:
                bars = bars[-limit:]

            self._bars_cache[cache_key] = (now, bars)
            return bars
        except Exception as exc:
            if cached_bars:
                logger.warning(
                    "get_bars degraded to cached data for symbol=%s timeframe=%s start=%s end=%s error=%s",
                    requested_symbol,
                    timeframe_key,
                    start_key,
                    end_key,
                    exc,
                )
                return cached_bars[1]

            logger.warning(
                "get_bars degraded to empty result for symbol=%s timeframe=%s start=%s end=%s error=%s",
                requested_symbol,
                timeframe_key,
                start_key,
                end_key,
                exc,
            )
            return []

    def _normalize_history_window(
        self,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
    ) -> tuple[datetime | None, datetime | None]:
        now = datetime.now(timezone.utc)
        normalized_end = self._as_utc(end) if end else now
        normalized_start = self._as_utc(start) if start else None

        if normalized_end > now:
            normalized_end = now

        if normalized_start and normalized_start > now:
            normalized_start = None

        if normalized_start is None or normalized_start >= normalized_end:
            fallback = self._default_history_lookback(timeframe)
            normalized_start = normalized_end - fallback

        return normalized_start, normalized_end

    def _get_instrument(self, client, symbol: str) -> dict:
        now = datetime.now(timezone.utc)
        cached = self._instrument_cache.get(symbol)
        if cached and now - cached[0] <= self._instrument_ttl:
            return cached[1]

        if client is None:
            response = self._request_with_retry(
                lambda current: current.get_instruments([symbol], current.Instrument.Projection.FUNDAMENTAL)
            )
        else:
            response = self._request_with_retry(
                lambda current: current.get_instruments([symbol], current.Instrument.Projection.FUNDAMENTAL)
            )
        response.raise_for_status()
        payload = response.json()
        instrument = payload.get(symbol)
        if instrument is None:
            instruments = payload.get("instruments")
            if isinstance(instruments, list) and instruments:
                instrument = instruments[0]
        if isinstance(instrument, list):
            instrument = instrument[0] if instrument else {}
        if not isinstance(instrument, dict):
            instrument = {}

        self._instrument_cache[symbol] = (now, instrument)
        return instrument

    def _get_price_history_response(self, client, symbol: str, timeframe: str, start: datetime | None, end: datetime | None):
        history_kwargs = {
            "start_datetime": start,
            "end_datetime": end,
            "need_extended_hours_data": settings.schwab_extended_hours_enabled,
        }

        if timeframe == "1m":
            return client.get_price_history_every_minute(symbol, **history_kwargs)
        if timeframe == "5m":
            return client.get_price_history_every_five_minutes(symbol, **history_kwargs)
        if timeframe == "10m":
            return client.get_price_history_every_ten_minutes(symbol, **history_kwargs)
        if timeframe == "15m":
            return client.get_price_history_every_fifteen_minutes(symbol, **history_kwargs)
        if timeframe in {"30m", "1h", "4h"}:
            return client.get_price_history_every_thirty_minutes(symbol, **history_kwargs)

        return client.get_price_history_every_day(
            symbol,
            start_datetime=start,
            end_datetime=end,
            need_extended_hours_data=False,
        )

    def _retry_price_history_without_range(
        self,
        symbol: str,
        timeframe: str,
        error: httpx.HTTPStatusError,
    ):
        response = error.response
        if response is None or response.status_code != 400:
            return None

        detail = response.text or ""
        if "Enddate" not in detail and "startDate" not in detail and "endDate" not in detail:
            return None

        return self._request_with_retry(
            lambda client: self._get_price_history_response(client, symbol, timeframe, None, None)
        )

    def _request_with_retry(self, operation):
        last_error: Exception | None = None
        for attempt in range(2):
            client = self.auth_service.create_client(force_new_client=attempt > 0)
            try:
                return operation(client)
            except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError, httpx.TransportError) as exc:
                last_error = exc
                logger.warning("schwab transport retry attempt=%s error=%s", attempt + 1, exc)
                self.auth_service.reset_client()
                if attempt == 0:
                    sleep(0.25)
                    continue
                raise

        if last_error:
            raise last_error
        raise RuntimeError("schwab request retry failed without explicit error")

    def _default_history_lookback(self, timeframe: str) -> timedelta:
        return {
            "1m": timedelta(days=5),
            "5m": timedelta(days=10),
            "10m": timedelta(days=20),
            "15m": timedelta(days=30),
            "30m": timedelta(days=45),
            "1h": timedelta(days=90),
            "4h": timedelta(days=180),
            "1d": timedelta(days=3650),
        }.get(timeframe, timedelta(days=30))

    def _aggregate_bars(self, bars: list[Bar], timeframe: str) -> list[Bar]:
        if not bars:
            return bars

        interval_hours = 1 if timeframe == "1h" else 4
        aggregated: list[Bar] = []
        current_bar: Bar | None = None
        current_bucket: datetime | None = None

        for bar in sorted(bars, key=lambda item: item.timestamp):
            bucket = self._bucket_start(bar.timestamp, interval_hours)
            if current_bucket != bucket:
                if current_bar is not None:
                    aggregated.append(current_bar)
                current_bucket = bucket
                current_bar = Bar(
                    timestamp=bucket,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                )
                continue

            current_bar.high = max(current_bar.high, bar.high)
            current_bar.low = min(current_bar.low, bar.low)
            current_bar.close = bar.close
            current_bar.volume += bar.volume

        if current_bar is not None:
            aggregated.append(current_bar)

        return aggregated

    def _bucket_start(self, timestamp: datetime, interval_hours: int) -> datetime:
        market_time = timestamp.astimezone(self._market_timezone)
        hour = market_time.hour - (market_time.hour % interval_hours)
        bucket = market_time.replace(hour=hour, minute=0, second=0, microsecond=0)
        return bucket.astimezone(timezone.utc)

    def _as_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def _extract_quote_payload(payload: dict, symbol: str) -> dict:
    if symbol in payload:
        return payload[symbol]

    if len(payload) == 1:
        return next(iter(payload.values()))

    return {}


def _extract_instruments(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []

    instruments = payload.get("instruments")
    if isinstance(instruments, list):
        return [item for item in instruments if isinstance(item, dict)]

    extracted: list[dict] = []
    for value in payload.values():
        if isinstance(value, dict):
            extracted.append(value)
        elif isinstance(value, list):
            extracted.extend(item for item in value if isinstance(item, dict))

    return extracted


def _symbol_profile_from_instrument(instrument: dict) -> SymbolProfile | None:
    raw_symbol = instrument.get("symbol")
    if not raw_symbol:
        return None

    display = denormalize_symbol(str(raw_symbol))
    return SymbolProfile(
        symbol=display,
        normalized_symbol=normalize_symbol(display),
        asset_type=instrument.get("assetType"),
        instrument_type=instrument.get("type"),
        description=instrument.get("description") or display,
        exchange=instrument.get("exchange") or "US",
        options_available=instrument.get("assetType") in {"EQUITY", "ETF", "INDEX", "INDX", "COLLECTIVE_INVESTMENT"},
    )


def _as_float(value) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value) -> int | None:
    parsed = _as_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _parse_option_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_exp_map_expiration(value: str) -> datetime | None:
    if not value:
        return None
    date_part = str(value).split(":")[0]
    try:
        return datetime.fromisoformat(date_part).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_days_from_exp_key(value: str) -> int | None:
    if ":" not in str(value):
        return None
    try:
        return int(str(value).split(":")[1])
    except (TypeError, ValueError, IndexError):
        return None


def _map_series_type(value: str | None) -> str:
    return {
        "D": "daily",
        "W": "week",
        "S": "month",
        "M": "month",
        "Q": "month",
    }.get((value or "").upper(), "unknown")


def _infer_series_type_from_exp_key(value: str) -> str:
    days = _parse_days_from_exp_key(value)
    if days is None:
        return "unknown"
    if days <= 1:
        return "daily"
    if days <= 7:
        return "week"
    return "month"


def _option_series_id(symbol: str, expiration: date) -> str:
    return f"{symbol.upper()}|{expiration.isoformat()}"
