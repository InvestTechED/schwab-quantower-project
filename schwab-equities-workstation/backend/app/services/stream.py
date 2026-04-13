import asyncio
import logging
from contextlib import suppress
from datetime import datetime, UTC
from typing import Any

from schwab.streaming import StreamClient

from app.models import StreamStatus
from app.services.auth import SchwabAuthService
from app.services.symbols import display_symbol, normalize_symbol

logger = logging.getLogger(__name__)
MAX_EVENT_QUEUE_SIZE = 256
STREAM_RESTART_DELAYS = (1, 2, 5)
IDLE_SHUTDOWN_DELAY_SECONDS = 60


class SchwabStreamingService:
    def __init__(self) -> None:
        self.auth_service = SchwabAuthService()
        self.stream_client: StreamClient | None = None
        self.stream_task: asyncio.Task | None = None
        self.started = False
        self.quote_initialized = False
        self.chart_initialized = False
        self.nasdaq_book_initialized = False
        self.nyse_book_initialized = False
        self.subscribed_symbols: set[str] = set()
        self.symbol_queues: dict[str, set[asyncio.Queue[dict[str, Any]]]] = {}
        self.display_to_actual: dict[str, str] = {}
        self.actual_to_displays: dict[str, set[str]] = {}
        self.lock = asyncio.Lock()
        self.runtime_lock = asyncio.Lock()
        self.idle_shutdown_task: asyncio.Task | None = None
        self.published_event_count = 0
        self.dropped_event_count = 0
        self.restart_count = 0
        self.start_count = 0
        self.shutdown_count = 0
        self.last_event_at: datetime | None = None
        self.last_started_at: datetime | None = None
        self.last_restarted_at: datetime | None = None
        self.last_error_at: datetime | None = None
        self.last_error_message: str | None = None

    async def register(self, symbol: str) -> asyncio.Queue[dict[str, Any]]:
        display = display_symbol(symbol)
        actual = normalize_symbol(display)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=MAX_EVENT_QUEUE_SIZE)

        async with self.lock:
            queues = self.symbol_queues.setdefault(display, set())
            queues.add(queue)
            self.display_to_actual[display] = actual
            self.actual_to_displays.setdefault(actual, set()).add(display)

        self._cancel_idle_shutdown()

        async with self.runtime_lock:
            await self._ensure_started()
            await self._subscribe_symbol(actual)

        return queue

    async def unregister(self, symbol: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        display = display_symbol(symbol)
        should_shutdown = False
        async with self.lock:
            queues = self.symbol_queues.get(display)
            if not queues:
                return

            queues.discard(queue)
            if not queues:
                self.symbol_queues.pop(display, None)
                actual = self.display_to_actual.pop(display, None)
                if actual:
                    displays = self.actual_to_displays.get(actual)
                    if displays:
                        displays.discard(display)
                        if not displays:
                            self.actual_to_displays.pop(actual, None)

            should_shutdown = not self.symbol_queues

        if should_shutdown:
            self._schedule_idle_shutdown()

    async def _ensure_started(self) -> None:
        if self.started and self.stream_task and not self.stream_task.done():
            return

        await self._start_client()
        self.stream_task = asyncio.create_task(self._run(), name="schwab-stream-loop")

    async def _start_client(self) -> None:
        client = self.auth_service.create_client()
        self.stream_client = StreamClient(client)
        self.stream_client.add_level_one_equity_handler(self._handle_quote)
        self.stream_client.add_chart_equity_handler(self._handle_chart)
        self.stream_client.add_nasdaq_book_handler(self._handle_book)
        self.stream_client.add_nyse_book_handler(self._handle_book)
        await self.stream_client.login()

        self.started = True
        self.quote_initialized = False
        self.chart_initialized = False
        self.nasdaq_book_initialized = False
        self.nyse_book_initialized = False
        self.subscribed_symbols.clear()
        self.start_count += 1
        self.last_started_at = datetime.now(UTC)
        logger.info("Schwab stream client started")

    async def _subscribe_symbol(self, symbol: str) -> None:
        if self.stream_client is None or symbol in self.subscribed_symbols:
            return

        if not self.quote_initialized:
            await self.stream_client.level_one_equity_subs([symbol])
            self.quote_initialized = True
        else:
            await self.stream_client.level_one_equity_add([symbol])

        if not self.chart_initialized:
            await self.stream_client.chart_equity_subs([symbol])
            self.chart_initialized = True
        else:
            await self.stream_client.chart_equity_add([symbol])

        await self._subscribe_book(symbol)

        self.subscribed_symbols.add(symbol)

    async def _unsubscribe_symbol(self, symbol: str) -> None:
        if self.stream_client is None or symbol not in self.subscribed_symbols:
            return

        with suppress(Exception):
            await self.stream_client.level_one_equity_unsubs([symbol])
        with suppress(Exception):
            await self.stream_client.chart_equity_unsubs([symbol])
        with suppress(Exception):
            await self.stream_client.nasdaq_book_unsubs([symbol])
        with suppress(Exception):
            await self.stream_client.nyse_book_unsubs([symbol])

        self.subscribed_symbols.discard(symbol)
        logger.info("Schwab stream symbol unsubscribed: %s", symbol)

    async def _subscribe_book(self, symbol: str) -> None:
        if self.stream_client is None:
            return

        # Schwab book entitlements can vary by symbol/listing venue, so try
        # both equity books independently and keep quote/chart streaming alive
        # even if one depth feed is unavailable.
        try:
            if not self.nasdaq_book_initialized:
                await self.stream_client.nasdaq_book_subs([symbol])
                self.nasdaq_book_initialized = True
            else:
                await self.stream_client.nasdaq_book_add([symbol])
        except Exception as exc:
            await self._publish(symbol, {"type": "book_error", "symbol": symbol, "payload": {"venue": "NASDAQ_BOOK", "error": str(exc)}})

        try:
            if not self.nyse_book_initialized:
                await self.stream_client.nyse_book_subs([symbol])
                self.nyse_book_initialized = True
            else:
                await self.stream_client.nyse_book_add([symbol])
        except Exception as exc:
            await self._publish(symbol, {"type": "book_error", "symbol": symbol, "payload": {"venue": "NYSE_BOOK", "error": str(exc)}})

    async def _run(self) -> None:
        restart_attempt = 0
        while True:
            try:
                while True:
                    if self.stream_client is None:
                        await asyncio.sleep(0.25)
                        continue
                    await self.stream_client.handle_message()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Schwab stream loop failed")
                self.last_error_at = datetime.now(UTC)
                self.last_error_message = str(exc)
                self._reset_runtime_state()

                async with self.lock:
                    active_symbols = list(self.actual_to_displays.keys())

                if not active_symbols:
                    return

                delay = STREAM_RESTART_DELAYS[min(restart_attempt, len(STREAM_RESTART_DELAYS) - 1)]
                restart_attempt += 1
                await asyncio.sleep(delay)

                async with self.runtime_lock:
                    async with self.lock:
                        symbols_to_restore = list(self.actual_to_displays.keys())

                    if not symbols_to_restore:
                        return

                    await self._start_client()
                    for actual in symbols_to_restore:
                        await self._subscribe_symbol(actual)

                self.restart_count += 1
                self.last_restarted_at = datetime.now(UTC)
                logger.warning("Schwab stream loop restarted after failure")
                restart_attempt = 0

    async def _handle_quote(self, message: dict[str, Any]) -> None:
        for item in message.get("content", []):
            symbol = item.get("SYMBOL") or item.get("key")
            if not symbol:
                continue
            await self._publish(normalize_symbol(symbol), {"type": "quote", "symbol": symbol, "payload": item})

    async def _handle_chart(self, message: dict[str, Any]) -> None:
        for item in message.get("content", []):
            symbol = item.get("SYMBOL") or item.get("key")
            if not symbol:
                continue
            await self._publish(normalize_symbol(symbol), {"type": "bar", "symbol": symbol, "payload": item})

    async def _handle_book(self, message: dict[str, Any]) -> None:
        venue = message.get("service")
        for item in message.get("content", []):
            symbol = item.get("SYMBOL") or item.get("key")
            if not symbol:
                continue
            await self._publish(
                normalize_symbol(symbol),
                {"type": "book", "symbol": symbol, "venue": venue, "payload": item},
            )

    async def _publish(self, symbol: str, event: dict[str, Any]) -> None:
        actual = normalize_symbol(symbol)
        self.published_event_count += 1
        self.last_event_at = datetime.now(UTC)
        displays = list(self.actual_to_displays.get(actual, set()))
        for display in displays:
            queues = list(self.symbol_queues.get(display, set()))
            if not queues:
                continue

            display_event = dict(event)
            display_event["symbol"] = display
            for queue in queues:
                if queue.full():
                    self.dropped_event_count += 1
                    with suppress(asyncio.QueueEmpty):
                        queue.get_nowait()

                with suppress(asyncio.QueueFull):
                    queue.put_nowait(display_event)

    async def get_status(self) -> StreamStatus:
        async with self.lock:
            registered_display_count = sum(len(queues) for queues in self.symbol_queues.values())
            return StreamStatus(
                started=self.started,
                stream_task_running=self.stream_task is not None and not self.stream_task.done(),
                has_client=self.stream_client is not None,
                quote_initialized=self.quote_initialized,
                chart_initialized=self.chart_initialized,
                nasdaq_book_initialized=self.nasdaq_book_initialized,
                nyse_book_initialized=self.nyse_book_initialized,
                registered_display_count=registered_display_count,
                active_display_symbol_count=len(self.symbol_queues),
                active_actual_symbol_count=len(self.actual_to_displays),
                subscribed_symbol_count=len(self.subscribed_symbols),
                published_event_count=self.published_event_count,
                dropped_event_count=self.dropped_event_count,
                restart_count=self.restart_count,
                start_count=self.start_count,
                shutdown_count=self.shutdown_count,
                queue_max_size=MAX_EVENT_QUEUE_SIZE,
                last_event_at=self.last_event_at,
                last_started_at=self.last_started_at,
                last_restarted_at=self.last_restarted_at,
                last_error_at=self.last_error_at,
                last_error_message=self.last_error_message,
                subscribed_symbols=sorted(self.subscribed_symbols),
            )

    def _reset_runtime_state(self) -> None:
        self.started = False
        self.quote_initialized = False
        self.chart_initialized = False
        self.nasdaq_book_initialized = False
        self.nyse_book_initialized = False
        self.subscribed_symbols.clear()
        self.stream_client = None

    async def shutdown(self) -> None:
        async with self.runtime_lock:
            self._cancel_idle_shutdown()
            await self._shutdown_runtime()

    async def _shutdown_runtime(self) -> None:
        task = self.stream_task
        self.stream_task = None

        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        client = self.stream_client
        self._reset_runtime_state()
        self.shutdown_count += 1

        if client is not None:
            with suppress(Exception):
                await client.logout()

        logger.info("Schwab stream client stopped")

    def _cancel_idle_shutdown(self) -> None:
        task = self.idle_shutdown_task
        if task and not task.done():
            task.cancel()
        self.idle_shutdown_task = None

    def _schedule_idle_shutdown(self) -> None:
        self._cancel_idle_shutdown()
        self.idle_shutdown_task = asyncio.create_task(self._idle_shutdown(), name="schwab-stream-idle-shutdown")

    async def _idle_shutdown(self) -> None:
        try:
            await asyncio.sleep(IDLE_SHUTDOWN_DELAY_SECONDS)

            async with self.lock:
                if self.symbol_queues:
                    return

            async with self.runtime_lock:
                async with self.lock:
                    if self.symbol_queues:
                        return

                await self._shutdown_runtime()
        except asyncio.CancelledError:
            pass


streaming_service = SchwabStreamingService()
