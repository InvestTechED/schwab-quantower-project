# Quantower Schwab Bridge Upgrade Review

Date: 2026-04-10

This document summarizes the upgrades made to connect Schwab market data and trading functionality into Quantower through a custom Schwab vendor bridge and a local Schwab backend.

## Current Addendum

Updated after Quantower UI testing on 2026-04-10:

```text
Backend Schwab live quote stream: PASS
Backend Schwab NASDAQ_BOOK Level II stream: PASS
Backend websocket forwarding quote events: PASS
Backend websocket forwarding book events: PASS
Quantower chart L1 header updates: PASS
Quantower DOM/Trading Ladder depth: RC1 FIX DEPLOYED / PENDING RETEST - plugin now pushes DOMQuote plus individual Level2Quote rows
Quantower Level II window: RC1 FIX DEPLOYED / PENDING RETEST
Quantower Time & Sales window: RC1 FALLBACK DEPLOYED / PENDING RETEST - plugin now publishes Last from live Level-One quote subscriptions; raw Schwab TIMESALE_EQUITY probe returned service unavailable
Quantower DOM scroll / off-market order placement usability: FAIL/OPEN - no visible scrollbar in current DOM layout
Quantower DOM gross profit/PnL: RC1 FIX DEPLOYED / PENDING RETEST - plugin now implements and forwards CalculatePnL from latest quote cache
Quantower ticker switching latency: RC1 FIX DEPLOYED / PENDING RETEST - initial snapshot fetch no longer blocks subscription setup
```

The backend is no longer the suspected blocker for AAPL Level II book data. Direct Schwab streaming and backend websocket tests both show live quote and book events. The remaining issues are now in the Quantower vendor event mapping, Quantower subscription behavior, or Quantower DOM/Time & Sales UI expectations.

RC1 bridge DLL build/deploy status:

```text
Build: PASS
Active Quantower vendor DLL updated: D:\Quantower\TradingPlatform\v1.146.4\bin\Vendors\SchwabVendor\SchwabVendor.dll
User-level vendor DLL updated: D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll
```

## Executive Summary

We created a custom Quantower vendor plugin named Schwab and connected it to a local Python/FastAPI backend powered by `schwab-py`.

The work has not been a single polished release cycle yet. It has been an iterative bring-up of a new broker/data-provider integration, including several reactive patches after live testing exposed missing Quantower behaviors. The most important current milestone is that Schwab live quote and Level II book data are now confirmed at the backend websocket layer after fixing a ticker-key parsing bug.

No global Quantower broker settings were intentionally changed. The restrictions and routing logic described below apply only to the custom Schwab add-on.

## Quantower Plugin

Project path:

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge`

Active Quantower deployment path:

`D:\Quantower\TradingPlatform\v1.146.4\bin\Vendors\SchwabVendor`

User-level Quantower deployment path:

`D:\Quantower\Settings\Scripts\Vendors`

Main plugin files:

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabConnectionScaffold.cs`

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\Services\SchwabBackendClient.cs`

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\BridgeSettings.cs`

## Backend

Project path:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation`

Backend path:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend`

Daily startup command:

```powershell
cd "D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend"
python -m uvicorn app.main:app --reload
```

One-click launcher added:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\Start_Schwab_Backend.bat`

## Implemented Features

### 1. Schwab Connection in Quantower

Added a custom Schwab vendor connector so Schwab appears in the Quantower connections list.

The plugin connects to the local backend at:

`http://127.0.0.1:8000`

The plugin uses:

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\BridgeSettings.cs`

Current backend routes used by the plugin:

```text
/api/auth/status
/api/market/snapshot/{symbol}
/api/market/bars/{symbol}
/api/stream/equities/{symbol}
/api/broker/accounts
/api/broker/positions
/api/broker/orders
/api/broker/orders/place
/api/broker/orders/{account_hash}/{order_id}
```

### 2. Schwab Authentication

Created Schwab OAuth/session support through the backend.

Token file:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\tokens\schwab_token.json`

Auth status endpoint:

`http://127.0.0.1:8000/api/auth/status`

Confirmed working:

```json
{
  "app_env": "production",
  "credentials_present": true,
  "schwab_py_available": true,
  "token_exists": true
}
```

### 3. Historical Chart Data

Quantower chart history was wired to Schwab backend bar endpoints.

Supported aggregations currently exposed:

```text
1 minute
5 minute
1 day
```

Quantower bridge code maps Quantower history requests to backend bar timeframes.

Main file:

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`

Backend routes:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend\app\routes\market.py`

### 4. Live Quote Streaming

Implemented backend websocket stream:

`/api/stream/equities/{symbol}`

Initial implementation subscribed to Schwab:

```text
LEVELONE_EQUITIES
CHART_EQUITY
```

Quantower bridge subscribes to the backend websocket and pushes:

```text
Quote
DayBar
Last
```

Important bug fixed:

Schwab stream payloads used `key` for the ticker, while our handlers were only checking `SYMBOL`. This caused live events to be silently discarded.

Fixed in:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend\app\services\stream.py`

The handlers now accept:

```python
item.get("SYMBOL") or item.get("key")
```

Verified after fix:

```text
AAPL websocket received quote events: yes
AAPL websocket received book events: yes
```

### 5. Level II / DOM Book Streaming

Added first-pass Level II support.

Backend now attempts Schwab:

```text
NASDAQ_BOOK
NYSE_BOOK
```

Quantower bridge now handles:

```text
SubscribeQuoteType.Level2
```

Quantower bridge maps Schwab book messages into:

```text
DOMQuote
Level2Quote
```

Main backend file:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend\app\services\stream.py`

Main Quantower file:

`D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`

Current validation:

Direct `schwab-py` test confirmed Schwab sends live `NASDAQ_BOOK` updates for `AAPL`.

Backend websocket test confirmed after the `key`/`SYMBOL` fix:

```text
quote events: yes
book events: yes
```

Still needs Quantower UI validation:

```text
DOM ladder bid/ask rows populate
Level II window populates
Rows update live to the second
```

### 6. Time & Sales

Quantower `Last` events are emitted from live Level-One quote updates using Schwab last-trade fields.

Quantower symbol flags were enabled:

```text
AllowCalculateRealtimeTicks = true
AllowCalculateRealtimeTrades = true
```

Current limitation:

The installed `schwab-py` package did not expose a dedicated `TIMESALE_EQUITY` helper in the local API surface. We currently feed Quantower Time & Sales using Level One `LAST_PRICE`, `LAST_SIZE`, and trade timestamps. This may be enough for visible updates, but it is not yet proven to be a full tick-by-tick tape equivalent.

RC1 correction:

```text
Completed bar/candle aggregate volume is no longer pushed as Time & Sales.
The bridge now uses Schwab TRADE_TIME_MILLIS / QUOTE_TIME_MILLIS and creates deterministic Last trade IDs.
The bridge now publishes Last events when Quantower has either a Quote or Last subscription for the symbol.
Raw `TIMESALE_EQUITY` was probed directly through Schwab's generic streamer API and Schwab returned service unavailable.
```

Still needs validation:

```text
Open Quantower Time & Sales for AAPL
Confirm rows populate
Confirm prints update live
Confirm timestamps are current
```

### 7. Account and Positions

Added Schwab account and position support to Quantower.

Quantower now calls backend endpoints:

```text
/api/broker/accounts
/api/broker/positions
```

Positions are mapped into Quantower open positions.

Known improvement area:

Position display in the DOM needs continued validation. Earlier screenshots showed positions visible in the side panel, but the bottom ladder row behavior still needs retesting after the latest DLL/backend changes.

RC1 PnL update:

```text
The Quantower vendor now implements CalculatePnL using the latest live quote cache and Schwab open position data.
GrossPnL and NetPnL are returned in USD when a current price is available.
The outer Quantower `SchwabVendor` wrapper now forwards CalculatePnL calls to the inner vendor. Without this forwarding, Quantower falls back to default `N/A` PnL behavior.
```

### 8. Order Routing

Added initial Schwab order routing from Quantower through the local backend.

Implemented actions:

```text
Place limit order
Cancel active order
Fetch working orders
Fetch historical Schwab orders from backend
```

Important safety decision:

This bridge is for Schwab only. It does not change IBKR, Alpaca, or other Quantower broker connection behavior.

### 9. Order Guardrails

Added Schwab-only restrictions while the new order-routing integration is being tested.

Current restrictions:

```text
Max order size: 1 whole share
Fractional shares: blocked
Market orders: blocked
Limit orders: allowed
Extended-hours routing: allowed through Schwab SEAMLESS session
Duplicate order protection: enabled
Limit-price deviation guard: enabled
Notional cap: enabled
Preview-before-place: enabled
```

Allowed Schwab instructions:

```text
BUY
SELL
SELL_SHORT
BUY_TO_COVER
```

Quantower only sends side intent as Buy/Sell, so the bridge infers:

```text
Buy while short -> BUY_TO_COVER
Buy while flat/long -> BUY
Sell while long -> SELL
Sell while flat/short -> SELL_SHORT
```

Backend settings:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend\app\config.py`

Example environment settings:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\.env.example`

Relevant env vars:

```text
SCHWAB_TRADING_ENABLED=true
SCHWAB_MAX_ORDER_SHARES=1
SCHWAB_MAX_ORDER_NOTIONAL=500
SCHWAB_LIMIT_PRICE_MAX_DEVIATION_PCT=20
SCHWAB_EXTENDED_HOURS_ENABLED=true
SCHWAB_DUPLICATE_WINDOW_SECONDS=5
```

### 10. Working Order Bug Fix

Major bug discovered:

Quantower working-orders window showed old filled/replaced Schwab orders as if they were still working.

Fix:

The bridge now filters open orders to active/cancelable statuses only.
The backend now returns Schwab order prices, and the Quantower bridge sets `MessageOpenOrder.Price` so limit orders do not render at `0.00`.
The Quantower bridge now polls Schwab order status and pushes close-order messages for canceled, filled, expired, rejected, or replaced orders. This is intended to clear orders that were canceled directly at Schwab instead of from Quantower.

Active statuses include:

```text
ACCEPTED
AWAITING_PARENT_ORDER
AWAITING_CONDITION
AWAITING_STOP_CONDITION
AWAITING_MANUAL_REVIEW
PENDING_ACTIVATION
QUEUED
WORKING
NEW
PARTIAL_FILL
PARTIALLY_FILLED
```

`REPLACED` is now mapped as canceled/not working rather than open.

Backend cancellation was also hardened:

Cancel requests are validated against the current active Schwab order set before sending to Schwab.

### 11. Audit Logging

Added backend trading audit logging.

Audit log path:

`D:\GitHub\Claude Code\apps\schwab-equities-workstation\backend\logs\schwab_trading_audit.jsonl`

Logged actions include:

```text
preview
place
cancel
```

### 12. Kill Switch

Implemented backend kill switch through:

```text
SCHWAB_TRADING_ENABLED
```

If disabled:

```text
New Schwab order preview/place requests are rejected
```

Cancels remain allowed by design, so turning off trading does not trap active orders.

Not implemented yet:

```text
Quantower button for toggling the kill switch
Separate local control-panel UI
Runtime toggle endpoint
```

Recommendation:

Keep the backend kill switch as the source of truth even if a button is later added.

## Deployment Actions Performed

Built bridge project with:

```powershell
dotnet build "D:\GitHub\Claude Code\apps\schwab-quantower-bridge\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj"
```

Deployed DLL to:

```text
D:\Quantower\TradingPlatform\v1.146.4\bin\Vendors\SchwabVendor\SchwabVendor.dll
D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll
```

Deployed icon:

```text
D:\Quantower\TradingPlatform\v1.146.4\bin\Vendors\SchwabVendor\schwab.svg
D:\Quantower\Settings\Scripts\Vendors\schwab.svg
```

Known deployment concern:

Quantower sometimes leaves `Starter.exe` and `CefSharp.BrowserSubprocess` processes running after the visible app is closed. These can lock the DLL and prevent deployment until stopped.

## Current Validation Status

Confirmed working:

```text
Schwab auth token exists
Backend auth status endpoint responds
Schwab quote endpoint works
Schwab historical bars work
Quantower can show Schwab connection
Quantower can load Schwab chart data
Quantower can see Schwab account/position data
Backend direct Schwab stream receives LEVELONE_EQUITIES
Backend direct Schwab stream receives NASDAQ_BOOK
Backend websocket now forwards quote events
Backend websocket now forwards book events
Order size > 1 share is rejected
Fractional orders are rejected
Market orders are rejected
```

Needs UI retesting in Quantower:

```text
DOM ladder bid/ask columns populate
DOM ladder updates to the second
Level II window populates
Time & Sales window populates
Open positions render correctly in ladder bottom row
Cancel working order behaves correctly after active-order filter
1-share limit order placement still works after latest changes
```

## Known Limitations and Risks

### Level II Venue Coverage

Current implementation subscribes to:

```text
NASDAQ_BOOK
NYSE_BOOK
```

Direct testing showed `NASDAQ_BOOK` updates for AAPL. Further testing is needed for NYSE-listed stocks and symbols with different primary venues.

### Time & Sales Fidelity

The current implementation uses Level One last-trade fields, not a confirmed dedicated Schwab Time & Sales stream. This may update the QT Time & Sales window, but it should not yet be treated as a full institutional tick tape until validated.

### Quantower Ladder Display Settings

Spread/row aggregation and scroll behavior may be controlled by Quantower DOM UI settings. The plugin currently advertises a `0.01` tick size. If Quantower exposes additional vendor-side ladder aggregation controls, those have not been implemented.

### Testing Has Been Reactive

Several fixes were made after live UI behavior revealed missing support. This was effective for discovery, but not efficient as a release process.

Recommended process from here:

```text
1. Freeze current feature scope.
2. Create a formal test matrix.
3. Validate market data first: quotes, charts, Level II, Time & Sales.
4. Validate account/position display second.
5. Validate order routing last, using 1-share limit orders only.
6. Only then expand order types, size limits, or additional brokers.
```

## Next Recommended Work

### Immediate

```text
Retest Quantower DOM after latest backend stream fix.
Retest Quantower Level II window.
Retest Quantower Time & Sales window.
Capture logs/screenshots if any window remains blank.
```

### Short-Term Code Improvements

```text
Add explicit stream diagnostics endpoint.
Add backend counters for quote/book/last events per symbol.
Add Quantower-side logging when Level2 events are pushed.
Add a safe local kill-switch button outside QT or inside a small control panel.
Add a release/deploy script that closes leftover QT processes safely and copies DLLs.
```

### Medium-Term

```text
Improve Time & Sales fidelity if Schwab exposes a true timesale service.
Add Level II support for more venues if needed.
Add configurable ladder depth/aggregation if Quantower API supports it.
Add a full UI test checklist for every bridge release.
```

## Important Boundary

All restrictions and guardrails described here are intended for the custom Schwab add-on only.

They should not alter:

```text
Interactive Brokers connection
Alpaca connection
Any native Quantower broker connection
Quantower global membership/settings
Quantower order settings outside Schwab
```
