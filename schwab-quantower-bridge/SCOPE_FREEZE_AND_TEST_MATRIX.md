# Schwab Quantower Bridge Scope Freeze and Test Matrix

Date: 2026-04-10

Status: Pre-production stabilization

## Purpose

This document freezes the current Schwab Quantower bridge scope and defines the test matrix required before any further feature expansion.

The goal is to stop reactive live patching. From this point, changes should be grouped into deliberate release candidates, tested against this matrix, and deployed only after the affected tests pass.

## Scope Freeze

Frozen scope means:

```text
No new broker connectors.
No new order types.
No increase above the 1-share Schwab test limit.
No options/derivatives routing.
No UI redesign.
No new analysis features.
No IBKR bridge work until Schwab market data and order basics are stable.
```

Allowed work during freeze:

```text
Fix Schwab quote, chart, DOM, Level II, and Time & Sales reliability.
Fix Schwab account, position, working-order, and cancel behavior.
Add diagnostics needed to prove whether data is flowing.
Add deployment/restart scripts that reduce manual error.
Document known limitations.
```

## Current Release Candidate Goal

Release candidate name:

```text
Schwab Bridge RC1 - Market Data and 1-Share Limit Order Validation
```

RC1 success means:

```text
Quantower can connect to Schwab.
Charts load and update.
DOM/Trading Ladder receives live Level II rows.
Level II window receives live rows.
Time & Sales receives visible live prints or is formally marked Level-One-only.
Accounts and positions display correctly.
Only 1-share Schwab limit orders can be placed.
Working orders show only active/cancelable orders.
Cancels work for active orders.
Other Quantower broker connections are not affected.
```

## Release Gate

No more ad-hoc live patches directly into the tested environment unless the change is classified as:

```text
P0 - prevents startup, corrupts order routing, or creates live trading risk
P1 - blocks the current test matrix
```

Every change must have:

```text
Problem statement
Suspected layer: Schwab API, backend, Quantower bridge, or Quantower UI
File list
Validation command or manual test
Pass/fail result
Known residual risk
```

## Test Matrix

### 1. Startup and Connection

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| CONN-01 | Start backend with `Start_Schwab_Backend.bat` | Backend starts without stack trace | Pending |
| CONN-02 | Open `/api/auth/status` | `credentials_present=true`, `token_exists=true` | Pass |
| CONN-03 | Open Quantower and connect Schwab | Connection status becomes connected | Pending retest |
| CONN-04 | Disconnect and reconnect Schwab | Reconnect succeeds without restarting QT | Pending |

### 2. Quote and Chart Data

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| MD-01 | Open AAPL chart | Chart loads historical bars | Pass previously, retest |
| MD-02 | Watch AAPL L1 header | Last/volume/date update live | Pass |
| MD-03 | Load 1-minute chart | 1m bars populate | Pass previously, retest |
| MD-04 | Load 5-minute chart | 5m bars populate | Pass previously, retest |
| MD-05 | Load 1-day chart | Daily bars populate | Pass previously, retest |

### 3. DOM / Trading Ladder

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| DOM-01 | Open AAPL DOM | DOM opens without error | Pass |
| DOM-02 | Subscribe to AAPL DOM | Bid/ask quantity columns fill with live sizes | Fail/Partial |
| DOM-03 | Wait 30 seconds | DOM rows update to current second | Fail/Partial |
| DOM-04 | Scroll below inside market | User can select a price below market for test order | Fail/Open |
| DOM-05 | Confirm ladder depth | More than top 3-4 levels visible if Schwab provides them | RC1 fix deployed, pending retest |
| DOM-06 | Compare backend book sample to QT DOM | QT shows same nearby bid/ask levels as backend book | Pending |

### 4. Level II Window

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| L2-01 | Open AAPL Level II window | Window opens | Pending |
| L2-02 | Subscribe through Schwab connection | Rows populate with bid/ask market depth | Pending |
| L2-03 | Wait 30 seconds | Rows update live to the second | Pending |
| L2-04 | Test NYSE-listed symbol, e.g. JNJ | Book rows populate or limitation is logged | Pending |

### 5. Time & Sales

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| TS-01 | Open AAPL Time & Sales | Window opens | Pending |
| TS-02 | Subscribe through Schwab connection | Prints populate | Pass visually on INTC |
| TS-03 | Wait 30 seconds | Prints update with current timestamps | Pass visually on INTC |
| TS-04 | Confirm feed source | Dedicated timesale stream or Level-One-last fallback is documented | Level-One-last fallback documented |

### 6. Account and Position Display

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| ACCT-01 | Open account selector | Schwab account appears | Pass previously, retest |
| POS-01 | Open positions panel | Current Schwab positions appear | Pass previously, retest |
| POS-02 | Open DOM with current position symbol | Position quantity/average appears correctly | Partial/Open |
| POS-04 | Confirm DOM gross profit | Gross profit/loss updates from latest live quote | RC1 fix deployed, pending retest |
| POS-03 | Compare with Schwab source data | Quantower position matches backend `/api/broker/positions` | Pending |

### 7. Order Routing Guardrails

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| ORD-01 | Attempt 2-share Schwab order | Rejected before Schwab placement | Pass |
| ORD-02 | Attempt fractional Schwab order | Rejected before Schwab placement | Pass |
| ORD-03 | Attempt market Schwab order | Rejected before Schwab placement | Pass |
| ORD-04 | Place 1-share limit buy below market | Accepted by Schwab or clear reject shown | Pending |
| ORD-05 | Working orders window after placement | Only active order appears | Pending |
| ORD-06 | Cancel active test order | Cancel succeeds | Pending |
| ORD-07 | Working orders after cancel | Canceled order disappears from working list | Pending |

### 8. Isolation From Other Brokers

| ID | Test | Expected | Status |
| --- | --- | --- | --- |
| ISO-01 | Open native IBKR connection after Schwab plugin loaded | IBKR settings unchanged | Pending after plan upgrade |
| ISO-02 | Schwab order guardrail check | 1-share guardrail applies only to Schwab | Pending |
| ISO-03 | Other Quantower connections list | Other broker connectors remain visible/unchanged | Pass visually, retest |

## Current Blockers

```text
BLOCKER-01: Cleared for RC1. Quantower Time & Sales displays live data using Level-One-last fallback; raw Schwab TIMESALE_EQUITY probe returned service unavailable.
BLOCKER-02: Quantower DOM/Trading Ladder shows partial or insufficient book depth. RC1 Level2Quote push fix deployed, pending retest.
BLOCKER-03: Quantower DOM has no visible scrollbar or easy price navigation for off-market test orders.
BLOCKER-04: Level II window has not yet been confirmed with live rows.
BLOCKER-05: DOM gross profit/PnL was not calculated. RC1 CalculatePnL implementation and outer-vendor forwarding fix deployed, pending retest.
```

## RC1 Code Changes Deployed

```text
Build: PASS
DLL deployed to D:\Quantower\TradingPlatform\v1.146.4\bin\Vendors\SchwabVendor
DLL deployed to D:\Quantower\Settings\Scripts\Vendors
Time & Sales: live Last events now use TRADE_TIME_MILLIS / QUOTE_TIME_MILLIS and deterministic trade IDs.
Time & Sales: Last fallback now publishes when Quantower has either a Quote or Last subscription for the symbol.
DOM depth: book updates now push DOMQuote plus individual Level2Quote bid/ask rows.
DOM PnL: vendor CalculatePnL now returns GrossPnL and NetPnL from latest quote cache.
DOM PnL: outer SchwabVendor now forwards CalculatePnL to the inner market/trading vendor.
Ticker switching: snapshot fetch now runs in the background after stream subscription begins.
Order diagnostics: C# backend client now surfaces backend/Schwab rejection details instead of generic Bad Request.
Order sync: backend now exposes Schwab order price so Quantower does not anchor working orders at 0.00.
Order sync: plugin now pushes close-order updates for terminal Schwab statuses so externally canceled orders do not remain as working orders.
```

## Diagnostic Plan

Next diagnostic step should not be another blind patch. It should isolate the layer:

```text
1. Backend websocket diagnostic endpoint/sample for AAPL quote/book/last counts.
2. Quantower plugin logging when it receives quote/book events from websocket.
3. Quantower plugin logging when it calls PushMessage for Quote, Last, DOMQuote, and Level2Quote.
4. Quantower UI retest with screenshots: DOM, Level II, Time & Sales.
5. Only then patch the exact failing mapping.
```

## Decision Required: Time & Sales

The installed `schwab-py` package exposes Level One and book streams but does not expose an obvious dedicated `TIMESALE_EQUITY` helper.

Choose one:

```text
Option A: Treat Time & Sales as Level-One-last-event based for RC1, and document the limitation.
Option B: Investigate Schwab streamer internals and implement a custom TIMESALE_EQUITY service if the Schwab backend accepts it.
Option C: Block RC1 until Time & Sales is true tick-by-tick.
```

Recommendation:

```text
Use Option A for RC1 only if Quantower Time & Sales displays stable live prints. If it remains blank, move to Option B before any release.
```
