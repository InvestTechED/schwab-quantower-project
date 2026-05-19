# Schwab Quantower Bridge Troubleshooting

Use this file as the first reference before deep investigation.

Authoring rule for all future entries:
- do not add short-form issue notes only
- every new recurring bug, production issue, or recovery sequence must use the full format below
- required sections for each issue:
  - Symptoms
  - Meaning
  - Checks
  - What Was Researched
  - What Did Not Help
  - Root Cause
  - Fix
  - Verification
  - Acceptance Check
  - Notes
- if a fix was validated only after reopening QT, reconnecting Schwab, opening a new DOM window, or any other UI refresh step, include that explicitly
- consult this file before deep research to reduce repeated investigation and token waste

Goals:
- recover quickly
- avoid repeating known mistakes
- reduce token waste
- preserve the stable QT market-data baseline

## Quick Triage

Run these first when QT says Schwab is connected but data is missing:

1. Bridge health
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/health'
```

2. Schwab market snapshot
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/market/snapshot/INTC'
```

3. Stream status
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/stream/status'
```

4. Broker accounts
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/accounts'
```

Healthy baseline:
- `/api/health` => `200`
- `/api/market/snapshot/INTC` => `200`
- `/api/broker/accounts` => `200`

## Known Good Paths

Correct bridge repo root:
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge`

Live Quantower vendor bundle:
- `D:\Quantower\TradingPlatform\v1.145.17\bin\Vendors\SchwabVendor`

Live Quantower POS-only vendor bundle:
- current reinstalled QT path: `D:\Quantower\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`
- older QT path: `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`
- QT connection name: `Schwab POS ONLY/NO DATA`
- purpose: Schwab account, positions, orders, order actions, trades, and account P/L only
- market data is intentionally disabled; use dxFeed for Quotes & Trades, Tick History, Minute History, Day History, and Volume analysis in QT symbol mapping
- do not install POS-only DLLs into the original `SchwabVendor` folder

Disabled legacy script DLL (must stay disabled to avoid duplicate assembly load):
- `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll.disabled-20260420_092609`

Quantower bridge launcher:
- `D:\Quantower\Start Schwab Bridge.bat`

Bridge token path:
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\tokens\schwab_token.json`

Bridge env path:
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\.env`

Quantower API reference from QT dev team:
- `D:\Quantower\Quantower _ API Documentatation _ High Impact Classes.docx`
- relevant classes to check before deep QT behavior changes:
  - `DepthOfMarket`
  - `HistoricalData`
  - `Symbol`
  - quote/last/mark/level2 processing methods

## Issue 01: QT Connected But No Data

Symptoms
- QT shows Schwab connected
- charts, quotes, DOM, or Level II stay empty

Meaning
- the QT connection shell is up
- but live bridge streaming or Schwab auth is not healthy

Checks
- `/api/stream/status`
- `/api/market/snapshot/INTC`

Fix
- identify whether the problem is stream startup or Schwab auth
- if `snapshot` returns `502`, go to Issue 03
- if stream is stale, go to Issue 02

Verification
- `/api/market/snapshot/INTC` returns `200`
- QT symbol data starts populating

---

## Issue 02: Bridge Already Running Or Stale Runtime

Symptoms
- launcher says bridge is already running
- no visible bridge window opens
- QT may appear connected but data is inconsistent or dead

Meaning
- a background Python bridge process is present
- launcher exits early because an old process makes the bridge look active

Checks
```powershell
Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -like '*uvicorn*app.main:app*' } | Select-Object ProcessId,CommandLine
```

Fix
```powershell
Stop-Process -Id <PID> -Force
Start-Process -FilePath python -ArgumentList '-m','uvicorn','--app-dir','backend','app.main:app' -WorkingDirectory 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge'
```

Verification
- `/api/health` returns `200`
- port `8000` is listening

---

## Issue 03: Snapshot Requests Return 502

Symptoms
- `GET /api/market/snapshot/...` returns `502 Bad Gateway`
- logs contain:
  - `refresh_token_authentication_error`
  - `unsupported_token_type`

Meaning
- Schwab is rejecting the saved refresh token
- bridge can run, but live Schwab data calls fail

Checks
- inspect bridge log for the auth error
- run:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/market/snapshot/INTC'
```

Fix
- regenerate the Schwab token using Issue 04

Verification
- `/api/market/snapshot/INTC` returns `200`
- `/api/broker/accounts` returns `200`

Notes
- this is not a QT license issue

---

## Issue 04: Regenerate Schwab Token

Symptoms
- Schwab rejects the refresh token
- old token family is no longer accepted
- bridge can still start, but live Schwab requests fail
- common errors include:
  - `refresh_token_authentication_error`
  - `unsupported_token_type`
  - `Failed refresh token authentication`

Meaning
- the active token file must be replaced with a fresh one
- the saved refresh token is no longer valid with Schwab
- this is an auth-state problem, not a QT connection-shell problem
- this is also not a QT license problem

Checks
- token path:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\tokens\schwab_token.json`
- confirm bridge root is correct:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge`
- confirm the failing endpoints really are auth-related:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/market/snapshot/INTC'
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/accounts'
```

Fix
Run from:
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\backend`

Command:
```powershell
python init_schwab_session.py --force-login
```

Expected callback URL:
- `https://127.0.0.1:8182`

What it does:
- backs up any existing token
- starts browser-assisted Schwab login
- writes a fresh token to the correct bridge token path

What Was Researched
- verified the correct bridge runtime path
- verified that token and `.env` files belong under:
  - `schwab-quantower-bridge`
- verified that the older broken flow was accidentally looking in the wrong application path during prior drift
- traced live bridge failures to auth exceptions instead of QT startup or vendor DLL problems

What Did Not Help
- restarting QT by itself
- restarting the bridge without replacing the token
- treating the error as a stream-only or DOM-only issue
- treating the error as a QT account or QT license restriction
- checking market-data code before confirming auth health

Root Cause
- the Schwab refresh token in the active token file was no longer accepted
- the bridge was healthy enough to launch but not healthy enough to complete live Schwab API calls
- because of that, QT could appear connected while live snapshot/account requests still failed

Implementation Notes
- the token regeneration flow was hardened so the bridge can:
  - back up the old token first
  - run a forced browser login flow
  - write the new token to the correct bridge token folder
- the expected active file is:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\tokens\schwab_token.json`

Verification
```powershell
Get-ChildItem 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\tokens' -Force | Select-Object Name,Length,LastWriteTime
```

Expected:
- a fresh `schwab_token.json`
- a backup file like `schwab_token.YYYYMMDDTHHMMSSZ.bak.json`

After regeneration:
1. start the bridge
2. verify `/api/market/snapshot/INTC` returns `200`
3. start QT

Acceptance Check
- fresh `schwab_token.json` exists in the bridge token path
- backup token file exists
- `/api/market/snapshot/INTC` returns `200`
- `/api/broker/accounts` returns `200`
- QT data can populate again

---

## Issue 05: Token Regeneration Appears To Run But Error Persists

Symptoms
- forced login was attempted
- same auth error still appears
- bridge logs still show the same token digest or same refresh-token rejection
- snapshot/account endpoints still fail after the login flow appears to finish

Meaning
- regeneration did not complete
- or the active token file was not replaced
- or the login flow wrote to the wrong location
- or the bridge is still reading an older token than expected

Checks
```powershell
Get-ChildItem 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\tokens' -Force | Select-Object Name,Length,LastWriteTime
```
- verify the active token file timestamp changed
- verify a backup file was created
- verify the bridge root and token path are under:
  - `schwab-quantower-bridge`
- compare current auth errors to see whether the same token digest keeps recurring

Fix
- confirm active `schwab_token.json` timestamp changed
- confirm a new backup file was created
- rerun Issue 04 if the active token was not replaced
- if the active token was written to the wrong project path, move back to the correct bridge path and rerun regeneration there
- restart the bridge only after the correct active token is confirmed

Verification
- active token has a fresh timestamp
- Schwab auth errors disappear

Notes
- if the same token digest keeps appearing in the logs, the active token was not truly replaced
- this usually means the replacement token either:
  - did not overwrite the active bridge token file
  - was created in the wrong folder
  - or the bridge was still pointed at the wrong runtime path

What Did Not Help
- assuming the login flow succeeded just because the browser opened
- assuming a new token was active without checking timestamps
- restarting QT before confirming the bridge endpoints recovered

Acceptance Check
- active `schwab_token.json` timestamp is newer than before
- a backup token file exists
- snapshot and accounts endpoints both return `200`
- auth error no longer appears in bridge logs

---

## Issue 06: QT Vendor Stream Stops After Backend Churn

Symptoms
- QT had subscriptions earlier
- vendor streams exited for active symbols
- backend recovered
- active symbol streams did not resume automatically

Meaning
- per-symbol WebSocket streams died and were not rehydrated

Checks
- inspect vendor log:
  - `%LOCALAPPDATA%\SchwabQuantowerBridge\SchwabVendor.debug.log`

Fix
- use the patched vendor that re-ensures active symbol streams during market-state pulse
- if needed, rebuild and redeploy the DLL using Issue 07

Verification
- vendor log shows stream reconnection attempts and active symbol recovery
- QT symbols resume data after backend recovery

---

## Issue 07: Rebuild And Deploy Vendor DLL

Symptoms
- vendor code was patched
- live Quantower still appears to use old behavior

Meaning
- the updated DLL has not been deployed yet

Checks
- compare build and live DLL hashes if needed

Fix
Build:
```powershell
dotnet build "D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj" -c Release
```

Deploy:
```powershell
Copy-Item 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\bin\Release\net8.0-windows\*' 'D:\Quantower\TradingPlatform\v1.145.17\bin\Vendors\SchwabVendor' -Recurse -Force
```

Verification
- live DLL timestamp or hash matches the built DLL

---

## Issue 09: QT License Limitation Versus Bridge Failure

Symptoms
- Option Analytics shows restricted strikes or license messages
- broad quotes, DOM, or Level II may also appear empty

Meaning
- some QT features can be license-limited
- but broad market-data emptiness is usually bridge auth or streaming, not QT licensing

Checks
- look for explicit QT license message
- compare with bridge endpoint health and snapshot behavior

Fix
- if only a feature-specific cap exists, this may be a QT license issue
- if snapshots return `502` or stream status is unhealthy, follow the bridge issues above

Verification
- correct root cause identified before making changes

---

## Issue 10: DOM Footer Shows `---` For Average Price And Gross P/L

Symptoms
- top Positions grid shows live open positions correctly
- DOM ladder and Level II are working
- bottom DOM footer still shows:
  - `Average open price` => `---`
  - `Gross Profit / Loss` => `---`
- after the fix is deployed, a brand new `DOM Trader` window may show the values while an older already-open DOM window may still stay blank

Meaning
- this is not a ladder or Level II market-data failure
- this is not a general positions failure
- the problem is in the DOM footer position/PnL binding path for the selected symbol/account
- Quantower is requesting PnL for the selected position context, but the bridge was not resolving that request robustly enough

Checks
1. Confirm the position actually exists in QT:
- Positions grid should show the symbol, avg price, qty, and gross P/L

2. Confirm ladder data is healthy:
- DOM price ladder is updating
- Level II / book columns are populated

3. Confirm the position bar is not hidden:
- in the DOM panel settings, `ShowPositionBar` must be enabled

4. Confirm this is footer-only:
- the issue is present only in the bottom DOM footer, not in the top Positions grid

What Was Researched
- reviewed bridge vendor position and PnL code in:
  - `schwab-quantower-bridge/src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`
- compared with Quantower example vendors under:
  - `D:\GitHub\Claude Code\tmp\quantower-examples`
- decompiled installed Quantower SDK types from:
  - `D:\Quantower\TradingPlatform\v1.146.5\bin\TradingPlatform.BusinessLayer.dll`
- decompiled installed DOM panel code from:
  - `D:\Quantower\TradingPlatform\v1.146.5\bin\plug-ins\LadderViewPanel\LadderViewPanel.dll`

Important SDK Findings
- `Position.UpdateByMessage(MessageOpenPosition)` hydrates:
  - `PositionId`
  - `AccountId`
  - `SymbolId`
  - `OpenPrice`
  - `OpenTime`
  - `Quantity`
  - `Side`
- Quantower `ForceRecalculatePnl()` builds `PnLRequestParameters` using:
  - `Symbol`
  - `Account`
  - `OpenPrice`
  - `ClosePrice`
  - `Side`
  - `Quantity`
  - `PositionId`
- therefore the footer can depend on more than just `PositionId`

What Did Not Help
- assuming this was a ladder or Level II problem
- changing or investigating DOM book subscription logic for this issue
- treating the missing footer values as a QT license issue
- pushing position messages alone as the only fix:
  - this was tried because Quantower example vendors use context updates
  - in this bridge/vendor base class, that by itself was not sufficient
- relying only on local latest-price cache for PnL calculation
- assuming `PositionId` alone would always be enough to resolve the position for footer PnL

Root Cause
- the bridge position/PnL path was too narrow for the way Quantower requested footer PnL
- bridge-side `CalculatePnL()` originally depended mainly on:
  - `parameters.PositionId`
  - local last-price cache
- in practice, Quantower can supply:
  - `Account`
  - `Symbol`
  - `ClosePrice`
  - `PositionId`
- the bridge needed to resolve positions by the broader request context, not just one key

Fix
Updated:
- `schwab-quantower-bridge/src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`

Bridge changes:
1. Added a position cache keyed by:
- `accountHash:symbol`
- `symbol`

2. Populated that cache in `GetPositions()`

3. Re-pushed fetched open position messages through the vendor message pipeline
- this helps Quantower refresh live position state for the panel

4. Hardened `CalculatePnL()` so it resolves positions by:
- `PositionId`
- `AccountId + SymbolId`
- `SymbolId`

5. Updated `CalculatePnL()` to use Quantower-supplied `ClosePrice` when present
- this aligns with the actual SDK PnL request flow

6. Corrected fallback signed-quantity math
- long and short PnL both calculate correctly

Verification
- rebuild vendor DLL
- deploy to:
  - `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll`
- restart Quantower
- reconnect Schwab
- open a `new` DOM Trader window for a symbol with an open position
- confirm footer now shows:
  - average open price
  - gross profit / loss

Known UI Note
- an already-open DOM window may not immediately pick up the refreshed footer binding
- opening a brand new `DOM Trader` window after the DLL update can show the correct values even when the old window stays blank
- treat that as a UI refresh behavior, not as a bridge failure, if the new DOM window shows correct values

Deferred Diagnostic Pass
- date:
  - `2026-04-17`
- purpose:
  - verify whether the original fix path had actually regressed before making any new functional code changes
- what was verified:
  - live backend health:
    - `http://127.0.0.1:8000/api/health` returned `200`
  - live Schwab positions endpoint:
    - `http://127.0.0.1:8000/api/broker/positions` returned `200`
  - live backend payload still included the fields required by the DOM footer path:
    - `average_price`
    - `market_price`
    - `unrealized_profit_loss`
  - live bridge source still contained the proven fix path in:
    - `src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`
  - confirmed present in source:
    - `GetPositions(...)` caches positions and pushes `MessageOpenPosition`
    - `CreatePosition(...)` maps `OpenPrice`
    - `CalculatePnL(...)` exists and resolves via:
      - `PositionId`
      - `AccountId + SymbolId`
      - `SymbolId`
    - `SetLatestPrice(...)` refreshes cached positions and republishes them
  - live scaffold still delegated:
    - `GetPositions(...)`
    - `CalculatePnL(...)`
  - live deployed DLL existed at:
    - `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll`
- conclusion from this diagnostic round:
  - the issue was not explained by missing backend fields
  - the issue was not explained by missing bridge-side position mapping
  - the issue was not explained by a missing `CalculatePnL(...)` override
  - the issue was not explained by an obviously old DLL missing the prior footer fix path
  - remaining likely cause:
    - a Quantower runtime binding/context mismatch for Schwab DOM footer requests
    - not a missing-feature problem in the bridge architecture itself
- diagnostic instrumentation added:
  - temporary bridge logging was added around:
    - `GetPositions(...)`
    - `CreatePosition(...)`
    - `CalculatePnL(...)`
    - `SetLatestPrice(...)`
    - `RepublishCachedPositions(...)`
  - intent:
    - verify whether QT is actually calling `CalculatePnL(...)` for Schwab DOM footer rendering
    - capture:
      - `symbol`
      - `account`
      - `positionId`
      - `quantity`
      - `openPrice`
      - cache resolution path
- deployment state for that diagnostic pass:
  - rebuilt the bridge successfully
  - deployed the diagnostic DLL to:
    - `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll`
  - deployed DLL verification at the time of defer:
    - length: `141824`
    - timestamp: `2026-04-17 5:31:00 PM`
- what did not help in this round:
  - re-assuming the problem was missing backend data
  - re-assuming the prior fix had simply disappeared
  - making a new speculative functional change before verifying live backend and live DLL state
- deferred decision:
  - issue deferred because the user could use the Positions grid above DOM as a workable fallback
  - if resumed later, start with the diagnostic log output first before any new code changes
- next-step checklist when resumed:
  1. reproduce the Schwab DOM footer issue once with the diagnostic DLL active
  2. read `SchwabVendor.debug.log`
  3. determine whether QT:
     - does not call `CalculatePnL(...)`
     - calls it with a Schwab `PositionId` mismatch
     - calls it with an `AccountId/SymbolId` mismatch
     - receives refreshed positions but does not bind them to the active Schwab DOM instance
  4. only then make one narrow fix in the Schwab position/PnL path

Safe Scope Rule
- this issue should be handled only in the position/PnL path
- do not treat it as a ladder, DOM book, or Level II subscription bug unless those data streams are also actually broken

Acceptance Check
- Positions grid shows the open position
- DOM ladder remains healthy
- a new DOM Trader window shows footer avg price and gross P/L correctly

---

## Issue 11: Gross P/L Stays Stale In After Hours While Quotes Still Move

Symptoms
- live prices continue updating in after-hours, premarket, or post-market sessions
- DOM ladder is still moving normally
- Positions window current price can update
- Gross P/L in the Positions window does not keep up with the latest live price
- Gross P/L in the DOM footer also stays stuck on an older value
- example pattern:
  - current price moves from one value to another
  - displayed Gross P/L still reflects the earlier price

Meaning
- this is not a ladder or Level II outage
- this is not a live quote outage
- this is a position refresh / PnL refresh issue
- the bridge is receiving newer live prices, but Quantower is still being fed stale unrealized P&L state

Checks
1. Confirm live ladder/quote health:
- DOM ladder is updating
- bid/ask/last are changing in extended hours

2. Confirm the position exists:
- the top Positions grid shows the symbol and quantity

3. Compare live price versus displayed Gross P/L:
- if the live price moved materially but Gross P/L still matches an older price reference, this issue is present

4. Confirm this is not a DOM-only issue:
- if both the Positions window and DOM footer show stale Gross P/L, the root cause is bridge-side PnL refresh logic

What Was Researched
- reviewed live quote event flow in:
  - `schwab-quantower-bridge/src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`
- inspected:
  - `PublishQuoteEvent(...)`
  - `PublishBarEvent(...)`
  - `SetLatestPrice(...)`
  - `GetPositions(...)`
  - `CalculatePnL(...)`
- compared the bridge behavior against earlier confirmed SDK findings:
  - Quantower position `GrossPnL` is stored state, not an automatically self-recomputing property
  - Quantower can keep `CurrentPrice` and `GrossPnL` out of sync if fresh PnL updates are not triggered
- validated that the bridge was already updating:
  - live quotes
  - live ladder / DOM
  - last-price cache
- isolated the defect to the position/PnL path instead of market-data subscriptions

What Did Not Help
- restarting research on ladder subscriptions
- treating the issue as a Level II problem
- treating the issue as a quote stream problem
- assuming stale Gross P/L meant the bridge had lost market data
- relying on backend `unrealized_profit_loss` as authoritative during extended hours
- leaving cached position state unchanged after new live prices arrived

Root Cause
- the bridge was still preferring cached backend `UnrealizedProfitLoss` values even after newer live after-hours prices were available
- at the same time, the bridge was updating the latest live quote cache but not refreshing cached open-position state for the affected symbol
- result:
  - ladder and quotes kept moving
  - Gross P/L remained anchored to older backend position values until a broader refresh happened

Fix
Updated:
- `schwab-quantower-bridge/src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`

Bridge changes:
1. Changed `CalculatePnL()` so that when a newer valid live close price is available, Gross P/L is recalculated from:
- effective open price
- latest close price
- signed quantity

2. Stopped stale cached backend `UnrealizedProfitLoss` from overriding newer live-price-derived PnL

3. Updated `SetLatestPrice()` so that when a live price changes for a symbol with open positions, the bridge also refreshes cached position fields:
- `MarketPrice`
- `MarketValue`
- `UnrealizedProfitLoss`

4. Re-pushed updated `MessageOpenPosition` messages for affected positions so Quantower receives fresh position-state updates during extended hours

5. Kept the fix narrowly scoped to the position/PnL path only
- no ladder logic changes
- no Level II logic changes
- no startup/autoconnect logic changes

Verification
1. Build the bridge:
```powershell
dotnet build "D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj" -c Release
```

2. Deploy the DLL:
```powershell
Copy-Item 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\bin\Release\net8.0-windows\*' 'D:\Quantower\TradingPlatform\v1.145.17\bin\Vendors\SchwabVendor' -Recurse -Force
```

3. Restart bridge and QT

4. Validate during extended hours using a live held position:
- confirm ladder still updates
- confirm current price updates
- confirm Gross P/L in both:
  - Positions window
  - DOM footer
  now follows the changing live price

Acceptance Check
- premarket, regular session, and post-market prices all continue updating normally
- DOM ladder remains healthy
- Positions window Gross P/L updates with live price changes
- DOM footer Gross P/L updates with live price changes
- no ladder or Level II regression is introduced

Notes
- this issue can look deceptive because market data appears healthy at first glance
- the distinguishing sign is:
  - live price changes
  - Gross P/L does not
- if that pattern appears again, check this issue before reopening deep ladder or streaming research
- this fix was intentionally designed to preserve the known-good market-data baseline

## Operational Rule

Before deep research:
1. read this file
2. match the symptom to the numbered issue
3. run the listed checks
4. apply the exact fix
5. verify with the listed acceptance check

## Maintenance Rule

Every resolved bridge bug, recurring production issue, or proven recovery sequence should be added to this file with:
- issue number
- symptoms
- meaning
- checks
- fix
- verification

## Issue 12: QT Upgrade Removes Schwab From Connections List

Symptoms
- Quantower upgrades itself
- Schwab disappears from the Connections list
- built-in vendors still appear, but Schwab is missing entirely
- logs contain assembly load failures at startup
- common log errors include:
  - `Could not load file or assembly 'SchwabVendor' ... Assembly with same name is already loaded`
  - `Could not load file or assembly 'System.Runtime, Version=10.0.0.0'`

Meaning
- this is a vendor discovery / runtime compatibility issue
- QT is failing before the Schwab connection shell can be registered in the Connections UI
- this is not a Schwab token issue and not a bridge health issue by itself

Checks
1. Verify installed QT version:
```powershell
Get-ChildItem 'D:\Quantower\TradingPlatform' | Select-Object Name,FullName,LastWriteTime
```

2. Inspect Quantower startup log:
```powershell
Select-String -Path 'D:\Quantower\Logs\Serilog\20260420.slog' -Pattern 'SchwabVendor|System.Runtime|Assembly with same name is already loaded' -Context 1,2
```

3. Verify live vendor bundle path:
- `D:\Quantower\TradingPlatform\v1.145.17\bin\Vendors\SchwabVendor`

4. Verify the old script DLL is not active:
- legacy location should not contain an active `SchwabVendor.dll`
- only the disabled backup should remain under:
  - `D:\Quantower\Settings\Scripts\Vendors`

What Was Researched
- inspected live QT startup errors in `D:\Quantower\Logs\Serilog\20260420.slog`
- confirmed QT v1.145.17 was the only installed runtime
- confirmed the earlier Schwab vendor build was compiled for `net10.0-windows`
- confirmed the project referenced a newer QT runtime path than the one actually installed
- confirmed QT tried to load `SchwabVendor` twice when both the runtime vendor folder and old script vendor location were active

What Did Not Help
- copying only the old DLL into the new vendor folder
- leaving the legacy script DLL active
- restarting QT without rebuilding the vendor
- treating the issue as a token, bridge, or workspace-only problem

Root Cause
- the Schwab vendor was compiled against the wrong runtime / QT version for the upgraded installation
- the old script-loaded vendor copy also caused a duplicate assembly-load conflict
- together these prevented Quantower from loading the Schwab vendor at startup

Fix
1. Rebuild `SchwabQuantowerBridge.csproj` to match the installed QT runtime:
- target framework: `net8.0-windows`
- QT reference paths:
  - `D:\Quantower\TradingPlatform\v1.145.17\bin\TradingPlatform.BusinessLayer.dll`
  - `D:\Quantower\TradingPlatform\v1.145.17\bin\TradingPlatform.PresentationLayer.Plugins.dll`

2. Build:
```powershell
dotnet build "D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj" -c Release
```

3. Deploy the rebuilt vendor bundle to:
- `D:\Quantower\TradingPlatform\v1.145.17\bin\Vendors\SchwabVendor`

4. Disable the legacy script DLL so QT does not load Schwab twice:
- rename `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll`
- keep only a disabled backup such as:
  - `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll.disabled-YYYYMMDD_HHMMSS`

Verification
- startup log no longer shows the duplicate `SchwabVendor` assembly error
- startup log no longer shows `System.Runtime, Version=10.0.0.0` load failure for Schwab
- Schwab returns to the Connections list after QT restart

Acceptance Check
- Schwab appears again in Connections
- the bridge can be connected from QT
- QT startup produces no Schwab vendor load failure

Notes
- current working live deployment model is the QT runtime vendor folder, not the legacy script vendor DLL
- keep the legacy script DLL disabled unless there is a deliberate rollback plan

---

## Issue 13: Two Schwab Connections Appear, But Only Schwab Works

Symptoms
- Connections panel shows both:
  - Schwab
  - Schwab #1
- plain Schwab connects successfully
- Schwab #1 fails or is stale
- workspace panels may still reference the dead custom Schwab connection ID

Meaning
- QT is holding both the live default Schwab connection record and an older dead custom record
- this is a settings/workspace consistency issue
- the live working connection should remain:
  - Schwab-Schwab-Default-Schwab

Checks
1. Inspect saved connection records in settings.xml:
`powershell
Select-String -Path 'D:\Quantower\Settings\settings.xml' -Pattern 'Schwab #1|Schwab-Schwab-Default-Schwab|Schwab-Schwab-Custom-SuLjXA5C3U2B8hcpX6RROg' -Context 2,8
`

2. Inspect workspace references:
`powershell
Select-String -Path 'D:\Quantower\Settings\Workspaces\MAIN .xml' -Pattern 'Schwab-Schwab-Default-Schwab|Schwab-Schwab-Custom-SuLjXA5C3U2B8hcpX6RROg' -Context 0,1
`

What Was Researched
- confirmed settings.xml contained two saved Schwab connection groups
- confirmed the working connection used the default ID:
  - Schwab-Schwab-Default-Schwab
- confirmed the dead connection used the custom ID:
  - Schwab-Schwab-Custom-SuLjXA5C3U2B8hcpX6RROg
- confirmed MAIN .xml still contained many references to the dead custom connection ID before cleanup

What Did Not Help
- leaving both connection records in place
- reconnecting manually without cleaning the saved IDs
- removing only the visible connection row in the UI without updating workspace references

Root Cause
- a stale custom Schwab connection record remained in QT settings after the default working Schwab connection was restored
- workspace symbols and accounts still pointed to the dead custom ID, so the duplicate connection kept resurfacing and causing confusion

Fix
1. Back up both files:
- D:\Quantower\Settings\settings.xml
- D:\Quantower\Settings\Workspaces\MAIN .xml

2. Replace all references from:
- Schwab-Schwab-Custom-SuLjXA5C3U2B8hcpX6RROg
  to:
- Schwab-Schwab-Default-Schwab

3. Remove the dead connection record named Schwab #1 from settings.xml

4. Keep only the working connection record:
- Schwab

Verification
- settings.xml contains Schwab only
- settings.xml no longer contains Schwab #1
- MAIN .xml no longer contains Schwab-Schwab-Custom-SuLjXA5C3U2B8hcpX6RROg

Acceptance Check
- only Schwab appears in Connections
- Schwab still connects successfully
- existing workspace symbols/accounts resolve through the working default Schwab connection ID

Notes
- if duplicate Schwab entries return later, inspect both settings.xml and MAIN .xml
- do not remove the working default record during cleanup

---

## Issue 14: QT Watchlist Triggers Schwab 429 Too Many Requests And Slows Charts

Symptoms
- bridge log fills with repeated quote failures such as:
  - get_snapshot failed for symbol=...
  - httpx.HTTPStatusError: Client error '429 Too Many Requests'
- charts take much longer to load than the known-good baseline
- QT feels sluggish even when the bridge is technically up
- watchlist rows may partially populate while other charts and panels lag behind

Meaning
- Schwab API request volume is exceeding rate limits
- this is not a bridge-startup failure by itself
- this is not necessarily a DOM or Level II bug by itself
- the request fan-out is overloading Schwab quote retrieval and starving other market-data calls

Checks
1. Inspect the bridge console or log for repeated 429 errors on quote endpoints
2. Confirm the affected calls are quote-heavy snapshot requests rather than a dead bridge:
`powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/health'
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/stream/status'
`
3. Compare behavior with the QT watchlist window open versus removed/closed

What Was Researched
- reviewed live bridge console output showing repeated 429 Too Many Requests responses from Schwab quote endpoints
- confirmed the user had already removed the QT watchlist from the active view and that behavior improved after eliminating that request source
- correlated chart slowness with snapshot throttling rather than a bridge crash or token failure

What Did Not Help
- treating the slowdown as a launcher-path issue
- treating the slowdown as a bridge-auth issue when bridge health endpoints were still healthy
- deep render or chart-only investigation while 429 quote throttling was active

Root Cause
- the QT watchlist created enough quote/snapshot fan-out to trigger Schwab API throttling
- once throttled, chart and market-data requests had to compete with the quote backlog, making the whole app feel slow

Fix
1. Remove or keep closed the QT watchlist window if it is not required for the current workflow
2. Prefer ToS watchlist / heatmap when that workflow is already in use
3. Do not reintroduce the QT watchlist into the default layout unless there is a deliberate need to retest request volume

Verification
- bridge health remains 200
- stream status remains healthy
- repeated 429 quote failures materially reduce or stop after the QT watchlist is removed
- chart load time returns closer to the known-good baseline

Acceptance Check
- QT remains responsive without the watchlist window active
- chart panels load at normal or near-normal speed
- quote-throttling spam is materially reduced in the bridge log

Notes
- the user explicitly removed the QT watchlist because it was the main source of the request pressure
- if similar 429 storms return later, check whether a quote-heavy QT panel was reintroduced before reopening deeper bridge research

---

## Issue 15: Forced QT Upgrade To `v1.146.5` Removes Schwab From Connections Again

Symptoms
- Quantower forces an upgrade to `v1.146.5`
- Schwab disappears from the Connections list again
- built-in vendors still appear, but Schwab is missing
- the new runtime folder exists:
  - `D:\Quantower\TradingPlatform\v1.146.5`
- the new runtime vendor directory does not yet contain:
  - `D:\Quantower\TradingPlatform\v1.146.5\bin\Vendors\SchwabVendor`

Meaning
- this is similar to Issue 12 in outcome, but different in runtime-compatibility details
- the custom Schwab vendor must be rebuilt and redeployed for the exact upgraded QT runtime
- the previous `v1.145.17` deployment path and framework assumptions are not sufficient by themselves after the forced move to `v1.146.5`

Checks
1. Verify the installed QT runtime folder:
```powershell
Get-ChildItem 'D:\Quantower\TradingPlatform' | Select-Object Name,FullName,LastWriteTime
```

2. Verify the new vendor folder contents:
```powershell
Get-ChildItem 'D:\Quantower\TradingPlatform\v1.146.5\bin\Vendors' | Select-Object Name,FullName,LastWriteTime
```

3. Verify the legacy script DLL is still disabled:
```powershell
Get-ChildItem 'D:\Quantower\Settings\Scripts\Vendors' | Select-Object Name,FullName,LastWriteTime
```

4. Verify the bridge project references the live QT runtime:
- `D:\Quantower\TradingPlatform\v1.146.5\bin\TradingPlatform.BusinessLayer.dll`
- `D:\Quantower\TradingPlatform\v1.146.5\bin\TradingPlatform.PresentationLayer.Plugins.dll`

What Was Researched
- confirmed `v1.146.5` was the only installed QT runtime after the forced upgrade
- confirmed the old script DLL remained disabled, so duplicate assembly loading was not the immediate cause this time
- confirmed the new `v1.146.5` vendor folder did not yet contain the Schwab vendor bundle
- tested the earlier `net8.0-windows` recovery path from Issue 12 and found it no longer applied to this specific upgraded runtime
- confirmed `TradingPlatform.BusinessLayer.dll` in `v1.146.5` now required `System.Runtime 10.0.0.0`

What Did Not Help
- assuming the old `v1.145.17` deployment was still enough
- assuming the prior `net8.0-windows` fix from Issue 12 would still compile and load cleanly for `v1.146.5`
- restarting QT before redeploying the vendor into the new runtime folder

Root Cause
- the forced QT upgrade created a brand new runtime vendor folder without the custom Schwab vendor bundle
- `v1.146.5` also changed the compatibility line so the bridge had to target `net10.0-windows` instead of the earlier `net8.0-windows` recovery used for `v1.145.17`

Fix
1. Update the bridge project file:
- target framework:
  - `net10.0-windows`
- QT references:
  - `D:\Quantower\TradingPlatform\v1.146.5\bin\TradingPlatform.BusinessLayer.dll`
  - `D:\Quantower\TradingPlatform\v1.146.5\bin\TradingPlatform.PresentationLayer.Plugins.dll`

2. Build:
```powershell
dotnet build 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj' -c Release
```

3. Deploy the vendor bundle to:
- `D:\Quantower\TradingPlatform\v1.146.5\bin\Vendors\SchwabVendor`

4. Keep the legacy script DLL disabled:
- `D:\Quantower\Settings\Scripts\Vendors\SchwabVendor.dll.disabled-20260420_092609`

Verification
- `SchwabVendor.dll` exists in:
  - `D:\Quantower\TradingPlatform\v1.146.5\bin\Vendors\SchwabVendor`
- bridge health returns `200`
- Schwab reappears in the QT Connections list after reopening QT

Acceptance Check
- QT `v1.146.5` opens normally
- Schwab is visible again in Connections
- the bridge backend remains reachable

Notes
- keep Issue 12 because it documents the earlier `v1.145.17` / `net8.0-windows` recovery
- this issue is intentionally separate because the outcome is similar but the framework/runtime fix is not identical

---

## Issue 16: Schwab Shows In Connections But Connect Fails With `Operation was cancelled`

Symptoms
- Schwab is visible again in the QT Connections list
- attempting to connect shows:
  - `Operation was cancelled`
- QT log shows:
  - `Schwab connecting...`
  - `Schwab connecting failed. Reason: Operation was cancelled`
- bridge health endpoint can still return `200`
- direct backend endpoints can still return valid data, for example:
  - `/api/broker/accounts` => `200`
  - `/api/market/snapshot/INTC` => `200`
- the bridge/vendor logs contain repeated timeouts such as:
  - `The request was canceled due to the configured HttpClient.Timeout of 8 seconds elapsing.`

Meaning
- this is not the same as Schwab being missing from Connections
- this is not the same as a dead Schwab token or a dead backend
- QT can see the Schwab vendor, but the bridge-side connect path is timing out too aggressively during startup calls

Checks
1. Verify bridge health:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/health' | Select-Object StatusCode,Content
```

2. Verify live account and snapshot endpoints:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/accounts' | Select-Object StatusCode,Content
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/market/snapshot/INTC' | Select-Object StatusCode,Content
```

3. Inspect vendor log:
```powershell
Get-Content "$env:LOCALAPPDATA\SchwabQuantowerBridge\SchwabVendor.debug.log" -Tail 200
```

4. Inspect QT log for the connect failure:
```powershell
Select-String -Path 'D:\Quantower\Logs\Serilog\*.slog' -Pattern 'Schwab connecting|Operation was cancelled|HttpClient.Timeout of 8 seconds' -Context 1,3
```

5. Inspect the bridge timeout setting:
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Services\SchwabBackendClient.cs`

What Was Researched
- confirmed bridge health was `200` while QT still reported `Operation was cancelled`
- confirmed `/api/broker/accounts` and `/api/market/snapshot/INTC` both returned `200`
- confirmed the running bridge process was alive under `python -m uvicorn --app-dir backend app.main:app`
- inspected QT and vendor logs and matched the connect failure timing to the hard-coded backend client timeout
- located the exact line in `SchwabBackendClient.cs`:
  - `this.httpClient.Timeout = TimeSpan.FromSeconds(8);`

What Did Not Help
- treating the issue as another missing-vendor problem
- treating the issue as a dead Schwab token when direct backend endpoints were still healthy
- treating the issue as a generic QT crash
- relying on the previous deployment fix alone without addressing the too-short timeout

Root Cause
- the bridge client used a hard-coded `8 second` timeout for backend calls
- during QT startup / reconnect, Schwab account and order calls could exceed that threshold
- QT then surfaced the resulting cancellation as the generic message:
  - `Operation was cancelled`

Fix
1. Update:
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Services\SchwabBackendClient.cs`

2. Change:
```csharp
this.httpClient.Timeout = TimeSpan.FromSeconds(30);
```

3. Rebuild:
```powershell
dotnet build 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj' -c Release
```

4. Redeploy the vendor bundle to:
- `D:\Quantower\TradingPlatform\v1.146.5\bin\Vendors\SchwabVendor`

5. Restart the bridge backend

6. Fully reopen QT so it loads the updated vendor DLL

Verification
- bridge health remains `200`
- QT no longer fails the Schwab connect attempt after the short timeout window
- the bridge log no longer shows the connect path failing at the old `8 second` threshold

Acceptance Check
- Schwab remains visible in Connections
- the Schwab connection completes without the generic cancel dialog
- data loads and the user observed that it loads faster after the fix

Notes
- keep this issue separate from Issue 12 and Issue 15 because those cover vendor visibility / runtime deployment problems, while this issue covers a live connect-timeout problem after the vendor is already visible
- the user reported a performance improvement after this timeout fix, so treat it as both a stability and startup-performance recovery

---

## Issue 16: DOM / Level II Ladder Drops Out Briefly Then Returns

Symptoms
- DOM and Level II windows briefly go blank or collapse, then return after about 1 to 2 seconds
- the issue appears as missing ladder rows even though Schwab and QT are still connected
- the `Realtime date/time` field continues advancing, which makes the drop look like a bridge/UI stability problem rather than a disconnected session
- the drop can happen repeatedly and is not acceptable for trading because it makes the ladder visually unstable

Meaning
- Schwab data is still flowing, but the bridge was switching between real book state and fallback snapshot-derived DOM state
- the Level II / DOM display was not being held stable once a real book existed
- this caused visible flicker or temporary ladder loss even when the connection itself was still alive

Checks
- confirm QT and the bridge are both down before changing code
- verify the DOM window is the one exhibiting the drop, not the chart itself
- check whether the real book is present in the bridge runtime and then briefly replaced by snapshot DOM
- inspect bridge logs for Level II / DOM refresh behavior and dropped events

What Was Researched
- traced DOM publishing inside `SchwabMarketDataVendor.cs`
- traced the fallback path that publishes a snapshot-derived synthetic ladder when a real book is not considered fresh
- confirmed the bridge had logic that could substitute snapshot DOM for a real book during freshness transitions
- confirmed the bridge already caches DOM state and can replay it, but the fallback policy was still too permissive

What Did Not Help
- relying on the prior real-book freshness window alone
- only increasing or decreasing the freshness timeout
- treating the issue as a QT-only rendering problem
- leaving the snapshot fallback path intact

Root Cause
- the bridge allowed snapshot-derived DOM to replace or supersede an existing real Level II book
- once the real book aged past the freshness threshold, the bridge could temporarily publish a smaller synthetic ladder
- that made the DOM appear to drop out and then return, even though the session was still live

Fix
- lock the real-book DOM path so that once a real Level II book exists for a symbol, the bridge keeps reusing it instead of falling back to snapshot DOM
- remove the real-book freshness gate that was allowing a snapshot ladder to overwrite a live ladder
- preserve the cached DOM until disconnect or unsubscribe clears it
- apply the same behavior to both DOM and Level II windows so they remain consistent

Implementation Notes
- file updated:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`
- key behavior change:
  - `PublishBestAvailableDom(...)` now prefers cached real DOM whenever it exists
  - `PublishSnapshotDom(...)` no longer replaces a cached real DOM just because the real-book freshness window elapsed
  - `realBookSeen` and `domCache` continue to act as the persistent bridge-side DOM memory until disconnect / unsubscribe
- the fix was intentionally narrow so it does not touch chart streaming, quote streaming, or order handling

Verification
- rebuilt the bridge successfully with `dotnet build`
- copied the rebuilt `SchwabVendor.dll` into:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`
- confirmed the build completed with `0 Error(s)`
- expected runtime behavior after restart:
  - real DOM / Level II should stay visible
  - snapshot fallback should no longer blank or shrink the ladder once a real book is established

Acceptance Check
- DOM and Level II windows no longer blink out for a second or two
- the ladder stays visually stable after the real book appears
- QT does not need repeated manual refreshing to preserve the ladder
- the bridge does not regress into snapshot DOM once a real Level II book has been established

Notes
- this issue is especially important because the user treats DOM stability as a hard trading requirement
- if the ladder disappears again after this fix, the next check should be for a Schwab-side book entitlement or a stream handler failure, not a fallback from real book to snapshot ladder
- QT and the bridge must be down before altering this code path in the future

---

## Issue 17: DOM Switch / Immediate Synthetic DOM Fallback Slows the App

Symptoms
- QT becomes dramatically slower after switching symbols from the Positions window or DOM symbol selector
- the DOM takes a long time to update after clicking a position or ticker
- the ladder may appear to stop moving or feel stuck while the bridge is busy
- the slowdown can make the app feel much worse than the baseline before the enhancement

Meaning
- the bridge is doing extra work on every symbol switch
- the new symbol is being forced through a local synthetic DOM fallback path before the normal live stream path can settle
- the fallback is meant to help responsiveness, but in practice it caused redundant refresh pressure and made QT slower

Checks
- confirm QT and the bridge are both down before modifying the code
- reproduce the issue by clicking a position and watching the DOM switch latency
- verify whether the bridge is pushing an immediate DOM fallback before the live stream has established the symbol
- watch the bridge logs for repeated snapshot / DOM refresh activity during symbol changes

What Was Researched
- traced the DOM subscribe path in `SchwabMarketDataVendor.cs`
- traced the new immediate DOM fallback that was added for `Level2` symbol switches and symbol priming
- confirmed that the fallback was intended to speed up display but instead introduced extra work on every symbol change
- confirmed the bridge still had the normal live stream and cached DOM paths available without the fallback

What Did Not Help
- keeping the immediate synthetic DOM fallback enabled
- trying to use the fallback as a universal fix for slow symbol switching
- leaving the fallback in place and hoping QT would absorb the extra refresh pressure

Root Cause
- the bridge published an extra synthetic DOM immediately on `Level2` subscribe and on symbol priming
- this added unnecessary work and caused QT to process more updates than needed during symbol switches
- the effect was a major slowdown compared with the prior baseline

Fix
- remove the immediate synthetic DOM fallback from the subscribe / prime path
- restore the previous faster behavior where the live stream and normal cached DOM logic handle the symbol switch
- keep the change narrow so it does not touch quote streaming, chart streaming, or order handling

Implementation Notes
- file updated:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`
- behavior removed:
  - `PublishImmediateDom(...)` helper
  - immediate `Level2` DOM publish in `SubscribeSymbol(...)`
  - immediate DOM publish in `PrimeRealtimeSymbol(...)`
- the rollback was intentionally narrow so the bridge returns to the faster pre-regression path

Verification
- rebuilt the bridge successfully with `dotnet build`
- copied the rebuilt `SchwabVendor.dll` into:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`
- confirmed the build completed with `0 Error(s)`

Acceptance Check
- switching symbols in the DOM should return to the prior fast behavior
- clicking positions should no longer impose a long DOM update delay
- the bridge should no longer flood QT with extra synthetic DOM work on symbol changes
- the app should return to the pre-enhancement responsiveness baseline

Notes
- this issue should stay separate from the real-book stability fix because it is a performance regression caused by the immediate fallback path
- do not reintroduce the immediate synthetic DOM fallback unless it is proven safe under load
- QT and the bridge must be down before changing this code path again

---

## Issue 18: Clicking A Position Row Triggers Repeated Orders Refresh And Slows DOM Switch

Symptoms
- clicking a symbol in the Positions window can take 25 to 30 seconds before the DOM fully switches
- the DOM symbol eventually changes, but QT feels blocked or sluggish during the switch
- bridge logs show many repeated `GET /api/broker/orders` requests while the symbol click is happening
- bridge logs also show multiple valid history requests for the clicked symbol across `1m`, `30m`, `1h`, `4h`, and `1d`
- there may be no hard errors in the bridge log, but the UI still feels too slow

Meaning
- this is a performance bottleneck, not a bridge crash
- QT is requesting several pieces of data on symbol switch, and the bridge was also re-hitting the backend for orders too often
- the orders path was adding avoidable synchronous work during an already busy symbol-change sequence

Checks
- confirm QT and the bridge are both down before editing this path
- reproduce by clicking a position row and watching bridge output
- look for this pattern in the bridge log:
  - repeated `GET /api/broker/orders HTTP/1.1`
  - interleaved `GET /api/stream/status HTTP/1.1`
  - many `GET /api/market/bars/<SYMBOL>?timeframe=...` calls for the same click
- confirm the issue is lag under load, not an auth or stream failure

What Was Researched
- reviewed `GetPendingOrders(...)` in `SchwabMarketDataVendor.cs`
- reviewed the bridge order polling loop and background order refresh path
- compared the observed log pattern with the vendor implementation
- confirmed that QT was repeatedly asking for pending orders during row clicks
- confirmed `GetPendingOrders(...)` was making a fresh backend `GetOrdersAsync(...)` call instead of serving from the already-maintained vendor order cache

What Did Not Help
- treating the issue as a Schwab auth failure
- treating the issue as a pure DOM-stream failure
- assuming the slowdown was caused only by history requests
- leaving `GetPendingOrders(...)` on direct backend fetch for every QT request

Root Cause
- on position-row clicks, QT legitimately requests several chart-history payloads for the selected symbol
- during that same burst, the bridge was also making repeated synchronous backend calls for open orders
- the vendor already maintains `orderCache` through order polling, but `GetPendingOrders(...)` was bypassing that cache and re-fetching orders directly
- that unnecessary extra backend work increased row-click latency and made the DOM switch feel stalled

Fix
- keep the normal order polling loop in place
- change `GetPendingOrders(...)` to serve from the vendor's fresh local order cache when the cache is recent
- use a short cache-serve window so QT can request orders repeatedly during a row click without forcing repeated backend calls
- only fall back to direct backend fetch when there is no usable cached order state yet
- serialize backend order refreshes so multiple overlapping refresh triggers cannot stack and hammer the backend during one symbol switch
- add a short minimum refresh interval so bursts of order-refresh requests collapse into one effective backend pull

Implementation Notes
- file updated:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`
- added:
  - `PendingOrdersCacheServeWindow = 1500ms`
  - `lastOrdersRefreshUtc`
  - `GetCachedPendingOrders()`
  - `OrderRefreshMinimumInterval = 750ms`
  - `orderRefreshSemaphore`
- updated:
  - `GetPendingOrders(...)` now returns cached pending orders when the order cache is fresh or when the normal polling task is already active
  - `RefreshOrdersAsync(...)` stamps `lastOrdersRefreshUtc` after successful backend refresh
  - `RefreshOrdersAsync(...)` now skips refreshes inside the minimum interval and allows only one in-flight backend order refresh at a time

Verification
- code compiled after the change
- bridge log should still show normal history requests for symbol switches, but the repeated `/api/broker/orders` flood should be materially reduced
- DOM switch should feel faster because the vendor is no longer blocking on redundant order fetches during the click path
- live DLL deployed to:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`

Acceptance Check
- clicking a row in the Positions window should switch the DOM materially faster than before
- the bridge should not spam repeated order-fetch calls during a single symbol switch
- live orders should still update because the regular order polling loop remains active
- this fix must not degrade chart streaming, DOM streaming, or backend stability

2026-04-23 Recurrence / Regression Check
- symptom returned as a smaller but still noticeable few-second lag when switching symbols from the Positions window into the DOM
- user-provided log showed the same signature:
  - legitimate `/api/market/bars/<SYMBOL>` bursts across multiple timeframes
  - many `/api/market/snapshot/<SYMBOL>` calls
  - repeated `/api/broker/orders` calls interleaved with the symbol-switch burst
- re-checking source confirmed `GetPendingOrders(...)` had regressed back to direct synchronous backend `GetOrdersAsync(...)`
- restored the narrow Issue 18 fix only:
  - `GetPendingOrders(...)` serves from the local vendor `orderCache` while order polling is active or the cache is fresh
  - `RefreshOrdersAsync(...)` uses a zero-wait semaphore so overlapping refresh triggers do not stack
  - `RefreshOrdersAsync(...)` applies a short minimum refresh interval before hitting the backend again
  - `lastOrdersRefreshUtc` is reset on disconnect
- validation:
  - `dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release`
  - build completed with `0 Error(s)`
  - deployed rebuilt DLL/PDB to:
    - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor`
  - backend health returned `200`
  - stream status returned `200`
- important scope note:
  - do not suppress the normal `/api/market/bars/...` fan-out unless there is separate proof it is causing lag
  - do not touch DOM book, Level II, snapshots, history, or startup behavior for this issue
- live rollback note:
  - after deploying the order-cache restore, the user reported worse live behavior:
    - Level II windows flickering
    - DOM symbol changed while linked charts stayed on the prior symbol
    - Positions-window lag still present
  - the order-cache patch was reverted immediately
  - source diff for `SchwabMarketDataVendor.cs` was returned to the pre-patch state
  - rebuilt and redeployed the reverted DLL/PDB to:
    - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor`
  - backend health returned `200`
  - stream status returned `200`
  - do not reapply this order-cache patch as-is without a controlled diagnostic proving it will not affect QT live behavior

Notes
- this issue is separate from Issue 17
- Issue 17 was caused by immediate synthetic DOM fallback pressure
- Issue 18 is caused by redundant order-fetch pressure during row clicks
- keep both issues documented because they are similar in symptom but different in root cause

---

## Issue 19: Experimental Symbol-Switch Optimizations Were Slower Than The GitHub Baseline

Symptoms
- clicking a row in the Positions window still felt slow after multiple bridge-side performance changes
- QT charts could remain on the prior ticker or show "Data is loading..." while the DOM had already switched symbols
- bridge logs showed large bursts of `/api/market/bars/<SYMBOL>` requests across multiple chart timeframes during symbol switches
- the experimental version felt worse than the known GitHub baseline
- reverting to the GitHub baseline version made the app feel better and more predictable

Meaning
- this was not an after-hours market-data issue
- after-hours can make prints thinner, but it does not explain a 20 to 30 second row-click or grouped-chart symbol-switch delay
- this was a bridge/QT interaction problem caused by local experimental changes layered on top of the GitHub baseline

Checks
- compare current local code to the GitHub baseline before making more changes:
  - `git diff -- src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`
  - `git diff -- src/SchwabQuantowerBridge/Quantower/SchwabConnectionScaffold.cs`
- inspect bridge logs during a row click and look for:
  - repeated `/api/market/bars/<SYMBOL>` calls across `1m`, `30m`, `1h`, `4h`, and `1d`
  - repeated `/api/broker/orders`
  - websocket close/open cycles around symbol switches
- confirm there are no auth failures, 429s, or backend crashes before blaming Schwab

What Was Researched
- compared the local working tree against the GitHub baseline
- reviewed the Quantower API classes document, especially `HistoricalData`, `DepthOfMarket`, and `Core`
- confirmed the classes document points toward event-driven historical-data / DOM behavior, not repeated bridge-side request suppression experiments
- confirmed the local bridge had materially drifted from the GitHub baseline in `SchwabMarketDataVendor.cs`

What Did Not Help
- assuming the behavior was caused only by after-hours trading
- layering multiple bridge-side optimizations without returning to a known-good baseline
- treating every repeated history request as an error; QT legitimately requests multiple chart histories when grouped windows are linked
- continuing to tune cache/throttle behavior after the user reported the GitHub baseline felt better

Root Cause
- the local bridge version had accumulated multiple performance experiments in the vendor layer
- those changes interacted poorly with QT's grouped-window symbol-switch workflow
- QT still requested several valid chart histories during symbol switches, but the experimental bridge changes made the UI feel worse than the GitHub baseline
- the correct recovery step was to stop tuning the experimental branch and return the vendor logic to the known GitHub baseline

Fix
- revert the core Quantower vendor logic files back to the GitHub baseline:
  - `src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs`
  - `src/SchwabQuantowerBridge/Quantower/SchwabConnectionScaffold.cs`
- keep the project reference updated for the installed Quantower version:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\TradingPlatform.BusinessLayer.dll`
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\TradingPlatform.PresentationLayer.Plugins.dll`
- rebuild the bridge
- deploy the rebuilt baseline DLL to the live Quantower vendor folder

Implementation Notes
- commands used:
  - `git restore --source=HEAD -- src/SchwabQuantowerBridge/Quantower/SchwabMarketDataVendor.cs src/SchwabQuantowerBridge/Quantower/SchwabConnectionScaffold.cs`
  - `dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release`
- live DLL deployed to:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`
- the user confirmed the baseline version is better than the experimental version

Verification
- bridge rebuilt successfully with `0 Error(s)`
- rebuilt DLL was copied into the live QT `v1.146.6` vendor folder
- user restarted QT and the bridge and reported that the baseline version was better

Acceptance Check
- baseline should be the default recovery point when experimental performance changes degrade QT responsiveness
- if symbol-switch lag remains on baseline, make only one narrow change at a time
- do not reintroduce layered cache/throttle/order/DOM experiments together
- preserve DOM, Level II, Positions, Orders, and chart stability over speculative performance changes

Notes
- keep this issue separate from Issue 17 and Issue 18
- Issue 17 documents immediate synthetic DOM fallback pressure
- Issue 18 documents redundant order-fetch pressure during row clicks
- Issue 19 documents that the combined experimental local version was worse than the GitHub baseline and required rollback
- future performance work should start from the baseline and target only one proven bottleneck at a time

---

## Issue 20: Schwab Order Accepted But Fill / Transaction Does Not Appear In QT

Symptoms
- QT shows `Place order request accepted`
- Schwab/TOS shows the order in activity or filled-order history
- QT does not show the order lifecycle update or transaction/fill after acceptance
- the user may see only the accepted order id message, for example:
  - `Order id: 1006108858538`
- Positions may update later or only after a manual refresh/reconnect

Meaning
- the order submission path worked
- Schwab accepted the order
- the bridge did not complete the full Quantower order lifecycle after acceptance
- this is different from a Schwab rejection, buying-power issue, token/auth failure, DOM/Level II issue, or chart issue

Checks
- confirm the order reached Schwab by checking:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\backend\logs\schwab_trading_audit.jsonl`
- look for:
  - `"action": "place"`
  - `"status_code": 201`
  - `"order_id": "..."`
  - Schwab preview `"status": "ACCEPTED"`
- confirm backend endpoints are reachable when diagnosing live:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/orders'
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/trades?lookback_days=1'
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/positions'
```
- if port `8000` is not listening, post-order refresh cannot update QT even if the order was submitted earlier

What Was Researched
- reviewed Quantower dev-team docs:
  - `D:\Quantower\Quantower _ API Documentatation _ Business Objects _ Order Position Trade Only.docx`
  - `D:\Quantower\Quantower _ API Documentatation _ Requests Classes.docx`
- confirmed Quantower separates:
  - `Order` / `MessageOpenOrder` for pending order state
  - `Trade` / `MessageTrade` for executed fills
  - `Position` / `MessageOpenPosition` for position updates after fills
- confirmed `OrderRequestParameters` mapping was correct for submission
- confirmed the missing piece was post-submit lifecycle publication, not initial order entry

What Did Not Help
- treating this as a DOM or Level II problem
- treating this as a Schwab rejection after the audit log shows `status_code: 201`
- reapplying the old Issue 18 order-cache optimization
- relying only on `GetTrades(...)` history requests, because QT may not call trade history immediately after a live fill

Root Cause
- after `PlaceOrder(...)` succeeded, the bridge pushed an optimistic open order and scheduled normal order refreshes
- the bridge could close terminal orders through `MessageCloseOrder`
- however, it did not actively reconcile the accepted `order_id` against Schwab executions/trades and push `MessageTrade` when a fill appeared
- QT therefore had an accepted order message, but not a reliable live transaction/fill message

Fix
- keep the existing order submission path unchanged
- after Schwab returns an order id, start a short post-order lifecycle reconciliation loop for that specific order id
- during that loop:
  - refresh broker orders
  - fetch broker trades/executions around the order submission time
  - push `MessageTrade` for matching fills
  - refresh and republish positions
  - deduplicate pushed trade ids so fills are not duplicated in QT
- keep the normal order polling loop in place

Implementation Notes
- file updated:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`
- added:
  - `PostOrderLifecycleRefreshScheduleMilliseconds`
  - `pushedTradeIds`
  - `RefreshOrderLifecycleInBackground(...)`
  - `RefreshTradesForOrderAsync(...)`
  - `RefreshPositionsAsync(...)`
- changed:
  - `PlaceOrder(...)` now starts order-specific lifecycle reconciliation after receiving a Schwab order id
- intentionally not changed:
  - DOM
  - Level II
  - chart history
  - market data streaming
  - startup behavior
  - previous Issue 18 order-cache optimization

Verification
- build command:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build completed with:
  - `0 Error(s)`
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Acceptance Check
- place a tiny controlled Schwab order from QT
- QT should show the accepted order id
- after Schwab fills the order, QT should receive a transaction/fill via `MessageTrade`
- order should leave the Orders window when terminal
- Positions should refresh after the fill
- bridge debug log should include `PushTrade orderId=... tradeId=...`

Notes
- this fix depends on Schwab exposing execution/trade data quickly through `/api/broker/trades`
- if Schwab delays execution visibility, QT may still update on the later reconciliation pass rather than instantly
- if backend port `8000` is not listening after order placement, no bridge-side post-order refresh can complete
- do not broaden this fix into order-cache throttling unless a separate controlled diagnostic proves it is safe

---

## Issue 21: QT Position Update Lags ToS After Schwab Fill Or Flatten

Symptoms
- ToS shows the Schwab position flattened or reduced
- QT still shows the old Schwab position for a few more seconds
- the final bridge state eventually becomes correct without a code restart
- example:
  - `ONDS` was sold in Schwab/ToS
  - QT still showed the stale `ONDS` position temporarily

Meaning
- the bridge is eventually converging to the right Schwab position state
- this is a sync-latency issue, not a persistent stale-cache corruption issue
- QT is lagging the broker update rather than inventing a wrong long-term position

Checks
- confirm live bridge position state:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/positions'
```
- inspect vendor debug log:
```powershell
Get-Content "$env:LOCALAPPDATA\SchwabQuantowerBridge\SchwabVendor.debug.log" -Tail 220
```
- look for:
  - `PlaceOrder symbol=...`
  - `PushTrade orderId=...`
  - `Backend heartbeat failed ...`
  - `OrderPolling error=... HttpClient.Timeout ...`

What Was Researched
- verified from live `/api/broker/positions` that the bridge eventually returned the correct flattened state after the user sold out of `ONDS`
- reviewed the bridge polling design in:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`
- confirmed the bridge refreshes positions after order activity only on a bounded schedule:
  - `100ms`
  - `600ms`
  - `1500ms`
  - `3000ms`
  - `6000ms`
  - `10000ms`
- confirmed there is no separate continuous low-latency position polling loop
- confirmed logs also showed occasional backend friction:
  - repeated `Backend heartbeat failed error=An error occurred while sending the request`
  - one `OrderPolling error=The request was canceled due to the configured HttpClient.Timeout of 30 seconds elapsing.`

What Did Not Help
- treating the stale QT position as permanent corruption before checking live `/api/broker/positions`
- assuming DOM or Level II code was responsible
- assuming the stale position meant the final Schwab state was still wrong
- proposing aggressive continuous position polling without considering latency/performance tradeoffs

Root Cause
- the bridge uses a short post-order position refresh burst rather than a continuous position-sync loop
- if Schwab makes the final fill/position update visible after that refresh burst, QT can temporarily lag ToS
- heartbeat/request slowdowns can stretch that lag further

Fix
- no code change was applied
- this issue is currently treated as a known design limitation of the stable baseline
- do not implement speculative continuous position polling unless the user explicitly approves the latency/performance tradeoff review first

Verification
- live `/api/broker/positions` no longer included `ONDS` after the sell
- vendor log showed the `598` share `ONDS` sell was placed
- final QT discrepancy resolved later without a bridge code change

Acceptance Check
- if QT lags ToS after a Schwab fill, first confirm whether `/api/broker/positions` has already converged
- if `/api/broker/positions` is correct and QT catches up later, classify it as this issue
- if `/api/broker/positions` remains stale for an extended period, open a new issue rather than reusing this one

Notes
- current intended post-order catch-up window is about `0` to `10` seconds
- practical lag can exceed `10` seconds if Schwab reports the fill after that window or if backend requests are slow
- this issue should stay separate from DOM/Level II performance work
- preserve the low-latency market-data baseline unless a future experiment is explicitly approved

---

## Issue 22: QT Level II Shows Aggregated Ladder Instead Of Schwab Venue Rows

Symptoms
- ToS Active Trader Level II shows venue-specific rows for the same symbol
- example venues in ToS:
  - `OTCBB`
  - `EDGX`
  - `ARCA`
  - `NSDQ`
  - `BATS`
- QT DOM/Level II shows a price ladder but less venue-level detail
- market data is connected and updating, so this is not a token or backend availability issue

What Was Researched
- checked Quantower docs for Level II/DOM support:
  - `Level2Item.MMID`
  - `DetailedLevels`
  - `GetMBOItems`
  - `AggregateMethod`
  - `LevelsCount`
- checked Schwab stream book subscription support in the backend
- confirmed the bridge already subscribes to:
  - `nasdaq_book_subs`
  - `nyse_book_subs`
- confirmed the backend already registers:
  - `add_nasdaq_book_handler(...)`
  - `add_nyse_book_handler(...)`
- checked schwab-py stream book field definitions
- confirmed Schwab book payloads expose nested per-exchange rows:
  - bid side: `EXCHANGE`, `BID_VOLUME`, `SEQUENCE`
  - ask side: `EXCHANGE`, `ASK_VOLUME`, `SEQUENCE`

What Did Not Help
- changing account selection
- restarting QT without changing the bridge mapping
- assuming Schwab did not provide any venue detail
- adding new polling loops
- changing backend startup behavior
- changing DOM or Level II subscription startup behavior

Root Cause
- the bridge subscribed to Schwab book feeds correctly
- the bridge flattened each Schwab book level into one aggregate QT `Level2Quote`
- nested Schwab per-exchange rows inside `BIDS` and `ASKS` were discarded before QT received them
- QT therefore could not display the same venue-level detail ToS shows

Fix
- updated `SchwabMarketDataVendor.cs` to map nested Schwab per-exchange book rows into separate QT `Level2Quote` rows
- preserved exchange/venue using QT `Level2Quote` broker/MMID-style metadata when available
- preserved stable per-row ids using Schwab sequence/exchange/price/side data
- retained aggregate fallback if Schwab sends no nested per-exchange rows
- preserved cached DOM row id and broker metadata across cached replay
- did not change:
  - backend stream subscriptions
  - startup behavior
  - order routing
  - account polling
  - DOM/Level II polling cadence

Verification
- build command:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build completed with:
  - `0 Warning(s)`
  - `0 Error(s)`
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Acceptance Check
- start bridge and QT
- open INTC or another active symbol in QT DOM/Level II
- compare against ToS Active Trader Level II
- QT should receive per-exchange Schwab book rows instead of only one aggregate row per price level when Schwab provides nested venue data
- if QT still visually aggregates rows by price, the next controlled diagnostic is the QT `LEVEL2_IS_AGGREGATED` rule; do not change that rule without a separate approval because it can affect DOM rendering behavior

Notes
- this is a bridge mapping fix, not a Schwab token fix
- this does not guarantee QT will visually match ToS exactly because QT may still apply its own DOM aggregation/display rules
- this fix gives QT the missing raw per-exchange rows so the platform has the data needed to display deeper venue detail

## Issue 24 - Closed Schwab Position Still Shows In QT Positions Window

Symptoms
- a Schwab position is closed in ToS / Schwab
- ToS shows the closing fill correctly
- QT still shows the old position in the Positions panel
- example seen on April 24, 2026:
  - ToS showed `BUY +2 TO CLOSE` for `APLD  260424C00036000`
  - QT still showed `Short APLD 260424C00036000 -2`

Meaning
- the Schwab/backend source of truth may already be correct
- QT can still retain a stale position if the bridge only publishes currently open positions and never explicitly publishes that a previously known position is now closed
- this is a QT position-state reconciliation problem, not a DOM, Level II, market-data, or token issue

Checks
- verify backend positions first:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/positions'
```
- if the stale symbol is absent from `/api/broker/positions`, Schwab/backend no longer considers it open
- verify recent orders:
```powershell
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/orders'
```
- for the APLD case, `/api/broker/orders` showed:
  - `status`: `FILLED`
  - `instruction`: `BUY_TO_CLOSE`
  - `symbol`: `APLD  260424C00036000`
  - `filled_quantity`: `2`
  - `position_id`: account hash plus exact option symbol

What Was Researched
- checked current bridge `GetPositions(...)`
- checked current bridge `RefreshPositionsAsync(...)`
- checked QT docs around:
  - `MessageOpenPosition`
  - `Position.UpdateByMessage(MessageOpenPosition)`
  - `Core.PositionRemoved`
- checked for a documented `MessageClosePosition` type in QT XML docs
- checked reflection path, but direct reflection was blocked by missing `System.Runtime, Version=10.0.0.0` loader dependencies in the shell

What Did Not Help
- assuming Schwab was still reporting the position open
- changing market-data / DOM / Level II behavior
- adding more polling
- changing backend startup behavior
- treating this as a ToS/QT display timing issue only

Root Cause
- bridge returned and pushed open positions only
- when Schwab stopped returning a closed position, the bridge skipped it entirely
- QT never received an explicit close-position message for that position ID
- QT could therefore keep displaying the last known `MessageOpenPosition` for that position ID
- this was most visible for fully closed options positions

Fix
- updated `SchwabMarketDataVendor.cs`
- added a small in-memory position cache keyed by `AccountHash:Symbol`
- on every existing position refresh:
  - publish current open positions as before
  - compare current position IDs against the previous cache
  - for any previously cached position missing from Schwab positions, publish QT `MessageClosePosition`
- no new polling was added
- DOM and Level II paths were not changed
- market-data subscriptions were not changed

Correction
- do not publish `MessageOpenPosition` with `Quantity = 0`
- QT may display that as an actual row with quantity `0`
- do not synthesize a closed position from a terminal order fill price
- using a closing fill as `OpenPrice` contaminates AVG P
- use `MessageClosePosition` for row removal instead

Verification
- backend proof before fix:
  - `/api/broker/positions` returned only open `INTC` and `FDD`
  - stale `APLD  260424C00036000` was absent
  - `/api/broker/orders` showed the filled `BUY_TO_CLOSE`
- build command:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build completed with:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.6\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Acceptance Check
- start bridge and QT
- connect Schwab
- close a small test position or wait for a real full close
- confirm `/api/broker/positions` no longer contains that exact symbol
- confirm QT Positions removes the position after the normal order/position refresh cycle
- debug log should show:
```text
PushClosedPosition positionId=...
```

Notes
- this is intentionally low-latency safe:
  - no new REST polling loop
  - no per-tick position refresh
  - no DOM/Level II changes
  - only a tiny in-memory set/dictionary comparison during existing position refreshes
- exact option-symbol spacing matters, for example `APLD  260424C00036000`
- if QT still shows a stale position after this fix, check whether the live DLL was actually deployed and QT fully restarted

## Issue 25: QT Upgrade To `v1.146.7` Removes Schwab From Connections

Symptoms
- Quantower updated to `v1.146.7`
- Schwab disappeared from the Connections list while built-in vendors still appeared
- installed runtime folder was:
  - `D:\Quantower\TradingPlatform\v1.146.7`
- live logs also showed Schwab auth failures:
```text
refresh_token_authentication_error
unsupported_token_type: 400 Bad Request
```

Root Cause
- the QT upgrade created a new runtime vendor folder without the custom Schwab vendor bundle
- the bridge project still referenced QT `v1.146.6` assemblies
- the auth error was a separate expired/invalid Schwab refresh-token problem; it does not explain Schwab missing from QT Connections

Fix
- updated project references to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\TradingPlatform.BusinessLayer.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\TradingPlatform.PresentationLayer.Plugins.dll`
- updated the bridge deploy path to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor`
- rebuild and copy the vendor bundle into the new QT runtime folder
- re-run Schwab OAuth login if backend endpoints still return `refresh_token_authentication_error`

## Issue 26: Duplicate Order Blocked By Schwab Bridge

Symptoms
- QT popup appears when placing another order at the same symbol/side/price:
```text
400 Bad Request: Duplicate order blocked by Schwab bridge duplicate-protection window
```
- user intentionally wants to place multiple separate orders at the identical price level

Root Cause
- this was a bridge-side backend guard, not a QT-native restriction
- `backend/app/services/broker.py` fingerprinted recent orders and rejected repeats inside `SCHWAB_DUPLICATE_WINDOW_SECONDS`
- that guard was too restrictive for active DOM trading because repeated same-price orders can be intentional

Fix
- removed duplicate-order fingerprint enforcement from `SchwabBrokerService.place_order`
- removed the unused `SCHWAB_DUPLICATE_WINDOW_SECONDS` config setting
- kept the actual safety checks intact:
  - trading kill switch
  - max shares / max notional limits
  - fractional share block
  - limit price deviation check
  - Schwab preview reject handling

Verification
- source search should show no active references to:
  - `Duplicate order blocked`
  - `enforce_duplicate`
  - `_is_duplicate_order`
  - `_remember_order`
  - `schwab_duplicate_window_seconds`
- syntax check:
```powershell
python -c "import pathlib; [compile(pathlib.Path(p).read_text(encoding='utf-8'), p, 'exec') for p in ['backend/app/services/broker.py','backend/app/config.py']]; print('syntax ok')"
```

Notes
- this change affects order validation only
- it does not touch DOM, Level II, market data, streaming, positions, or avg-price/P&L behavior
- after changing backend Python code, restart the bridge so the running process loads the update

## Issue 27: QT Orders Window Shows Stale Orders And Buttons Do Not Work

Symptoms
- QT Orders window shows orders that were already canceled in ToS/Schwab
- selecting an order row does not make `Cancel selected`, `Modify order`, or `Change to market` work reliably
- backend diagnostic can show:
```text
/api/broker/orders => []
```
while QT still displays old order rows

Root Cause
- QT order rows are driven by `MessageOpenOrder` and cleared by `MessageCloseOrder`
- the bridge handled explicit terminal statuses returned by Schwab, for example `CANCELED`, `FILLED`, `REPLACED`
- but if an order disappeared from Schwab's active/recent order response, the bridge did not compare the new successful order set against cached open orders
- result: QT could keep a stale local order row with an order id that Schwab no longer considers active
- `/api/broker/orders` also returned `[]` on Schwab auth failures, which was unsafe because a failed order fetch could look like a valid empty order set

Fix
- update `SchwabMarketDataVendor.ReconcileOrderStatuses(...)`
- build a current order-id set from each successful backend order refresh
- for every cached cancelable/open order missing from the successful refresh, push:
```csharp
new MessageCloseOrder { OrderId = cachedOrder.OrderId }
```
- remove that order from the bridge cache and log:
```text
PushClosedOrder orderId=...
```
- update `/api/broker/orders` so auth failures raise an error instead of returning an empty list

Verification
- Python syntax check:
```powershell
python -c "import pathlib; [compile(pathlib.Path(p).read_text(encoding='utf-8'), p, 'exec') for p in ['backend/app/routes/broker.py','backend/app/services/broker.py','backend/app/config.py']]; print('python syntax ok')"
```
- bridge build:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build result:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Notes
- this fix uses the existing order refresh/polling path
- no DOM, Level II, market-data, streaming, positions, or P&L logic was changed
- `Change to market` is still separate: bridge modification currently supports limit-order modification only

## Issue 28: QT Still Shows Non-Cancelable Stale Orders After Close Messages

Symptoms
- QT Orders window still shows old order rows
- selecting those rows does not successfully cancel
- bridge/backend logs do not show a matching:
```text
DELETE /api/broker/orders/{account_hash}/{order_id}
```
- backend `/api/broker/orders` can show zero active/cancelable orders while QT still displays rows

Diagnostic
- check current Schwab active orders:
```powershell
$orders = (Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/orders').Content | ConvertFrom-Json
$orders | Where-Object { $_.status -match 'OPEN|WORKING|QUEUED|PENDING|ACCEPTED|AWAITING|NEW' }
```
- if this returns nothing, Schwab/backend has no live orders to cancel
- if QT still shows rows, they are local stale QT rows
- check bridge debug log:
```powershell
Get-Content "$env:LOCALAPPDATA\SchwabQuantowerBridge\SchwabVendor.debug.log" -Tail 200
```
- expected stale-row cleanup messages:
```text
PushClosedOrder orderId=...
```

Root Cause
- Issue 27 added `MessageCloseOrder` for terminal/missing orders
- however, each closed order message was pushed only once per bridge process
- if QT missed that one close event during reconnect/startup/window timing, the stale row could remain visible
- after that, QT would not send a backend cancel request for the stale row, so no `DELETE /api/broker/orders/...` appeared in backend logs

Fix
- keep the existing stale-order reconciliation
- add a throttled close-message rebroadcast for terminal orders
- rebroadcast interval:
```text
ClosedOrderRebroadcastInterval = 5 seconds
```
- active orders clear the closed-message throttle state
- stale terminal close messages can be resent periodically until QT removes the rows

Verification
- backend showed no active/cancelable orders:
```text
REPLACED 56
CANCELED 21
FILLED 14
REJECTED 5
EXPIRED 1
```
- bridge build:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build result:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Notes
- this does not touch DOM, Level II, market data, positions, P&L, or order placement
- this does not add new REST polling
- it only changes how often already-known terminal order close messages are resent to QT
- if QT still shows stale rows after deployment, fully restart QT so the updated live vendor DLL is loaded

## Issue 29: Short Option Position Shows Red P/L When It Is Profitable In ToS

Symptoms
- ToS shows a profitable short option position, for example:
```text
SHORT INTC 100 (Weeklys) 8 MAY 26 99 CALL
Qty: -10
Trade price: 4.00
Mark: about 3.30
P/L Open: about +700.00
```
- QT shows the same option row as short with the correct average price, but P/L is negative
- example QT row:
```text
Short INTC 260508C00099000
Quantity: -10
Avg P: 4.00
Last: about 3.45
P/L: negative
```

Meaning
- this is a bridge position/P&L mapping issue, not a DOM, Level II, quote-stream, or Schwab account issue
- for short options, profit increases when option price falls
- options also require the 100x contract multiplier

Checks
- inspect live backend positions:
```powershell
(Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/positions').Content
```
- confirm the short option has:
```text
quantity < 0
average_price around contract premium, for example 4.00
asset_type OPTION or instrument_type VANILLA
market_price around contract premium, for example 3.30
unrealized_profit_loss populated from Schwab shortOpenProfitLoss
```
- if `market_price` is around `330.00` instead of `3.30`, the option multiplier normalization is broken
- if `unrealized_profit_loss` is null for a short option, the backend is only reading long P/L fields

What Was Researched
- compared ToS position display against `/api/broker/positions`
- confirmed Schwab backend payload can provide separate long/short open P/L fields
- confirmed option market value is contract-multiplied
- reviewed QT bridge `MessageOpenPosition` and `CalculatePnL(...)`

What Did Not Help
- changing DOM columns or QT window settings
- restarting only the DOM window
- treating the short option as an equity position
- using the raw signed short quantity directly in `CalculatePnL(...)`

Root Cause
- backend `market_price` used:
```text
marketValue / quantity
```
which is wrong for options because Schwab option market value includes the 100x contract multiplier
- backend only mapped `longOpenProfitLoss`, so short-option open P/L could be missing
- QT `CalculatePnL(...)` used a short-aware price difference but multiplied by raw `parameters.Quantity`; if QT supplied a negative short quantity, the result flipped back to negative
- QT option multiplier fallback referenced option helper methods that were missing from the live source

Fix
- backend `get_positions(...)` now computes signed quantity once and passes it through position mapping
- backend `_resolve_market_price(...)` divides option market value by `quantity * 100`
- backend `_resolve_open_profit_loss(...)` reads:
```text
shortOpenProfitLoss for short positions
longOpenProfitLoss for long positions
```
- QT `CalculatePnL(...)` now uses `Math.Abs(parameters.Quantity)` after determining long/short side
- QT option P/L multiplier detection now supports:
```text
assetType == OPTION
instrumentType == VANILLA
OCC-style symbols like INTC 260508C00099000
```
- QT option price normalization keeps a defensive guard for accidental 100x market prices

Verification
- Python no-write syntax check:
```powershell
python -c "from pathlib import Path; p=Path('backend/app/services/broker.py'); compile(p.read_text(encoding='utf-8'), str(p), 'exec'); print('python syntax ok')"
```
- bridge build:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build result:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Acceptance Check
- for a short option:
```text
open premium 4.00
current/mark premium 3.30
contracts 10
```
- expected P/L:
```text
(4.00 - 3.30) * 10 * 100 = +700.00
```
- QT position row should show positive P/L when ToS shows the short option profitable

Notes
- this fix does not touch DOM ladder data, Level II, market-depth subscriptions, snapshot fanout, order placement, cancel/modify behavior, or latency-sensitive polling
- if QT was already open during deployment, fully restart QT so it loads the updated vendor DLL

## Issue 30: Option DOM Ladder Price-Level P/L Is Divided By 100

Symptoms
- QT Positions window and DOM bottom summary show correct option P/L, for example:
```text
Short 10 contracts
Avg price: 4.00
Current/selected price: 3.05
Gross P/L: 950.00 USD
```
- but the DOM ladder price-level `P&L` column shows:
```text
9.5
```
instead of:
```text
950.00
```

Meaning
- this is not a Schwab quote or backend position problem if the Positions row and DOM bottom summary are already correct
- the per-price ladder column is using QT symbol contract metadata
- listed equity options need a 100x contract multiplier

Checks
- verify the math:
```text
(4.00 - 3.05) * 10 contracts * 100 = 950.00
```
- if QT ladder shows `9.5`, it is calculating:
```text
(4.00 - 3.05) * 10 contracts
```
- inspect option symbol creation in:
```text
src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs
```

What Was Researched
- compared QT Positions P/L, DOM bottom Gross P/L, and DOM ladder per-price `P&L`
- reviewed `CalculatePnL(...)`
- reviewed option `MessageSymbol` creation
- checked QT API/business-object docs locally

What Did Not Help
- changing backend position P/L mapping
- changing `CalculatePnL(...)` alone
- treating this as a Level II, ladder-depth, or market-data feed issue

Root Cause
- option `MessageSymbol` was created with:
```csharp
LotSize = 1d
```
- QT's DOM ladder price-level P/L uses symbol contract metadata for row-by-row P/L
- therefore QT displayed per-contract-dollar math instead of equity-option 100x contract math
- a fallback path could also create OCC-style option symbols through generic symbol creation with `LotSize = 1d`

Fix
- set listed option symbols to:
```csharp
LotSize = 100d
```
- update generic symbol fallback so OCC-style option symbols resolve to `SymbolType.Options`
- set generic fallback `LotSize = 100d` when `symbolType == SymbolType.Options`

Verification
- bridge build:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build result:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Acceptance Check
- open the same option DOM after QT reloads the vendor DLL
- at selected/row price `3.05` for short 10 contracts opened at `4.00`, expected DOM ladder P/L is:
```text
950.00
```

Notes
- this only changes option symbol metadata
- it does not touch DOM depth, Level II, streaming, backend polling, order cancellation, duplicate order behavior, or equity P/L
- if QT was already running during deployment, restart QT so it reloads the vendor DLL and symbol metadata

## Issue 31: Option Position P/L Only Updates After Selecting The Option Row

Symptoms
- QT Positions window shows an option position, for example:
```text
Short INTC 260508C00099000
Quantity: -10
Avg P: 4.00
```
- ToS option mark/P&L changes automatically
- QT option P/L remains stale until the option row is selected in the Positions window
- after selecting the row, QT updates the option value/P&L

Meaning
- selection causes QT to request/subscribe the exact option symbol
- before selection, the option symbol may not have active quote/mark traffic inside QT
- this is separate from equity DOM, Level II, Time & Sales, and order flow

Checks
- verify the option position appears in:
```powershell
(Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/positions').Content
```
- compare the option `market_price` / P&L in backend output against QT before and after selecting the row
- if QT updates only after selection, the issue is stale QT option-position republishing, not the Schwab account itself

What Was Researched
- reviewed `GetPositions(...)`
- reviewed `RefreshPositionsAsync(...)`
- reviewed `ReconcilePositions(...)`
- reviewed `PrimeRealtimeSymbol(...)`
- confirmed the existing bridge refreshed positions on initial QT request and after order lifecycle events, but did not run a continuous position-refresh loop

What Did Not Help
- changing DOM/Level II/market-depth subscriptions
- changing order polling cadence
- forcing option stream subscriptions from DOM/Level II code paths
- relying on manual row selection to trigger QT symbol activity

Root Cause
- options are not always actively subscribed by QT unless selected/opened
- the bridge did not continuously republish changed option positions
- therefore option P/L could stay stale in the Positions window until selection caused QT to request that option symbol

Fix
- add a separate option-position-only polling loop:
```text
OptionPositionPollingInterval = 10 seconds
```
- this loop fetches positions from the backend, filters to open option positions only, and pushes `MessageOpenPosition` only when option qty/price/P&L values changed
- it updates cached latest option prices for P/L math
- it sends `MessageClosePosition` for stale option positions that disappear from Schwab

Verification
- bridge build:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- build result:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed rebuilt files to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.pdb`

Acceptance Check
- start bridge and QT
- leave the option row unselected
- when Schwab option mark/P&L changes, QT option position P/L should update within about 10 seconds
- selecting the row should no longer be required just to refresh option position P/L

Notes
- this does not touch DOM depth
- this does not touch Level II
- this does not start quote streams
- this does not alter order flow, order placement, cancel/modify, or order polling cadence
- this does not alter equity position refresh behavior
- interval is intentionally low-frequency to protect latency and platform performance

## Issue 32: Schwab POS-Only Equity P/L Stale After Hours While dxFeed DOM Price Is Live

Symptoms
- QT is connected using:
  - `Schwab POS ONLY/NO DATA` for trading/positions
  - `dxFeed` for Quotes & Trades / history / volume analysis
- DOM ladder shows the live after-hours dxFeed price
- Positions row and DOM footer P/L stay stale after regular market close
- example:
```text
INTC avg: 56.04995
dxFeed last: 111.15
Positions/Dom footer P/L still lower than live-price math
```

Meaning
- this is not a dxFeed data problem if the DOM price ladder is live
- this is not a DOM/Level II subscription problem
- the stale value is coming from the bridge P/L calculation path using a cached Schwab/position price instead of QT's current mapped dxFeed close/last price

Checks
- compare:
  - Positions `LAST`
  - DOM current price
  - DOM footer Gross P/L
  - manual math:
```text
(current price - average price) * quantity
```
- inspect:
```text
src\SchwabQuantowerBridge.PosOnly\Quantower\SchwabMarketDataVendor.cs
src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs
```
- confirm `CalculatePnL(...)` uses `parameters.ClosePrice` before falling back to bridge cached latest price

What Was Researched
- checked this file first, especially Issue 10 around DOM footer P/L binding
- compared full connector and POS-only connector `CalculatePnL(...)`
- confirmed both connectors were using only `TryGetLatestPrice(...)`
- confirmed POS-only intentionally ignores quote subscriptions, so it must use QT-supplied dxFeed price when QT requests P/L

What Did Not Help
- changing DOM or Level II data subscriptions
- adding Schwab market-data subscriptions back into POS-only
- increasing polling
- touching order flow, order polling, or depth logic

Root Cause
- `CalculatePnL(...)` ignored `PnLRequestParameters.ClosePrice`
- in POS-only mode, the bridge cache can reflect slower Schwab/position refresh values, especially after hours
- QT already has the live dxFeed price and passes it through the P/L request; the bridge was not prioritizing it

Fix
- in both connector implementations, update `CalculatePnL(...)`:
```csharp
var currentPrice = parameters.ClosePrice;
if (currentPrice <= 0 &&
    (!this.TryGetLatestPrice(symbolId, out currentPrice) || currentPrice <= 0))
    return base.CalculatePnL(parameters);
```
- changed files:
  - `src\SchwabQuantowerBridge.PosOnly\Quantower\SchwabMarketDataVendor.cs`
  - `src\SchwabQuantowerBridge\Quantower\SchwabMarketDataVendor.cs`

Verification
- build POS-only connector:
```powershell
dotnet build .\src\SchwabQuantowerBridge.PosOnly\SchwabQuantowerBridge.PosOnly.csproj -c Release
```
- build full connector:
```powershell
dotnet build .\src\SchwabQuantowerBridge\SchwabQuantowerBridge.csproj -c Release
```
- both builds completed with:
  - `0 Error(s)`
  - existing XML-comment warnings only
- deployed to:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor\SchwabPosOnlyVendor.dll`
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabVendor\SchwabVendor.dll`

Acceptance Check
- restart QT so it loads the updated vendor DLL
- reconnect `Schwab POS ONLY/NO DATA`
- keep dxFeed mapped as the market-data source
- after hours, confirm Positions P/L and DOM footer P/L match live dxFeed price math

Notes
- this is latency-safe:
  - no new REST polling
  - no new quote subscriptions
  - no DOM/Level II/order-flow changes
  - no bridge market-data re-enable for POS-only
- this only changes which already-available price source `CalculatePnL(...)` prefers

---

## Issue 33 - Quantower Opens `MAIN.xml` Instead Of `MAIN` Workspace

### Symptoms
- QT startup selects a workspace displayed as `MAIN.xml` instead of the intended `MAIN` workspace.
- Workspace menu shows duplicate-looking entries such as `MAIN`, `MAIN.xml`, and `MAIN BACKUP`.
- QT may keep reopening the wrong workspace after restart.

### Meaning
- Quantower treats files in `D:\Quantower\Settings\Workspaces` as workspace definitions.
- A workspace saved with `.xml` in the display name can become a real file named `MAIN.xml.xml`, which QT then displays as `MAIN.xml`.
- If multiple workspace XML files have `isActive=true`, startup selection can become ambiguous.

### Checks
```powershell
Get-ChildItem 'D:\Quantower\Settings\Workspaces' -File | Select-Object Name,FullName,Length,LastWriteTime
```

```powershell
foreach($f in Get-ChildItem 'D:\Quantower\Settings\Workspaces' -File -Filter '*.xml') {
  $xml=[xml](Get-Content -LiteralPath $f.FullName -Raw)
  $active=($xml.settings.Item | Where-Object { $_.Name -eq 'isActive' } | Select-Object -First 1).Value
  "{0} = {1}" -f $f.Name,$active
}
```

### What Was Researched
- Inspected `D:\Quantower\Settings\Workspaces`.
- Found valid `MAIN` workspace stored as `D:\Quantower\Settings\Workspaces\ MAIN.xml`.
- Found accidental duplicate `D:\Quantower\Settings\Workspaces\MAIN.xml.xml`.
- Confirmed `MAIN.xml.xml` had `isActive=false`, but its presence still made QT show a `MAIN.xml` workspace entry.
- Found ` MAIN.xml` and ` MAIN BACKUP.xml` were both marked active, which is not ideal for deterministic startup.

### What Did Not Help
- Keeping `MAIN.xml.xml` in the live workspace folder, even inactive, because QT still lists it as a workspace.
- Relying only on the QT UI to choose the right workspace when duplicate files exist.

### Root Cause
- Duplicate workspace file naming caused by a workspace name containing `.xml`.
- Multiple active workspace flags existed in the workspace folder.

### Fix
- Close QT first so it cannot overwrite workspace settings on exit.
- Back up workspace files to a timestamped folder under `D:\Quantower\Settings\Workspaces`.
- Set `D:\Quantower\Settings\Workspaces\ MAIN.xml` to `isActive=true`.
- Set `D:\Quantower\Settings\Workspaces\ MAIN BACKUP.xml` to `isActive=false`.
- Move `D:\Quantower\Settings\Workspaces\MAIN.xml.xml` out of the live workspace folder into the backup folder as `MAIN.xml.xml.disabled`.

### Verification
```powershell
Get-ChildItem 'D:\Quantower\Settings\Workspaces' -File | Select-Object Name,Length,LastWriteTime
```

Expected live workspace files should include ` MAIN.xml` and ` MAIN BACKUP.xml`, but not `MAIN.xml.xml`.

```powershell
foreach($f in Get-ChildItem 'D:\Quantower\Settings\Workspaces' -File -Filter '*.xml') {
  $xml=[xml](Get-Content -LiteralPath $f.FullName -Raw)
  $active=($xml.settings.Item | Where-Object { $_.Name -eq 'isActive' } | Select-Object -First 1).Value
  "{0} = {1}" -f $f.Name,$active
}
```

Expected:
- ` MAIN.xml = true`
- ` MAIN BACKUP.xml = false`

### Acceptance Check
- Start QT.
- Workspace selector should load/select `MAIN`, not `MAIN.xml`.
- `MAIN.xml` should not reappear unless a workspace is manually saved with `.xml` in the name.

### Notes
- Do not name a QT workspace with `.xml` in the UI. Use `MAIN`, not `MAIN.xml`.
- Backup from this fix was created at `D:\Quantower\Settings\Workspaces\_codex_workspace_fix_20260509_160002`.
- This is a QT settings/workspace fix only; bridge code and market-data paths are not involved.

Issue 33 update on 2026-05-09:
- QT recreated `D:\Quantower\Settings\Workspaces\MAIN.xml.xml` after the first cleanup because it had reopened/saved that duplicate workspace.
- Stronger fix used: copy the newest `MAIN.xml.xml` layout into the real `D:\Quantower\Settings\Workspaces\ MAIN.xml`, set ` MAIN.xml` active, set ` MAIN BACKUP.xml` inactive, then move `MAIN.xml.xml` out of the live folder.
- Strong-fix backup: `D:\Quantower\Settings\Workspaces\_codex_workspace_fix_strong_20260509_160506`.
- Final live check showed only ` MAIN.xml = true` and ` MAIN BACKUP.xml = false`; no live `MAIN.xml.xml` reference remained.

Issue 33 second update on 2026-05-09:
- Prior cleanup still failed because the intended workspace file itself had a leading space in the filename: `D:\Quantower\Settings\Workspaces\ MAIN.xml`.
- QT recreated `MAIN.xml.xml` because the canonical `MAIN.xml` file did not exist without the leading space.
- Correct permanent fix is to use canonical workspace filenames with no leading spaces:
  - `D:\Quantower\Settings\Workspaces\MAIN.xml` active true
  - `D:\Quantower\Settings\Workspaces\MAIN BACKUP.xml` active false
  - no live `D:\Quantower\Settings\Workspaces\MAIN.xml.xml`
- Latest duplicate layout was preserved into canonical `MAIN.xml` before moving the bad files out of the live folder.
- Canonical-fix backup: `D:\Quantower\Settings\Workspaces\_codex_workspace_fix_canonical_20260509_160756`.

---

## Issue 34: POS-Only DOM Footer Blank While Positions Grid Shows Correct P/L

Symptoms
- QT Positions grid shows the Schwab POS-only position correctly.
- Symbol mapping is active with:
  - tradeable symbol from `SCH POS` / `Schwab POS ONLY/NO DATA`
  - data symbol from `dxFeed`
- DOM ladder and dxFeed price/volume data are live.
- DOM footer still shows:
  - `Quantity & Average open price` => `---`
  - `Gross Profit / Loss` => `---`
- Example seen on 2026-05-11:
  - INTC long 1000 shares displayed correctly in Positions grid
  - DOM selected `INTC SCH POS`
  - DOM footer stayed blank even though Positions P/L was populated

Meaning
- This is not a dxFeed issue if the DOM ladder/price/volume is visible.
- This is not a Level II/order-flow issue.
- This is not a missing Schwab position issue if the Positions grid row exists.
- This is specifically a Quantower DOM-footer `CalculatePnL(...)` context issue in the POS-only connector.

Checks
- Confirm Positions grid has the symbol, quantity, average price, and P/L.
- Confirm DOM selected connection is `SCH POS` / POS-only, not IBKR or another account.
- Confirm symbol mapping has dxFeed selected for market data.
- Confirm `src\SchwabQuantowerBridge.PosOnly\Quantower\SchwabMarketDataVendor.cs` has the cached-position fallback inside `CalculatePnL(...)`.
- Confirm the live DLL was deployed to the reinstalled QT vendor folder:
  - `D:\Quantower\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor\SchwabPosOnlyVendor.dll`

What Was Researched
- Checked this troubleshooting file first, especially Issue 10 and Issue 32.
- Compared the symptom against prior DOM-footer issues:
  - Positions grid correct
  - DOM market data correct
  - only footer avg/P&L blank
- Inspected POS-only `CalculatePnL(...)` implementation.
- Confirmed the method used `parameters.OpenPrice`, `parameters.Quantity`, and `parameters.Side` directly.
- Confirmed Quantower can request DOM-footer P/L without sending a complete open-position context.
- Confirmed POS-only already caches Schwab positions in `positionCache` through `GetPositions(...)` / `ReconcilePositions(...)`.

What Did Not Help
- Changing DOM columns.
- Changing dxFeed mapping.
- Touching Level II, ladder, quotes, trades, or history subscriptions.
- Re-enabling Schwab market data in POS-only mode.
- Treating this as a data-feed issue.

Root Cause
- POS-only `CalculatePnL(...)` returned `base.CalculatePnL(parameters)` when Quantower's DOM-footer request did not provide a positive `OpenPrice`.
- In that path, QT had enough information elsewhere because the bridge already had the Schwab position cached, but the method did not fall back to that cache.
- Result: Positions grid was correct, but DOM footer rendered blank.

Fix
- Changed only the POS-only connector P/L path:
  - `src\SchwabQuantowerBridge.PosOnly\Quantower\SchwabMarketDataVendor.cs`
- Added `ResolvePositionForPnl(...)` to resolve cached positions by:
  - `PositionId`
  - `AccountId + SymbolId`
  - `SymbolId`
- Updated `CalculatePnL(...)` to fall back to cached Schwab position values when QT does not supply them:
  - `AveragePrice` for missing `OpenPrice`
  - cached absolute position quantity for missing request quantity
  - cached signed quantity for long/short side
  - cached market price only if QT close price and local latest price are unavailable
- Preserved the prior Issue 32 behavior:
  - when QT provides `parameters.ClosePrice`, use it first so dxFeed remains the live price source.

Verification
- Built POS-only connector only:
```powershell
dotnet build .\src\SchwabQuantowerBridge.PosOnly\SchwabQuantowerBridge.PosOnly.csproj -c Release
```
- Build result:
  - 0 errors
  - warnings only for existing XML documentation warnings
- Deployed only POS-only connector artifacts to:
  - `D:\Quantower\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`
- Deployed DLL timestamp observed:
  - `SchwabPosOnlyVendor.dll` => `2026-05-11 07:33:56`

Acceptance Check
- Restart QT so it loads the new POS-only vendor DLL.
- Reconnect `Schwab POS ONLY/NO DATA`.
- Keep dxFeed mapped for Quotes & Trades / history / volume analysis.
- Open a fresh DOM Trader window for a Schwab POS-only position.
- Confirm DOM footer now shows:
  - quantity and average open price
  - gross profit/loss
- Existing already-open DOM windows may need to be reopened because QT can cache the old footer binding per window instance.

Notes
- This is latency-safe:
  - no REST polling added
  - no dxFeed path changed
  - no Schwab market-data path re-enabled
  - no DOM ladder or Level II code changed
  - no order-flow code changed
- This is a POS-only connector fix only.
- If this recurs, inspect `CalculatePnL(...)` and cached position resolution before doing broader research.
---

## Issue 35: DOM B/S Own-Order Columns Do Not Show Open Schwab POS Orders

Symptoms
- QT Orders window shows an open Schwab POS-only order correctly.
- Example live case on 2026-05-11:
  - `CORZ` buy limit
  - price `22.99`
  - quantity `800`
  - status `WORKING`
  - remaining quantity `800`
- DOM Trader for the same mapped `SCH POS` symbol does not show the user's own order size in the `B` / bid column or `S` / ask column.
- After the first fix attempt, the order could flash/show briefly at the correct DOM price level and then disappear.
- dxFeed DOM ladder, volume, and market data remain healthy.

Meaning
- This is not a Schwab order-placement failure if `/api/broker/orders` shows the order as open with nonzero remaining quantity.
- This is not a dxFeed, DOM ladder, Level II, or market-data issue.
- This is specifically a QT open-order binding/update issue in the POS-only connector.

Checks
1. Confirm backend order payload:
```powershell
(Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8000/api/broker/orders').Content
```
2. Verify the live order has:
- matching symbol
- `status` such as `WORKING`
- correct `price`
- correct `quantity`
- positive `remaining_quantity`
3. Confirm QT Orders window displays the order.
4. Confirm the DOM own-order `B` / `S` column remains blank at the order price.

What Was Researched
- Checked this troubleshooting file first for prior order lifecycle issues.
- Reviewed POS-only order path in:
  - `src\SchwabQuantowerBridge.PosOnly\Quantower\SchwabMarketDataVendor.cs`
- Verified live backend order payload for the current `CORZ` order.
- Confirmed the backend reported the order correctly:
  - `WORKING`
  - `BUY`
  - `LIMIT`
  - `22.99`
  - `quantity = 800`
  - `remaining_quantity = 800`
- Reviewed `CreateOpenOrder(...)`, `CreateOptimisticOrder(...)`, `ReconcileOrderStatuses(...)`, and order polling.
- Reviewed live debug log:
  - `C:\Users\Owner\AppData\Local\SchwabQuantowerBridge\SchwabVendor.debug.log`
- Confirmed the follow-up failure pattern:
  - `PushClosedOrder` was repeatedly emitted for many old canceled/replaced/filled order ids every refresh
  - this close-message storm could clear QT's DOM own-order overlay after the correct open-order message briefly appeared

What Did Not Help
- Treating the issue as missing Schwab order data.
- Treating it as a dxFeed data problem.
- Changing DOM ladder, Level II, quote, history, or volume-analysis code.
- Changing polling cadence or adding new REST loops.
- Repeatedly rebroadcasting `MessageCloseOrder` for already-closed historical orders.

Root Cause
- When an order is placed from QT, the connector immediately pushes an optimistic `MessageOpenOrder`.
- The optimistic order had `Quantity` and `Price`, but did not set:
  - `FilledQuantity`
  - `RemainingQuantity`
  - `AverageFillPrice`
- QT could show the order in the Orders grid, but the DOM own-order overlay may rely on the open-order message's display quantity/remaining quantity.
- The later Schwab refresh returned the correct `remaining_quantity`, but `ReconcileOrderStatuses(...)` only considered status changes.
- Since the optimistic status and real status were both `WORKING`, the connector did not re-push the corrected open-order message.
- A second root cause appeared after the first fix:
  - historical terminal orders were allowed to rebroadcast close messages every few seconds
  - QT kept receiving close-order messages for old order ids
  - even though the active order was valid, the DOM own-order overlay could be cleared after briefly showing

Fix
- In POS-only connector only:
  - optimistic orders now set `FilledQuantity = 0`, `RemainingQuantity = quantity`, and `AverageFillPrice = price`
  - order reconciliation now detects meaningful display-state changes, not only status changes
  - if price, total quantity, filled quantity, remaining quantity, symbol, instruction, order type, or duration changes, the connector re-pushes `MessageOpenOrder`
  - do not periodically re-publish active open orders; this can cause repeated QT alerts/sounds
  - terminal order close messages are now one-shot per order id instead of rebroadcast every few seconds
- This lets QT refresh the DOM `B` / `S` own-order columns without touching market-data paths.

Files Updated
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SchwabQuantowerBridge.PosOnly\Quantower\SchwabMarketDataVendor.cs`

Verification
- Build:
```powershell
dotnet build .\src\SchwabQuantowerBridge.PosOnly\SchwabQuantowerBridge.PosOnly.csproj -c Release
```
- Result:
  - `0 Error(s)`
  - existing XML-doc warnings only
- Deployed to:
  - active production QT path: `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`

Path Warning
- Do not deploy this fix only to the duplicate/nested install:
  - `D:\Quantower\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`
- The active reinstall path is:
  - `D:\Quantower`

Acceptance Check
- Restart QT so it loads the updated POS-only DLL.
- Keep bridge running; bridge restart is not required for this DLL-only change.
- Connect `SCH POS` / `Schwab POS ONLY/NO DATA`.
- Place or keep an open limit order.
- Confirm QT Orders window shows the order.
- Confirm DOM `B` column shows buy orders and DOM `S` column shows sell orders at the correct price level.

Safety Notes
- No dxFeed market-data code changed.
- No DOM ladder or Level II code changed.
- No quote, trade, history, volume-analysis, or order-flow data path changed.
- No new polling loop was added.
- Avoid any recurring open-order re-publish loop unless QT support confirms it is required; the attempted 2-second active-order refresh caused repeated QT alerts.
- This is limited to POS-only open-order message completeness and re-publish criteria.

Deployment Path Note - 2026-05-11
- Correct active Quantower install for current production use is:
  - `D:\Quantower`
- Correct active POS-only vendor deployment path is:
  - `D:\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`
- Do not deploy production fixes only to the nested duplicate install:
  - `D:\Quantower\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SchwabPosOnlyVendor`
- The nested path exists from reinstall/migration history and can cause false-positive deployments where source builds succeed but QT still runs the older DLL.
- Future QT vendor fixes must verify the live DLL timestamp in the active `D:\Quantower\TradingPlatform\...` path before telling the user the patch is deployed.

---

## Issue 15: SCH TRD Executions Fill But QT Positions Do Not Update Immediately

Symptoms
- Schwab/thinkorswim shows filled executions.
- QT Orders panel removes the filled order.
- QT Positions panel still shows the old quantity, old average price, or missing updated P/L.
- Example observed on 2026-05-14:
  - IREN buy fills appeared in Schwab.
  - QT still showed the prior IREN position quantity instead of immediately reflecting the new fills.

Meaning
- The order lifecycle was reaching QT, but position lifecycle messages were not being pushed immediately after fills.
- QT was waiting for the next position snapshot/reconnect path instead of receiving an immediate position update.

Checks
- QT order/fill evidence:
```powershell
Select-String -LiteralPath 'D:\Quantower _ LATEST\Quantower\Logs\Serilog\20260514.slog' -Pattern 'IREN|Order update|Order remove|Trading operation result|SCH TRD'
```
- Confirm whether the order disappears after execution but no position row update follows in QT.
- Confirm the active SCH TRD DLL path:
  - `D:\Quantower _ LATEST\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SCHTRDVendor\SCHTRDVendor.dll`

What Was Researched
- Quantower BusinessLayer XML in the active install:
  - `D:\Quantower _ LATEST\Quantower\TradingPlatform\v1.146.7\bin\TradingPlatform.BusinessLayer.xml`
- Relevant QT protocol:
  - `Position.UpdateByMessage(MessageOpenPosition)` is the QT path for applying position changes.
  - `MessageClosePosition` is the QT path for removing a position that no longer exists.
- Existing working bridge pattern:
  - reconcile broker positions
  - push changed `MessageOpenPosition`
  - push `MessageClosePosition` for stale/closed positions

What Did Not Help
- Treating this as a dxFeed problem.
- Treating this as DOM or T&S configuration.
- Waiting for normal order-only polling to update positions.
- Updating only `MessageOpenOrder` / `MessageCloseOrder`; those fix order display, not position quantity.

Root Cause
- SCH TRD order polling detected open/closed order changes, but did not refresh and push positions immediately after a fill/removal/fill-state change.
- Quantower does not infer updated position quantity from the order row alone.
- QT needs explicit position messages from the vendor:
  - `MessageOpenPosition` for updated/open positions
  - `MessageClosePosition` for positions that disappeared

Fix
- In SCH TRD connector only:
  - added position reconciliation using `MessageOpenPosition`
  - added stale/closed position cleanup using `MessageClosePosition`
  - order polling now triggers `RefreshPositionsAsync(...)` when an order fill/removal/fill-state change is detected
  - scheduled post-order reconciliation also refreshes positions when order state implies execution activity
- This is bounded to order lifecycle changes.
- It does not touch dxFeed, DOM market data, T&S, Level II, history, or volume analysis.

Files Updated
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SCHTRD\Quantower\SchwabTradingVendor.cs`

Verification
- Build:
```powershell
dotnet build "D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SCHTRD\SCHTRD.csproj" -c Release
```
- Result:
  - `0 Error(s)`
  - existing XML-doc warnings only
- Deployed to active QT install:
  - `D:\Quantower _ LATEST\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SCHTRDVendor`
- Deployed DLL timestamp:
  - `SCHTRDVendor.dll` modified `2026-05-14 11:05:45`

Acceptance Check
- Restart QT after deployment so it loads the updated vendor DLL.
- Bridge restart is not required for this DLL-only change.
- Connect SCH TRD.
- Place a small limit order that fills, or observe a real fill.
- Confirm:
  - QT Orders panel removes/updates the filled order.
  - QT Positions panel updates the symbol quantity immediately after the fill.
  - QT P/L and average price refresh from the new position message.

Notes
- This fix follows Quantower's documented position-message protocol.
- Do not solve position update lag with market-data changes.
- Do not add high-frequency market-data or DOM polling.
- If position updates are still late after this fix, inspect Schwab `/api/broker/positions` response timing because QT can only display the position after SCH TRD receives the updated broker position snapshot.

---

## Issue 16: SCH TRD Order Action Timing Baseline From QT Logs

Symptoms
- User needs to know whether QT-to-Schwab order action timing is acceptable.
- DOM order placement, modification, or cancellation may feel slow during fast market movement.

Meaning
- There are two separate timing legs:
  - QT request to Schwab operation result: broker/API round trip.
  - Schwab operation result to QT order update/remove: QT vendor message propagation.
- The first leg is mostly outside QT once `Vendor.PlaceOrder`, `Vendor.ModifyOrder`, or `Vendor.CancelOrder` has called the backend.
- The second leg is controlled by QT vendor protocol and should be nearly immediate when `MessageOpenOrder` / `MessageCloseOrder` is pushed after success.

Checks
- Extract SCH TRD order request/result timing from QT Serilog:
```powershell
Select-String -LiteralPath 'D:\Quantower _ LATEST\Quantower\Logs\Serilog\20260514.slog' -Pattern 'Limit order placing request|Limit order modify request|Limit order cancel request|Trading operation result|Order update|Order remove'
```

What Was Researched
- QT log entries on 2026-05-14 for SCH TRD order actions.
- Request-to-result timing was calculated by matching `RequestId`.
- The most important comparison is:
  - `Limit order ... request`
  - matching `Trading operation result`
  - then immediate `Order update` / `Order remove`

What Did Not Help
- Changing dxFeed, T&S, DOM, or symbol mapping settings.
- Treating order routing latency as market-data latency.
- Waiting for order polling as the primary display mechanism.

Root Cause
- Remaining user-visible order action delay is primarily Schwab API round-trip time, not QT display propagation, after the immediate order-message fix.
- QT display propagation after success is typically milliseconds when the vendor pushes `MessageOpenOrder` / `MessageCloseOrder` immediately.

Fix
- Keep the QT vendor path:
  - user action enters `Vendor.PlaceOrder`, `Vendor.ModifyOrder`, or `Vendor.CancelOrder`
  - backend sends the Schwab order action
  - after Schwab confirms, return `TradingOperationResult.CreateSuccess`
  - immediately push `MessageOpenOrder` for open/replaced orders
  - immediately push `MessageCloseOrder` for stale/replaced/canceled order ids
- Do not fake pre-confirmation order states in QT.

Verification
- 2026-05-14 successful SCH TRD timing from QT logs:
  - place orders: average about `3.7s`, min about `2.7s`, max about `6.1s`
  - modify orders: average about `2.6s`, min about `1.5s`, max about `6.6s`
  - cancel orders: average about `3.6s`, min about `2.6s`, max about `4.5s`
- After the immediate order-message fix, examples show result-to-QT update/remove near immediate:
  - `10:07:23.051` modify request RXT to `6.08`
  - `10:07:25.026` Schwab/QT operation success
  - `10:07:25.027` QT order update at `6.08`
  - QT post-success display propagation was effectively immediate; the `~1.98s` delay was the broker/API leg.

Acceptance Check
- For future tests, calculate:
  - request to result: acceptable if typically near `1.5s-3.5s`, but not ideal for ultra-fast scalping
  - result to QT order update/remove: should be near immediate, usually milliseconds
- If request-to-result spikes above `5s`, treat as Schwab/backend/API latency investigation.
- If result-to-QT update is above `1s`, treat as QT vendor message/caching defect.

Notes
- This baseline is specific to SCH TRD over Schwab's API and Quantower vendor integration.
- It is not comparable to a direct-access broker or native IBKR-style low-latency order route.
- For QT performance, avoid adding high-frequency polling or market-data diagnostics to solve broker order latency.

---

## Issue 17: SCH TRD Open Order Shows In Orders Grid But Not DOM / DOM Surface Order Overlay

Symptoms
- QT Orders panel shows an active SCH TRD order.
- The same symbol is open in DOM Trader or DOM Surface with SCH TRD as the tradable connection and dxFeed as the mapped data connection.
- The resting order price is visible in the ladder, but the DOM own-order `B` / `S` column or DOM Surface order line stays blank at that price.
- Example observed on 2026-05-18:
  - `ZETA`
  - `Buy`
  - `Limit`
  - price `18.75`
  - quantity `1`
  - order appeared in Orders grid but not in the DOM `B`/bid own-order column.
- Validated breakthrough example observed on 2026-05-19:
  - `NOW`
  - `Sell`
  - `Limit`
  - price `103.50`
  - quantity `1`
  - order appeared in Orders grid, DOM Trader, and DOM Surface/heatmap after the fix.

Meaning
- The broker order exists and QT receives enough order state to populate the Orders grid.
- The failure is not a dxFeed market-data problem.
- The failure is in how SCH TRD publishes the Quantower open-order lifecycle message used by DOM own-order overlays and DOM Surface order lines.

Checks
- Confirm the order is active in QT Orders grid.
- Confirm the same symbol/account is selected in DOM Trader.
- Confirm the DOM View menu has `Orders` enabled.
- Confirm the SCH TRD source sends `MessageOpenOrder` for the active order.
- Confirm `MessageOpenOrder.PositionId` is set.
- Do not change dxFeed, Level II, T&S, chart, or symbol-mapping data settings for this issue.

What Was Researched
- Checked the prior DOM own-order troubleshooting section around lines 2696-2745.
- Verified the active SCH TRD code path in:
  - `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SCHTRD\Quantower\SchwabTradingVendor.cs`
- Verified current Quantower v1.146.7 build behavior by compiling against:
  - `D:\Quantower _ LATEST\Quantower\TradingPlatform\v1.146.7\bin\TradingPlatform.BusinessLayer.dll`
- Confirmed `MessageOpenOrder` in this QT build exposes:
  - `AccountId`
  - `OrderId`
  - `GroupId`
  - `PositionId`
  - `SymbolId`
  - `Price`
  - `TriggerPrice`
  - `OrderTypeId`
  - `Side`
  - `Status`
  - `TotalQuantity`
  - `FilledQuantity`
- Compared against the working IBKR behavior:
  - IBKR open limit orders display as native QT order overlays in DOM Trader and DOM Surface.
  - A `SNAP IBKR` sell limit order at `5.74` quantity `1` displayed as `LMT 1` at the exact price level.
- Rechecked SCH TRD's `CreateOpenOrder()` path and confirmed it had a helper capable of generating a normalized position id:
  - `GetOrderPositionId(order)`

What Did Not Help
- Adding `RemainingQuantity` to `MessageOpenOrder`; the active QT API does not support that property.
- Treating this as a market-data display problem.
- Changing dxFeed or DOM Level II settings.
- Adding more polling.
- Removing `PositionId` from open-order messages. That interpretation was wrong for the active v1.146.7 behavior and must not be repeated.

Root Cause
- SCH TRD was not setting `MessageOpenOrder.PositionId`.
- QT could still display the order in the Orders grid from account/order/symbol fields.
- DOM Trader and DOM Surface order overlays require the order to bind into QT's position/order layer with a matching position id.
- Without `PositionId`, SCH TRD orders were visible in the Orders panel but did not attach to the price ladder or DOM Surface order overlay.

Fix
- In SCH TRD only:
  - set `PositionId = GetOrderPositionId(order)` inside `CreateOpenOrder()`
  - keep `AccountId`, `OrderId`, `GroupId`, normalized symbol id, side, price, order type, status, `TotalQuantity`, and `FilledQuantity`
  - keep using dxFeed through symbol mapping for market data
- This fix stays inside QT's intended `MessageOpenOrder` protocol.
- This fix does not touch dxFeed, Level II, T&S, DOM data settings, chart settings, or symbol mapping.

Files Updated
- `D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SCHTRD\Quantower\SchwabTradingVendor.cs`

Code Shape
```csharp
private static MessageOpenOrder CreateOpenOrder(BrokerOrderDto order)
{
    var filledQuantity = Math.Abs(order.FilledQuantity ?? 0d);
    var totalQuantity = Math.Abs(order.Quantity ?? 0d);
    var message = new MessageOpenOrder(NormalizeSymbolKey(order.Symbol ?? string.Empty))
    {
        AccountId = order.AccountHash,
        OrderId = order.OrderId,
        GroupId = ResolveOrderGroupId(order),
        PositionId = GetOrderPositionId(order),
        Price = order.Price ?? double.NaN,
        TriggerPrice = order.TriggerPrice ?? order.StopPrice ?? double.NaN,
        OrderTypeId = ConvertSchwabOrderType(order.OrderType),
        Side = ConvertInstructionSide(order.Instruction),
        Status = ConvertOrderStatus(order.Status),
        TotalQuantity = totalQuantity,
        FilledQuantity = filledQuantity
    };

    return message;
}
```

Verification
- Build:
```powershell
dotnet build 'D:\GitHub\Claude Code\schwab-quantower-project\schwab-quantower-bridge\src\SCHTRD\SCHTRD.csproj' -c Release
```
- Result:
  - `0 Error(s)`
  - `0 Warning(s)` on the 2026-05-19 validation build
- Deployment path:
  - `D:\Quantower _ LATEST\Quantower\TradingPlatform\v1.146.7\bin\Vendors\SCHTRDVendor\SCHTRDVendor.dll`
- Restart requirement:
  - QT must be closed before copying the DLL.
  - QT must be restarted after deployment.
  - Schwab backend restart is not required for this DLL-only change.
- Runtime validation:
  - Place a small SCH TRD limit order.
  - Confirm it appears in Orders grid.
  - Confirm it appears as an order marker in DOM Trader.
  - Confirm it appears as an order line/marker in DOM Surface/heatmap.
  - Confirm cancel removes the marker.

Acceptance Check
- Restart QT after deployment.
- Connect dxFeed first, then SCH TRD.
- Open a mapped SCH TRD DOM Trader.
- Place a small resting limit order away from market.
- Confirm:
  - Orders grid shows the active order.
  - DOM own-order column displays the order quantity at the exact price level.
  - DOM Surface/heatmap displays the order line/marker at the exact price level.
  - Modify/cancel still updates the order through `MessageOpenOrder` / `MessageCloseOrder`.

Notes
- This was a major SCH TRD/QT integration breakthrough.
- The fix confirms the issue was QT open-order message binding, not dxFeed, not DOM settings, not T&S, and not Schwab order placement.
- If this issue returns, inspect `CreateOpenOrder()` first and confirm `PositionId = GetOrderPositionId(order)` is still present before changing anything else.
