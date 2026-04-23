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
