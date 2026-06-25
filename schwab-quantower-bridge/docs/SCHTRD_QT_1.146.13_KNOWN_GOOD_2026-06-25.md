# SCH TRD / dxFeed Known-Good State - 2026-06-25

This snapshot documents the first confirmed working Quantower v1.146.13 state where SCH TRD mapped equities display correctly across charts, DOM, Time & Sales, daily history, and native Volume Analysis tools.

## Architecture

- SCH TRD is execution/account only.
- dxFeed provides all quotes, trades, DOM, tick history, minute history, day history, and Volume Analysis for mapped equities.
- SCH TRD must not advertise or provide chart market data, history, or Volume Analysis capability.

## Key Fixes

- SCH TRD equity symbol metadata now uses dxFeed-like equity semantics where QT interpretation depends on it:
  - Exchange: Composite
  - Lot size: 100
  - Tick size: 0.01
  - US equities ETH sessions
  - Country: US
  - Classification of Financial: ESXXXX
- SCH TRD quote subscription is a no-op.
- SCH TRD history metadata explicitly advertises no supported history/VA capability.
- SCH TRD symbol volume type is disabled so QT does not treat SCH TRD as a market-data source.
- Active QT workspace/settings caches were cleaned so simple-equity chart/data/history/VA bindings resolve through dxFeed, not SCH TRD.
- Stale RXT option cache rows were removed because no options data is subscribed.

## Verified Deployment

- Quantower version: 1.146.13
- Live vendor DLL:
  - `D:\Quantower _ LATEST\Quantower\TradingPlatform\v1.146.13\bin\Vendors\SCHTRDVendor\SCHTRDVendor.dll`
  - SHA256: `11F4C941244E0A98AA2BE6CE3DEC47A39024B7F9F6E2C397C9748F2FD7BA6233`
  - Timestamp: `2026-06-25 11:28:03 AM`
- Final active settings hashes after option-row cleanup:
  - `settings.xml`: `CF0B7EDB15FAB2B5A1406E3916E5C5A35B181474F6980374FC988D8532682507`
  - `MAIN  .xml`: `D6106DA60E797A4F6E9998F92D35264EEB5F3C7FBF65434D23FF76AAE9B36862`

## Operating Rule

Do not change vendor lot/DOM/Time & Sales metadata casually. If QT display or sizing looks wrong later, compare SCH TRD symbol info against dxFeed symbol info before changing execution or market-data code.

Reference screenshot:

- `docs/SCHTRD_vs_dxFeed_symbol_config_2026-06-25.png`
