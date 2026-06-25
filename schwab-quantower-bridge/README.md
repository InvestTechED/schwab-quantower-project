# SCH TRD Quantower Connector

SCH TRD is a Schwab trading connector for Quantower. The final working architecture is intentionally simple:

- SCH TRD handles Schwab account access, positions, orders, and execution.
- dxFeed handles all market data, chart data, DOM data, history, and native Quantower Volume Analysis.

SCH TRD is not a market-data bridge. It should not be used as the source for quotes, trades, tick history, minute history, daily history, clusters, profiles, or Volume Analysis.

## Known-Good Version

- Quantower: v1.146.13
- Known-good commit: `c2bf40333cf8dbbf2fbb58b9df955a1835445fdc`
- Current documentation commit: `3b2f071`
- Live deployed DLL hash from the tested environment: `11F4C941244E0A98AA2BE6CE3DEC47A39024B7F9F6E2C397C9748F2FD7BA6233`

## Required Symbol Mapping

For SCH TRD tradable equities, Quantower symbol mapping should route every data category to dxFeed:

- Quotes & Trades: dxFeed
- Tick History: dxFeed
- Minute History: dxFeed
- Day History: dxFeed
- Volume Analysis: dxFeed

Execution remains SCH TRD. Data remains dxFeed.

## Important Equity Metadata

The SCH TRD equity symbol metadata is configured to match normal US equity interpretation inside Quantower:

- Exchange: Composite
- Symbol type: Equities
- Quoting currency: USD
- Lot size: 100
- Tick size: 0.01
- Country: US
- Classification of Financial: ESXXXX
- US equities extended-hours trading sessions

Do not casually change lot size, tick size, sessions, or market-data capability flags. Those fields affect how Quantower interprets DOM, Time & Sales, chart history, and Volume Analysis behavior.

## Documentation

- One-page manual: `docs/SCHTRD_Quantower_Connector_One_Page_Manual.md`
- Known-good technical snapshot: `docs/SCHTRD_QT_1.146.13_KNOWN_GOOD_2026-06-25.md`
- Reference screenshot: `docs/SCHTRD_vs_dxFeed_symbol_config_2026-06-25.png`

## Build

Build the current connector from:

`src/SCHTRD/SCHTRD.csproj`

The project is wired for the Quantower v1.146.13 SDK path used in the tested environment.

## Deployment Note

After replacing the vendor DLL in Quantower, Quantower must be restarted so the updated connector loads.
