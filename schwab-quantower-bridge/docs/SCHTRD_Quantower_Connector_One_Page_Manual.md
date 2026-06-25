# SCH TRD Quantower Connector - One-Page Manual

## Purpose

SCH TRD is a Schwab trading connector for Quantower. Its intended role is account access and order execution for Schwab equities while Quantower uses dxFeed for all market-data services.

## Core Architecture

- SCH TRD is execution-only.
- dxFeed provides quotes, trades, DOM, tick history, minute history, day history, and native Volume Analysis.
- SCH TRD mapped equities should resolve market-data, history, and Volume Analysis through dxFeed symbol mapping.
- SCH TRD should not be treated as a chart-data, history, or Volume Analysis provider.

## Supported SCH TRD Functions

- Schwab account connection.
- Account and position display.
- Order placement and modification through Quantower.
- Equity order entry from panels such as DOM/Ladder when the tradable symbol is SCH TRD.
- Schwab execution/account state while market data remains sourced from dxFeed.

## Market Data and History Model

SCH TRD intentionally does not provide:

- Real-time chart market data.
- Native chart history.
- Tick history.
- Minute history.
- Daily history.
- Native Quantower Volume Analysis.
- Cluster/profile/Right Profile/Left Profile calculations.

Those services must come from dxFeed through Quantower symbol mapping.

## Required Symbol Mapping

For SCH TRD tradable equities, map each market-data category to dxFeed:

- Quotes & Trades: dxFeed
- Tick History: dxFeed
- Minute History: dxFeed
- Day History: dxFeed
- Volume Analysis: dxFeed

Execution remains SCH TRD. Data remains dxFeed.

## Important Equity Metadata

The SCH TRD symbol metadata must be consistent with normal US equity interpretation in Quantower:

- Exchange: Composite
- Symbol type: Equities
- Quoting currency: USD
- Lot size: 100
- Tick size: 0.01
- Country: US
- Classification of Financial: ESXXXX
- US equities ETH sessions:
  - Pre-market: 4:00 AM-9:30 AM
  - Primary: 9:30 AM-4:00 PM
  - Post-market: 4:00 PM-8:00 PM

Do not casually change lot size, tick size, sessions, or market-data capability flags. These fields affect Quantower's interpretation of DOM, Time & Sales, chart history, and native Volume Analysis behavior.

## Tested Version

- Quantower: v1.146.13
- Known-good commit: `c2bf40333cf8dbbf2fbb58b9df955a1835445fdc`
- Confirmed behavior: SCH TRD mapped equities display correctly across charts, DOM, Time & Sales, daily history, and native Volume Analysis when mapped to dxFeed for data.

## Deployment Notes

Build the `src/SCHTRD` project against the matching Quantower SDK version and deploy the resulting `SCHTRDVendor.dll` into Quantower's vendor plugin folder for that platform version. Quantower must be restarted after replacing the vendor DLL.
