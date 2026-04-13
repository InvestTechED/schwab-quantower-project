# Schwab -> Quantower Data Bridge

Data-only bridge for feeding Schwab-backed equities market data into Quantower.

## Scope

- No order routing
- No account trading
- Quantower remains your execution surface for IBKR or other supported brokers
- This bridge exists only to provide Schwab market data to Quantower

## Current Foundation

- `SchwabBackendClient`: pulls normalized snapshots and bars from the local Schwab workstation backend
- DTO models aligned to the backend API
- Quantower project references wired to the local Quantower install
- Plugin deployment target documented for your machine

## Your Quantower Runtime

- Runtime root:
  `D:\Quantower\TradingPlatform\v1.146.4\bin`
- Plugin deploy path:
  `D:\Quantower\TradingPlatform\v1.146.4\bin\plug-ins\SchwabQuantowerBridge`
