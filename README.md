# Schwab Quantower Project

Standalone project containing the two production components of the Schwab-to-Quantower workflow:

- `schwab-equities-workstation`
- `schwab-quantower-bridge`

## Purpose

This repository is intended to be the dedicated source-control home for the Schwab / Quantower integration work, separate from any unrelated projects.

## Components

### `schwab-equities-workstation`

Python/FastAPI workstation backend and frontend shell used for:

- Schwab authentication
- normalized market data endpoints
- account, position, and order endpoints
- option chain and series endpoints

### `schwab-quantower-bridge`

C# Quantower vendor/plugin integration used for:

- real-time equities data
- Level II / DOM support
- account and position surfaces
- order synchronization
- options analytics support

## Publishing Guidance

This project should be pushed to GitHub as its **own private repository** first.

Recommended repo name:

- `schwab-quantower-project`

## Notes

- secrets and tokens should never be committed
- `.env` remains local-only
- token JSON files remain local-only

