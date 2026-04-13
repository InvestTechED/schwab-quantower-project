# Schwab Equities Workstation

Modern equities analysis workstation scaffold focused on Schwab-backed market data.

## Goals

- Modern desktop-style web UI
- Equities-first price action and volume analysis
- Clean backend boundaries for Schwab integration
- Structured market snapshots and signal analysis
- Easy expansion into options and derivatives later

## Architecture

- `frontend/`: React + TypeScript + Vite dashboard
- `backend/`: FastAPI service with Schwab adapter boundaries

## Do You Need A Schwab API Key?

Not to build the app shell.

You do need Schwab developer credentials to:

- authenticate against Schwab
- pull live quotes and historical bars
- test streaming and account-linked features

This scaffold includes mock-friendly service boundaries so UI work can continue before live credentials are added.

## Planned First Features

- Watchlist and symbol search
- Price action dashboard
- Volume analysis panel
- Signal summary cards
- Schwab health / connection status
- Historical bar endpoint

## Next Steps

1. Create Schwab developer credentials.
2. Add OAuth/token handling to the backend.
3. Replace mock snapshots with live Schwab data.
4. Add streaming updates and richer signal analysis.
