"""
Create or refresh a Schwab token for the Schwab Quantower bridge backend.

Run this after installing backend dependencies:

    python init_schwab_session.py
"""

import argparse

from app.services.auth import SchwabAuthService


def main() -> None:
    parser = argparse.ArgumentParser(description="Create or refresh the Schwab bridge token.")
    parser.add_argument(
        "--force-login",
        action="store_true",
        help="Back up any existing token and run the browser-assisted Schwab login flow."
    )
    parser.add_argument(
        "--callback-timeout",
        type=float,
        default=300.0,
        help="Seconds to wait for the Schwab browser callback. Use 0 to wait indefinitely."
    )
    args = parser.parse_args()

    service = SchwabAuthService()
    if args.force_login:
        backup_path = service.backup_existing_token()
        if backup_path is not None:
            print(f"Existing token backed up to: {backup_path}")
        print("Starting browser-assisted Schwab login flow...")
        client = service.create_client_via_login_flow(callback_timeout=args.callback_timeout)
    else:
        client = service.create_client()

    print("Schwab client initialized.")
    print(f"Token written to: {service.token_path}")
    print(f"Client type: {type(client).__name__}")


if __name__ == "__main__":
    main()

