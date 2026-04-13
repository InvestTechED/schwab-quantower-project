"""
Create or refresh a Schwab token for the workstation backend.

Run this after installing backend dependencies:

    uvicorn app.main:app --reload
    python init_schwab_session.py
"""

from app.services.auth import SchwabAuthService


def main() -> None:
    service = SchwabAuthService()
    client = service.create_client()
    print("Schwab client initialized.")
    print(f"Token written to: {service.token_path}")
    print(f"Client type: {type(client).__name__}")


if __name__ == "__main__":
    main()
