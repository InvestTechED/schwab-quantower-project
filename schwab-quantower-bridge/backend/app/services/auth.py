from pathlib import Path
import os
from datetime import datetime, UTC

from app.config import settings

try:
    from schwab.auth import client_from_login_flow, client_from_token_file
    SCHWAB_AUTH_AVAILABLE = True
except Exception:
    client_from_login_flow = None
    client_from_token_file = None
    SCHWAB_AUTH_AVAILABLE = False


class SchwabAuthService:
    def __init__(self) -> None:
        self.token_path = Path(settings.schwab_token_path)

    @staticmethod
    def _configure_client(client):
        return client

    @staticmethod
    def _clear_proxy_env() -> None:
        for key in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "http_proxy",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ):
            os.environ.pop(key, None)

    def status(self) -> dict[str, object]:
        return {
            "app_env": settings.app_env,
            "callback_url": settings.schwab_callback_url,
            "credentials_present": settings.has_schwab_credentials,
            "schwab_py_available": SCHWAB_AUTH_AVAILABLE,
            "token_path": str(self.token_path),
            "token_exists": self.token_path.exists()
        }

    def ensure_token_directory(self) -> None:
        self.token_path.parent.mkdir(parents=True, exist_ok=True)

    def reset_client(self) -> None:
        return None

    def backup_existing_token(self) -> Path | None:
        if not self.token_path.exists():
            return None

        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        backup_path = self.token_path.with_name(f"{self.token_path.stem}.{timestamp}.bak{self.token_path.suffix}")
        self.token_path.replace(backup_path)
        return backup_path

    def create_client_via_login_flow(self):
        if not SCHWAB_AUTH_AVAILABLE:
            raise RuntimeError("schwab-py is not installed")
        if not settings.has_schwab_credentials:
            raise RuntimeError("Schwab credentials are missing from environment")

        self._clear_proxy_env()
        self.ensure_token_directory()

        client = client_from_login_flow(
            settings.schwab_app_key,
            settings.schwab_app_secret,
            settings.schwab_callback_url,
            str(self.token_path),
            interactive=False
        )
        return self._configure_client(client)

    def create_client(self, force_new_client: bool = False):
        if not SCHWAB_AUTH_AVAILABLE:
            raise RuntimeError("schwab-py is not installed")
        if not settings.has_schwab_credentials:
            raise RuntimeError("Schwab credentials are missing from environment")

        self._clear_proxy_env()
        self.ensure_token_directory()

        if force_new_client:
            return self.create_client_via_login_flow()

        if self.token_path.exists():
            client = client_from_token_file(
                str(self.token_path),
                settings.schwab_app_key,
                settings.schwab_app_secret
            )
            return self._configure_client(client)

        raise RuntimeError(
            f"Schwab token file is missing at {self.token_path}. "
            "Run the login flow manually to create a fresh token."
        )
