from pathlib import Path
import os

from dotenv import load_dotenv


load_dotenv(Path(__file__).resolve().parents[2] / ".env")


class Settings:
    def __init__(self) -> None:
        self.app_env = os.getenv("APP_ENV", "production")
        self.schwab_app_key = os.getenv("SCHWAB_APP_KEY", "")
        self.schwab_app_secret = os.getenv("SCHWAB_APP_SECRET", "")
        self.schwab_callback_url = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182")
        self.schwab_token_path = os.getenv(
            "SCHWAB_TOKEN_PATH",
            str(Path(__file__).resolve().parents[2] / "tokens" / "schwab_token.json")
        )
        self.schwab_trading_enabled = os.getenv("SCHWAB_TRADING_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
        self.schwab_max_order_shares = float(os.getenv("SCHWAB_MAX_ORDER_SHARES", "1"))
        self.schwab_max_order_notional = float(os.getenv("SCHWAB_MAX_ORDER_NOTIONAL", "500"))
        self.schwab_limit_price_max_deviation_pct = float(os.getenv("SCHWAB_LIMIT_PRICE_MAX_DEVIATION_PCT", "20"))
        self.schwab_extended_hours_enabled = os.getenv("SCHWAB_EXTENDED_HOURS_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
        self.schwab_duplicate_window_seconds = float(os.getenv("SCHWAB_DUPLICATE_WINDOW_SECONDS", "5"))

    @property
    def has_schwab_credentials(self) -> bool:
        return bool(self.schwab_app_key and self.schwab_app_secret and self.schwab_callback_url)


settings = Settings()
