SYMBOL_ALIASES: dict[str, str] = {
    "VIX": "$VIX",
    "SPX": "$SPX",
    "RUT": "$RUT",
}

DISPLAY_SYMBOL_ALIASES: dict[str, str] = {value: key for key, value in SYMBOL_ALIASES.items()}


def display_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def normalize_symbol(symbol: str) -> str:
    normalized = display_symbol(symbol)
    return SYMBOL_ALIASES.get(normalized, normalized)


def denormalize_symbol(symbol: str) -> str:
    normalized = display_symbol(symbol)
    return DISPLAY_SYMBOL_ALIASES.get(normalized, normalized)
