# src/exceptions.py

class MarketClosedError(Exception):
    """Raised when current time exceeds market close."""
    pass
