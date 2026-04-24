from app.models import PriceActionReport, Signal


class PriceActionAnalyzer:
    """Local first price action and volume analysis."""

    def build_report(self, snapshot):
        signals = [
            Signal(
                name="trend_alignment",
                score=78,
                summary=f"{snapshot.symbol} is trading in a {snapshot.trend_state} structure."
            ),
            Signal(
                name="relative_volume",
                score=min(snapshot.relative_volume * 40, 100),
                summary=f"Relative volume is running at {snapshot.relative_volume:.2f}x normal."
            ),
            Signal(
                name="vwap_location",
                score=70 if snapshot.vwap_bias == "above" else 45,
                summary=f"Price is {snapshot.vwap_bias} session VWAP."
            )
        ]

        conclusion = (
            "Momentum is constructive and volume is supportive."
            if snapshot.trend_state == "bullish" and snapshot.relative_volume >= 1.2
            else "Setup quality is mixed; monitor for stronger trend and cleaner volume confirmation."
        )

        return PriceActionReport(
            symbol=snapshot.symbol,
            snapshot=snapshot,
            signals=signals,
            conclusion=conclusion
        )
