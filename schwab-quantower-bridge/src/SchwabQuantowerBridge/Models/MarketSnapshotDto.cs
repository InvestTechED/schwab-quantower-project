using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class MarketSnapshotDto
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("as_of")]
    public DateTimeOffset As_Of { get; set; }

    [JsonPropertyName("last")]
    public double Last { get; set; }

    [JsonPropertyName("bid")]
    public double Bid { get; set; }

    [JsonPropertyName("ask")]
    public double Ask { get; set; }

    [JsonPropertyName("bid_size")]
    public long Bid_Size { get; set; }

    [JsonPropertyName("ask_size")]
    public long Ask_Size { get; set; }

    [JsonPropertyName("open")]
    public double Open { get; set; }

    [JsonPropertyName("high")]
    public double High { get; set; }

    [JsonPropertyName("low")]
    public double Low { get; set; }

    [JsonPropertyName("close")]
    public double Close { get; set; }

    [JsonPropertyName("volume")]
    public long Volume { get; set; }

    [JsonPropertyName("open_interest")]
    public long Open_Interest { get; set; }

    [JsonPropertyName("volatility")]
    public double Volatility { get; set; }

    [JsonPropertyName("delta")]
    public double Delta { get; set; }

    [JsonPropertyName("gamma")]
    public double Gamma { get; set; }

    [JsonPropertyName("theta")]
    public double Theta { get; set; }

    [JsonPropertyName("vega")]
    public double Vega { get; set; }

    [JsonPropertyName("rho")]
    public double Rho { get; set; }

    [JsonPropertyName("asset_type")]
    public string? Asset_Type { get; set; }

    [JsonPropertyName("description")]
    public string? Description { get; set; }

    [JsonPropertyName("exchange")]
    public string? Exchange { get; set; }

    [JsonPropertyName("relative_volume")]
    public double Relative_Volume { get; set; }

    [JsonPropertyName("trend_state")]
    public string Trend_State { get; set; } = string.Empty;

    [JsonPropertyName("vwap_bias")]
    public string Vwap_Bias { get; set; } = string.Empty;
}
