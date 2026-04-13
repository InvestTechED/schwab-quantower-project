using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class BrokerPositionDto
{
    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("quantity")]
    public double Quantity { get; set; }

    [JsonPropertyName("average_price")]
    public double? AveragePrice { get; set; }

    [JsonPropertyName("market_value")]
    public double? MarketValue { get; set; }

    [JsonPropertyName("market_price")]
    public double? MarketPrice { get; set; }

    [JsonPropertyName("asset_type")]
    public string? AssetType { get; set; }

    [JsonPropertyName("instrument_type")]
    public string? InstrumentType { get; set; }

    [JsonPropertyName("description")]
    public string? Description { get; set; }

    [JsonPropertyName("day_profit_loss")]
    public double? DayProfitLoss { get; set; }

    [JsonPropertyName("day_profit_loss_percent")]
    public double? DayProfitLossPercent { get; set; }

    [JsonPropertyName("unrealized_profit_loss")]
    public double? UnrealizedProfitLoss { get; set; }
}
