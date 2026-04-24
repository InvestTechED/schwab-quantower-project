using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class PlaceBrokerOrderRequestDto
{
    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("quantity")]
    public double Quantity { get; set; }

    [JsonPropertyName("instruction")]
    public string Instruction { get; set; } = string.Empty;

    [JsonPropertyName("order_type")]
    public string OrderType { get; set; } = "LIMIT";

    [JsonPropertyName("limit_price")]
    public double? LimitPrice { get; set; }

    [JsonPropertyName("time_in_force")]
    public string? TimeInForce { get; set; }

    [JsonPropertyName("stop_loss_price")]
    public double? StopLossPrice { get; set; }

    [JsonPropertyName("take_profit_price")]
    public double? TakeProfitPrice { get; set; }

    [JsonPropertyName("trailing_stop_offset")]
    public double? TrailingStopOffset { get; set; }
}
