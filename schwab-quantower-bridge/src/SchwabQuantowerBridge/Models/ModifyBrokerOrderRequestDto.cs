using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class ModifyBrokerOrderRequestDto
{
    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("order_id")]
    public string OrderId { get; set; } = string.Empty;

    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("quantity")]
    public double Quantity { get; set; }

    [JsonPropertyName("instruction")]
    public string Instruction { get; set; } = string.Empty;

    [JsonPropertyName("order_type")]
    public string OrderType { get; set; } = "LIMIT";

    [JsonPropertyName("limit_price")]
    public double LimitPrice { get; set; }

    [JsonPropertyName("time_in_force")]
    public string? TimeInForce { get; set; }
}
