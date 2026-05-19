using System;
using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class BrokerTradeDto
{
    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("trade_id")]
    public string TradeId { get; set; } = string.Empty;

    [JsonPropertyName("order_id")]
    public string? OrderId { get; set; }

    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("instruction")]
    public string? Instruction { get; set; }

    [JsonPropertyName("quantity")]
    public double Quantity { get; set; }

    [JsonPropertyName("price")]
    public double Price { get; set; }

    [JsonPropertyName("executed_time")]
    public DateTimeOffset ExecutedTime { get; set; }

    [JsonPropertyName("gross_amount")]
    public double? GrossAmount { get; set; }

    [JsonPropertyName("fees")]
    public double? Fees { get; set; }

    [JsonPropertyName("net_amount")]
    public double? NetAmount { get; set; }

    [JsonPropertyName("position_id")]
    public string? PositionId { get; set; }
}
