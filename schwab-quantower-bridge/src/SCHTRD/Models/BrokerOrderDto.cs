using System;
using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class BrokerOrderDto
{
    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("order_id")]
    public string OrderId { get; set; } = string.Empty;

    [JsonPropertyName("symbol")]
    public string? Symbol { get; set; }

    [JsonPropertyName("instruction")]
    public string? Instruction { get; set; }

    [JsonPropertyName("order_type")]
    public string? OrderType { get; set; }

    [JsonPropertyName("order_strategy_type")]
    public string? OrderStrategyType { get; set; }

    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("original_status")]
    public string? OriginalStatus { get; set; }

    [JsonPropertyName("duration")]
    public string? Duration { get; set; }

    [JsonPropertyName("session")]
    public string? Session { get; set; }

    [JsonPropertyName("entered_time")]
    public DateTimeOffset? EnteredTime { get; set; }

    [JsonPropertyName("close_time")]
    public DateTimeOffset? CloseTime { get; set; }

    [JsonPropertyName("expiration_time")]
    public DateTimeOffset? ExpirationTime { get; set; }

    [JsonPropertyName("quantity")]
    public double? Quantity { get; set; }

    [JsonPropertyName("filled_quantity")]
    public double? FilledQuantity { get; set; }

    [JsonPropertyName("remaining_quantity")]
    public double? RemainingQuantity { get; set; }

    [JsonPropertyName("average_fill_price")]
    public double? AverageFillPrice { get; set; }

    [JsonPropertyName("price")]
    public double? Price { get; set; }

    [JsonPropertyName("stop_price")]
    public double? StopPrice { get; set; }

    [JsonPropertyName("trail_offset")]
    public double? TrailOffset { get; set; }

    [JsonPropertyName("trigger_price")]
    public double? TriggerPrice { get; set; }

    [JsonPropertyName("position_id")]
    public string? PositionId { get; set; }

    [JsonPropertyName("group_id")]
    public string? GroupId { get; set; }
}
