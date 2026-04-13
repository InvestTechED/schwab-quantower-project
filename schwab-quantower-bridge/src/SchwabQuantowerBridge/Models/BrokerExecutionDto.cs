using System;
using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class BrokerExecutionDto
{
    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("execution_id")]
    public string ExecutionId { get; set; } = string.Empty;

    [JsonPropertyName("order_id")]
    public string OrderId { get; set; } = string.Empty;

    [JsonPropertyName("symbol")]
    public string? Symbol { get; set; }

    [JsonPropertyName("instruction")]
    public string? Instruction { get; set; }

    [JsonPropertyName("execution_type")]
    public string? ExecutionType { get; set; }

    [JsonPropertyName("position_effect")]
    public string? PositionEffect { get; set; }

    [JsonPropertyName("executed_time")]
    public DateTimeOffset? ExecutedTime { get; set; }

    [JsonPropertyName("quantity")]
    public double? Quantity { get; set; }

    [JsonPropertyName("price")]
    public double? Price { get; set; }

    [JsonPropertyName("gross_amount")]
    public double? GrossAmount { get; set; }

    [JsonPropertyName("fees")]
    public double? Fees { get; set; }
}
