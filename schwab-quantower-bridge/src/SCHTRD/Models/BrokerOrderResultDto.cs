using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class BrokerOrderResultDto
{
    [JsonPropertyName("status_code")]
    public int StatusCode { get; set; }

    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("order_id")]
    public string? OrderId { get; set; }
}
