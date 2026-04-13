using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class OptionSeriesDto
{
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    [JsonPropertyName("underlier_symbol")]
    public string UnderlierSymbol { get; set; } = string.Empty;

    [JsonPropertyName("expiration_date")]
    public DateTime ExpirationDate { get; set; }

    [JsonPropertyName("days_to_expiration")]
    public int? DaysToExpiration { get; set; }

    [JsonPropertyName("series_type")]
    public string SeriesType { get; set; } = string.Empty;

    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    [JsonPropertyName("exchange")]
    public string? Exchange { get; set; }
}
