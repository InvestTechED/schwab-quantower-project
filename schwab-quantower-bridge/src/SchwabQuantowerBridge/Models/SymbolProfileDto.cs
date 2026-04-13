using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class SymbolProfileDto
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("normalized_symbol")]
    public string NormalizedSymbol { get; set; } = string.Empty;

    [JsonPropertyName("asset_type")]
    public string? AssetType { get; set; }

    [JsonPropertyName("instrument_type")]
    public string? InstrumentType { get; set; }

    [JsonPropertyName("description")]
    public string? Description { get; set; }

    [JsonPropertyName("exchange")]
    public string? Exchange { get; set; }

    [JsonPropertyName("options_available")]
    public bool OptionsAvailable { get; set; }
}
