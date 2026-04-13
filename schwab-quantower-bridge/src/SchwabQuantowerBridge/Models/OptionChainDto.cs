using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class OptionChainDto
{
    [JsonPropertyName("underlier_symbol")]
    public string UnderlierSymbol { get; set; } = string.Empty;

    [JsonPropertyName("underlier_last")]
    public double? UnderlierLast { get; set; }

    [JsonPropertyName("underlier_bid")]
    public double? UnderlierBid { get; set; }

    [JsonPropertyName("underlier_ask")]
    public double? UnderlierAsk { get; set; }

    [JsonPropertyName("series")]
    public List<OptionSeriesDto> Series { get; set; } = [];

    [JsonPropertyName("contracts")]
    public List<OptionContractDto> Contracts { get; set; } = [];
}
