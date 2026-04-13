using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class OptionContractDto
{
    [JsonPropertyName("symbol")]
    public string Symbol { get; set; } = string.Empty;

    [JsonPropertyName("underlier_symbol")]
    public string UnderlierSymbol { get; set; } = string.Empty;

    [JsonPropertyName("description")]
    public string Description { get; set; } = string.Empty;

    [JsonPropertyName("exchange")]
    public string? Exchange { get; set; }

    [JsonPropertyName("option_type")]
    public string OptionType { get; set; } = string.Empty;

    [JsonPropertyName("strike_price")]
    public double StrikePrice { get; set; }

    [JsonPropertyName("expiration_date")]
    public DateTime ExpirationDate { get; set; }

    [JsonPropertyName("days_to_expiration")]
    public int? DaysToExpiration { get; set; }

    [JsonPropertyName("bid")]
    public double? Bid { get; set; }

    [JsonPropertyName("ask")]
    public double? Ask { get; set; }

    [JsonPropertyName("last")]
    public double? Last { get; set; }

    [JsonPropertyName("mark")]
    public double? Mark { get; set; }

    [JsonPropertyName("bid_size")]
    public int? BidSize { get; set; }

    [JsonPropertyName("ask_size")]
    public int? AskSize { get; set; }

    [JsonPropertyName("last_size")]
    public int? LastSize { get; set; }

    [JsonPropertyName("open_interest")]
    public int? OpenInterest { get; set; }

    [JsonPropertyName("volume")]
    public int? Volume { get; set; }

    [JsonPropertyName("volatility")]
    public double? Volatility { get; set; }

    [JsonPropertyName("delta")]
    public double? Delta { get; set; }

    [JsonPropertyName("gamma")]
    public double? Gamma { get; set; }

    [JsonPropertyName("theta")]
    public double? Theta { get; set; }

    [JsonPropertyName("vega")]
    public double? Vega { get; set; }

    [JsonPropertyName("rho")]
    public double? Rho { get; set; }
}
