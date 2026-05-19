using System.Text.Json.Serialization;

namespace SchwabQuantowerBridge.Models;

public sealed class BrokerAccountDto
{
    [JsonPropertyName("account_number")]
    public string AccountNumber { get; set; } = string.Empty;

    [JsonPropertyName("account_hash")]
    public string AccountHash { get; set; } = string.Empty;

    [JsonPropertyName("account_type")]
    public string? AccountType { get; set; }

    [JsonPropertyName("liquidation_value")]
    public double? LiquidationValue { get; set; }

    [JsonPropertyName("cash_balance")]
    public double? CashBalance { get; set; }

    [JsonPropertyName("buying_power")]
    public double? BuyingPower { get; set; }

    [JsonPropertyName("cash_available_for_trading")]
    public double? CashAvailableForTrading { get; set; }

    [JsonPropertyName("cash_available_for_withdrawal")]
    public double? CashAvailableForWithdrawal { get; set; }

    [JsonPropertyName("total_cash")]
    public double? TotalCash { get; set; }

    [JsonPropertyName("unsettled_cash")]
    public double? UnsettledCash { get; set; }

    [JsonPropertyName("long_market_value")]
    public double? LongMarketValue { get; set; }
}
