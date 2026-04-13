using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using SchwabQuantowerBridge.Models;

namespace SchwabQuantowerBridge.Services;

public sealed class SchwabBackendClient
{
    private readonly HttpClient httpClient;
    private readonly BridgeSettings settings;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public SchwabBackendClient(HttpClient httpClient, BridgeSettings settings)
    {
        this.httpClient = httpClient;
        this.settings = settings;
        this.httpClient.Timeout = TimeSpan.FromSeconds(8);
    }

    public async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        using var response = await this.httpClient.GetAsync(
            $"{this.settings.BackendBaseUrl}/api/health",
            cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<BridgeStreamStatusDto?> GetStreamStatusAsync(CancellationToken cancellationToken = default)
    {
        return await this.httpClient.GetFromJsonAsync<BridgeStreamStatusDto>(
            this.settings.StreamStatusRoute(),
            JsonOptions,
            cancellationToken);
    }

    public async Task<MarketSnapshotDto?> GetSnapshotAsync(string symbol, CancellationToken cancellationToken = default)
    {
        return await this.httpClient.GetFromJsonAsync<MarketSnapshotDto>(
            this.settings.SnapshotRoute(symbol.ToUpperInvariant()),
            JsonOptions,
            cancellationToken);
    }

    public async Task<SymbolProfileDto?> GetSymbolProfileAsync(string symbol, CancellationToken cancellationToken = default)
    {
        return await this.httpClient.GetFromJsonAsync<SymbolProfileDto>(
            this.settings.SymbolRoute(symbol.ToUpperInvariant()),
            JsonOptions,
            cancellationToken);
    }

    public async Task<IReadOnlyList<SymbolProfileDto>> SearchSymbolsAsync(
        string query,
        int limit = 10,
        CancellationToken cancellationToken = default)
    {
        var symbols = await this.httpClient.GetFromJsonAsync<List<SymbolProfileDto>>(
            this.settings.SearchRoute(query, limit),
            JsonOptions,
            cancellationToken);

        return symbols ?? [];
    }

    public async Task<IReadOnlyList<BarDto>> GetBarsAsync(
        string symbol,
        string timeframe = "5m",
        int limit = 500,
        DateTime? start = null,
        DateTime? end = null,
        CancellationToken cancellationToken = default)
    {
        var bars = await this.httpClient.GetFromJsonAsync<List<BarDto>>(
            this.settings.BarsRoute(symbol.ToUpperInvariant(), timeframe, limit, start, end),
            JsonOptions,
            cancellationToken);

        return bars ?? [];
    }

    public async Task<IReadOnlyList<OptionSeriesDto>> GetOptionSeriesAsync(
        string symbol,
        CancellationToken cancellationToken = default)
    {
        using var response = await this.httpClient.GetAsync(
            this.settings.OptionSeriesRoute(symbol.ToUpperInvariant()),
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return ParseOptionSeries(body);
    }

    public async Task<OptionChainDto?> GetOptionChainAsync(
        string symbol,
        DateTime? expiration = null,
        CancellationToken cancellationToken = default)
    {
        using var response = await this.httpClient.GetAsync(
            this.settings.OptionChainRoute(symbol.ToUpperInvariant(), expiration),
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        return ParseOptionChain(body);
    }

    public async Task<IReadOnlyList<BrokerAccountDto>> GetAccountsAsync(CancellationToken cancellationToken = default)
    {
        var accounts = await this.httpClient.GetFromJsonAsync<List<BrokerAccountDto>>(
            $"{this.settings.BackendBaseUrl}/api/broker/accounts",
            JsonOptions,
            cancellationToken);

        return accounts ?? [];
    }

    public async Task<IReadOnlyList<BrokerPositionDto>> GetPositionsAsync(CancellationToken cancellationToken = default)
    {
        var positions = await this.httpClient.GetFromJsonAsync<List<BrokerPositionDto>>(
            $"{this.settings.BackendBaseUrl}/api/broker/positions",
            JsonOptions,
            cancellationToken);

        return positions ?? [];
    }

    public async Task<IReadOnlyList<BrokerOrderDto>> GetOrdersAsync(CancellationToken cancellationToken = default)
    {
        var orders = await this.httpClient.GetFromJsonAsync<List<BrokerOrderDto>>(
            $"{this.settings.BackendBaseUrl}/api/broker/orders",
            JsonOptions,
            cancellationToken);

        return orders ?? [];
    }

    public async Task<BrokerOrderResultDto?> PlaceOrderAsync(
        string accountHash,
        string symbol,
        double quantity,
        string instruction,
        string orderType,
        double? limitPrice,
        string? timeInForce,
        CancellationToken cancellationToken = default)
    {
        var body = JsonSerializer.Serialize(new
        {
            account_hash = accountHash,
            symbol,
            quantity,
            instruction,
            order_type = orderType,
            limit_price = limitPrice,
            time_in_force = timeInForce
        }, JsonOptions);

        using var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await this.httpClient.PostAsync(
            $"{this.settings.BackendBaseUrl}/api/broker/orders/place",
            content,
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<BrokerOrderResultDto>(JsonOptions, cancellationToken);
    }

    public async Task<BrokerOrderResultDto?> ModifyOrderAsync(
        ModifyBrokerOrderRequestDto request,
        CancellationToken cancellationToken = default)
    {
        var body = JsonSerializer.Serialize(request, JsonOptions);

        using var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await this.httpClient.PostAsync(
            $"{this.settings.BackendBaseUrl}/api/broker/orders/modify",
            content,
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<BrokerOrderResultDto>(JsonOptions, cancellationToken);
    }

    public async Task CancelOrderAsync(string accountHash, string orderId, CancellationToken cancellationToken = default)
    {
        using var response = await this.httpClient.DeleteAsync(
            $"{this.settings.BackendBaseUrl}/api/broker/orders/{accountHash}/{orderId}",
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
    }

    private static async Task EnsureSuccessWithDetailAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var detail = ExtractErrorDetail(body);
        throw new InvalidOperationException(
            string.IsNullOrWhiteSpace(detail)
                ? $"{(int)response.StatusCode} {response.ReasonPhrase}"
                : $"{(int)response.StatusCode} {response.ReasonPhrase}: {detail}");
    }

    private static string ExtractErrorDetail(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return string.Empty;

        try
        {
            using var document = JsonDocument.Parse(body);
            if (document.RootElement.TryGetProperty("detail", out var detail))
                return detail.ValueKind == JsonValueKind.String ? detail.GetString() ?? string.Empty : detail.ToString();
        }
        catch (JsonException)
        {
        }

        return body;
    }

    private static IReadOnlyList<OptionSeriesDto> ParseOptionSeries(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return [];

        using var document = JsonDocument.Parse(body);
        if (document.RootElement.ValueKind != JsonValueKind.Array)
            return [];

        var series = new List<OptionSeriesDto>();
        foreach (var item in document.RootElement.EnumerateArray())
        {
            series.Add(new OptionSeriesDto
            {
                Id = ReadString(item, "id"),
                UnderlierSymbol = ReadString(item, "underlier_symbol"),
                ExpirationDate = ReadDateTime(item, "expiration_date"),
                DaysToExpiration = ReadNullableInt(item, "days_to_expiration"),
                SeriesType = ReadString(item, "series_type"),
                Name = ReadString(item, "name"),
                Exchange = ReadNullableString(item, "exchange")
            });
        }

        return series;
    }

    private static OptionChainDto? ParseOptionChain(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
            return null;

        using var document = JsonDocument.Parse(body);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
            return null;

        var root = document.RootElement;
        var chain = new OptionChainDto
        {
            UnderlierSymbol = ReadString(root, "underlier_symbol"),
            UnderlierLast = ReadNullableDouble(root, "underlier_last"),
            UnderlierBid = ReadNullableDouble(root, "underlier_bid"),
            UnderlierAsk = ReadNullableDouble(root, "underlier_ask")
        };

        if (root.TryGetProperty("series", out var seriesElement) && seriesElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in seriesElement.EnumerateArray())
            {
                chain.Series.Add(new OptionSeriesDto
                {
                    Id = ReadString(item, "id"),
                    UnderlierSymbol = ReadString(item, "underlier_symbol"),
                    ExpirationDate = ReadDateTime(item, "expiration_date"),
                    DaysToExpiration = ReadNullableInt(item, "days_to_expiration"),
                    SeriesType = ReadString(item, "series_type"),
                    Name = ReadString(item, "name"),
                    Exchange = ReadNullableString(item, "exchange")
                });
            }
        }

        if (root.TryGetProperty("contracts", out var contractsElement) && contractsElement.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in contractsElement.EnumerateArray())
            {
                chain.Contracts.Add(new OptionContractDto
                {
                    Symbol = ReadString(item, "symbol"),
                    UnderlierSymbol = ReadString(item, "underlier_symbol"),
                    Description = ReadString(item, "description"),
                    Exchange = ReadNullableString(item, "exchange"),
                    OptionType = ReadString(item, "option_type"),
                    StrikePrice = ReadDouble(item, "strike_price"),
                    ExpirationDate = ReadDateTime(item, "expiration_date"),
                    DaysToExpiration = ReadNullableInt(item, "days_to_expiration"),
                    Bid = ReadNullableDouble(item, "bid"),
                    Ask = ReadNullableDouble(item, "ask"),
                    Last = ReadNullableDouble(item, "last"),
                    Mark = ReadNullableDouble(item, "mark"),
                    BidSize = ReadNullableInt(item, "bid_size"),
                    AskSize = ReadNullableInt(item, "ask_size"),
                    LastSize = ReadNullableInt(item, "last_size"),
                    OpenInterest = ReadNullableInt(item, "open_interest"),
                    Volume = ReadNullableInt(item, "volume"),
                    Volatility = ReadNullableDouble(item, "volatility"),
                    Delta = ReadNullableDouble(item, "delta"),
                    Gamma = ReadNullableDouble(item, "gamma"),
                    Theta = ReadNullableDouble(item, "theta"),
                    Vega = ReadNullableDouble(item, "vega"),
                    Rho = ReadNullableDouble(item, "rho")
                });
            }
        }

        return chain;
    }

    private static string ReadString(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : string.Empty;
    }

    private static string? ReadNullableString(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
    }

    private static DateTime ReadDateTime(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value))
            return default;

        return value.ValueKind == JsonValueKind.String && value.TryGetDateTime(out var parsed)
            ? parsed
            : default;
    }

    private static int? ReadNullableInt(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value))
            return null;

        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var parsed))
            return parsed;

        return null;
    }

    private static double ReadDouble(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value))
            return 0d;

        return value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var parsed)
            ? parsed
            : 0d;
    }

    private static double? ReadNullableDouble(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var value))
            return null;

        if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var parsed))
            return parsed;

        return null;
    }
}
