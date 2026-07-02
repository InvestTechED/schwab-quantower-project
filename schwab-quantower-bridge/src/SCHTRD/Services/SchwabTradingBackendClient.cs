using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using SchwabQuantowerBridge.Models;

namespace SchwabQuantowerBridge.Services;

public sealed class SchwabTradingBackendClient
{
    private const string BackendBaseUrl = "http://127.0.0.1:8000";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient httpClient;

    public SchwabTradingBackendClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;
        this.httpClient.Timeout = TimeSpan.FromSeconds(30);
    }

    public async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        using var response = await this.httpClient.GetAsync(
            $"{BackendBaseUrl}/api/health",
            cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<IReadOnlyList<BrokerAccountDto>> GetAccountsAsync(CancellationToken cancellationToken = default)
    {
        var accounts = await this.httpClient.GetFromJsonAsync<List<BrokerAccountDto>>(
            $"{BackendBaseUrl}/api/broker/accounts",
            JsonOptions,
            cancellationToken);

        return accounts ?? [];
    }

    public async Task<IReadOnlyList<BrokerPositionDto>> GetPositionsAsync(CancellationToken cancellationToken = default)
    {
        var positions = await this.httpClient.GetFromJsonAsync<List<BrokerPositionDto>>(
            $"{BackendBaseUrl}/api/broker/positions",
            JsonOptions,
            cancellationToken);

        return positions ?? [];
    }

    public async Task<IReadOnlyList<BrokerOrderDto>> GetOrdersAsync(CancellationToken cancellationToken = default)
    {
        var orders = await this.httpClient.GetFromJsonAsync<List<BrokerOrderDto>>(
            $"{BackendBaseUrl}/api/broker/orders",
            JsonOptions,
            cancellationToken);

        return orders ?? [];
    }

    public async Task<BrokerOrderResultDto?> PlaceOrderAsync(
        PlaceBrokerOrderRequestDto request,
        CancellationToken cancellationToken = default)
    {
        var body = JsonSerializer.Serialize(request, JsonOptions);
        using var content = new StringContent(body, Encoding.UTF8, "application/json");
        using var response = await this.httpClient.PostAsync(
            $"{BackendBaseUrl}/api/broker/orders/place",
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
            $"{BackendBaseUrl}/api/broker/orders/modify",
            content,
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<BrokerOrderResultDto>(JsonOptions, cancellationToken);
    }

    public async Task CancelOrderAsync(string accountHash, string orderId, CancellationToken cancellationToken = default)
    {
        using var response = await this.httpClient.DeleteAsync(
            $"{BackendBaseUrl}/api/broker/orders/{Uri.EscapeDataString(accountHash)}/{Uri.EscapeDataString(orderId)}",
            cancellationToken);

        await EnsureSuccessWithDetailAsync(response, cancellationToken);
    }

    private static async Task EnsureSuccessWithDetailAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        throw new InvalidOperationException(
            string.IsNullOrWhiteSpace(body)
                ? $"{(int)response.StatusCode} {response.ReasonPhrase}"
                : $"{(int)response.StatusCode} {response.ReasonPhrase}: {body}");
    }
}
