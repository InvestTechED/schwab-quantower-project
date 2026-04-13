using System;
using System.Collections.Generic;

namespace SchwabQuantowerBridge;

public sealed class BridgeSettings
{
    public string BackendBaseUrl { get; init; } = "http://127.0.0.1:8000";

    public string SnapshotRoute(string symbol) => $"{BackendBaseUrl}/api/market/snapshot/{symbol}";

    public string SymbolRoute(string symbol) => $"{BackendBaseUrl}/api/market/symbol/{symbol}";

    public string SearchRoute(string query, int limit = 10) =>
        $"{BackendBaseUrl}/api/market/search?q={Uri.EscapeDataString(query)}&limit={limit}";

    public string StreamStatusRoute() => $"{BackendBaseUrl}/api/stream/status";

    public string OptionSeriesRoute(string symbol) =>
        $"{BackendBaseUrl}/api/market/options/{symbol}/series";

    public string OptionChainRoute(string symbol, DateTime? expiration = null)
    {
        if (!expiration.HasValue)
            return $"{BackendBaseUrl}/api/market/options/{symbol}/chain";

        return $"{BackendBaseUrl}/api/market/options/{symbol}/chain?expiration={Uri.EscapeDataString(expiration.Value.ToString("yyyy-MM-dd"))}";
    }

    public string BarsRoute(
        string symbol,
        string timeframe = "5m",
        int limit = 500,
        DateTime? start = null,
        DateTime? end = null)
    {
        var queryParts = new List<string>
        {
            $"timeframe={Uri.EscapeDataString(timeframe)}",
            $"limit={limit}"
        };

        if (start.HasValue)
            queryParts.Add($"start={Uri.EscapeDataString(start.Value.ToUniversalTime().ToString("O"))}");

        if (end.HasValue)
            queryParts.Add($"end={Uri.EscapeDataString(end.Value.ToUniversalTime().ToString("O"))}");

        return $"{BackendBaseUrl}/api/market/bars/{symbol}?{string.Join("&", queryParts)}";
    }

    public string StreamRoute(string symbol)
    {
        var builder = new UriBuilder($"{BackendBaseUrl}/api/stream/equities/{symbol}");
        builder.Scheme = builder.Scheme.Equals("https", System.StringComparison.OrdinalIgnoreCase) ? "wss" : "ws";
        return builder.Uri.ToString();
    }
}
