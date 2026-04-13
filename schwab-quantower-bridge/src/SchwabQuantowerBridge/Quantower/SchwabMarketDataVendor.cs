using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using SchwabQuantowerBridge.Models;
using SchwabQuantowerBridge.Services;
using TradingPlatform.BusinessLayer;
using TradingPlatform.BusinessLayer.Integration;

namespace SchwabQuantowerBridge.Quantower;

internal sealed class SchwabMarketDataVendor : Vendor
{
    private const string ExchangeId = "US";
    private const string OptionExchangeId = "OPR";
    // Keep startup-safe defaults small; broader named universes should be loaded explicitly later.
    private static readonly string[] DefaultUniverseSymbols =
    {
        "AAPL", "MSFT", "NVDA", "AMD", "META", "TSLA", "INTC",
        "SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "XLI", "XLV",
        "MRVL", "SNAP", "FDD", "ASTS", "VIX", "SPX", "RUT"
    };
    private static readonly TimeSpan OrderPollingInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan PositionPollingInterval = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan BackendStatusPollingInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan BackendHeartbeatGraceWindow = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan BackendSuccessGraceWindow = TimeSpan.FromSeconds(90);
    private const int BackendHeartbeatFailureThreshold = 3;
    private static readonly TimeSpan MarketStatePulseInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan StreamStopDebounce = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RealBookFreshnessWindow = TimeSpan.FromSeconds(15);
    private static readonly int[] ActionRefreshScheduleMilliseconds = [100, 600, 1500];
    private static readonly TimeSpan[] StreamReconnectBackoff = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)];
    private static readonly bool VerboseDiagnosticsEnabled =
        string.Equals(Environment.GetEnvironmentVariable("SCHWAB_BRIDGE_VERBOSE_DIAGNOSTICS"), "1", StringComparison.OrdinalIgnoreCase);
    private readonly BridgeSettings settings = new();
    private readonly HttpClient httpClient = new();
    private readonly Dictionary<string, int> quoteSubscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> lastSubscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> level2Subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> markSubscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> lastTradeTimes = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, double> latestPrices = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, MarketSnapshotDto> snapshotCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, CachedDomState> domCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Dictionary<string, CachedDomState>> domVenueCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SymbolProfileDto> symbolProfileCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, MessageSymbol> symbolCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<MessageOptionSerie>> optionSeriesCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, OptionContractDto> optionContractCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, SymbolStreamState> streamStates = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> pendingSnapshotRefreshes = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> pendingOptionHydrations = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> realBookSeen = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> primedSymbols = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string?> orderStatusCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BrokerOrderDto> orderCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> closedOrderMessagesPushed = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BrokerPositionDto> positionCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly object syncRoot = new();
    private SchwabBackendClient? backendClient;
    private CancellationTokenSource? orderPollingCancellation;
    private Task? orderPollingTask;
    private CancellationTokenSource? positionPollingCancellation;
    private Task? positionPollingTask;
    private CancellationTokenSource? backendStatusPollingCancellation;
    private Task? backendStatusPollingTask;
    private CancellationTokenSource? marketStatePulseCancellation;
    private Task? marketStatePulseTask;
    private bool connected;
    private bool backendHealthy;
    private TimeSpan backendPingTime = TimeSpan.Zero;
    private DateTime lastBackendHeartbeatUtc = DateTime.MinValue;
    private DateTime lastBackendSuccessUtc = DateTime.MinValue;
    private string? lastBackendHeartbeatError;
    private int consecutiveBackendHeartbeatFailures;
    private string? lastLoggedBackendHeartbeatError;
    private int lastObservedBackendRestartCount = -1;
    private int lastObservedDroppedEventCount = -1;
    private BridgeStreamStatusDto? lastBackendStreamStatus;

    public override ConnectionResult Connect(ConnectRequestParameters connectRequestParameters)
    {
        this.backendClient = new SchwabBackendClient(this.httpClient, this.settings);

        try
        {
            var isHealthy = this.backendClient.PingAsync(connectRequestParameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            if (!isHealthy)
                return ConnectionResult.CreateFail("Local Schwab backend is unavailable.");

            this.connected = true;
            this.backendHealthy = true;
            this.lastBackendHeartbeatUtc = DateTime.UtcNow;
            this.lastBackendSuccessUtc = DateTime.UtcNow;
            this.lastBackendHeartbeatError = null;
            this.consecutiveBackendHeartbeatFailures = 0;
            this.lastLoggedBackendHeartbeatError = null;
            this.StartOrderPolling();
            this.StartPositionPolling();
            this.StartBackendStatusPolling();
            this.StartMarketStatePulse();
            LogDiagnostic("Connect success");
            return ConnectionResult.CreateSuccess();
        }
        catch (Exception ex)
        {
            LogDiagnostic($"Connect failed error={ex.Message}");
            return ConnectionResult.CreateFail(ex.Message);
        }
    }

    public override void Disconnect()
    {
        this.StopOrderPolling();
        this.StopPositionPolling();
        this.StopBackendStatusPolling();
        this.StopMarketStatePulse();

        List<SymbolStreamState> states;
        lock (this.syncRoot)
        {
            states = this.streamStates.Values.ToList();
            this.streamStates.Clear();
            this.quoteSubscriptions.Clear();
            this.lastSubscriptions.Clear();
            this.level2Subscriptions.Clear();
            this.markSubscriptions.Clear();
            this.lastTradeTimes.Clear();
            this.latestPrices.Clear();
            this.snapshotCache.Clear();
            this.domCache.Clear();
            this.domVenueCache.Clear();
            this.symbolProfileCache.Clear();
            this.optionSeriesCache.Clear();
            this.optionContractCache.Clear();
            this.pendingOptionHydrations.Clear();
            this.orderStatusCache.Clear();
            this.orderCache.Clear();
            this.positionCache.Clear();
            this.realBookSeen.Clear();
            this.primedSymbols.Clear();
            this.pendingSnapshotRefreshes.Clear();
            this.closedOrderMessagesPushed.Clear();
            this.connected = false;
            this.backendHealthy = false;
            this.backendPingTime = TimeSpan.Zero;
            this.lastBackendHeartbeatUtc = DateTime.MinValue;
            this.lastBackendSuccessUtc = DateTime.MinValue;
            this.lastBackendHeartbeatError = null;
            this.consecutiveBackendHeartbeatFailures = 0;
            this.lastLoggedBackendHeartbeatError = null;
            this.lastObservedBackendRestartCount = -1;
            this.lastObservedDroppedEventCount = -1;
            this.lastBackendStreamStatus = null;
        }

        foreach (var state in states)
        {
            state.StopDebounceCancellation?.Cancel();
            state.Cancellation.Cancel();
            state.Socket?.Dispose();
        }

        LogDiagnostic("Disconnect completed");
        base.Disconnect();
    }

    public override PingResult Ping() => new()
    {
        State = this.IsBackendOperational() ? PingEnum.Connected : PingEnum.Disconnected,
        PingTime = this.backendPingTime
    };

    private void MarkBackendSuccess()
    {
        var now = DateTime.UtcNow;
        this.lastBackendSuccessUtc = now;
        this.backendHealthy = true;
        this.lastBackendHeartbeatError = null;
        this.consecutiveBackendHeartbeatFailures = 0;
    }

    public override IList<MessageRule> GetRules(CancellationToken token)
    {
        var rules = base.GetRules(token);
        rules.Add(new MessageRule { Name = Rule.ALLOW_TRADING, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_SL, Value = false });
        rules.Add(new MessageRule { Name = Rule.ALLOW_TP, Value = false });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_ORDER, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_PRICE, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_AMOUNT, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_TIF, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_ORDER_TYPE, Value = false });
        rules.Add(new MessageRule { Name = Rule.LEVEL2_IS_AGGREGATED, Value = true });
        rules.Add(new MessageRule { Name = Rule.PLACE_ORDER_TRADING_OPERATION_HAS_ORDER_ID, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_SCREENER, Value = false });
        rules.Add(new MessageRule { Name = Rule.ALLOW_CONTAINS_SCREENER_CONDITIONS, Value = false });
        return rules;
    }

    public override IList<MessageAccount> GetAccounts(CancellationToken token)
    {
        if (this.backendClient == null)
            return new List<MessageAccount>();

        var accounts = this.backendClient.GetAccountsAsync(token)
            .GetAwaiter()
            .GetResult();
        this.MarkBackendSuccess();
        return accounts.Select(CreateAccount).ToList();
    }

    public override IList<MessageCryptoAssetBalances> GetCryptoAssetBalances(CancellationToken token)
    {
        if (this.backendClient == null)
            return new List<MessageCryptoAssetBalances>();

        var accounts = this.backendClient.GetAccountsAsync(token)
            .GetAwaiter()
            .GetResult();
        this.MarkBackendSuccess();
        return accounts.Select(CreateAssetBalance).ToList();
    }

    public override IList<MessageOpenPosition> GetPositions(CancellationToken token)
    {
        if (this.backendClient == null)
            return new List<MessageOpenPosition>();

        var positions = this.backendClient.GetPositionsAsync(token)
            .GetAwaiter()
            .GetResult()
            .Where(p => !string.IsNullOrWhiteSpace(p.Symbol) && Math.Abs(p.Quantity) > 0)
            .ToList();
        this.MarkBackendSuccess();

        foreach (var position in positions)
        {
            if (string.IsNullOrWhiteSpace(position.Symbol))
                continue;

            this.PrimeRealtimeSymbol(position.Symbol);

            if (position.MarketPrice is > 0)
                this.SetLatestPrice(position.Symbol, position.MarketPrice.Value);
        }

        return positions
            .Select(CreatePosition)
            .ToList();
    }

    public override IList<MessageOpenOrder> GetPendingOrders(CancellationToken token)
    {
        if (this.backendClient == null)
            return new List<MessageOpenOrder>();

        var orders = this.backendClient.GetOrdersAsync(token)
            .GetAwaiter()
            .GetResult();
        this.MarkBackendSuccess();

        this.ReconcileOrderStatuses(orders, pushInitialTerminalCloses: true, pushActiveChanges: false);

        return orders
            .Where(o => !string.IsNullOrWhiteSpace(o.Symbol) && IsCancelableOrderStatus(o.Status))
            .Select(CreateOpenOrder)
            .ToList();
    }

    public override IList<MessageOrderHistory> GetOrdersHistory(OrdersHistoryRequestParameters requestParameters)
    {
        if (this.backendClient == null)
            return new List<MessageOrderHistory>();

        var normalizedFrom = NormalizeHistoryBoundary(requestParameters.From, false);
        var normalizedTo = NormalizeHistoryBoundary(requestParameters.To, true);
        var orders = this.backendClient.GetOrdersAsync(normalizedFrom, normalizedTo, CancellationToken.None)
            .GetAwaiter()
            .GetResult();
        this.MarkBackendSuccess();
        this.PrimeHistorySymbols(orders.Select(order => order.Symbol));

        return orders
            .Where(order => MatchesOrderHistoryRequest(order, normalizedFrom, normalizedTo, requestParameters.SymbolIds))
            .Select(CreateOrderHistory)
            .ToList();
    }

    public override IList<MessageTrade> GetTrades(TradesHistoryRequestParameters requestParameters)
    {
        if (this.backendClient == null)
            return new List<MessageTrade>();

        var normalizedFrom = NormalizeHistoryBoundary(requestParameters.From, false);
        var normalizedTo = NormalizeHistoryBoundary(requestParameters.To, true);
        var executions = this.backendClient.GetExecutionsAsync(normalizedFrom, normalizedTo, CancellationToken.None)
            .GetAwaiter()
            .GetResult();
        this.MarkBackendSuccess();
        this.PrimeHistorySymbols(executions.Select(execution => execution.Symbol));

        return executions
            .Where(execution => MatchesTradesHistoryRequest(execution, normalizedFrom, normalizedTo, requestParameters.SymbolIds))
            .Select(CreateTrade)
            .ToList();
    }

    public override void GetTrades(TradesHistoryRequestParameters requestParameters, TradingPlatform.BusinessLayer.Integration.AccountTradesLoadingCallback callback)
    {
        var trades = this.GetTrades(requestParameters);
        callback?.Invoke(trades, true);
    }

    public override TradesHistoryMetadata GetTradesMetadata() => new()
    {
        AllowLocalStorage = false,
        AllowReloadFromServer = true,
        AllowSingleSymbolLoading = true,
        LoadTradesFromCurrentTradingDate = false
    };

    public override IList<OrderType> GetAllowedOrderTypes(CancellationToken token) => new List<OrderType>
    {
        new LimitOrderType(TimeInForce.Day, TimeInForce.GTC)
    };

    public override PnL CalculatePnL(PnLRequestParameters parameters)
    {
        var symbolId = parameters.Symbol?.Id;
        if (string.IsNullOrWhiteSpace(symbolId))
            return base.CalculatePnL(parameters);

        if (!this.TryGetLatestPrice(symbolId, out var currentPrice) || currentPrice <= 0)
            return base.CalculatePnL(parameters);

        var grossValue = (currentPrice - parameters.OpenPrice) * parameters.Quantity;
        var pnlItem = new PnLItem
        {
            AssetID = "USD",
            Value = grossValue,
            ValuePercent = parameters.OpenPrice != 0 ? (currentPrice / parameters.OpenPrice) - 1d : double.NaN
        };

        return new PnL
        {
            GrossPnL = pnlItem,
            NetPnL = pnlItem
        };
    }

    public override TradingOperationResult PlaceOrder(PlaceOrderRequestParameters parameters)
    {
        if (this.backendClient == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");

        if (parameters.OrderTypeId != OrderType.Limit)
            return TradingOperationResult.CreateError(parameters.RequestId, $"Schwab bridge currently supports only Limit orders, not {parameters.OrderTypeId}.");

        if (double.IsNaN(parameters.Price))
            return TradingOperationResult.CreateError(parameters.RequestId, "Limit price is required.");

        if (parameters.Quantity <= 0 || parameters.Quantity % 1 != 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab bridge allows whole-share orders only.");

        var instruction = this.ResolveInstruction(parameters.Account.Id, parameters.Symbol.Id, parameters.Side, parameters.CancellationToken);

        try
        {
            var result = this.backendClient.PlaceOrderAsync(
                    parameters.Account.Id,
                    parameters.Symbol.Id,
                    parameters.Quantity,
                    instruction,
                    "LIMIT",
                    parameters.Price,
                    ConvertTimeInForce(parameters.TimeInForce),
                    parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            var orderId = result?.OrderId;
            if (!string.IsNullOrWhiteSpace(orderId))
                this.PushOptimisticOpenOrder(CreateOptimisticOrder(
                    parameters.Account.Id,
                    orderId,
                    parameters.Symbol.Id,
                    instruction,
                    "LIMIT",
                    parameters.Price,
                    parameters.Quantity,
                    parameters.TimeInForce,
                    ResolveOptimisticSession(parameters.TimeInForce, null)));

            this.RefreshOrdersInBackground();
            return TradingOperationResult.CreateSuccess(parameters.RequestId, result?.OrderId);
        }
        catch (Exception ex)
        {
            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    private string ResolveInstruction(string accountId, string symbol, Side side, CancellationToken cancellationToken)
    {
        if (this.backendClient == null)
            return side == Side.Buy ? "BUY" : "SELL";

        var signedPosition = this.backendClient.GetPositionsAsync(cancellationToken)
            .GetAwaiter()
            .GetResult()
            .Where(p => string.Equals(p.AccountHash, accountId, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(p.Symbol, symbol, StringComparison.OrdinalIgnoreCase))
            .Sum(p => p.Quantity);
        this.MarkBackendSuccess();

        if (side == Side.Buy)
            return signedPosition < 0 ? "BUY_TO_COVER" : "BUY";

        return signedPosition > 0 ? "SELL" : "SELL_SHORT";
    }

    public override TradingOperationResult CancelOrder(CancelOrderRequestParameters parameters)
    {
        if (this.backendClient == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");

        try
        {
            this.backendClient.CancelOrderAsync(
                    parameters.Order.Account.Id,
                    parameters.Order.Id,
                    parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            this.PushMessage(new MessageCloseOrder { OrderId = parameters.Order.Id });
            this.RefreshOrdersInBackground();
            return TradingOperationResult.CreateSuccess(parameters.RequestId, parameters.Order.Id);
        }
        catch (Exception ex)
        {
            if (IsAlreadyInactiveOrderError(ex))
            {
                this.PushMessage(new MessageCloseOrder { OrderId = parameters.Order.Id });
                this.RefreshOrdersInBackground();
                return TradingOperationResult.CreateSuccess(parameters.RequestId, parameters.Order.Id);
            }

            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    public override TradingOperationResult ModifyOrder(ModifyOrderRequestParameters parameters)
    {
        if (this.backendClient == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");

        if (!TryGetCachedOrder(parameters, out var currentOrder))
            return TradingOperationResult.CreateError(parameters.RequestId, $"Order {parameters.OrderId} is not available in the Schwab bridge cache.");

        if (!string.Equals(currentOrder.OrderType, "LIMIT", StringComparison.OrdinalIgnoreCase) ||
            parameters.OrderTypeId != OrderType.Limit)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab bridge currently supports only LIMIT order modification.");

        if (double.IsNaN(parameters.Price) || parameters.Price <= 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "A valid limit price is required.");

        if (parameters.Quantity <= 0 || parameters.Quantity % 1 != 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab bridge allows whole-share quantity changes only.");

        try
        {
            var result = this.backendClient.ModifyOrderAsync(
                    new ModifyBrokerOrderRequestDto
                    {
                        AccountHash = currentOrder.AccountHash,
                        OrderId = currentOrder.OrderId,
                        Symbol = currentOrder.Symbol ?? parameters.SymbolId,
                        Quantity = parameters.Quantity,
                        Instruction = currentOrder.Instruction ?? (parameters.Side == Side.Buy ? "BUY" : "SELL"),
                        OrderType = "LIMIT",
                        LimitPrice = parameters.Price,
                        TimeInForce = ConvertTimeInForce(parameters.TimeInForce)
                    },
                    parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            var replacementOrderId = string.IsNullOrWhiteSpace(result?.OrderId) ? parameters.OrderId : result!.OrderId!;
            var optimisticOrder = CreateOptimisticOrder(
                currentOrder.AccountHash,
                replacementOrderId,
                currentOrder.Symbol ?? parameters.SymbolId,
                currentOrder.Instruction ?? (parameters.Side == Side.Buy ? "BUY" : "SELL"),
                "LIMIT",
                parameters.Price,
                parameters.Quantity,
                parameters.TimeInForce,
                ResolveOptimisticSession(parameters.TimeInForce, currentOrder.Session));

            this.PushOptimisticOpenOrder(optimisticOrder);

            if (!string.Equals(replacementOrderId, currentOrder.OrderId, StringComparison.OrdinalIgnoreCase))
                this.PushOptimisticCloseOrder(currentOrder.AccountHash, currentOrder.OrderId, "REPLACED");

            this.RefreshOrdersInBackground();
            return TradingOperationResult.CreateSuccess(parameters.RequestId, replacementOrderId);
        }
        catch (Exception ex)
        {
            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    public override MessageSymbolTypes GetSymbolTypes(CancellationToken token) => new()
    {
        SymbolTypes = new[] { SymbolType.Equities, SymbolType.ETF, SymbolType.Indexes, SymbolType.Options }
    };

    public override IList<MessageAsset> GetAssets(CancellationToken token) => new List<MessageAsset>
    {
        new()
        {
            Id = "USD",
            Name = "USD",
            MinimumChange = 0.01
        }
    };

    public override IList<MessageExchange> GetExchanges(CancellationToken token) => new List<MessageExchange>
    {
        new()
        {
            Id = ExchangeId,
            ExchangeName = "US Equities"
        },
        new()
        {
            Id = OptionExchangeId,
            ExchangeName = "US Options"
        }
    };

    public override IList<MessageSymbol> GetSymbols(CancellationToken token)
    {
        return DefaultUniverseSymbols
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(this.CreateMessageSymbol)
            .ToList();
    }

    public override IList<MessageOptionSerie> GetAllOptionSeries(CancellationToken token)
    {
        if (this.backendClient != null)
        {
            List<string> underliersToHydrate;
            lock (this.syncRoot)
            {
                underliersToHydrate = this.symbolCache.Keys
                    .Concat(this.snapshotCache.Keys)
                    .Concat(this.symbolProfileCache.Keys)
                    .Concat(this.latestPrices.Keys)
                    .Where(symbol => !string.IsNullOrWhiteSpace(symbol))
                    .Select(symbol => symbol.Trim().ToUpperInvariant())
                    .Where(symbol => !this.IsOptionSymbol(symbol) && !this.optionSeriesCache.ContainsKey(symbol))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }

            foreach (var underlier in underliersToHydrate)
            {
                try
                {
                    var chain = this.backendClient.GetOptionChainAsync(underlier, cancellationToken: token)
                        .GetAwaiter()
                        .GetResult();
                    var seriesDtos = this.backendClient.GetOptionSeriesAsync(underlier, token)
                        .GetAwaiter()
                        .GetResult();
                    this.MarkBackendSuccess();
                    var series = seriesDtos
                        .Select(dto => CreateOptionSerie(dto, dto.Exchange ?? ExchangeId))
                        .ToList();

                    lock (this.syncRoot)
                    {
                        this.optionSeriesCache[underlier] = series;
                        if (chain != null)
                        {
                            foreach (var contract in chain.Contracts)
                                this.CacheOptionContract(contract);
                        }
                    }

                    LogDiagnostic($"GetAllOptionSeries hydrated underlier={underlier} series={series.Count} contracts={(chain?.Contracts.Count ?? 0)}");
                }
                catch (Exception ex)
                {
                    LogDiagnostic($"GetAllOptionSeries hydrate failed underlier={underlier} error={ex.Message}");
                }
            }
        }

        lock (this.syncRoot)
        {
            return this.optionSeriesCache.Values.SelectMany(items => items).ToList();
        }
    }

    public override IList<MessageOptionSerie> GetOptionSeries(GetOptionSeriesRequestParameters requestParameters)
    {
        if (string.IsNullOrWhiteSpace(requestParameters.UnderlierId) || this.backendClient == null)
            return new List<MessageOptionSerie>();

        var underlierId = requestParameters.UnderlierId.Trim().ToUpperInvariant();
        try
        {
            var chain = this.backendClient.GetOptionChainAsync(underlierId, cancellationToken: requestParameters.CancellationToken)
                .GetAwaiter()
                .GetResult();
            var seriesDtos = this.backendClient.GetOptionSeriesAsync(underlierId, requestParameters.CancellationToken)
                .GetAwaiter()
                .GetResult();
            this.MarkBackendSuccess();

            var series = seriesDtos
                .Select(dto => CreateOptionSerie(dto, requestParameters.ExchangeId))
                .ToList();

            lock (this.syncRoot)
            {
                this.optionSeriesCache[underlierId] = series;
                if (chain != null)
                {
                    foreach (var contract in chain.Contracts)
                        this.CacheOptionContract(contract);
                }
            }

            LogDiagnostic($"GetOptionSeries underlier={underlierId} series={series.Count} contracts={(chain?.Contracts.Count ?? 0)}");

            return series;
        }
        catch (Exception ex)
        {
            LogDiagnostic($"GetOptionSeries failed underlier={underlierId} error={ex.Message}");
            lock (this.syncRoot)
            {
                return this.optionSeriesCache.TryGetValue(underlierId, out var cached)
                    ? cached.ToList()
                    : new List<MessageOptionSerie>();
            }
        }
    }

    public override IList<MessageSymbolInfo> GetStrikes(GetStrikesRequestParameters requestParameters)
    {
        if (string.IsNullOrWhiteSpace(requestParameters.UnderlierId) || this.backendClient == null)
            return new List<MessageSymbolInfo>();

        var underlierId = requestParameters.UnderlierId.Trim().ToUpperInvariant();
        var expiration = requestParameters.ExpirationDate == default
            ? (DateTime?)null
            : requestParameters.ExpirationDate;

        try
        {
            var chain = this.backendClient.GetOptionChainAsync(
                    underlierId,
                    expiration,
                    requestParameters.CancellationToken)
                .GetAwaiter()
                .GetResult();
            this.MarkBackendSuccess();

            if (chain == null)
                return new List<MessageSymbolInfo>();

            lock (this.syncRoot)
            {
                if (!this.optionSeriesCache.ContainsKey(underlierId))
                {
                    this.optionSeriesCache[underlierId] = chain.Contracts
                        .Select(contract => CreateOptionSerie(
                            new OptionSeriesDto
                            {
                                Id = $"{contract.UnderlierSymbol.ToUpperInvariant()}|{contract.ExpirationDate:yyyy-MM-dd}",
                                UnderlierSymbol = contract.UnderlierSymbol.ToUpperInvariant(),
                                ExpirationDate = contract.ExpirationDate,
                                DaysToExpiration = Math.Max((contract.ExpirationDate.Date - DateTimeOffset.UtcNow.Date).Days, 0),
                                SeriesType = "month",
                                Name = $"{contract.UnderlierSymbol.ToUpperInvariant()} {contract.ExpirationDate:yyyy-MM-dd}",
                                Exchange = contract.Exchange ?? ExchangeId
                            },
                            contract.Exchange ?? ExchangeId))
                        .DistinctBy(serie => serie.Id)
                        .OrderBy(serie => serie.ExpirationDate)
                        .ToList();
                }

                foreach (var contract in chain.Contracts)
                    this.CacheOptionContract(contract);
            }

            var results = chain.Contracts
                .Where(contract =>
                    string.Equals(contract.UnderlierSymbol, underlierId, StringComparison.OrdinalIgnoreCase) &&
                    (expiration == null || contract.ExpirationDate.Date == expiration.Value.Date))
                .OrderBy(contract => contract.StrikePrice)
                .ThenBy(contract => string.Equals(contract.OptionType, "CALL", StringComparison.OrdinalIgnoreCase) ? 0 : 1)
                .Select(this.CreateOptionStrikeInfo)
                .ToList();

            LogDiagnostic($"GetStrikes underlier={underlierId} expiration={expiration:yyyy-MM-dd} count={results.Count}");

            return results;
        }
        catch (Exception ex)
        {
            LogDiagnostic($"GetStrikes failed underlier={underlierId} expiration={expiration:O} error={ex.Message}");
            lock (this.syncRoot)
            {
                return this.optionContractCache.Values
                    .Where(contract =>
                        string.Equals(contract.UnderlierSymbol, underlierId, StringComparison.OrdinalIgnoreCase) &&
                        (expiration == null || contract.ExpirationDate.Date == expiration.Value.Date))
                    .OrderBy(contract => contract.StrikePrice)
                    .ThenBy(contract => string.Equals(contract.OptionType, "CALL", StringComparison.OrdinalIgnoreCase) ? 0 : 1)
                    .Select(this.CreateOptionStrikeInfo)
                    .ToList();
            }
        }
    }

    public override MessageSymbol GetNonFixedSymbol(GetSymbolRequestParameters requestParameters)
    {
        if (string.IsNullOrWhiteSpace(requestParameters.SymbolId))
            return base.GetNonFixedSymbol(requestParameters);

        this.PrimeRealtimeSymbol(requestParameters.SymbolId);
        return this.CreateMessageSymbol(requestParameters.SymbolId);
    }

    public override IList<MessageSymbolInfo> SearchSymbols(SearchSymbolsRequestParameters requestParameters)
    {
        if (string.IsNullOrWhiteSpace(requestParameters.FilterName))
            return new List<MessageSymbolInfo>();

        if (this.backendClient == null)
            return new List<MessageSymbolInfo> { this.CreateSearchResultSymbol(requestParameters.FilterName) };

        try
        {
            var matches = this.backendClient.SearchSymbolsAsync(requestParameters.FilterName, 50)
                .GetAwaiter()
                .GetResult();

            if (matches.Count == 0)
                return new List<MessageSymbolInfo> { this.CreateSearchResultSymbol(requestParameters.FilterName) };

            return matches
                .Select(profile => this.CreateMessageSymbolFromProfile(profile, primeRealtimeIfMissing: false))
                .Cast<MessageSymbolInfo>()
                .ToList();
        }
        catch
        {
            return new List<MessageSymbolInfo> { this.CreateSearchResultSymbol(requestParameters.FilterName) };
        }
    }

    public override void SubscribeSymbol(SubscribeQuotesParameters parameters)
    {
        var symbolId = parameters.SymbolId?.ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(symbolId) || this.backendClient == null)
            return;

        LogDiagnostic($"SubscribeSymbol symbol={symbolId} type={parameters.SubscribeType}");
        var publishCachedLevel2 = false;
        var publishCachedMarket = false;
        lock (this.syncRoot)
        {
            this.IncrementSubscription(parameters.SubscribeType, symbolId);
            publishCachedLevel2 = parameters.SubscribeType == SubscribeQuoteType.Level2 && this.domCache.ContainsKey(symbolId);
            publishCachedMarket = parameters.SubscribeType != SubscribeQuoteType.Level2 && this.snapshotCache.ContainsKey(symbolId);
        }

        if (!this.IsOptionSymbol(symbolId))
            this.EnsureStream(symbolId);
        if (publishCachedMarket)
            this.PublishCachedSnapshotState(symbolId, parameters.SubscribeType);
        if (publishCachedLevel2)
            this.PublishCachedDom(symbolId);
        if (!this.IsOptionSymbol(symbolId))
            this.ScheduleSnapshotRefresh(symbolId, $"subscribe:{parameters.SubscribeType}");
    }

    public override void UnSubscribeSymbol(SubscribeQuotesParameters parameters)
    {
        var symbolId = parameters.SymbolId?.ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(symbolId))
            return;

        LogDiagnostic($"UnSubscribeSymbol symbol={symbolId} type={parameters.SubscribeType}");
        // Quantower can emit aggressive unsubscribe storms during workspace/panel
        // lifecycle churn. Treat subscriptions as session-sticky and only clear
        // them on full vendor disconnect so DOM/Level2/options data does not
        // disappear and then recover seconds later.
    }

    public override HistoryMetadata GetHistoryMetadata(CancellationToken cancelationToken) => new()
    {
        AllowedAggregations = new[]
        {
            HistoryAggregation.TIME
        },
        AllowedPeriodsHistoryAggregationTime = new[]
        {
            Period.MIN1,
            Period.MIN5,
            Period.MIN10,
            Period.MIN15,
            Period.MIN30,
            Period.HOUR1,
            Period.HOUR4,
            Period.DAY1
        },
        AllowedHistoryTypesHistoryAggregationTime = new[]
        {
            HistoryType.Last
        },
        DownloadingStep_Minute = TimeSpan.FromDays(1),
        BuildUncompletedBars = true
    };

    public override IList<IHistoryItem> LoadHistory(HistoryRequestParameters requestParameters)
    {
        if (this.backendClient == null)
            return new List<IHistoryItem>();

        this.ScheduleSnapshotRefresh(requestParameters.SymbolId, "history");

        try
        {
            var timeframe = this.ResolveTimeframe(requestParameters.Aggregation);
            var bars = this.backendClient.GetBarsAsync(
                    requestParameters.SymbolId,
                    timeframe,
                    5000,
                    requestParameters.FromTime,
                    requestParameters.ToTime,
                    requestParameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            return bars
                .Where(b => b.Timestamp.UtcDateTime >= requestParameters.FromTime && b.Timestamp.UtcDateTime <= requestParameters.ToTime)
                .Select(bar => (IHistoryItem)CreateHistoryItem(bar))
                .ToList();
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
            LogDiagnostic(
                $"LoadHistory error symbol={requestParameters.SymbolId} timeframe={this.ResolveTimeframe(requestParameters.Aggregation)} " +
                $"from={requestParameters.FromTime:O} to={requestParameters.ToTime:O} error={ex.Message}");
            return new List<IHistoryItem>();
        }
    }

    private string ResolveTimeframe(HistoryAggregation aggregation) =>
        aggregation switch
        {
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.MIN1 => "1m",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.MIN5 => "5m",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.MIN10 => "10m",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.MIN15 => "15m",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.MIN30 => "30m",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.HOUR1 => "1h",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.HOUR4 => "4h",
            HistoryAggregationTime historyAggregationTime when historyAggregationTime.Period == Period.DAY1 => "1d",
            _ => "5m"
        };

    private void StartOrderPolling()
    {
        this.StopOrderPolling();

        var cancellation = new CancellationTokenSource();
        this.orderPollingCancellation = cancellation;
        this.orderPollingTask = Task.Run(() => this.RunOrderPollingAsync(cancellation.Token));
    }

    private void StartPositionPolling()
    {
        this.StopPositionPolling();

        var cancellation = new CancellationTokenSource();
        this.positionPollingCancellation = cancellation;
        this.positionPollingTask = Task.Run(() => this.RunPositionPollingAsync(cancellation.Token));
    }

    private void StopOrderPolling()
    {
        var cancellation = this.orderPollingCancellation;
        this.orderPollingCancellation = null;
        this.orderPollingTask = null;

        if (cancellation == null)
            return;

        cancellation.Cancel();
        cancellation.Dispose();
    }

    private async Task RunOrderPollingAsync(CancellationToken token)
    {
        if (this.IsBackendOperational())
            await this.RefreshOrdersAsync(token);

        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(OrderPollingInterval, token);
                if (this.IsBackendOperational())
                    await this.RefreshOrdersAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Core.Instance.Loggers.Log(ex);
                LogDiagnostic($"OrderPolling error={ex.Message}");
            }
        }
    }

    private async Task RunPositionPollingAsync(CancellationToken token)
    {
        if (this.IsBackendOperational())
            await this.RefreshPositionsAsync(token);

        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(PositionPollingInterval, token);
                if (this.IsBackendOperational())
                    await this.RefreshPositionsAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Core.Instance.Loggers.Log(ex);
                LogDiagnostic($"PositionPolling error={ex.Message}");
            }
        }
    }

    private void RefreshOrdersInBackground()
    {
        if (!this.IsBackendOperational())
            return;

        var token = this.orderPollingCancellation?.Token ?? CancellationToken.None;
        Task.Run(async () =>
        {
            try
            {
                foreach (var delay in ActionRefreshScheduleMilliseconds)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delay), token);
                    await this.RefreshOrdersAsync(token);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Core.Instance.Loggers.Log(ex);
                LogDiagnostic($"OrderRefresh error={ex.Message}");
            }
        }, token);
    }

    private async Task RefreshPositionsAsync(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return;

        var positions = await client.GetPositionsAsync(token);
        this.MarkBackendSuccess();
        this.ReconcilePositions(positions);
    }

    private void ReconcilePositions(IReadOnlyList<BrokerPositionDto> positions)
    {
        var openMessages = new List<MessageOpenPosition>();

        lock (this.syncRoot)
        {
            foreach (var position in positions.Where(p => !string.IsNullOrWhiteSpace(p.Symbol) && Math.Abs(p.Quantity) > 0))
            {
                var key = GetPositionKey(position);
                var changed = !this.positionCache.TryGetValue(key, out var existing) || PositionChanged(existing, position);
                this.positionCache[key] = position;

                this.PrimeRealtimeSymbol(position.Symbol);
                if (position.MarketPrice is > 0)
                    this.latestPrices[NormalizeSymbolKey(position.Symbol)] = position.MarketPrice.Value;

                if (changed)
                    openMessages.Add(CreatePosition(position));
            }
        }

        foreach (var message in openMessages)
            this.PushMessage(message);
    }

    private void StartBackendStatusPolling()
    {
        this.StopBackendStatusPolling();

        var cancellation = new CancellationTokenSource();
        this.backendStatusPollingCancellation = cancellation;
        this.backendStatusPollingTask = Task.Run(() => this.RunBackendStatusPollingAsync(cancellation.Token));
    }

    private void StartMarketStatePulse()
    {
        this.StopMarketStatePulse();

        var cancellation = new CancellationTokenSource();
        this.marketStatePulseCancellation = cancellation;
        this.marketStatePulseTask = Task.Run(() => this.RunMarketStatePulseAsync(cancellation.Token));
    }

    private void StopMarketStatePulse()
    {
        var cancellation = this.marketStatePulseCancellation;
        this.marketStatePulseCancellation = null;
        this.marketStatePulseTask = null;

        if (cancellation == null)
            return;

        cancellation.Cancel();
        cancellation.Dispose();
    }

    private void StopPositionPolling()
    {
        var cancellation = this.positionPollingCancellation;
        this.positionPollingCancellation = null;
        this.positionPollingTask = null;

        if (cancellation == null)
            return;

        cancellation.Cancel();
        cancellation.Dispose();
    }

    private void StopBackendStatusPolling()
    {
        var cancellation = this.backendStatusPollingCancellation;
        this.backendStatusPollingCancellation = null;
        this.backendStatusPollingTask = null;

        if (cancellation == null)
            return;

        cancellation.Cancel();
        cancellation.Dispose();
    }

    private async Task RunBackendStatusPollingAsync(CancellationToken token)
    {
        await this.RefreshBackendStatusAsync(token);

        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(BackendStatusPollingInterval, token);
                await this.RefreshBackendStatusAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Core.Instance.Loggers.Log(ex);
                LogDiagnostic($"BackendStatusPolling error={ex.Message}");
            }
        }
    }

    private async Task RefreshBackendStatusAsync(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return;

        var stopwatch = Stopwatch.StartNew();

        try
        {
            var status = await client.GetStreamStatusAsync(token) ?? new BridgeStreamStatusDto();
            stopwatch.Stop();

            var recovered = !this.backendHealthy;
            this.backendHealthy = true;
            this.backendPingTime = stopwatch.Elapsed;
            this.lastBackendHeartbeatUtc = DateTime.UtcNow;
            this.lastBackendSuccessUtc = this.lastBackendHeartbeatUtc;
            this.lastBackendHeartbeatError = null;
            this.consecutiveBackendHeartbeatFailures = 0;
            this.lastBackendStreamStatus = status;

            if (recovered)
                LogDiagnostic($"Backend heartbeat recovered latencyMs={this.backendPingTime.TotalMilliseconds:F0}");

            if (status.RestartCount > this.lastObservedBackendRestartCount)
            {
                if (this.lastObservedBackendRestartCount >= 0)
                {
                    LogDiagnostic(
                        $"Backend stream restart observed restartCount={status.RestartCount} activeSymbols={status.ActiveActualSymbolCount} subscribed={status.SubscribedSymbolCount}");
                }

                this.lastObservedBackendRestartCount = status.RestartCount;
            }

            if (status.DroppedEventCount > this.lastObservedDroppedEventCount)
            {
                if (this.lastObservedDroppedEventCount >= 0)
                {
                    LogDiagnostic(
                        $"Backend dropped events observed dropped={status.DroppedEventCount} activeSymbols={status.ActiveActualSymbolCount} queueMax={status.QueueMaxSize}");
                }

                this.lastObservedDroppedEventCount = status.DroppedEventCount;
            }

            if (status.ActiveActualSymbolCount > 0 && !status.StreamTaskRunning)
            {
                LogDiagnostic(
                    $"Backend stream unhealthy activeSymbols={status.ActiveActualSymbolCount} streamTaskRunning={status.StreamTaskRunning}");
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();

            var wasHealthy = this.backendHealthy;
            this.backendPingTime = stopwatch.Elapsed;
            this.lastBackendHeartbeatError = ex.Message;
            this.consecutiveBackendHeartbeatFailures++;

            var heartbeatAge = this.lastBackendHeartbeatUtc == DateTime.MinValue
                ? TimeSpan.MaxValue
                : DateTime.UtcNow - this.lastBackendHeartbeatUtc;
            var successAge = this.lastBackendSuccessUtc == DateTime.MinValue
                ? TimeSpan.MaxValue
                : DateTime.UtcNow - this.lastBackendSuccessUtc;
            var shouldMarkUnhealthy =
                this.consecutiveBackendHeartbeatFailures >= BackendHeartbeatFailureThreshold &&
                heartbeatAge >= BackendHeartbeatGraceWindow &&
                successAge >= BackendSuccessGraceWindow;

            if (shouldMarkUnhealthy)
                this.backendHealthy = false;

            if (wasHealthy || !string.Equals(this.lastLoggedBackendHeartbeatError, ex.Message, StringComparison.OrdinalIgnoreCase))
            {
                LogDiagnostic(
                    $"Backend heartbeat failed error={ex.Message} failures={this.consecutiveBackendHeartbeatFailures} heartbeatAgeMs={heartbeatAge.TotalMilliseconds:F0} successAgeMs={successAge.TotalMilliseconds:F0} markedUnhealthy={shouldMarkUnhealthy}");
                this.lastLoggedBackendHeartbeatError = ex.Message;
            }
        }
    }

    private bool IsBackendOperational()
    {
        if (!this.connected)
            return false;

        if (this.backendHealthy)
            return true;

        if (this.lastBackendSuccessUtc != DateTime.MinValue &&
            DateTime.UtcNow - this.lastBackendSuccessUtc < BackendSuccessGraceWindow)
            return true;

        if (this.lastBackendHeartbeatUtc == DateTime.MinValue)
            return false;

        return DateTime.UtcNow - this.lastBackendHeartbeatUtc < BackendHeartbeatGraceWindow;
    }

    private async Task RefreshOrdersAsync(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return;

        var orders = await client.GetOrdersAsync(token);
        this.MarkBackendSuccess();
        this.ReconcileOrderStatuses(orders, pushInitialTerminalCloses: true, pushActiveChanges: true);
    }

    private void ReconcileOrderStatuses(
        IReadOnlyList<BrokerOrderDto> orders,
        bool pushInitialTerminalCloses,
        bool pushActiveChanges)
    {
        var openMessages = new List<MessageOpenOrder>();
        var closeMessages = new List<MessageCloseOrder>();

        lock (this.syncRoot)
        {
            foreach (var order in orders)
            {
                if (string.IsNullOrWhiteSpace(order.OrderId))
                    continue;

                var key = GetOrderKey(order);
                this.orderCache[key] = order;
                var wasKnown = this.orderStatusCache.TryGetValue(key, out var previousStatus);
                var changed = !wasKnown || !string.Equals(previousStatus, order.Status, StringComparison.OrdinalIgnoreCase);
                this.orderStatusCache[key] = order.Status;

                if (IsCancelableOrderStatus(order.Status))
                {
                    this.closedOrderMessagesPushed.Remove(key);

                    if (pushActiveChanges && changed && !string.IsNullOrWhiteSpace(order.Symbol))
                        openMessages.Add(CreateOpenOrder(order));

                    continue;
                }

                if (!IsTerminalOrderStatus(order.Status))
                    continue;

                if ((pushInitialTerminalCloses || changed) && !this.closedOrderMessagesPushed.Contains(key))
                {
                    closeMessages.Add(new MessageCloseOrder { OrderId = order.OrderId });
                    this.closedOrderMessagesPushed.Add(key);
                }
            }
        }

        foreach (var message in openMessages)
            this.PushMessage(message);

        foreach (var message in closeMessages)
            this.PushMessage(message);
    }

    private void IncrementSubscription(SubscribeQuoteType subscribeType, string symbol)
    {
        var bucket = this.GetSubscriptionBucket(subscribeType);
        bucket.TryGetValue(symbol, out var count);
        bucket[symbol] = count + 1;
    }

    private void DecrementSubscription(SubscribeQuoteType subscribeType, string symbol)
    {
        var bucket = this.GetSubscriptionBucket(subscribeType);
        if (!bucket.TryGetValue(symbol, out var count))
            return;

        if (count <= 1)
            bucket.Remove(symbol);
        else
            bucket[symbol] = count - 1;
    }

    private Dictionary<string, int> GetSubscriptionBucket(SubscribeQuoteType subscribeType) =>
        subscribeType switch
        {
            SubscribeQuoteType.Quote => this.quoteSubscriptions,
            SubscribeQuoteType.Last => this.lastSubscriptions,
            SubscribeQuoteType.Level2 => this.level2Subscriptions,
            SubscribeQuoteType.Mark => this.markSubscriptions,
            _ => this.quoteSubscriptions
        };

    private bool HasQuoteSubscription(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.HasQuoteSubscriptionUnsafe(symbol);
        }
    }

    private bool HasLastSubscription(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.lastSubscriptions.ContainsKey(symbol);
        }
    }

    private bool HasLevel2Subscription(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.level2Subscriptions.ContainsKey(symbol);
        }
    }

    private bool HasAnySubscription(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.HasAnySubscriptionUnsafe(symbol);
        }
    }

    private bool HasQuoteSubscriptionUnsafe(string symbol) =>
        this.quoteSubscriptions.ContainsKey(symbol) || this.markSubscriptions.ContainsKey(symbol);

    private bool HasAnySubscriptionUnsafe(string symbol) =>
        this.quoteSubscriptions.ContainsKey(symbol) ||
        this.lastSubscriptions.ContainsKey(symbol) ||
        this.level2Subscriptions.ContainsKey(symbol) ||
        this.markSubscriptions.ContainsKey(symbol);

    private static string NormalizeSymbolKey(string symbol) =>
        symbol.Trim().ToUpperInvariant();

    private void SetLatestPrice(string symbol, double price)
    {
        if (price <= 0)
            return;

        var normalized = NormalizeSymbolKey(symbol);
        lock (this.syncRoot)
        {
            this.latestPrices[normalized] = price;
        }
    }

    private MarketSnapshotDto? TryGetCachedSnapshot(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.snapshotCache.TryGetValue(symbol, out var snapshot) ? snapshot : null;
        }
    }

    private MarketSnapshotDto BuildLiveSnapshot(
        string symbol,
        DateTime timestamp,
        double last,
        double bid,
        double ask,
        long bidSize,
        long askSize,
        double open,
        double high,
        double low,
        double close,
        long volume)
    {
        var cached = this.TryGetCachedSnapshot(symbol);
        var effectiveLast = last > 0 ? last : cached?.Last ?? 0d;
        var effectiveOpen = open > 0 ? open : cached?.Open > 0 ? cached.Open : effectiveLast;
        var effectiveHigh = high > 0 ? high : cached?.High > 0 ? cached.High : effectiveLast;
        var effectiveLow = low > 0 ? low : cached?.Low > 0 ? cached.Low : effectiveLast;
        var effectiveClose = ResolveReferenceClose(cached, close, effectiveOpen, effectiveLast);
        var effectiveBid = bid > 0 ? bid : cached?.Bid > 0 ? cached.Bid : effectiveLast;
        var effectiveAsk = ask > 0 ? ask : cached?.Ask > 0 ? cached.Ask : effectiveLast;
        var effectiveBidSize = bidSize > 0 ? bidSize : cached?.Bid_Size ?? 0L;
        var effectiveAskSize = askSize > 0 ? askSize : cached?.Ask_Size ?? 0L;
        var effectiveVolume = volume > 0
            ? Math.Max(volume, cached?.Volume ?? 0L)
            : cached?.Volume ?? 0L;

        var snapshot = new MarketSnapshotDto
        {
            Symbol = symbol,
            As_Of = timestamp,
            Last = effectiveLast,
            Bid = effectiveBid,
            Ask = effectiveAsk,
            Bid_Size = effectiveBidSize,
            Ask_Size = effectiveAskSize,
            Open = effectiveOpen,
            High = effectiveHigh,
            Low = effectiveLow,
            Close = effectiveClose,
            Volume = effectiveVolume,
            Asset_Type = cached?.Asset_Type,
            Description = cached?.Description,
            Exchange = cached?.Exchange,
            Relative_Volume = cached?.Relative_Volume ?? 0d,
            Trend_State = cached?.Trend_State ?? string.Empty,
            Vwap_Bias = cached?.Vwap_Bias ?? string.Empty
        };

        lock (this.syncRoot)
        {
            this.snapshotCache[symbol] = snapshot;
        }

        return snapshot;
    }

    private bool TryGetLatestPrice(string symbol, out double price)
    {
        var normalized = NormalizeSymbolKey(symbol);
        lock (this.syncRoot)
        {
            return this.latestPrices.TryGetValue(normalized, out price);
        }
    }

    private void ScheduleSnapshotRefresh(string? symbolId, string reason)
    {
        var normalized = symbolId?.Trim().ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(normalized) || this.backendClient == null)
            return;

        lock (this.syncRoot)
        {
            if (!this.pendingSnapshotRefreshes.Add(normalized))
                return;
        }

        Task.Run(() =>
        {
            try
            {
                this.PublishSnapshot(normalized);
            }
            finally
            {
                lock (this.syncRoot)
                {
                    this.pendingSnapshotRefreshes.Remove(normalized);
                }
            }
        });

        LogDiagnostic($"Snapshot refresh scheduled symbol={normalized} reason={reason}");
    }

    private void EnsureStream(string symbol)
    {
        if (!this.connected || this.backendClient == null)
            return;

        lock (this.syncRoot)
        {
            if (this.streamStates.TryGetValue(symbol, out var existing))
            {
                existing.StopDebounceCancellation?.Cancel();
                existing.StopDebounceCancellation = null;
                existing.StopDebounceTask = null;
                return;
            }

            var state = new SymbolStreamState(symbol);
            this.streamStates[symbol] = state;
            state.Task = Task.Run(() => this.RunStreamAsync(state));
        }

        LogDiagnostic($"Stream ensured symbol={symbol}");
    }

    private void PrimeRealtimeSymbol(string? symbolId)
    {
        var normalized = symbolId?.Trim().ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(normalized) || this.backendClient == null)
            return;

        var shouldPrime = false;
        lock (this.syncRoot)
        {
            if (this.snapshotCache.ContainsKey(normalized))
                return;

            shouldPrime = this.primedSymbols.Add(normalized);
        }

        if (!shouldPrime)
            return;

        this.ScheduleSnapshotRefresh(normalized, "prime");
    }

    private void StopStream(string symbol)
    {
        SymbolStreamState? state;
        lock (this.syncRoot)
        {
            if (!this.streamStates.TryGetValue(symbol, out state))
                return;

            if (state.StopDebounceTask is { IsCompleted: false })
                return;

            var cancellation = new CancellationTokenSource();
            state.StopDebounceCancellation = cancellation;
            state.StopDebounceTask = Task.Run(() => this.StopStreamAfterDelayAsync(state, cancellation.Token));
        }

        LogDiagnostic($"Stream stop scheduled symbol={symbol} delayMs={StreamStopDebounce.TotalMilliseconds}");
    }

    private async Task RunStreamAsync(SymbolStreamState state)
    {
        var reconnectAttempt = 0;
        while (!state.Cancellation.IsCancellationRequested)
        {
            ClientWebSocket? socket = null;
            try
            {
                socket = new ClientWebSocket();
                socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
                state.Socket = socket;
                await socket.ConnectAsync(new Uri(this.settings.StreamRoute(state.Symbol)), state.Cancellation.Token);
                reconnectAttempt = 0;
                LogDiagnostic($"Stream connected symbol={state.Symbol}");

                while (!state.Cancellation.IsCancellationRequested && socket.State == WebSocketState.Open)
                {
                    var message = await this.ReceiveMessageAsync(socket, state.Cancellation.Token);
                    if (string.IsNullOrWhiteSpace(message))
                        continue;

                    this.HandleStreamMessage(state.Symbol, message);
                }
            }
            catch (OperationCanceledException) when (state.Cancellation.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Core.Instance.Loggers.Log(ex);
                LogDiagnostic($"Stream error symbol={state.Symbol} error={ex.Message}");
            }
            finally
            {
                socket?.Dispose();
                state.Socket = null;
            }

            if (state.Cancellation.IsCancellationRequested)
                break;

            // Quantower can aggressively churn panel subscriptions even while the
            // user is still in an active workspace session. Keep symbol streams
            // alive for the full vendor connection lifetime and let explicit
            // vendor disconnect be the teardown boundary.
            if (!this.connected)
                break;

            var delay = StreamReconnectBackoff[Math.Min(reconnectAttempt, StreamReconnectBackoff.Length - 1)];
            reconnectAttempt++;
            LogDiagnostic($"Stream reconnect scheduled symbol={state.Symbol} delayMs={delay.TotalMilliseconds}");
            await Task.Delay(delay, state.Cancellation.Token);
        }

        lock (this.syncRoot)
        {
            if (this.streamStates.TryGetValue(state.Symbol, out var current) && ReferenceEquals(current, state))
                this.streamStates.Remove(state.Symbol);
        }

        LogDiagnostic($"Stream exited symbol={state.Symbol}");
    }

    private async Task StopStreamAfterDelayAsync(SymbolStreamState state, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(StreamStopDebounce, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        var shouldStop = false;
        lock (this.syncRoot)
        {
            if (!this.streamStates.TryGetValue(state.Symbol, out var current) || !ReferenceEquals(current, state))
                return;

            if (this.connected || this.HasAnySubscriptionUnsafe(state.Symbol))
            {
                state.StopDebounceCancellation = null;
                state.StopDebounceTask = null;
                return;
            }

            this.streamStates.Remove(state.Symbol);
            this.primedSymbols.Remove(state.Symbol);
            state.StopDebounceCancellation = null;
            state.StopDebounceTask = null;
            shouldStop = true;
        }

        if (!shouldStop)
            return;

        state.Cancellation.Cancel();
        state.Socket?.Dispose();
        LogDiagnostic($"Stream stopped symbol={state.Symbol}");
    }

    private async Task<string> ReceiveMessageAsync(ClientWebSocket socket, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        var buffer = new byte[8192];

        while (true)
        {
            var result = await socket.ReceiveAsync(buffer, cancellationToken);

            if (result.MessageType == WebSocketMessageType.Close)
                return string.Empty;

            stream.Write(buffer, 0, result.Count);
            if (result.EndOfMessage)
                break;
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private void HandleStreamMessage(string expectedSymbol, string message)
    {
        using var document = JsonDocument.Parse(message);
        var root = document.RootElement;

        var eventType = root.TryGetProperty("type", out var typeElement) ? typeElement.GetString() : null;
        var symbol = root.TryGetProperty("symbol", out var symbolElement)
            ? symbolElement.GetString() ?? expectedSymbol
            : expectedSymbol;

        if (!root.TryGetProperty("payload", out var payload))
        {
            if (string.Equals(eventType, "heartbeat", StringComparison.OrdinalIgnoreCase))
            {
                this.RepublishCachedState(symbol);
                return;
            }

            return;
        }

        if (string.Equals(eventType, "heartbeat", StringComparison.OrdinalIgnoreCase))
        {
            this.RepublishCachedState(symbol);
            return;
        }

        if (string.Equals(eventType, "quote", StringComparison.OrdinalIgnoreCase))
        {
            this.PublishQuoteEvent(symbol, payload);
            return;
        }

        if (string.Equals(eventType, "bar", StringComparison.OrdinalIgnoreCase))
        {
            this.PublishBarEvent(symbol, payload);
            return;
        }

        if (string.Equals(eventType, "book", StringComparison.OrdinalIgnoreCase))
        {
            var venue = root.TryGetProperty("venue", out var venueElement)
                ? venueElement.GetString()
                : null;
            this.PublishBookEvent(symbol, payload, venue);
        }
    }

    private void PublishQuoteEvent(string symbol, JsonElement payload)
    {
        var last = GetDouble(payload, "LAST_PRICE");
        var bid = GetDouble(payload, "BID_PRICE");
        var ask = GetDouble(payload, "ASK_PRICE");
        var bidSize = GetDouble(payload, "BID_SIZE");
        var askSize = GetDouble(payload, "ASK_SIZE");
        var lastSize = GetDouble(payload, "LAST_SIZE");
        var totalVolume = GetDouble(payload, "TOTAL_VOLUME");
        var open = GetDouble(payload, "OPEN_PRICE");
        var high = GetDouble(payload, "HIGH_PRICE");
        var low = GetDouble(payload, "LOW_PRICE");
        var close = GetDouble(payload, "CLOSE_PRICE");
        var timestamp = GetTimestamp(payload, "TRADE_TIME_MILLIS")
            ?? GetTimestamp(payload, "QUOTE_TIME_MILLIS")
            ?? DateTime.UtcNow;

        var snapshot = this.BuildLiveSnapshot(
            symbol,
            timestamp,
            last,
            bid,
            ask,
            (long)bidSize,
            (long)askSize,
            open,
            high,
            low,
            close,
            (long)totalVolume);

        if (this.HasQuoteSubscription(symbol))
        {
            this.PushMessage(CreateQuote(snapshot));
            this.PushMessage(CreateDayBar(snapshot));
        }

        if (this.HasLevel2Subscription(symbol))
            this.PublishBestAvailableDom(symbol, snapshot);

        this.SetLatestPrice(symbol, snapshot.Last);
        this.PublishLastEvent(symbol, snapshot.Last, lastSize, timestamp);
    }

    private void PublishBarEvent(string symbol, JsonElement payload)
    {
        if (!this.HasQuoteSubscription(symbol))
            return;

        var last = GetDouble(payload, "CLOSE_PRICE");
        var open = GetDouble(payload, "OPEN_PRICE");
        var high = GetDouble(payload, "HIGH_PRICE");
        var low = GetDouble(payload, "LOW_PRICE");
        var volume = GetDouble(payload, "VOLUME");
        var timestamp = GetTimestamp(payload, "CHART_TIME_MILLIS") ?? DateTime.UtcNow;

        var snapshot = this.BuildLiveSnapshot(
            symbol,
            timestamp,
            last,
            0d,
            0d,
            0L,
            0L,
            open,
            high,
            low,
            0d,
            (long)volume);

        this.PushMessage(CreateDayBar(snapshot));
        this.SetLatestPrice(symbol, snapshot.Last);

        // Do not publish completed-bar volume as Time & Sales. Trade tape should
        // come from live last-trade fields, not candle aggregate volume.
    }

    private void PublishBookEvent(string symbol, JsonElement payload, string? venue)
    {
        if (!this.HasLevel2Subscription(symbol))
            return;

        var timestamp = GetTimestamp(payload, "BOOK_TIME") ?? DateTime.UtcNow;
        var dom = new DOMQuote(symbol, timestamp);

        if (payload.TryGetProperty("BIDS", out var bids) && bids.ValueKind == JsonValueKind.Array)
        {
            foreach (var bid in bids.EnumerateArray())
            {
                var price = GetDouble(bid, "BID_PRICE");
                var size = GetDouble(bid, "TOTAL_VOLUME");
                if (price <= 0)
                    continue;

                dom.Bids.Add(new Level2Quote(QuotePriceType.Bid, symbol, $"B_{price:0.####}", price, size, timestamp)
                {
                    Closed = size <= 0,
                    NumberOrders = (int)GetDouble(bid, "NUM_BIDS")
                });
            }
        }

        if (payload.TryGetProperty("ASKS", out var asks) && asks.ValueKind == JsonValueKind.Array)
        {
            foreach (var ask in asks.EnumerateArray())
            {
                var price = GetDouble(ask, "ASK_PRICE");
                var size = GetDouble(ask, "TOTAL_VOLUME");
                if (price <= 0)
                    continue;

                dom.Asks.Add(new Level2Quote(QuotePriceType.Ask, symbol, $"A_{price:0.####}", price, size, timestamp)
                {
                    Closed = size <= 0,
                    NumberOrders = (int)GetDouble(ask, "NUM_ASKS")
                });
            }
        }

        if (dom.Bids.Count > 0 || dom.Asks.Count > 0)
        {
            CachedDomState mergedDom;
            lock (this.syncRoot)
            {
                this.realBookSeen.Add(symbol);
                if (!this.domVenueCache.TryGetValue(symbol, out var venueStates))
                {
                    venueStates = new Dictionary<string, CachedDomState>(StringComparer.OrdinalIgnoreCase);
                    this.domVenueCache[symbol] = venueStates;
                }

                var venueKey = string.IsNullOrWhiteSpace(venue) ? "__BOOK__" : venue!;
                venueStates.TryGetValue(venueKey, out var existingVenueState);
                venueStates[venueKey] = CachedDomState.MergeBookUpdate(existingVenueState, dom, isRealBook: true);
                var cutoffUtc = DateTime.UtcNow - RealBookFreshnessWindow;
                foreach (var staleVenue in venueStates
                             .Where(pair => pair.Value.TimestampUtc < cutoffUtc)
                             .Select(pair => pair.Key)
                             .ToList())
                {
                    venueStates.Remove(staleVenue);
                }

                mergedDom = CachedDomState.MergeVenueStates(venueStates.Values);
                if (this.snapshotCache.TryGetValue(symbol, out var snapshot))
                {
                    mergedDom = CachedDomState.ClampToNbbo(mergedDom, snapshot.Bid, snapshot.Ask);
                }
                this.domCache[symbol] = mergedDom;
            }
            LogDiagnostic($"PublishBook symbol={symbol} venue={venue ?? "UNKNOWN"} bids={mergedDom.Bids.Count} asks={mergedDom.Asks.Count}", verbose: true);
            this.PushMessage(mergedDom.ToDomQuote(symbol));
        }
    }

    private void PublishLastEvent(string symbol, double last, double lastSize, DateTime timestamp)
    {
        if ((!this.HasLastSubscription(symbol) && !this.HasQuoteSubscription(symbol)) || last <= 0)
            return;

        if (timestamp == default)
            timestamp = DateTime.UtcNow;

        var eventKey = timestamp.Ticks;
        lock (this.syncRoot)
        {
            if (this.lastTradeTimes.TryGetValue(symbol, out var lastTicks) && eventKey <= lastTicks)
                return;

            this.lastTradeTimes[symbol] = eventKey;
        }

        this.PushMessage(new Last(symbol, last, lastSize, timestamp)
        {
            TradeId = $"{symbol}-{eventKey}-{last:0.####}-{lastSize:0.####}"
        });
        LogDiagnostic($"PublishLast symbol={symbol} price={last} size={lastSize} time={timestamp:O}", verbose: true);
    }

    private void PublishSnapshot(string symbol)
    {
        if (this.backendClient == null)
            return;

        try
        {
            var snapshot = this.backendClient.GetSnapshotAsync(symbol).GetAwaiter().GetResult();
            if (snapshot == null)
                return;

            this.MarkBackendSuccess();

            lock (this.syncRoot)
            {
                this.snapshotCache[symbol] = snapshot;
            }
            this.CreateMessageSymbol(symbol);

            if (this.HasQuoteSubscription(symbol))
            {
                this.PushMessage(CreateQuote(snapshot));
                this.PushMessage(CreateDayBar(snapshot));
            }

            this.SetLatestPrice(symbol, snapshot.Last);

            if (this.HasLevel2Subscription(symbol))
                this.PublishBestAvailableDom(symbol, snapshot);

            if (this.HasLastSubscription(symbol) || this.HasQuoteSubscription(symbol))
                this.PushMessage(CreateLast(snapshot));
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
            LogDiagnostic($"PublishSnapshot error symbol={symbol} error={ex.Message}");

            MarketSnapshotDto? cachedSnapshot = null;
            lock (this.syncRoot)
            {
                this.snapshotCache.TryGetValue(symbol, out cachedSnapshot);
            }

            if (cachedSnapshot == null)
                return;

            if (this.HasQuoteSubscription(symbol))
            {
                this.PushMessage(CreateQuote(cachedSnapshot));
                this.PushMessage(CreateDayBar(cachedSnapshot));
            }

            this.SetLatestPrice(symbol, cachedSnapshot.Last);

            if (this.HasLevel2Subscription(symbol))
                this.PublishBestAvailableDom(symbol, cachedSnapshot);

            if (this.HasLastSubscription(symbol) || this.HasQuoteSubscription(symbol))
                this.PushMessage(CreateLast(cachedSnapshot));
        }
    }

    private MessageSymbol CreateSearchResultSymbol(string rawSymbol) => this.CreateMessageSymbol(rawSymbol, primeRealtimeIfMissing: false);

    private MessageSymbol CreateMessageSymbol(string rawSymbol) => this.CreateMessageSymbol(rawSymbol, primeRealtimeIfMissing: true);

    private MessageSymbol CreateMessageSymbol(string rawSymbol, bool primeRealtimeIfMissing)
    {
        var symbol = rawSymbol.Trim().ToUpperInvariant();
        lock (this.syncRoot)
        {
            if (this.optionContractCache.TryGetValue(symbol, out var optionContract))
                return this.CreateOptionMessageSymbol(optionContract);
        }

        MarketSnapshotDto? snapshot;
        SymbolProfileDto? profile;
        lock (this.syncRoot)
        {
            this.snapshotCache.TryGetValue(symbol, out snapshot);
            this.symbolProfileCache.TryGetValue(symbol, out profile);
        }

        if (snapshot == null && primeRealtimeIfMissing)
            this.PrimeRealtimeSymbol(symbol);

        return this.CreateMessageSymbolCore(symbol, snapshot, profile);
    }

    private MessageSymbol CreateMessageSymbolFromProfile(SymbolProfileDto profile, bool primeRealtimeIfMissing = true)
    {
        var symbol = profile.Symbol.Trim().ToUpperInvariant();
        lock (this.syncRoot)
        {
            this.symbolProfileCache[symbol] = profile;
            this.symbolProfileCache[profile.NormalizedSymbol.Trim().ToUpperInvariant()] = profile;
        }

        MarketSnapshotDto? snapshot = null;
        lock (this.syncRoot)
        {
            this.snapshotCache.TryGetValue(symbol, out snapshot);
        }

        if (snapshot == null && primeRealtimeIfMissing)
            this.PrimeRealtimeSymbol(symbol);

        return this.CreateMessageSymbolCore(symbol, snapshot, profile);
    }

    private MessageSymbol CreateMessageSymbolCore(string symbol, MarketSnapshotDto? snapshot, SymbolProfileDto? profile)
    {
        if (snapshot == null)
        {
            lock (this.syncRoot)
            {
                if (this.symbolCache.TryGetValue(symbol, out var cached))
                    return cached;
            }
        }

        var message = new MessageSymbol(symbol)
        {
            Name = symbol,
            Description = profile?.Description ?? snapshot?.Description ?? $"{symbol} via local Schwab bridge",
            ProductAssetId = symbol,
            QuotingCurrencyAssetID = "USD",
            SymbolType = ResolveSymbolType(symbol, snapshot, profile),
            ExchangeId = ExchangeId,
            HistoryType = HistoryType.Last,
            VolumeType = SymbolVolumeType.Volume,
            NettingType = NettingType.OnePosition,
            QuotingType = SymbolQuotingType.LotSize,
            DeltaCalculationType = DeltaCalculationType.TickDirection,
            VariableTickList = new List<VariableTick> { new(ResolveMinimumTick(snapshot)) },
            LotSize = 1d,
            LotStep = 1d,
            NotionalValueStep = 1d,
            MinLot = 1d,
            MaxLot = int.MaxValue,
            AllowCalculateRealtimeChange = true,
            AllowCalculateRealtimeVolume = true,
            AllowCalculateRealtimeTicks = true,
            AllowCalculateRealtimeTrades = true,
            AllowAbbreviatePriceByTickSize = true
        };

        if (profile?.OptionsAvailable == true && message.SymbolType is SymbolType.Equities or SymbolType.ETF or SymbolType.Indexes)
        {
            message.AvailableOptions = AvailableDerivatives.Present;
            message.AvailableOptionsExchanges = [OptionExchangeId];
            this.ScheduleOptionHydration(symbol);
        }

        lock (this.syncRoot)
        {
            this.symbolCache[symbol] = message;
        }

        return message;
    }

    private static SymbolType ResolveSymbolType(string symbol, MarketSnapshotDto? snapshot, SymbolProfileDto? profile)
    {
        var assetType = profile?.AssetType?.ToUpperInvariant() ?? snapshot?.Asset_Type?.ToUpperInvariant();
        var instrumentType = profile?.InstrumentType?.ToUpperInvariant();

        return (assetType, instrumentType) switch
        {
            ("EQUITY", _) => SymbolType.Equities,
            ("ETF", _) => SymbolType.ETF,
            ("COLLECTIVE_INVESTMENT", "EXCHANGE_TRADED_FUND") => SymbolType.ETF,
            ("COLLECTIVE_INVESTMENT", _) => SymbolType.ETF,
            ("INDEX", _) => SymbolType.Indexes,
            ("INDX", _) => SymbolType.Indexes,
            _ when symbol.StartsWith('$') || symbol is "VIX" or "SPX" => SymbolType.Indexes,
            _ => SymbolType.Equities
        };
    }

    private static double ResolveMinimumTick(MarketSnapshotDto? snapshot)
    {
        var reference = snapshot?.Bid > 0
            ? snapshot.Bid
            : snapshot?.Ask > 0
                ? snapshot.Ask
                : snapshot?.Last ?? 0d;

        return reference > 0 && reference < 1 ? 0.0001d : 0.01d;
    }

    private MessageOptionSerie CreateOptionSerie(OptionSeriesDto dto, string exchangeId)
    {
        var underlier = dto.UnderlierSymbol.Trim().ToUpperInvariant();
        var seriesId = $"{underlier}|{dto.ExpirationDate:yyyy-MM-dd}";
        return new MessageOptionSerie
        {
            Id = seriesId,
            Name = dto.Name,
            UnderlierId = underlier,
            ExpirationDate = dto.ExpirationDate,
            SerieType = ResolveSerieType(dto.SeriesType),
        };
    }

    private void ScheduleOptionHydration(string underlierSymbol)
    {
        if (this.backendClient == null || string.IsNullOrWhiteSpace(underlierSymbol))
            return;

        var underlierId = underlierSymbol.Trim().ToUpperInvariant();

        lock (this.syncRoot)
        {
            if (this.pendingOptionHydrations.Contains(underlierId))
                return;

            this.pendingOptionHydrations.Add(underlierId);
        }

        _ = Task.Run(async () =>
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                var chainTask = this.backendClient.GetOptionChainAsync(underlierId, cancellationToken: cts.Token);
                var seriesTask = this.backendClient.GetOptionSeriesAsync(underlierId, cts.Token);
                await Task.WhenAll(chainTask, seriesTask).ConfigureAwait(false);

                var chain = await chainTask.ConfigureAwait(false);
                var seriesDtos = await seriesTask.ConfigureAwait(false);
                this.MarkBackendSuccess();
                var series = seriesDtos
                    .Select(dto => CreateOptionSerie(dto, dto.Exchange ?? OptionExchangeId))
                    .ToList();

                lock (this.syncRoot)
                {
                    if (series.Count > 0)
                        this.optionSeriesCache[underlierId] = series;

                    if (chain != null)
                    {
                        foreach (var contract in chain.Contracts)
                            this.CacheOptionContract(contract);

                        if (series.Count == 0 && chain.Contracts.Count > 0)
                        {
                            this.optionSeriesCache[underlierId] = chain.Contracts
                                .Select(contract => CreateOptionSerie(
                                    new OptionSeriesDto
                                    {
                                        Id = $"{contract.UnderlierSymbol.ToUpperInvariant()}|{contract.ExpirationDate:yyyy-MM-dd}",
                                        UnderlierSymbol = contract.UnderlierSymbol.ToUpperInvariant(),
                                        ExpirationDate = contract.ExpirationDate,
                                        DaysToExpiration = Math.Max((contract.ExpirationDate.Date - DateTimeOffset.UtcNow.Date).Days, 0),
                                        SeriesType = "unknown",
                                        Name = $"{contract.UnderlierSymbol.ToUpperInvariant()} {contract.ExpirationDate:yyyy-MM-dd}",
                                        Exchange = contract.Exchange ?? OptionExchangeId
                                    },
                                    contract.Exchange ?? OptionExchangeId))
                                .DistinctBy(serie => serie.Id)
                                .OrderBy(serie => serie.ExpirationDate)
                                .ToList();
                        }
                    }
                }

                LogDiagnostic($"ScheduleOptionHydration underlier={underlierId} series={series.Count} contracts={(chain?.Contracts.Count ?? 0)}");
            }
            catch (Exception ex)
            {
                LogDiagnostic($"ScheduleOptionHydration failed underlier={underlierId} error={ex.Message}");
            }
            finally
            {
                lock (this.syncRoot)
                    this.pendingOptionHydrations.Remove(underlierId);
            }
        });
    }

    private void CacheOptionContract(OptionContractDto contract)
    {
        var symbol = contract.Symbol.Trim().ToUpperInvariant();
        this.optionContractCache[symbol] = contract;
        this.snapshotCache[symbol] = CreateSnapshotFromOptionContract(contract);
        this.symbolCache[symbol] = this.CreateOptionMessageSymbol(contract);
    }

    private MessageSymbol CreateOptionMessageSymbol(OptionContractDto contract)
    {
        var symbol = contract.Symbol.Trim().ToUpperInvariant();
        return new MessageSymbol(symbol)
        {
            Name = symbol,
            Description = contract.Description,
            ProductAssetId = symbol,
            QuotingCurrencyAssetID = "USD",
            SymbolType = SymbolType.Options,
            ExchangeId = NormalizeOptionExchange(contract.Exchange),
            HistoryType = HistoryType.Last,
            VolumeType = SymbolVolumeType.Volume,
            NettingType = NettingType.OnePosition,
            QuotingType = SymbolQuotingType.LotSize,
            DeltaCalculationType = DeltaCalculationType.TickDirection,
            VariableTickList = new List<VariableTick> { new(contract.StrikePrice < 1 ? 0.0001d : 0.01d) },
            LotSize = 1d,
            LotStep = 1d,
            NotionalValueStep = 1d,
            MinLot = 1d,
            MaxLot = int.MaxValue,
            AllowCalculateRealtimeChange = true,
            AllowCalculateRealtimeVolume = true,
            // Schwab option-chain payloads do not include a trustworthy per-contract
            // tick/trade count in this integration path. Letting QT derive tick-based
            // metrics here produces repeated misleading values in the option grid.
            AllowCalculateRealtimeTicks = false,
            // Schwab option-chain payloads give us volume/OI/greeks, but not a
            // trustworthy per-contract trade-count metric in this path. Letting
            // QT infer trades here produces repeated misleading values.
            AllowCalculateRealtimeTrades = false,
            AllowAbbreviatePriceByTickSize = true,
            OptionType = string.Equals(contract.OptionType, "PUT", StringComparison.OrdinalIgnoreCase) ? OptionType.Put : OptionType.Call,
            StrikePrice = contract.StrikePrice,
            ExpirationDate = contract.ExpirationDate,
            LastTradingDate = contract.ExpirationDate,
            UnderlierId = contract.UnderlierSymbol.ToUpperInvariant(),
            OptionSerieId = $"{contract.UnderlierSymbol.ToUpperInvariant()}|{contract.ExpirationDate:yyyy-MM-dd}",
            Root = contract.UnderlierSymbol.ToUpperInvariant(),
        };
    }

    private MessageSymbolInfo CreateOptionStrikeInfo(OptionContractDto contract)
    {
        var symbol = contract.Symbol.Trim().ToUpperInvariant();
        return new MessageSymbolInfo(symbol)
        {
            Name = symbol,
            Description = contract.Description,
            ExchangeId = NormalizeOptionExchange(contract.Exchange),
            SymbolType = SymbolType.Options,
            UnderlierId = contract.UnderlierSymbol.ToUpperInvariant(),
            OptionSerieId = $"{contract.UnderlierSymbol.ToUpperInvariant()}|{contract.ExpirationDate:yyyy-MM-dd}",
            ExpirationDate = contract.ExpirationDate,
            LastTradingDate = contract.ExpirationDate,
            StrikePrice = contract.StrikePrice,
            OptionType = string.Equals(contract.OptionType, "PUT", StringComparison.OrdinalIgnoreCase) ? OptionType.Put : OptionType.Call,
            Root = contract.UnderlierSymbol.ToUpperInvariant(),
        };
    }

    private static string NormalizeOptionExchange(params string?[] exchanges)
    {
        foreach (var exchange in exchanges)
        {
            if (string.IsNullOrWhiteSpace(exchange))
                continue;

            var normalized = exchange.Trim().ToUpperInvariant();
            if (normalized is "US" or "NYSE" or "NASDAQ" or "ARCA")
                continue;

            return normalized;
        }

        return OptionExchangeId;
    }

    private static MarketSnapshotDto CreateSnapshotFromOptionContract(OptionContractDto contract)
    {
        var last = contract.Last ?? contract.Mark ?? contract.Bid ?? contract.Ask ?? 0d;
        var bid = contract.Bid ?? last;
        var ask = contract.Ask ?? last;
        return new MarketSnapshotDto
        {
            Symbol = contract.Symbol.Trim().ToUpperInvariant(),
            As_Of = DateTimeOffset.UtcNow,
            Last = last,
            Bid = bid,
            Ask = ask,
            Bid_Size = contract.BidSize ?? 0,
            Ask_Size = contract.AskSize ?? 0,
            Open = contract.Last ?? last,
            High = contract.Last ?? last,
            Low = contract.Last ?? last,
            Close = contract.Mark ?? last,
            Volume = contract.Volume ?? 0,
            Open_Interest = contract.OpenInterest ?? 0,
            Volatility = contract.Volatility ?? 0d,
            Delta = contract.Delta ?? 0d,
            Gamma = contract.Gamma ?? 0d,
            Theta = contract.Theta ?? 0d,
            Vega = contract.Vega ?? 0d,
            Rho = contract.Rho ?? 0d,
            Asset_Type = "OPTION",
            Description = contract.Description,
            Exchange = contract.Exchange ?? ExchangeId,
            Relative_Volume = 0d,
            Trend_State = "neutral",
            Vwap_Bias = "flat"
        };
    }

    private bool IsOptionSymbol(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.optionContractCache.ContainsKey(symbol);
        }
    }

    private async Task RunMarketStatePulseAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(MarketStatePulseInterval, token);
                this.PulseActiveMarketState();
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Core.Instance.Loggers.Log(ex);
                LogDiagnostic($"MarketStatePulse error={ex.Message}");
            }
        }
    }

    private void PulseActiveMarketState()
    {
        List<string> activeSymbols;
        lock (this.syncRoot)
        {
            activeSymbols = this.quoteSubscriptions.Keys
                .Concat(this.lastSubscriptions.Keys)
                .Concat(this.level2Subscriptions.Keys)
                .Concat(this.markSubscriptions.Keys)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        foreach (var symbol in activeSymbols)
        {
            this.RepublishCachedState(symbol);
        }
    }

    private void RepublishCachedState(string symbol)
    {
        MarketSnapshotDto? snapshot;
        lock (this.syncRoot)
        {
            this.snapshotCache.TryGetValue(symbol, out snapshot);
        }

        if (snapshot == null)
        {
            if (this.HasLevel2Subscription(symbol) && this.HasAnyCachedDom(symbol))
                this.PublishCachedDom(symbol);
            else if (!this.IsOptionSymbol(symbol))
                this.ScheduleSnapshotRefresh(symbol, "pulse");

            return;
        }

        if (this.HasQuoteSubscription(symbol))
        {
            this.PushMessage(CreateQuote(snapshot));
            this.PushMessage(CreateDayBar(snapshot));
        }

        if (this.HasLastSubscription(symbol) || this.HasQuoteSubscription(symbol))
            this.PushMessage(CreateLast(snapshot));

        if (this.HasLevel2Subscription(symbol))
            this.PublishBestAvailableDom(symbol, snapshot);
    }

    private static OptionSerieType ResolveSerieType(string? seriesType) =>
        (seriesType ?? string.Empty).ToLowerInvariant() switch
        {
            "daily" => OptionSerieType.Daily,
            "week" => OptionSerieType.Week,
            "month" => OptionSerieType.Month,
            _ => OptionSerieType.Unknown,
        };

    private static double GetDouble(JsonElement payload, string propertyName)
    {
        if (!payload.TryGetProperty(propertyName, out var value))
            return 0d;

        return value.ValueKind switch
        {
            JsonValueKind.Number => value.GetDouble(),
            JsonValueKind.String when double.TryParse(value.GetString(), out var parsed) => parsed,
            _ => 0d
        };
    }

    private static DateTime? GetTimestamp(JsonElement payload, string propertyName)
    {
        if (!payload.TryGetProperty(propertyName, out var value))
            return null;

        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var millis))
            return DateTimeOffset.FromUnixTimeMilliseconds(millis).UtcDateTime;

        if (value.ValueKind == JsonValueKind.String && long.TryParse(value.GetString(), out var millisString))
            return DateTimeOffset.FromUnixTimeMilliseconds(millisString).UtcDateTime;

        return null;
    }

    private void PublishSnapshotDom(MarketSnapshotDto snapshot) =>
        this.PublishSyntheticDom(
            snapshot.Symbol,
            snapshot.Bid,
            snapshot.Ask,
            snapshot.Last,
            snapshot.Bid_Size,
            snapshot.Ask_Size,
            snapshot.As_Of.UtcDateTime);

    private void PublishSyntheticDom(
        string symbol,
        double bid,
        double ask,
        double last,
        double bidSize,
        double askSize,
        DateTime timestamp)
    {
        var resolvedBid = bid > 0 ? bid : last;
        var resolvedAsk = ask > 0 ? ask : last;
        if (resolvedBid <= 0 && resolvedAsk <= 0)
            return;

        lock (this.syncRoot)
        {
            if (this.domCache.TryGetValue(symbol, out var existing)
                && existing.IsRealBook
                && (DateTime.UtcNow - existing.TimestampUtc) <= RealBookFreshnessWindow)
                return;
        }

        var dom = new DOMQuote(symbol, timestamp);
        if (resolvedBid > 0)
        {
            dom.Bids.Add(new Level2Quote(QuotePriceType.Bid, symbol, $"B_{resolvedBid:0.####}", resolvedBid, bidSize, timestamp)
            {
                Closed = bidSize <= 0
            });
        }

        if (resolvedAsk > 0)
        {
            dom.Asks.Add(new Level2Quote(QuotePriceType.Ask, symbol, $"A_{resolvedAsk:0.####}", resolvedAsk, askSize, timestamp)
            {
                Closed = askSize <= 0
            });
        }

        lock (this.syncRoot)
        {
            this.domCache[symbol] = CachedDomState.FromDomQuote(dom, isRealBook: false);
        }

        this.PushMessage(dom);
    }

    private void PublishCachedDom(string symbol)
    {
        CachedDomState? cached;
        lock (this.syncRoot)
        {
            this.domCache.TryGetValue(symbol, out cached);
        }

        if (cached == null)
            return;

        this.PushMessage(cached.ToDomQuote(symbol));
    }

    private void PublishBestAvailableDom(string symbol, MarketSnapshotDto snapshot)
    {
        if (this.HasFreshRealBook(symbol))
        {
            this.PublishCachedDom(symbol);
            return;
        }

        if (this.TryGetCachedDom(symbol, out var cachedDom) && !cachedDom.IsRealBook)
        {
            this.PublishCachedDom(symbol);
            return;
        }

        this.PublishSnapshotDom(snapshot);
    }

    private void PublishCachedSnapshotState(string symbol, SubscribeQuoteType subscribeType)
    {
        MarketSnapshotDto? cachedSnapshot;
        lock (this.syncRoot)
        {
            this.snapshotCache.TryGetValue(symbol, out cachedSnapshot);
        }

        if (cachedSnapshot == null)
            return;

        if (subscribeType is SubscribeQuoteType.Quote or SubscribeQuoteType.Mark)
        {
            this.PushMessage(CreateQuote(cachedSnapshot));
            this.PushMessage(CreateDayBar(cachedSnapshot));
            this.SetLatestPrice(symbol, cachedSnapshot.Last);
        }

        if (subscribeType is SubscribeQuoteType.Last or SubscribeQuoteType.Quote or SubscribeQuoteType.Mark)
            this.PushMessage(CreateLast(cachedSnapshot));
    }

    private bool HasRealBook(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.realBookSeen.Contains(symbol);
        }
    }

    private bool HasAnyCachedDom(string symbol)
    {
        lock (this.syncRoot)
        {
            return this.domCache.ContainsKey(symbol);
        }
    }

    private bool TryGetCachedDom(string symbol, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out CachedDomState? dom)
    {
        lock (this.syncRoot)
        {
            return this.domCache.TryGetValue(symbol, out dom);
        }
    }

    private bool HasFreshRealBook(string symbol)
    {
        lock (this.syncRoot)
        {
            if (!this.domCache.TryGetValue(symbol, out var cached))
                return false;

            return cached.IsRealBook && (DateTime.UtcNow - cached.TimestampUtc) <= RealBookFreshnessWindow;
        }
    }

    private static Quote CreateQuote(MarketSnapshotDto snapshot)
    {
        var bid = snapshot.Bid > 0 ? snapshot.Bid : snapshot.Last;
        var ask = snapshot.Ask > 0 ? snapshot.Ask : snapshot.Last;
        var bidSize = snapshot.Bid_Size > 0 ? snapshot.Bid_Size : double.NaN;
        var askSize = snapshot.Ask_Size > 0 ? snapshot.Ask_Size : double.NaN;
        return new Quote(snapshot.Symbol, bid, bidSize, ask, askSize, snapshot.As_Of.UtcDateTime);
    }

    private static Last CreateLast(MarketSnapshotDto snapshot)
    {
        var last = new Last(snapshot.Symbol, snapshot.Last, 0d, snapshot.As_Of.UtcDateTime);
        if (snapshot.Open_Interest > 0)
            last.OpenInterest = snapshot.Open_Interest;

        return last;
    }

    private static double ResolveReferenceClose(MarketSnapshotDto? cachedSnapshot, double close, double open, double last)
    {
        if (close > 0)
            return close;

        if (cachedSnapshot?.Close > 0)
            return cachedSnapshot.Close;

        if (open > 0)
            return open;

        if (cachedSnapshot?.Open > 0)
            return cachedSnapshot.Open;

        return last > 0 ? last : cachedSnapshot?.Last ?? 0d;
    }

    private static DayBar CreateDayBar(MarketSnapshotDto snapshot) => new(snapshot.Symbol, snapshot.As_Of.UtcDateTime)
    {
        Last = snapshot.Last,
        Mark = snapshot.Last,
        Bid = snapshot.Bid > 0 ? snapshot.Bid : snapshot.Last,
        BidSize = snapshot.Bid_Size > 0 ? snapshot.Bid_Size : 0d,
        Ask = snapshot.Ask > 0 ? snapshot.Ask : snapshot.Last,
        AskSize = snapshot.Ask_Size > 0 ? snapshot.Ask_Size : 0d,
        Open = snapshot.Open,
        High = snapshot.High,
        Low = snapshot.Low,
        PreviousClose = snapshot.Close,
        Volume = snapshot.Volume,
        OpenInterest = snapshot.Open_Interest,
        IV = snapshot.Volatility,
        Delta = snapshot.Delta,
        Vega = snapshot.Vega,
        Gamma = snapshot.Gamma,
        Theta = snapshot.Theta,
        Rho = snapshot.Rho,
        Change = snapshot.Last - snapshot.Close,
        ChangePercentage = snapshot.Close != 0 ? ((snapshot.Last / snapshot.Close) - 1d) * 100d : 0d
    };

    private static HistoryItemBar CreateHistoryItem(BarDto bar) => new()
    {
        TicksLeft = bar.Timestamp.UtcDateTime.Ticks,
        Open = bar.Open,
        High = bar.High,
        Low = bar.Low,
        Close = bar.Close,
        Volume = bar.Volume
    };

    private static string GetPositionKey(BrokerPositionDto position) =>
        $"{position.AccountHash}:{NormalizeSymbolKey(position.Symbol)}";

    private static bool PositionChanged(BrokerPositionDto existing, BrokerPositionDto updated) =>
        Math.Abs(existing.Quantity - updated.Quantity) > double.Epsilon ||
        NullableChanged(existing.AveragePrice, updated.AveragePrice) ||
        NullableChanged(existing.MarketPrice, updated.MarketPrice) ||
        NullableChanged(existing.MarketValue, updated.MarketValue) ||
        NullableChanged(existing.DayProfitLoss, updated.DayProfitLoss) ||
        NullableChanged(existing.DayProfitLossPercent, updated.DayProfitLossPercent) ||
        NullableChanged(existing.UnrealizedProfitLoss, updated.UnrealizedProfitLoss);

    private static bool NullableChanged(double? left, double? right)
    {
        if (left is null && right is null)
            return false;
        if (left is null || right is null)
            return true;

        return Math.Abs(left.Value - right.Value) > double.Epsilon;
    }

    private static void LogDiagnostic(string message, bool verbose = false)
    {
        if (verbose && !VerboseDiagnosticsEnabled)
            return;

        try
        {
            var path = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "SchwabQuantowerBridge",
                "SchwabVendor.debug.log");
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.AppendAllText(path, $"{DateTime.UtcNow:O} {message}{Environment.NewLine}");
        }
        catch
        {
            // Diagnostics must never interfere with market data or order handling.
        }
    }

    private static MessageAccount CreateAccount(BrokerAccountDto account)
    {
        var liquidationValue = account.LiquidationValue ?? account.TotalCash ?? account.CashBalance ?? 0d;
        var cashAvailableForTrading = account.CashAvailableForTrading ?? account.CashBalance ?? 0d;
        var cashAvailableForWithdrawal = account.CashAvailableForWithdrawal ?? 0d;
        var totalCash = account.TotalCash ?? account.CashBalance ?? 0d;
        var unsettledCash = account.UnsettledCash ?? 0d;
        var longMarketValue = account.LongMarketValue ?? 0d;
        return new MessageAccount
        {
            AccountId = account.AccountHash,
            AccountName = $"Schwab {account.AccountNumber}",
            AssetId = "USD",
            Balance = liquidationValue,
            NettingType = NettingType.OnePosition,
            AccountAdditionalInfo = new List<AdditionalInfoItem>
            {
                new()
                {
                    Id = nameof(Account.Balance),
                    Hidden = true,
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = liquidationValue
                },
                new()
                {
                    Id = nameof(Account.AccountCurrency),
                    Hidden = true,
                    Value = "USD"
                },
                new()
                {
                    Id = "accountNumber",
                    NameKey = loc.key("Account number"),
                    Value = account.AccountNumber
                },
                new()
                {
                    Id = "accountType",
                    NameKey = loc.key("Account type"),
                    Value = account.AccountType ?? "Unknown"
                },
                new()
                {
                    Id = "liquidationValue",
                    NameKey = loc.key("Liquidation value"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = account.LiquidationValue ?? 0d
                },
                new()
                {
                    Id = "cashBalance",
                    NameKey = loc.key("Cash balance"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = account.CashBalance ?? 0d
                },
                new()
                {
                    Id = "cashAvailableForTrading",
                    NameKey = loc.key("Cash available for trading"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = cashAvailableForTrading
                },
                new()
                {
                    Id = "cashAvailableForWithdrawal",
                    NameKey = loc.key("Cash available for withdrawal"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = cashAvailableForWithdrawal
                },
                new()
                {
                    Id = "totalCash",
                    NameKey = loc.key("Total cash"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = totalCash
                },
                new()
                {
                    Id = "unsettledCash",
                    NameKey = loc.key("Unsettled cash"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = unsettledCash
                },
                new()
                {
                    Id = "longMarketValue",
                    NameKey = loc.key("Long market value"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = longMarketValue
                },
                new()
                {
                    Id = "buyingPower",
                    NameKey = loc.key("Buying power"),
                    DataType = ComparingType.Double,
                    FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                    Value = account.BuyingPower ?? 0d
                }
            }
        };
    }

    private static MessageCryptoAssetBalances CreateAssetBalance(BrokerAccountDto account)
    {
        var total = account.LiquidationValue ?? account.TotalCash ?? account.CashBalance ?? 0d;
        var available = account.CashAvailableForTrading ?? account.CashBalance ?? total;
        var reserved = Math.Max(total - available, 0d);

        return new MessageCryptoAssetBalances
        {
            AccountId = account.AccountHash,
            AssetId = "USD",
            TotalBalance = total,
            AvailableBalance = available,
            ReservedBalance = reserved,
            TotalInUSD = total
        };
    }

    private static MessageOpenPosition CreatePosition(BrokerPositionDto position)
    {
        var message = new MessageOpenPosition(position.Symbol)
        {
            AccountId = position.AccountHash,
            Side = position.Quantity >= 0 ? Side.Buy : Side.Sell,
            PositionId = $"{position.AccountHash}:{position.Symbol}",
            Quantity = Math.Abs(position.Quantity),
            OpenPrice = position.AveragePrice ?? 0d,
            OpenTime = DateTime.UtcNow,
            Comment = position.Description ?? position.AssetType ?? string.Empty
        };

        message.AdditionalInfoItems = new List<AdditionalInfoItem>
        {
            new()
            {
                Id = "marketValue",
                NameKey = loc.key("Market value"),
                DataType = ComparingType.Double,
                FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                Value = position.MarketValue ?? 0d
            },
            new()
            {
                Id = "marketPrice",
                NameKey = loc.key("Market price"),
                DataType = ComparingType.Double,
                Value = position.MarketPrice ?? 0d
            },
            new()
            {
                Id = "dayProfitLoss",
                NameKey = loc.key("Day P&L"),
                DataType = ComparingType.Double,
                FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                Value = position.DayProfitLoss ?? 0d
            },
            new()
            {
                Id = "dayProfitLossPercent",
                NameKey = loc.key("Day P&L %"),
                DataType = ComparingType.Double,
                Value = position.DayProfitLossPercent ?? 0d
            },
            new()
            {
                Id = "unrealizedProfitLoss",
                NameKey = loc.key("Unrealized P&L"),
                DataType = ComparingType.Double,
                FormatingType = AdditionalInfoItemFormatingType.AssetBalance,
                Value = position.UnrealizedProfitLoss ?? 0d
            },
            new()
            {
                Id = "assetType",
                NameKey = loc.key("Asset type"),
                Value = position.AssetType ?? string.Empty
            },
            new()
            {
                Id = "instrumentType",
                NameKey = loc.key("Instrument type"),
                Value = position.InstrumentType ?? string.Empty
            },
            new()
            {
                Id = "description",
                NameKey = loc.key("Description"),
                Value = position.Description ?? string.Empty
            }
        };

        return message;
    }

    private static MessageOpenOrder CreateOpenOrder(BrokerOrderDto order)
    {
        var message = new MessageOpenOrder(order.Symbol ?? string.Empty)
        {
            AccountId = order.AccountHash,
            OrderId = order.OrderId,
            Price = order.Price ?? double.NaN,
            OrderTypeId = string.Equals(order.OrderType, "LIMIT", StringComparison.OrdinalIgnoreCase)
                ? OrderType.Limit
                : OrderType.Market,
            Side = ResolveSide(order.Instruction),
            Status = ConvertOrderStatus(order.Status),
            TimeInForce = ConvertTimeInForce(order.Duration),
            TotalQuantity = order.Quantity ?? 0d,
            LastUpdateTime = order.EnteredTime?.UtcDateTime ?? DateTime.UtcNow,
            Comment = BuildOrderComment(order)
        };

        return message;
    }

    private static MessageOrderHistory CreateOrderHistory(BrokerOrderDto order)
    {
        return new MessageOrderHistory(order.Symbol ?? string.Empty)
        {
            AccountId = order.AccountHash,
            OrderId = order.OrderId,
            Price = order.Price ?? double.NaN,
            AverageFillPrice = order.AverageFillPrice ?? order.Price ?? double.NaN,
            OrderTypeId = string.Equals(order.OrderType, "LIMIT", StringComparison.OrdinalIgnoreCase)
                ? OrderType.Limit
                : OrderType.Market,
            Side = ResolveSide(order.Instruction),
            Status = ConvertOrderStatus(order.Status),
            TimeInForce = ConvertTimeInForce(order.Duration),
            TotalQuantity = order.Quantity ?? 0d,
            FilledQuantity = order.FilledQuantity ?? 0d,
            LastUpdateTime = order.CloseTime?.UtcDateTime ?? order.EnteredTime?.UtcDateTime ?? DateTime.UtcNow,
            OriginalStatus = order.Status,
            Comment = BuildOrderComment(order)
        };
    }

    private static MessageTrade CreateTrade(BrokerExecutionDto execution)
    {
        var grossPnl = new PnLItem
        {
            AssetID = "USD",
            Value = execution.GrossAmount ?? 0d
        };
        var fee = new PnLItem
        {
            AssetID = "USD",
            Value = execution.Fees ?? 0d
        };
        var net = new PnLItem
        {
            AssetID = "USD",
            Value = (execution.GrossAmount ?? 0d) - (execution.Fees ?? 0d)
        };

        return new MessageTrade
        {
            AccountId = execution.AccountHash,
            TradeId = execution.ExecutionId,
            SymbolId = execution.Symbol ?? string.Empty,
            OrderId = execution.OrderId,
            Price = execution.Price ?? 0d,
            Quantity = execution.Quantity ?? 0d,
            DateTime = execution.ExecutedTime?.UtcDateTime ?? DateTime.UtcNow,
            Side = ResolveSide(execution.Instruction),
            PositionImpactType = ConvertPositionImpactType(execution.PositionEffect),
            GrossPnl = grossPnl,
            Fee = fee,
            NetPnl = net,
            OrderTypeId = string.Empty,
            PositionId = $"{execution.AccountHash}:{execution.Symbol}",
            Comment = execution.ExecutionType ?? string.Empty
        };
    }

    private static OrderStatus ConvertOrderStatus(string? status) =>
        status?.ToUpperInvariant() switch
        {
            "FILLED" => OrderStatus.Filled,
            "CANCELED" or "CANCELLED" => OrderStatus.Cancelled,
            "REJECTED" => OrderStatus.Refused,
            "EXPIRED" => OrderStatus.Cancelled,
            "REPLACED" => OrderStatus.Cancelled,
            "PARTIAL_FILL" or "PARTIALLY_FILLED" => OrderStatus.PartiallyFilled,
            _ => OrderStatus.Opened
        };

    private static bool IsCancelableOrderStatus(string? status) =>
        status?.ToUpperInvariant() switch
        {
            "ACCEPTED" => true,
            "AWAITING_PARENT_ORDER" => true,
            "AWAITING_CONDITION" => true,
            "AWAITING_STOP_CONDITION" => true,
            "AWAITING_MANUAL_REVIEW" => true,
            "PENDING_ACTIVATION" => true,
            "QUEUED" => true,
            "WORKING" => true,
            "NEW" => true,
            "PARTIAL_FILL" => true,
            "PARTIALLY_FILLED" => true,
            _ => false
        };

    private static bool IsTerminalOrderStatus(string? status) =>
        status?.ToUpperInvariant() switch
        {
            "FILLED" => true,
            "CANCELED" => true,
            "CANCELLED" => true,
            "REJECTED" => true,
            "EXPIRED" => true,
            "REPLACED" => true,
            _ => false
        };

    private static bool IsAlreadyInactiveOrderError(Exception ex) =>
        ex.Message.Contains("not active/cancelable", StringComparison.OrdinalIgnoreCase) ||
        ex.Message.Contains("not active", StringComparison.OrdinalIgnoreCase);

    private static string GetOrderKey(BrokerOrderDto order) => $"{order.AccountHash}:{order.OrderId}";

    private static bool MatchesOrderHistoryRequest(BrokerOrderDto order, DateTime from, DateTime to, IEnumerable<string>? symbolIds)
    {
        if (string.IsNullOrWhiteSpace(order.Symbol))
            return false;

        if (!MatchesSymbolIds(order.Symbol, symbolIds))
            return false;

        var timestamp = order.CloseTime?.UtcDateTime ?? order.EnteredTime?.UtcDateTime ?? DateTime.MinValue;
        if (from != default && timestamp < from)
            return false;
        if (to != default && timestamp > to)
            return false;

        return true;
    }

    private static bool MatchesTradesHistoryRequest(BrokerExecutionDto execution, DateTime from, DateTime to, IEnumerable<string>? symbolIds)
    {
        if (string.IsNullOrWhiteSpace(execution.Symbol))
            return false;

        if (!MatchesSymbolIds(execution.Symbol, symbolIds))
            return false;

        var timestamp = execution.ExecutedTime?.UtcDateTime ?? DateTime.MinValue;
        if (from != default && timestamp < from)
            return false;
        if (to != default && timestamp > to)
            return false;

        return true;
    }

    private static bool MatchesSymbolIds(string symbol, IEnumerable<string>? symbolIds)
    {
        if (symbolIds == null)
            return true;

        var ids = symbolIds.Where(id => !string.IsNullOrWhiteSpace(id)).ToArray();
        if (ids.Length == 0)
            return true;

        return ids.Any(id => string.Equals(id, symbol, StringComparison.OrdinalIgnoreCase));
    }

    private static PositionImpactType ConvertPositionImpactType(string? positionEffect) =>
        positionEffect?.ToUpperInvariant() switch
        {
            "OPENING" => PositionImpactType.Open,
            "CLOSING" => PositionImpactType.Close,
            _ => PositionImpactType.Undefined
        };

    private static PositionImpactType DerivePositionImpactType(string? instruction) =>
        instruction?.ToUpperInvariant() switch
        {
            "BUY_TO_OPEN" => PositionImpactType.Open,
            "SELL_TO_OPEN" => PositionImpactType.Open,
            "BUY_TO_CLOSE" => PositionImpactType.Close,
            "SELL_TO_CLOSE" => PositionImpactType.Close,
            _ => PositionImpactType.Undefined
        };

    private static Side ResolveSide(string? instruction) =>
        instruction?.StartsWith("BUY", StringComparison.OrdinalIgnoreCase) == true
            ? Side.Buy
            : Side.Sell;

    private static TimeInForce ConvertTimeInForce(string? duration) =>
        duration?.ToUpperInvariant() switch
        {
            "GOOD_TILL_CANCEL" or "GTC" => TimeInForce.GTC,
            "FILL_OR_KILL" => TimeInForce.FOK,
            "IMMEDIATE_OR_CANCEL" => TimeInForce.IOC,
            _ => TimeInForce.Day
        };

    private static string? ConvertTimeInForce(TimeInForce timeInForce) =>
        timeInForce switch
        {
            TimeInForce.Day => "DAY",
            TimeInForce.GTC => "GTC",
            TimeInForce.FOK => "FOK",
            TimeInForce.IOC => "IOC",
            _ => null
        };

    private static string? BuildOrderComment(BrokerOrderDto order)
    {
        var instructionComment = BuildInstructionComment(order.Instruction);
        if (string.Equals(order.Session, "SEAMLESS", StringComparison.OrdinalIgnoreCase))
            return CombineComments(instructionComment, "Extended hours");

        if (string.Equals(order.Session, "AM", StringComparison.OrdinalIgnoreCase))
            return CombineComments(instructionComment, "AM session");

        if (string.Equals(order.Session, "PM", StringComparison.OrdinalIgnoreCase))
            return CombineComments(instructionComment, "PM session");

        return instructionComment;
    }

    private static string? BuildInstructionComment(string? instruction) =>
        instruction?.ToUpperInvariant() switch
        {
            "BUY_TO_OPEN" => "Buy to open",
            "BUY_TO_CLOSE" => "Buy to close",
            "SELL_TO_OPEN" => "Sell to open",
            "SELL_TO_CLOSE" => "Sell to close",
            _ => null
        };

    private static string? CombineComments(string? first, string? second)
    {
        if (string.IsNullOrWhiteSpace(first))
            return second;
        if (string.IsNullOrWhiteSpace(second))
            return first;

        return $"{first} | {second}";
    }

    private static string? ResolveOptimisticSession(TimeInForce timeInForce, string? currentSession)
    {
        if (timeInForce == TimeInForce.GTC)
            return "NORMAL";

        if (string.IsNullOrWhiteSpace(currentSession))
            return null;

        return currentSession;
    }

    private void PrimeHistorySymbols(IEnumerable<string?> symbols)
    {
        lock (this.syncRoot)
        {
            foreach (var rawSymbol in symbols)
            {
                if (string.IsNullOrWhiteSpace(rawSymbol))
                    continue;

                var symbol = rawSymbol.Trim().ToUpperInvariant();
                if (this.optionContractCache.ContainsKey(symbol))
                    continue;

                if (!TryParseOccOptionSymbol(symbol, out var parsed))
                    continue;

                var contract = new OptionContractDto
                {
                    Symbol = symbol,
                    UnderlierSymbol = parsed.Underlier,
                    Description = $"{parsed.Underlier} {parsed.ExpirationDate:yyyy-MM-dd} {(parsed.OptionType == "PUT" ? "Put" : "Call")} {parsed.StrikePrice:0.##}",
                    Exchange = OptionExchangeId,
                    OptionType = parsed.OptionType,
                    StrikePrice = parsed.StrikePrice,
                    ExpirationDate = parsed.ExpirationDate
                };

                this.CacheOptionContract(contract);
            }
        }
    }

    private static DateTime NormalizeHistoryBoundary(DateTime value, bool isUpperBound)
    {
        if (value == default)
            return value;

        if (isUpperBound && value.TimeOfDay == TimeSpan.Zero)
            return value.Date.AddDays(1).AddTicks(-1);

        return value;
    }

    private static bool TryParseOccOptionSymbol(string symbol, out ParsedOccOption parsed)
    {
        parsed = default!;
        var trimmed = symbol.Trim().ToUpperInvariant();
        if (trimmed.Length < 16)
            return false;

        var spaceIndex = trimmed.IndexOf(' ');
        if (spaceIndex <= 0 || spaceIndex >= trimmed.Length - 15)
            return false;

        var underlier = trimmed[..spaceIndex].Trim();
        var contractPart = trimmed[(spaceIndex + 1)..].Trim();
        if (contractPart.Length != 15)
            return false;

        var datePart = contractPart[..6];
        var optionType = contractPart[6];
        var strikePart = contractPart[7..];

        if (!DateTime.TryParseExact(datePart, "yyMMdd", null, System.Globalization.DateTimeStyles.None, out var expirationDate))
            return false;

        if (optionType is not ('C' or 'P'))
            return false;

        if (!int.TryParse(strikePart, out var strikeRaw))
            return false;

        parsed = new ParsedOccOption
        {
            Underlier = underlier,
            ExpirationDate = expirationDate,
            OptionType = optionType == 'P' ? "PUT" : "CALL",
            StrikePrice = strikeRaw / 1000d
        };
        return true;
    }

    private static BrokerOrderDto CreateOptimisticOrder(
        string accountHash,
        string orderId,
        string symbol,
        string instruction,
        string orderType,
        double price,
        double quantity,
        TimeInForce timeInForce,
        string? session) =>
        new()
        {
            AccountHash = accountHash,
            OrderId = orderId,
            Symbol = symbol,
            Instruction = instruction,
            OrderType = orderType,
            Status = "WORKING",
            Duration = ConvertTimeInForce(timeInForce),
            Session = session,
            EnteredTime = DateTimeOffset.UtcNow,
            Quantity = quantity,
            Price = price
        };

    private void PushOptimisticOpenOrder(BrokerOrderDto order)
    {
        var key = GetOrderKey(order);
        lock (this.syncRoot)
        {
            this.orderCache[key] = order;
            this.orderStatusCache[key] = order.Status;
            this.closedOrderMessagesPushed.Remove(key);
        }

        this.PushMessage(CreateOpenOrder(order));
    }

    private void PushOptimisticCloseOrder(string accountHash, string orderId, string status)
    {
        var key = $"{accountHash}:{orderId}";
        lock (this.syncRoot)
        {
            this.orderCache.Remove(key);
            this.orderStatusCache[key] = status;
            this.closedOrderMessagesPushed.Add(key);
        }

        this.PushMessage(new MessageCloseOrder { OrderId = orderId });
    }

    private bool TryGetCachedOrder(ModifyOrderRequestParameters parameters, out BrokerOrderDto order)
    {
        lock (this.syncRoot)
        {
            var match = this.orderCache.Values.FirstOrDefault(o =>
                string.Equals(o.OrderId, parameters.OrderId, StringComparison.OrdinalIgnoreCase));

            if (match != null)
            {
                order = match;
                return true;
            }
        }

        order = null!;
        return false;
    }

    private sealed class SymbolStreamState
    {
        public SymbolStreamState(string symbol)
        {
            this.Symbol = symbol;
            this.Cancellation = new CancellationTokenSource();
        }

        public string Symbol { get; }

        public ClientWebSocket? Socket { get; set; }

        public CancellationTokenSource Cancellation { get; }

        public Task? Task { get; set; }

        public CancellationTokenSource? StopDebounceCancellation { get; set; }

        public Task? StopDebounceTask { get; set; }
    }

    private sealed class CachedDomState
    {
        private const double PriceTolerance = 0.0000001d;

        public DateTime TimestampUtc { get; init; }

        public bool IsRealBook { get; init; }

        public List<CachedDomLevel> Bids { get; init; } = new();

        public List<CachedDomLevel> Asks { get; init; } = new();

        public static CachedDomState FromDomQuote(DOMQuote dom, bool isRealBook) =>
            new()
            {
                TimestampUtc = dom.Time,
                IsRealBook = isRealBook,
                Bids = dom.Bids
                    .Select(level => new CachedDomLevel
                    {
                        Price = level.Price,
                        Size = level.Size,
                        NumberOrders = level.NumberOrders,
                        Closed = level.Closed
                    })
                    .ToList(),
                Asks = dom.Asks
                    .Select(level => new CachedDomLevel
                    {
                        Price = level.Price,
                        Size = level.Size,
                        NumberOrders = level.NumberOrders,
                        Closed = level.Closed
                    })
                    .ToList()
            };

        public static CachedDomState MergeBookUpdate(CachedDomState? existing, DOMQuote dom, bool isRealBook)
        {
            var bidMap = existing?.Bids.ToDictionary(level => level.Price) ?? new Dictionary<double, CachedDomLevel>();
            var askMap = existing?.Asks.ToDictionary(level => level.Price) ?? new Dictionary<double, CachedDomLevel>();

            ApplyUpdates(
                bidMap,
                dom.Bids.Select(level => new CachedDomLevel
                {
                    Price = level.Price,
                    Size = level.Size,
                    NumberOrders = level.NumberOrders,
                    Closed = level.Closed || level.Size <= 0
                }));

            ApplyUpdates(
                askMap,
                dom.Asks.Select(level => new CachedDomLevel
                {
                    Price = level.Price,
                    Size = level.Size,
                    NumberOrders = level.NumberOrders,
                    Closed = level.Closed || level.Size <= 0
                }));

            return new CachedDomState
            {
                TimestampUtc = dom.Time,
                IsRealBook = isRealBook || existing?.IsRealBook == true,
                Bids = bidMap.Values.OrderByDescending(level => level.Price).ToList(),
                Asks = askMap.Values.OrderBy(level => level.Price).ToList()
            };
        }

        public static CachedDomState MergeVenueStates(IEnumerable<CachedDomState> states)
        {
            var stateList = states.ToList();
            var bidMap = new Dictionary<double, CachedDomLevel>();
            var askMap = new Dictionary<double, CachedDomLevel>();

            foreach (var state in stateList)
            {
                MergeSide(bidMap, state.Bids);
                MergeSide(askMap, state.Asks);
            }

            return new CachedDomState
            {
                TimestampUtc = stateList.Count == 0 ? DateTime.UtcNow : stateList.Max(state => state.TimestampUtc),
                IsRealBook = stateList.Any(state => state.IsRealBook),
                Bids = bidMap.Values.OrderByDescending(level => level.Price).ToList(),
                Asks = askMap.Values.OrderBy(level => level.Price).ToList()
            };
        }

        public static CachedDomState ClampToNbbo(CachedDomState state, double bestBid, double bestAsk)
        {
            var bids = state.Bids;
            var asks = state.Asks;

            if (bestBid > 0)
            {
                bids = bids
                    .Where(level => level.Price <= bestBid + PriceTolerance)
                    .ToList();
            }

            if (bestAsk > 0)
            {
                asks = asks
                    .Where(level => level.Price >= bestAsk - PriceTolerance)
                    .ToList();
            }

            return new CachedDomState
            {
                TimestampUtc = state.TimestampUtc,
                IsRealBook = state.IsRealBook,
                Bids = bids,
                Asks = asks
            };
        }

        public DOMQuote ToDomQuote(string symbol)
        {
            var dom = new DOMQuote(symbol, this.TimestampUtc);
            foreach (var bid in this.Bids)
            {
                dom.Bids.Add(new Level2Quote(QuotePriceType.Bid, symbol, $"B_{bid.Price:0.####}", bid.Price, bid.Size, this.TimestampUtc)
                {
                    Closed = bid.Closed,
                    NumberOrders = bid.NumberOrders
                });
            }

            foreach (var ask in this.Asks)
            {
                dom.Asks.Add(new Level2Quote(QuotePriceType.Ask, symbol, $"A_{ask.Price:0.####}", ask.Price, ask.Size, this.TimestampUtc)
                {
                    Closed = ask.Closed,
                    NumberOrders = ask.NumberOrders
                });
            }

            return dom;
        }

        private static void ApplyUpdates(Dictionary<double, CachedDomLevel> target, IEnumerable<CachedDomLevel> updates)
        {
            foreach (var update in updates)
            {
                if (update.Closed || update.Size <= 0)
                {
                    target.Remove(update.Price);
                    continue;
                }

                target[update.Price] = update with { Closed = false };
            }
        }

        private static void MergeSide(Dictionary<double, CachedDomLevel> target, IEnumerable<CachedDomLevel> levels)
        {
            foreach (var level in levels)
            {
                if (level.Closed || level.Size <= 0)
                    continue;

                if (target.TryGetValue(level.Price, out var existing))
                {
                    target[level.Price] = new CachedDomLevel
                    {
                        Price = level.Price,
                        Size = existing.Size + level.Size,
                        NumberOrders = existing.NumberOrders + level.NumberOrders,
                        Closed = false
                    };
                    continue;
                }

                target[level.Price] = level with { Closed = false };
            }
        }
    }

    private sealed record CachedDomLevel
    {
        public double Price { get; init; }

        public double Size { get; init; }

        public int NumberOrders { get; init; }

        public bool Closed { get; init; }
    }

    private sealed record ParsedOccOption
    {
        public string Underlier { get; init; } = string.Empty;

        public DateTime ExpirationDate { get; init; }

        public string OptionType { get; init; } = string.Empty;

        public double StrikePrice { get; init; }
    }
}
