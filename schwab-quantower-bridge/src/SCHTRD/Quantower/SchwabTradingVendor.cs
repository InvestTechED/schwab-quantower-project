using System.Collections;
using System.Reflection;
using SchwabQuantowerBridge.Models;
using SchwabQuantowerBridge.Services;
using TradingPlatform.BusinessLayer;
using TradingPlatform.BusinessLayer.Integration;

namespace SchwabQuantowerBridge.Quantower;

internal sealed class SchwabTradingVendor : Vendor
{
    private const string ExchangeId = "Composite";
    private const string EquitySessionsContainerId = "US_EQUITIES_ETH";
    private const double EquityLotSize = 100d;
    private const string ExcludedSchwabAccountNumberSuffix = "6462";
    private static readonly TimeSpan OrderPollingInterval = TimeSpan.FromSeconds(1);
    private static readonly int[] ActionRefreshScheduleMilliseconds = [100, 600, 1500];
    private readonly HttpClient httpClient = new();
    private readonly Dictionary<string, BrokerOrderDto> orderCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BrokerPositionDto> positionCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> excludedAccountIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> closedPositionMessagesPushed = new(StringComparer.OrdinalIgnoreCase);
    private readonly object syncRoot = new();
    private SchwabTradingBackendClient? backendClient;
    private CancellationTokenSource? orderPollingCts;
    private bool connected;

    public override ConnectionResult Connect(ConnectRequestParameters connectRequestParameters)
    {
        this.backendClient = new SchwabTradingBackendClient(this.httpClient);

        try
        {
            var healthy = this.backendClient.PingAsync(connectRequestParameters.CancellationToken).GetAwaiter().GetResult();
            if (!healthy)
                return ConnectionResult.CreateFail("Local Schwab backend is unavailable.");

            this.RefreshAccountFilter(connectRequestParameters.CancellationToken);
            this.connected = true;
            this.StartOrderPolling();
            return ConnectionResult.CreateSuccess();
        }
        catch (Exception ex)
        {
            return ConnectionResult.CreateFail(ex.Message);
        }
    }

    public override void Disconnect()
    {
        lock (this.syncRoot)
        {
            this.orderCache.Clear();
            this.positionCache.Clear();
            this.excludedAccountIds.Clear();
            this.closedPositionMessagesPushed.Clear();
            this.connected = false;
        }

        this.StopOrderPolling();
        base.Disconnect();
    }

    public override void OnConnected(CancellationToken token)
    {
        base.OnConnected(token);
        this.PushAccountSnapshot(token);
        this.StartOrderPolling();
    }

    public override PingResult Ping() => new()
    {
        State = this.connected ? PingEnum.Connected : PingEnum.Disconnected
    };

    public override IList<MessageRule> GetRules(CancellationToken token)
    {
        var rules = base.GetRules(token);
        rules.Add(new MessageRule { Name = Rule.ALLOW_TRADING, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_SL, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_TP, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_ORDER, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_PRICE, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_AMOUNT, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_TIF, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_MODIFY_ORDER_TYPE, Value = false });
        rules.Add(new MessageRule { Name = Rule.PLACE_ORDER_TRADING_OPERATION_HAS_ORDER_ID, Value = true });
        rules.Add(new MessageRule { Name = Rule.ALLOW_SCREENER, Value = false });
        rules.Add(new MessageRule { Name = Rule.ALLOW_CONTAINS_SCREENER_CONDITIONS, Value = false });
        return rules;
    }

    public override IList<MessageAccount> GetAccounts(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return [];

        return this.GetVisibleBrokerAccounts(token).Select(CreateAccount).ToList();
    }

    public override IList<MessageCryptoAssetBalances> GetCryptoAssetBalances(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return [];

        return this.GetVisibleBrokerAccounts(token).Select(CreateAssetBalance).ToList();
    }

    public override IList<MessageOpenPosition> GetPositions(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return [];

        var positions = client.GetPositionsAsync(token)
            .GetAwaiter()
            .GetResult()
            .Where(p => this.IsVisibleAccountHash(p.AccountHash) && !string.IsNullOrWhiteSpace(p.Symbol) && Math.Abs(p.Quantity) > 0)
            .ToList();

        lock (this.syncRoot)
        {
            this.positionCache.Clear();
            foreach (var position in positions)
                this.positionCache[$"{position.AccountHash}:{NormalizeSymbolKey(position.Symbol)}"] = position;
        }

        return positions.Select(CreatePosition).ToList();
    }

    public override IList<MessageOpenOrder> GetPendingOrders(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return [];

        var orders = this.FilterVisibleOrders(client.GetOrdersAsync(token).GetAwaiter().GetResult());
        this.UpdateOrderCache(orders);

        return orders
            .Where(o => !string.IsNullOrWhiteSpace(o.Symbol) && IsCancelableOrderStatus(o.Status))
            .Select(CreateOpenOrder)
            .ToList();
    }

    private void PushAccountSnapshot(CancellationToken token)
    {
        foreach (var account in this.GetAccounts(token))
            this.PushMessage(account);
        foreach (var balance in this.GetCryptoAssetBalances(token))
            this.PushMessage(balance);
        foreach (var position in this.GetPositions(token))
            this.PushMessage(position);
        foreach (var order in this.GetPendingOrders(token))
            this.PushMessage(order);
    }

    private void RefreshAccountFilter(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return;

        this.UpdateAccountFilter(client.GetAccountsAsync(token).GetAwaiter().GetResult());
    }

    private List<BrokerAccountDto> GetVisibleBrokerAccounts(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return [];

        var accounts = client.GetAccountsAsync(token).GetAwaiter().GetResult();
        this.UpdateAccountFilter(accounts);
        return accounts.Where(IsVisibleAccount).ToList();
    }

    private void UpdateAccountFilter(IEnumerable<BrokerAccountDto> accounts)
    {
        lock (this.syncRoot)
        {
            this.excludedAccountIds.Clear();
            foreach (var account in accounts.Where(a => IsExcludedSchwabAccountNumber(a.AccountNumber)))
            {
                if (!string.IsNullOrWhiteSpace(account.AccountHash))
                    this.excludedAccountIds.Add(account.AccountHash);
            }
        }
    }

    private static bool IsVisibleAccount(BrokerAccountDto account) =>
        !IsExcludedSchwabAccountNumber(account.AccountNumber);

    private static bool IsExcludedSchwabAccountNumber(string? accountNumber) =>
        !string.IsNullOrWhiteSpace(accountNumber) &&
        accountNumber.Trim().EndsWith(ExcludedSchwabAccountNumberSuffix, StringComparison.OrdinalIgnoreCase);

    private bool IsVisibleAccountHash(string? accountHash)
    {
        if (string.IsNullOrWhiteSpace(accountHash))
            return false;

        lock (this.syncRoot)
            return !this.excludedAccountIds.Contains(accountHash);
    }

    private List<BrokerOrderDto> FilterVisibleOrders(IEnumerable<BrokerOrderDto> orders) =>
        orders.Where(o => this.IsVisibleAccountHash(o.AccountHash)).ToList();

    private List<BrokerPositionDto> FilterVisiblePositions(IEnumerable<BrokerPositionDto> positions) =>
        positions.Where(p => this.IsVisibleAccountHash(p.AccountHash)).ToList();

    private void StartOrderPolling()
    {
        if (this.orderPollingCts != null)
            return;

        var client = this.backendClient;
        if (client == null)
            return;

        var cts = new CancellationTokenSource();
        this.orderPollingCts = cts;
        _ = Task.Run(() => this.PollOrdersAsync(client, cts.Token), cts.Token);
    }

    private void StopOrderPolling()
    {
        var cts = this.orderPollingCts;
        this.orderPollingCts = null;
        if (cts == null)
            return;

        cts.Cancel();
        cts.Dispose();
    }

    private async Task PollOrdersAsync(SchwabTradingBackendClient client, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                var orders = this.FilterVisibleOrders(await client.GetOrdersAsync(token).ConfigureAwait(false));
                if (this.PublishOrderChanges(orders))
                {
                    await this.RefreshPositionsAsync(token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Keep the connector alive; the next poll will retry account/trading state.
            }

            try
            {
                await Task.Delay(OrderPollingInterval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private bool PublishOrderChanges(IReadOnlyList<BrokerOrderDto> orders)
    {
        var openOrders = orders
            .Where(o => !string.IsNullOrWhiteSpace(o.OrderId) && !string.IsNullOrWhiteSpace(o.Symbol) && IsCancelableOrderStatus(o.Status))
            .ToList();
        Dictionary<string, BrokerOrderDto> previous;
        var shouldRefreshPositions = false;

        lock (this.syncRoot)
        {
            previous = new Dictionary<string, BrokerOrderDto>(this.orderCache, StringComparer.OrdinalIgnoreCase);
            this.UpdateOrderCacheCore(openOrders);
        }

        foreach (var order in openOrders)
        {
            var isNewOrder = !previous.TryGetValue(order.OrderId!, out var previousOrder);
            var hasOrderChanged = !isNewOrder && HasOrderChanged(previousOrder!, order);

            if (isNewOrder || hasOrderChanged)
            {
                this.PushMessage(CreateOpenOrder(order));

                if (!isNewOrder && previousOrder != null && HasFillStateChanged(previousOrder, order))
                    shouldRefreshPositions = true;
            }
        }

        foreach (var oldOrderId in previous.Keys.Except(openOrders.Select(o => o.OrderId!), StringComparer.OrdinalIgnoreCase))
        {
            this.PushMessage(new MessageCloseOrder { OrderId = oldOrderId });
            shouldRefreshPositions = true;
        }

        return shouldRefreshPositions;
    }

    private static bool HasOrderChanged(BrokerOrderDto previous, BrokerOrderDto current) =>
        ConvertOrderStatus(previous.Status) != ConvertOrderStatus(current.Status) ||
        !string.Equals(NormalizeSymbolKey(previous.Symbol ?? string.Empty), NormalizeSymbolKey(current.Symbol ?? string.Empty), StringComparison.OrdinalIgnoreCase) ||
        ConvertInstructionSide(previous.Instruction) != ConvertInstructionSide(current.Instruction) ||
        !string.Equals(ConvertSchwabOrderType(previous.OrderType), ConvertSchwabOrderType(current.OrderType), StringComparison.OrdinalIgnoreCase) ||
        ConvertTimeInForce(previous.Duration) != ConvertTimeInForce(current.Duration) ||
        Math.Abs((previous.Quantity ?? 0d) - (current.Quantity ?? 0d)) > 0.000001 ||
        Math.Abs((previous.FilledQuantity ?? 0d) - (current.FilledQuantity ?? 0d)) > 0.000001 ||
        Math.Abs((previous.RemainingQuantity ?? 0d) - (current.RemainingQuantity ?? 0d)) > 0.000001 ||
        Math.Abs((previous.Price ?? 0d) - (current.Price ?? 0d)) > 0.000001 ||
        Math.Abs((previous.StopPrice ?? previous.TriggerPrice ?? 0d) - (current.StopPrice ?? current.TriggerPrice ?? 0d)) > 0.000001 ||
        !string.Equals(ResolveOrderGroupId(previous), ResolveOrderGroupId(current), StringComparison.OrdinalIgnoreCase) ||
        !string.Equals(ResolveOpenOrderPositionId(previous), ResolveOpenOrderPositionId(current), StringComparison.OrdinalIgnoreCase);

    private static bool HasFillStateChanged(BrokerOrderDto previous, BrokerOrderDto current) =>
        Math.Abs((previous.FilledQuantity ?? 0d) - (current.FilledQuantity ?? 0d)) > 0.000001 ||
        Math.Abs((previous.RemainingQuantity ?? 0d) - (current.RemainingQuantity ?? 0d)) > 0.000001 ||
        IsTerminalOrderStatus(current.Status);

    public override IList<OrderType> GetAllowedOrderTypes(CancellationToken token) =>
    [
        new MarketOrderType(TimeInForce.Day, TimeInForce.GTC),
        new LimitOrderType(TimeInForce.Day, TimeInForce.GTC),
        new StopOrderType(TimeInForce.Day, TimeInForce.GTC),
        new StopLimitOrderType(TimeInForce.Day, TimeInForce.GTC)
    ];

    public override TradingOperationResult PlaceOrder(PlaceOrderRequestParameters parameters)
    {
        var client = this.backendClient;
        if (client == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");
        if (!IsSupportedPlaceOrderType(parameters.OrderTypeId))
            return TradingOperationResult.CreateError(parameters.RequestId, "SCH TRD supports MARKET, LIMIT, STOP, and STOP_LIMIT orders only.");
        if (RequiresLimitPrice(parameters.OrderTypeId) && (double.IsNaN(parameters.Price) || parameters.Price <= 0))
            return TradingOperationResult.CreateError(parameters.RequestId, "Limit price is required.");
        var stopPrice = ResolveStopPrice(parameters);
        if (RequiresStopPrice(parameters.OrderTypeId) && stopPrice <= 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "Stop trigger price is required.");
        var shareQuantity = ToBrokerShareQuantity(parameters.Quantity);
        if (shareQuantity <= 0 || shareQuantity % 1 != 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "SCH TRD allows whole-share orders only.");

        try
        {
            var instruction = this.ResolveInstruction(parameters.Account.Id, parameters.Symbol.Id, parameters.Side, parameters.CancellationToken);
            var orderType = ConvertOrderTypeId(parameters.OrderTypeId);
            var result = client.PlaceOrderAsync(
                    new PlaceBrokerOrderRequestDto
                    {
                        AccountHash = parameters.Account.Id,
                        Symbol = parameters.Symbol.Id,
                        Quantity = shareQuantity,
                        Instruction = instruction,
                        OrderType = orderType,
                        LimitPrice = RequiresLimitPrice(parameters.OrderTypeId) ? parameters.Price : null,
                        StopPrice = RequiresStopPrice(parameters.OrderTypeId) ? stopPrice : null,
                        TimeInForce = ConvertTimeInForce(parameters.TimeInForce)
                    },
                    parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            if (!string.IsNullOrWhiteSpace(result?.OrderId))
            {
                this.PublishImmediateOpenOrder(CreateImmediateOrder(
                    parameters.Account.Id,
                    result.OrderId,
                    parameters.Symbol.Id,
                    instruction,
                    orderType,
                    RequiresLimitPrice(parameters.OrderTypeId) ? parameters.Price : null,
                    RequiresStopPrice(parameters.OrderTypeId) ? stopPrice : null,
                    shareQuantity,
                    ConvertTimeInForce(parameters.TimeInForce)));
                this.RefreshOrdersInBackground();
            }

            return TradingOperationResult.CreateSuccess(parameters.RequestId, result?.OrderId);
        }
        catch (Exception ex)
        {
            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    public override TradingOperationResult ModifyOrder(ModifyOrderRequestParameters parameters)
    {
        var client = this.backendClient;
        if (client == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");
        if (!IsSupportedPlaceOrderType(parameters.OrderTypeId))
            return TradingOperationResult.CreateError(parameters.RequestId, "SCH TRD supports MARKET, LIMIT, STOP, and STOP_LIMIT order modification only.");
        if (RequiresLimitPrice(parameters.OrderTypeId) && (double.IsNaN(parameters.Price) || parameters.Price <= 0))
            return TradingOperationResult.CreateError(parameters.RequestId, "A valid limit price is required.");
        var stopPrice = ResolveStopPrice(parameters);
        if (RequiresStopPrice(parameters.OrderTypeId) && stopPrice <= 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "A valid stop trigger price is required.");
        var shareQuantity = ToBrokerShareQuantity(parameters.Quantity);
        if (shareQuantity <= 0 || shareQuantity % 1 != 0)
            return TradingOperationResult.CreateError(parameters.RequestId, "SCH TRD allows whole-share quantity changes only.");

        if (!this.TryGetCachedOrder(parameters.OrderId, out var currentOrder))
            return TradingOperationResult.CreateError(parameters.RequestId, $"Order {parameters.OrderId} is not available in SCH TRD.");

        try
        {
            var orderType = ConvertOrderTypeId(parameters.OrderTypeId);
            var result = client.ModifyOrderAsync(
                    new ModifyBrokerOrderRequestDto
                    {
                        AccountHash = currentOrder.AccountHash,
                        OrderId = currentOrder.OrderId,
                        Symbol = currentOrder.Symbol ?? parameters.SymbolId,
                        Quantity = shareQuantity,
                        Instruction = currentOrder.Instruction ?? (parameters.Side == Side.Buy ? "BUY" : "SELL"),
                        OrderType = orderType,
                        LimitPrice = RequiresLimitPrice(parameters.OrderTypeId) ? parameters.Price : null,
                        StopPrice = RequiresStopPrice(parameters.OrderTypeId) ? stopPrice : null,
                        TimeInForce = ConvertTimeInForce(parameters.TimeInForce)
                    },
                    parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            var replacementOrderId = string.IsNullOrWhiteSpace(result?.OrderId) ? parameters.OrderId : result.OrderId;
            this.PublishImmediateReplacement(
                currentOrder,
                replacementOrderId,
                orderType,
                RequiresLimitPrice(parameters.OrderTypeId) ? parameters.Price : null,
                RequiresStopPrice(parameters.OrderTypeId) ? stopPrice : null,
                shareQuantity,
                ConvertTimeInForce(parameters.TimeInForce));
            this.RefreshOrdersInBackground();

            return TradingOperationResult.CreateSuccess(parameters.RequestId, replacementOrderId);
        }
        catch (Exception ex)
        {
            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    public override TradingOperationResult CancelOrder(CancelOrderRequestParameters parameters)
    {
        var client = this.backendClient;
        if (client == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");

        try
        {
            client.CancelOrderAsync(parameters.Order.Account.Id, parameters.Order.Id, parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();
            this.PushMessage(new MessageCloseOrder { OrderId = parameters.Order.Id });
            this.RefreshOrdersInBackground();
            return TradingOperationResult.CreateSuccess(parameters.RequestId, parameters.Order.Id);
        }
        catch (Exception ex)
        {
            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    public override TradingOperationResult ClosePosition(ClosePositionRequestParameters parameters)
    {
        var client = this.backendClient;
        if (client == null)
            return TradingOperationResult.CreateError(parameters.RequestId, "Schwab backend is not connected.");

        try
        {
            var requestPosition = ResolveObjectProperty(parameters, "Position");
            var accountId = ResolveStringProperty(parameters, "Account") ??
                ResolveStringProperty(parameters, "AccountId") ??
                ResolveStringProperty(requestPosition, "Account");
            var positionId = ResolveStringProperty(parameters, "PositionId") ??
                ResolveStringProperty(requestPosition, "Id");
            var symbolId = ResolveStringProperty(parameters, "Symbol") ??
                ResolveStringProperty(parameters, "SymbolId") ??
                ResolveStringProperty(requestPosition, "Symbol");
            var closeQuantity = ResolveDoubleProperty(parameters, "CloseQuantity");

            if (string.IsNullOrWhiteSpace(positionId) && string.IsNullOrWhiteSpace(symbolId))
                return TradingOperationResult.CreateError(parameters.RequestId, "SCH TRD close-position requests require a position or symbol.");

            var positions = this.FilterVisiblePositions(client.GetPositionsAsync(parameters.CancellationToken)
                .GetAwaiter()
                .GetResult());

            var hasPositionId = !string.IsNullOrWhiteSpace(positionId);
            var hasSymbolId = !string.IsNullOrWhiteSpace(symbolId);
            var position = positions.FirstOrDefault(p =>
                (string.IsNullOrWhiteSpace(accountId) || string.Equals(p.AccountHash, accountId, StringComparison.OrdinalIgnoreCase)) &&
                ((hasPositionId && string.Equals($"{p.AccountHash}:{NormalizeSymbolKey(p.Symbol)}", positionId, StringComparison.OrdinalIgnoreCase)) ||
                 (hasSymbolId && !string.IsNullOrWhiteSpace(symbolId) && string.Equals(NormalizeSymbolKey(p.Symbol), NormalizeSymbolKey(symbolId), StringComparison.OrdinalIgnoreCase))));

            if (position == null || string.IsNullOrWhiteSpace(position.Symbol) || Math.Abs(position.Quantity) <= 0)
                return TradingOperationResult.CreateError(parameters.RequestId, "No matching Schwab position was found to close.");

            var limitPrice = ResolveCloseLimitPriceFromMappedQuote(parameters, position.Quantity > 0);
            if (limitPrice <= 0d)
            {
                var requiredSide = position.Quantity > 0 ? "Ask" : "Bid";
                return TradingOperationResult.CreateError(parameters.RequestId, $"SCH TRD close-position requires a mapped dxFeed {requiredSide} quote.");
            }

            var closeShareQuantity = closeQuantity > 0 ? ToBrokerShareQuantity(closeQuantity) : 0d;
            var quantityToClose = closeShareQuantity > 0
                ? Math.Min(Math.Abs(position.Quantity), closeShareQuantity)
                : Math.Abs(position.Quantity);
            if (quantityToClose <= 0 || quantityToClose % 1 != 0)
                return TradingOperationResult.CreateError(parameters.RequestId, "SCH TRD allows whole-share close orders only.");

            var result = client.PlaceOrderAsync(
                    new PlaceBrokerOrderRequestDto
                    {
                        AccountHash = position.AccountHash,
                        Symbol = NormalizeSymbolKey(position.Symbol),
                        Quantity = quantityToClose,
                        Instruction = position.Quantity > 0 ? "SELL" : "BUY_TO_COVER",
                        OrderType = "LIMIT",
                        LimitPrice = limitPrice,
                        TimeInForce = ConvertTimeInForce(ResolveTimeInForceProperty(parameters, "TimeInForce"))
                    },
                    parameters.CancellationToken)
                .GetAwaiter()
                .GetResult();

            return TradingOperationResult.CreateSuccess(parameters.RequestId, result?.OrderId);
        }
        catch (Exception ex)
        {
            return TradingOperationResult.CreateError(parameters.RequestId, ex.Message);
        }
    }

    public override PnL CalculatePnL(PnLRequestParameters parameters)
    {
        var symbol = NormalizeSymbolKey(parameters.Symbol?.Id ?? string.Empty);
        if (string.IsNullOrWhiteSpace(symbol) || parameters.ClosePrice <= 0)
            return base.CalculatePnL(parameters);

        var openPrice = parameters.OpenPrice;
        var side = parameters.Side;
        BrokerPositionDto? cachedPosition = null;

        if (this.TryResolvePositionForPnl(symbol, out var resolvedPosition))
        {
            cachedPosition = resolvedPosition;
            if (openPrice <= 0)
                openPrice = cachedPosition.AveragePrice ?? 0d;
            side = cachedPosition.Quantity >= 0 ? Side.Buy : Side.Sell;
        }

        var shareQuantity = cachedPosition != null
            ? Math.Abs(cachedPosition.Quantity)
            : Math.Abs(ToBrokerShareQuantity(parameters.Quantity));

        if (openPrice <= 0 || shareQuantity <= 0)
            return base.CalculatePnL(parameters);

        var isShort = side == Side.Sell;
        var priceDifference = isShort ? openPrice - parameters.ClosePrice : parameters.ClosePrice - openPrice;
        var value = priceDifference * shareQuantity;
        var percent = isShort
            ? (openPrice - parameters.ClosePrice) / openPrice
            : (parameters.ClosePrice / openPrice) - 1d;
        var item = new PnLItem { AssetID = "USD", Value = value, ValuePercent = percent };
        return new PnL { GrossPnL = item, NetPnL = item };
    }

    public override MessageSymbolTypes GetSymbolTypes(CancellationToken token) => new()
    {
        SymbolTypes = [SymbolType.Equities, SymbolType.ETF]
    };

    public override IList<MessageAsset> GetAssets(CancellationToken token) =>
    [
        new MessageAsset { Id = "USD", Name = "USD", MinimumChange = 0.01 }
    ];

    public override IList<MessageExchange> GetExchanges(CancellationToken token) =>
    [
        new MessageExchange { Id = ExchangeId, ExchangeName = "Composite", SessionsContainerId = EquitySessionsContainerId }
    ];

    public override IList<MessageSessionsContainer> GetSessions(CancellationToken token) =>
    [
        new MessageSessionsContainer
        {
            Id = EquitySessionsContainerId,
            Name = "US ETH",
            Description = "US equities extended trading hours",
            Holidays = [new HolidayInfo { Date = new DateTime(2026, 6, 19), Name = string.Empty }],
            SessionsSets =
            [
                new SessionsSet
                {
                    Days =
                    [
                        DayOfWeek.Monday,
                        DayOfWeek.Tuesday,
                        DayOfWeek.Wednesday,
                        DayOfWeek.Thursday,
                        DayOfWeek.Friday
                    ],
                    Sessions =
                    [
                        new Session("US ETH", TimeSpan.FromHours(8), new TimeSpan(13, 30, 0), SessionType.PreMarket, false),
                        new Session("US ETH", new TimeSpan(13, 30, 0), TimeSpan.FromHours(20), SessionType.Main, true),
                        new Session("US ETH", TimeSpan.FromHours(20), TimeSpan.FromHours(24), SessionType.PostMarket, false)
                    ]
                }
            ]
        }
    ];

    public override IList<MessageSymbol> GetSymbols(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return [];

        return client.GetPositionsAsync(token)
            .GetAwaiter()
            .GetResult()
            .Where(p => this.IsVisibleAccountHash(p.AccountHash))
            .Where(p => !string.IsNullOrWhiteSpace(p.Symbol))
            .Select(p => CreateMessageSymbol(p.Symbol))
            .GroupBy(s => s.Id, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();
    }

    public override bool AllowNonFixedList => true;

    public override MessageSymbol GetNonFixedSymbol(GetSymbolRequestParameters requestParameters)
    {
        return string.IsNullOrWhiteSpace(requestParameters.SymbolId)
            ? base.GetNonFixedSymbol(requestParameters)
            : CreateMessageSymbol(requestParameters.SymbolId);
    }

    public override IList<MessageSymbolInfo> SearchSymbols(SearchSymbolsRequestParameters requestParameters)
    {
        return string.IsNullOrWhiteSpace(requestParameters.FilterName)
            ? []
            : [CreateMessageSymbol(requestParameters.FilterName)];
    }

    public override IList<MessageOptionSerie> GetAllOptionSeries(CancellationToken token) => [];

    public override IList<MessageOptionSerie> GetOptionSeries(GetOptionSeriesRequestParameters requestParameters) => [];

    public override IList<MessageSymbolInfo> GetStrikes(GetStrikesRequestParameters requestParameters) => [];

    public override void SubscribeSymbol(SubscribeQuotesParameters parameters)
    {
    }

    public override void UnSubscribeSymbol(SubscribeQuotesParameters parameters)
    {
    }

    public override HistoryMetadata GetHistoryMetadata(CancellationToken cancelationToken) => CreateNoHistoryMetadata();

    public override IList<IHistoryItem> LoadHistory(HistoryRequestParameters requestParameters) => [];

    private static MessageSymbol CreateMessageSymbol(string rawSymbol)
    {
        var symbol = NormalizeSymbolKey(rawSymbol);
        return new MessageSymbol(symbol)
        {
            Name = symbol,
            Description = $"{symbol} SCH TRD trading symbol",
            ProductAssetId = symbol,
            QuotingCurrencyAssetID = "USD",
            SymbolType = SymbolType.Equities,
            ExchangeId = ExchangeId,
            SessionsContainerId = EquitySessionsContainerId,
            VolumeType = SymbolVolumeType.Volume,
            QuotingType = SymbolQuotingType.LotSize,
            VariableTickList = [new VariableTick(0.01, 0.01, 2)],
            LotSize = EquityLotSize,
            NotionalValueStep = 1d,
            AllowAbbreviatePriceByTickSize = true,
            AvailableOptions = AvailableDerivatives.None,
            SymbolAdditionalInfo =
            [
                new AdditionalInfoItem
                {
                    GroupInfo = "General",
                    Id = "Country",
                    NameKey = loc.key("Country"),
                    ToolTipKey = loc.key("Country"),
                    DataType = ComparingType.String,
                    Value = "US",
                    SortIndex = 100
                },
                new AdditionalInfoItem
                {
                    GroupInfo = "General",
                    Id = "Classification of Financial",
                    NameKey = loc.key("Classification of Financial"),
                    ToolTipKey = loc.key("Classification of Financial"),
                    DataType = ComparingType.String,
                    Value = "ESXXXX",
                    SortIndex = 110
                }
            ]
        };
    }

    private static HistoryMetadata CreateNoHistoryMetadata() => new()
    {
        AllowedAggregations = [],
        AllowedPeriodsHistoryAggregationTime = [],
        AllowedBasePeriodsHistoryAggregationTime = [],
        AllowedHistoryTypesHistoryAggregationTime = [],
        AllowedHistoryTypesHistoryAggregationTick = [],
        AllowedPeriodsHistoryAggregationTimeStatistics = [],
        AllowedBasePeriodsHistoryAggregationTimeStatistics = [],
        DegreeOfParallelism = 1,
        UseHistoryLocalCache = false,
        BuildUncompletedBars = false,
        ServerSideTickDirectionAvailable = false
    };

    private string ResolveInstruction(string accountId, string symbol, Side side, CancellationToken cancellationToken)
    {
        var client = this.backendClient;
        if (client == null)
            return side == Side.Buy ? "BUY" : "SELL";

        var signedPosition = client.GetPositionsAsync(cancellationToken)
            .GetAwaiter()
            .GetResult()
            .Where(p => this.IsVisibleAccountHash(p.AccountHash))
            .Where(p => string.Equals(p.AccountHash, accountId, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(NormalizeSymbolKey(p.Symbol), NormalizeSymbolKey(symbol), StringComparison.OrdinalIgnoreCase))
            .Sum(p => p.Quantity);

        return side == Side.Buy
            ? signedPosition < 0 ? "BUY_TO_COVER" : "BUY"
            : signedPosition > 0 ? "SELL" : "SELL_SHORT";
    }

    private bool TryGetCachedOrder(string orderId, out BrokerOrderDto order)
    {
        lock (this.syncRoot)
            return this.orderCache.TryGetValue(orderId, out order!);
    }

    private void RefreshOrdersInBackground()
    {
        var client = this.backendClient;
        if (client == null)
            return;

        var token = this.orderPollingCts?.Token ?? CancellationToken.None;
        _ = Task.Run(async () =>
        {
            try
            {
                foreach (var delay in ActionRefreshScheduleMilliseconds)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(delay), token).ConfigureAwait(false);
                    var orders = this.FilterVisibleOrders(await client.GetOrdersAsync(token).ConfigureAwait(false));
                    if (this.PublishOrderChanges(orders))
                    {
                        await this.RefreshPositionsAsync(token).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch
            {
                // Background reconciliation must never block or fail the user-initiated order action.
            }
        }, token);
    }

    private async Task RefreshPositionsAsync(CancellationToken token)
    {
        var client = this.backendClient;
        if (client == null)
            return;

        var positions = this.FilterVisiblePositions(await client.GetPositionsAsync(token).ConfigureAwait(false));
        var messages = this.ReconcilePositions(positions, out var closeMessages);

        foreach (var message in closeMessages)
            this.PushMessage(message);
        foreach (var message in messages)
            this.PushMessage(message);
    }

    private List<MessageOpenPosition> ReconcilePositions(IReadOnlyList<BrokerPositionDto> positions, out List<MessageClosePosition> closeMessages)
    {
        var messages = new List<MessageOpenPosition>();
        closeMessages = new List<MessageClosePosition>();
        var currentPositionIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var openPositions = positions
            .Where(p => !string.IsNullOrWhiteSpace(p.Symbol) && Math.Abs(p.Quantity) > 0)
            .ToList();

        lock (this.syncRoot)
        {
            foreach (var position in openPositions)
            {
                var positionId = GetPositionId(position);
                currentPositionIds.Add(positionId);

                var shouldPush = !this.positionCache.TryGetValue(positionId, out var cachedPosition) ||
                    HasPositionChanged(cachedPosition, position);

                this.positionCache[positionId] = position;
                this.closedPositionMessagesPushed.Remove(positionId);

                if (shouldPush)
                    messages.Add(CreatePosition(position));
            }

            foreach (var positionId in this.positionCache.Keys.ToList())
            {
                if (currentPositionIds.Contains(positionId))
                    continue;

                this.positionCache.Remove(positionId);

                if (this.closedPositionMessagesPushed.Add(positionId))
                    closeMessages.Add(CreateClosedPositionMessage(positionId));
            }
        }

        return messages;
    }

    private void PublishImmediateOpenOrder(BrokerOrderDto order)
    {
        if (string.IsNullOrWhiteSpace(order.OrderId))
            return;

        lock (this.syncRoot)
            this.orderCache[order.OrderId] = order;

        this.PushMessage(CreateOpenOrder(order));
    }

    private void PublishImmediateReplacement(
        BrokerOrderDto currentOrder,
        string replacementOrderId,
        string orderType,
        double? limitPrice,
        double? stopPrice,
        double quantity,
        string duration)
    {
        if (string.IsNullOrWhiteSpace(replacementOrderId))
            return;

        var filledQuantity = currentOrder.FilledQuantity ?? 0d;
        var replacement = CreateImmediateOrder(
            currentOrder.AccountHash ?? string.Empty,
            replacementOrderId,
            currentOrder.Symbol ?? string.Empty,
            currentOrder.Instruction ?? string.Empty,
            orderType,
            limitPrice,
            stopPrice,
            quantity,
            duration);
        replacement.OrderStrategyType = currentOrder.OrderStrategyType;
        replacement.Session = currentOrder.Session;
        replacement.ExpirationTime = currentOrder.ExpirationTime;
        replacement.FilledQuantity = filledQuantity;
        replacement.RemainingQuantity = Math.Max(quantity - filledQuantity, 0d);
        replacement.AverageFillPrice = currentOrder.AverageFillPrice;
        replacement.PositionId = currentOrder.PositionId;
        replacement.GroupId = currentOrder.GroupId;

        lock (this.syncRoot)
        {
            this.orderCache.Remove(currentOrder.OrderId);
            this.orderCache[replacement.OrderId] = replacement;
        }

        if (!string.Equals(currentOrder.OrderId, replacement.OrderId, StringComparison.OrdinalIgnoreCase))
            this.PushMessage(new MessageCloseOrder { OrderId = currentOrder.OrderId });

        this.PushMessage(CreateOpenOrder(replacement));
    }

    private static BrokerOrderDto CreateImmediateOrder(
        string accountHash,
        string orderId,
        string symbol,
        string instruction,
        string orderType,
        double? limitPrice,
        double? stopPrice,
        double quantity,
        string duration) => new()
    {
        AccountHash = accountHash,
        OrderId = orderId,
        Symbol = symbol,
        Instruction = instruction,
        OrderType = orderType,
        Status = "WORKING",
        OriginalStatus = "WORKING",
        Duration = duration,
        Session = string.Equals(duration, "GOOD_TILL_CANCEL", StringComparison.OrdinalIgnoreCase) ? "NORMAL" : "SEAMLESS",
        EnteredTime = DateTimeOffset.UtcNow,
        Quantity = quantity,
        FilledQuantity = 0d,
        RemainingQuantity = quantity,
        Price = limitPrice,
        StopPrice = stopPrice,
        TriggerPrice = stopPrice
    };


    private bool TryResolvePositionForPnl(string symbol, out BrokerPositionDto position)
    {
        var normalizedSymbol = NormalizeSymbolKey(symbol);
        lock (this.syncRoot)
        {
            foreach (var candidate in this.positionCache.Values)
            {
                if (string.Equals(NormalizeSymbolKey(candidate.Symbol), normalizedSymbol, StringComparison.OrdinalIgnoreCase) &&
                    Math.Abs(candidate.Quantity) > 0)
                {
                    position = candidate;
                    return true;
                }
            }
        }

        position = default!;
        return false;
    }

    private void UpdateOrderCache(IReadOnlyList<BrokerOrderDto> orders)
    {
        lock (this.syncRoot)
            this.UpdateOrderCacheCore(orders);
    }

    private void UpdateOrderCacheCore(IReadOnlyList<BrokerOrderDto> orders)
    {
        this.orderCache.Clear();
        foreach (var order in orders.Where(o => !string.IsNullOrWhiteSpace(o.OrderId)))
            this.orderCache[order.OrderId!] = order;
    }

    private static string GetPositionId(BrokerPositionDto position) =>
        $"{position.AccountHash}:{NormalizeSymbolKey(position.Symbol)}";

    private static bool HasPositionChanged(BrokerPositionDto previous, BrokerPositionDto current) =>
        Math.Abs(previous.Quantity - current.Quantity) > 0.000001 ||
        Math.Abs((previous.AveragePrice ?? 0d) - (current.AveragePrice ?? 0d)) > 0.000001 ||
        Math.Abs((previous.MarketPrice ?? 0d) - (current.MarketPrice ?? 0d)) > 0.000001 ||
        Math.Abs((previous.MarketValue ?? 0d) - (current.MarketValue ?? 0d)) > 0.000001 ||
        Math.Abs((previous.DayProfitLoss ?? 0d) - (current.DayProfitLoss ?? 0d)) > 0.000001 ||
        Math.Abs((previous.UnrealizedProfitLoss ?? 0d) - (current.UnrealizedProfitLoss ?? 0d)) > 0.000001;

    private static MessageAccount CreateAccount(BrokerAccountDto account)
    {
        var total = account.LiquidationValue ?? account.TotalCash ?? account.CashBalance ?? 0d;
        return new MessageAccount
        {
            AccountId = account.AccountHash,
            AccountName = string.IsNullOrWhiteSpace(account.AccountNumber) ? account.AccountHash : $"Schwab {account.AccountNumber}",
            AssetId = "USD",
            Balance = total,
            NettingType = NettingType.OnePosition,
            AccountAdditionalInfo = new List<AdditionalInfoItem>
            {
                new() { Id = "accountType", NameKey = loc.key("Account type"), Value = account.AccountType ?? string.Empty },
                new() { Id = "buyingPower", NameKey = loc.key("Buying power"), DataType = ComparingType.Double, FormatingType = AdditionalInfoItemFormatingType.AssetBalance, Value = account.BuyingPower ?? 0d },
                new() { Id = "cashAvailableForTrading", NameKey = loc.key("Cash available for trading"), DataType = ComparingType.Double, FormatingType = AdditionalInfoItemFormatingType.AssetBalance, Value = account.CashAvailableForTrading ?? 0d }
            }
        };
    }

    private static MessageCryptoAssetBalances CreateAssetBalance(BrokerAccountDto account)
    {
        var total = account.LiquidationValue ?? account.TotalCash ?? account.CashBalance ?? 0d;
        var available = account.CashAvailableForTrading ?? account.CashBalance ?? total;
        return new MessageCryptoAssetBalances
        {
            AccountId = account.AccountHash,
            AssetId = "USD",
            TotalBalance = total,
            AvailableBalance = available,
            ReservedBalance = Math.Max(total - available, 0d),
            TotalInUSD = total
        };
    }

    private static MessageOpenPosition CreatePosition(BrokerPositionDto position)
    {
        var message = new MessageOpenPosition(NormalizeSymbolKey(position.Symbol))
        {
            AccountId = position.AccountHash,
            Side = position.Quantity >= 0 ? Side.Buy : Side.Sell,
            PositionId = $"{position.AccountHash}:{NormalizeSymbolKey(position.Symbol)}",
            Quantity = ToQuantowerLotQuantity(Math.Abs(position.Quantity)),
            OpenPrice = position.AveragePrice ?? 0d,
            OpenTime = DateTime.UtcNow,
            Comment = position.Description ?? position.AssetType ?? string.Empty
        };

        var pnl = new PnLItem
        {
            AssetID = "USD",
            Value = ResolvePositionPnlValue(position),
            ValuePercent = ResolvePositionPnlPercent(position)
        };
        TrySetProperty(message, "GrossPnL", pnl);
        TrySetProperty(message, "NetPnL", pnl);
        TrySetProperty(message, "UnrealizedPnL", pnl);
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
                Value = ResolvePositionPnlValue(position)
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
            }
        };
        return message;
    }

    private static double ResolvePositionPnlValue(BrokerPositionDto position)
    {
        var averagePrice = position.AveragePrice ?? 0d;
        var marketPrice = position.MarketPrice ?? 0d;
        var quantity = Math.Abs(position.Quantity);

        if (averagePrice > 0d && marketPrice > 0d && quantity > 0d)
        {
            var priceDifference = position.Quantity >= 0d
                ? marketPrice - averagePrice
                : averagePrice - marketPrice;
            return priceDifference * quantity;
        }

        return position.UnrealizedProfitLoss ?? 0d;
    }

    private static double ResolvePositionPnlPercent(BrokerPositionDto position)
    {
        var averagePrice = position.AveragePrice ?? 0d;
        var marketPrice = position.MarketPrice ?? 0d;

        if (averagePrice <= 0d || marketPrice <= 0d)
            return 0d;

        return position.Quantity >= 0d
            ? (marketPrice / averagePrice) - 1d
            : (averagePrice - marketPrice) / averagePrice;
    }

    private static MessageClosePosition CreateClosedPositionMessage(string positionId) => new() { PositionId = positionId };

    private static MessageOpenOrder CreateOpenOrder(BrokerOrderDto order)
    {
        var filledQuantity = ToQuantowerLotQuantity(Math.Abs(order.FilledQuantity ?? 0d));
        var totalQuantity = ToQuantowerLotQuantity(Math.Abs(order.Quantity ?? 0d));
        var message = new MessageOpenOrder(NormalizeSymbolKey(order.Symbol ?? string.Empty))
        {
            AccountId = order.AccountHash,
            OrderId = order.OrderId,
            GroupId = ResolveOrderGroupId(order),
            PositionId = ResolveOpenOrderPositionId(order),
            Price = order.Price ?? double.NaN,
            TriggerPrice = order.TriggerPrice ?? order.StopPrice ?? double.NaN,
            TrailOffset = order.TrailOffset ?? double.NaN,
            OrderTypeId = ConvertSchwabOrderType(order.OrderType),
            Side = ConvertInstructionSide(order.Instruction),
            Status = ConvertOrderStatus(order.Status),
            OriginalStatus = order.OriginalStatus ?? order.Status ?? string.Empty,
            TimeInForce = ConvertTimeInForce(order.Duration),
            ExpirationTime = order.ExpirationTime?.UtcDateTime ?? DateTime.MinValue,
            TotalQuantity = totalQuantity,
            FilledQuantity = filledQuantity,
            AverageFillPrice = filledQuantity > 0d ? order.AverageFillPrice ?? double.NaN : double.NaN,
            LastUpdateTime = DateTime.UtcNow,
            Comment = ResolveOpenOrderComment(order),
            AdditionalInfoItems = new List<AdditionalInfoItem>
            {
                new()
                {
                    Id = "schwabSession",
                    NameKey = loc.key("Schwab session"),
                    Value = order.Session ?? string.Empty
                },
                new()
                {
                    Id = "schwabStrategy",
                    NameKey = loc.key("Schwab strategy"),
                    Value = order.OrderStrategyType ?? string.Empty
                }
            }
        };

        return message;
    }

    private static string ResolveOrderGroupId(BrokerOrderDto order)
    {
        var groupId = order.GroupId?.Trim();
        if (string.IsNullOrWhiteSpace(groupId))
            return string.Empty;

        return groupId.Equals("SINGLE", StringComparison.OrdinalIgnoreCase)
            ? string.Empty
            : groupId;
    }

    private static string GetOrderPositionId(BrokerOrderDto order)
    {
        if (!string.IsNullOrWhiteSpace(order.PositionId))
        {
            var parts = order.PositionId.Split(':', 2);
            if (parts.Length == 2)
                return $"{parts[0]}:{NormalizeSymbolKey(parts[1])}";

            return order.PositionId;
        }

        return $"{order.AccountHash}:{NormalizeSymbolKey(order.Symbol ?? string.Empty)}";
    }

    private static string ResolveOpenOrderPositionId(BrokerOrderDto order)
    {
        // Match Quantower's native DOM behavior: plain DOM ladder orders should publish
        // like IBKR LMT/STP orders with no position attachment. A PositionId makes QT
        // classify them as TP/SL-style overlays (for example "Limit(TP for ...)").
        if (!IsPositionAttachedOrder(order))
            return string.Empty;

        return GetOrderPositionId(order);
    }

    private static bool IsPositionAttachedOrder(BrokerOrderDto order)
    {
        var strategy = order.OrderStrategyType?.Trim();
        if (!string.IsNullOrWhiteSpace(strategy) && !strategy.Equals("SINGLE", StringComparison.OrdinalIgnoreCase))
            return true;

        var groupId = order.GroupId?.Trim();
        return !string.IsNullOrWhiteSpace(groupId) && !groupId.Equals("SINGLE", StringComparison.OrdinalIgnoreCase);
    }

    private static string ResolveOpenOrderComment(BrokerOrderDto order) =>
        IsPositionAttachedOrder(order) ? FormatInstructionLabel(order.Instruction) : string.Empty;

    private static bool IsSupportedPlaceOrderType(string orderTypeId) =>
        string.Equals(orderTypeId, OrderType.Market, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(orderTypeId, OrderType.Limit, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(orderTypeId, OrderType.Stop, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(orderTypeId, OrderType.StopLimit, StringComparison.OrdinalIgnoreCase);

    private static string ConvertOrderTypeId(string orderTypeId) =>
        string.Equals(orderTypeId, OrderType.StopLimit, StringComparison.OrdinalIgnoreCase) ? "STOP_LIMIT" :
        string.Equals(orderTypeId, OrderType.Stop, StringComparison.OrdinalIgnoreCase) ? "STOP" :
        string.Equals(orderTypeId, OrderType.Market, StringComparison.OrdinalIgnoreCase) ? "MARKET" :
        "LIMIT";

    private static string ConvertSchwabOrderType(string? orderType) =>
        string.Equals(orderType, "STOP_LIMIT", StringComparison.OrdinalIgnoreCase) ? OrderType.StopLimit :
        string.Equals(orderType, "STOP", StringComparison.OrdinalIgnoreCase) ? OrderType.Stop :
        string.Equals(orderType, "LIMIT", StringComparison.OrdinalIgnoreCase) ? OrderType.Limit :
        OrderType.Market;

    private static bool RequiresLimitPrice(string orderTypeId) =>
        string.Equals(orderTypeId, OrderType.Limit, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(orderTypeId, OrderType.StopLimit, StringComparison.OrdinalIgnoreCase);

    private static bool RequiresStopPrice(string orderTypeId) =>
        string.Equals(orderTypeId, OrderType.Stop, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(orderTypeId, OrderType.StopLimit, StringComparison.OrdinalIgnoreCase);

    private static double ResolveStopPrice(PlaceOrderRequestParameters parameters)
    {
        if (!double.IsNaN(parameters.TriggerPrice) && parameters.TriggerPrice > 0)
            return parameters.TriggerPrice;
        return !double.IsNaN(parameters.Price) && parameters.Price > 0 ? parameters.Price : 0d;
    }

    private static double ResolveStopPrice(ModifyOrderRequestParameters parameters)
    {
        if (!double.IsNaN(parameters.TriggerPrice) && parameters.TriggerPrice > 0)
            return parameters.TriggerPrice;
        return !double.IsNaN(parameters.Price) && parameters.Price > 0 ? parameters.Price : 0d;
    }

    private static Side ConvertInstructionSide(string? instruction)
    {
        var normalized = (instruction ?? string.Empty).Trim().ToUpperInvariant();
        return normalized switch
        {
            "BUY" or "BUY_TO_OPEN" or "BUY_TO_CLOSE" or "BUY_TO_COVER" => Side.Buy,
            "SELL" or "SELL_SHORT" or "SELL_TO_OPEN" or "SELL_TO_CLOSE" => Side.Sell,
            _ => Side.Sell
        };
    }

    private static string FormatInstructionLabel(string? instruction)
    {
        var normalized = (instruction ?? string.Empty).Trim().ToUpperInvariant();
        return normalized switch
        {
            "BUY_TO_CLOSE" => "BTC",
            "BUY_TO_OPEN" => "BTO",
            "SELL_TO_CLOSE" => "STC",
            "SELL_TO_OPEN" => "STO",
            "SELL_SHORT" => "Sell Short",
            "BUY_TO_COVER" => "BTC",
            "BUY" => "Buy",
            "SELL" => "Sell",
            _ => normalized
        };
    }

    private static bool IsCancelableOrderStatus(string? status)
    {
        var normalized = (status ?? string.Empty).ToUpperInvariant();
        return normalized is "ACCEPTED" or "AWAITING_PARENT_ORDER" or "AWAITING_CONDITION" or "AWAITING_MANUAL_REVIEW"
            or "QUEUED" or "PENDING_ACTIVATION" or "WORKING" or "PENDING_CANCEL" or "PENDING_REPLACE";
    }

    private static bool IsTerminalOrderStatus(string? status)
    {
        var normalized = (status ?? string.Empty).ToUpperInvariant();
        return normalized is "FILLED" or "CANCELED" or "CANCELLED" or "REPLACED" or "REJECTED" or "EXPIRED";
    }

    private static OrderStatus ConvertOrderStatus(string? status) =>
        (status ?? string.Empty).ToUpperInvariant() switch
        {
            "FILLED" => OrderStatus.Filled,
            "CANCELED" or "CANCELLED" or "REPLACED" => OrderStatus.Cancelled,
            "REJECTED" => OrderStatus.Refused,
            "EXPIRED" => OrderStatus.Cancelled,
            "PENDING_CANCEL" => OrderStatus.Opened,
            _ => OrderStatus.Opened
        };

    private static TimeInForce ConvertTimeInForce(string? duration) =>
        string.Equals(duration, "GOOD_TILL_CANCEL", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(duration, "GTC", StringComparison.OrdinalIgnoreCase)
            ? TimeInForce.GTC
            : TimeInForce.Day;

    private static string ConvertTimeInForce(TimeInForce tif) =>
        tif == TimeInForce.GTC ? "GTC" : "DAY";

    private static string NormalizeSymbolKey(string symbol) => (symbol ?? string.Empty).Trim().ToUpperInvariant();

    private static string? ResolveStringProperty(object? target, string propertyName)
    {
        var value = ResolveObjectProperty(target, propertyName);
        if (value == null)
            return null;

        return value switch
        {
            string text => text,
            Account account => account.Id,
            Symbol symbol => symbol.Id,
            _ => ResolveStringProperty(value, "Id")
        };
    }

    private static object? ResolveObjectProperty(object? target, string propertyName)
    {
        if (target == null)
            return null;

        var type = target.GetType();
        var property = type.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
        if (property != null)
            return property.GetValue(target);

        var field = type.GetField(propertyName, BindingFlags.Instance | BindingFlags.Public);
        return field?.GetValue(target);
    }

    private static double ResolveDoubleProperty(object target, string propertyName)
    {
        var value = ResolveObjectProperty(target, propertyName);
        if (value == null)
            return 0d;

        return value switch
        {
            double d => d,
            float f => f,
            decimal m => (double)m,
            int i => i,
            long l => l,
            _ => 0d
        };
    }

    private static double ResolveFirstPositiveDoubleProperty(object target, params string[] propertyNames)
    {
        foreach (var propertyName in propertyNames)
        {
            var value = ResolveDoubleProperty(target, propertyName);
            if (value > 0)
                return value;
        }

        return 0d;
    }

    private static double ResolveCloseLimitPriceFromMappedQuote(ClosePositionRequestParameters parameters, bool closingLong)
    {
        var symbol = ResolveObjectProperty(parameters, "Symbol") ??
            ResolveObjectProperty(ResolveObjectProperty(parameters, "Position"), "Symbol");
        if (symbol == null)
            return 0d;

        return closingLong
            ? ResolveFirstPositiveDoubleProperty(symbol, "Ask")
            : ResolveFirstPositiveDoubleProperty(symbol, "Bid");
    }

    private static TimeInForce ResolveTimeInForceProperty(object target, string propertyName)
    {
        var property = target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public);
        var value = property?.GetValue(target);
        return value is TimeInForce timeInForce ? timeInForce : TimeInForce.Day;
    }

    private static double ToBrokerShareQuantity(double quantowerLotQuantity) =>
        Math.Round(quantowerLotQuantity * EquityLotSize, 6);

    private static double ToQuantowerLotQuantity(double brokerShareQuantity) =>
        brokerShareQuantity / EquityLotSize;

    private static void TrySetProperty(object target, string propertyName, object? value)
    {
        target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public)?.SetValue(target, value);
    }
}



