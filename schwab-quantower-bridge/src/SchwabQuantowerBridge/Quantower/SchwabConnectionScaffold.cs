using System.Collections.Generic;
using System.IO;
using TradingPlatform.BusinessLayer;
using TradingPlatform.BusinessLayer.Integration;

namespace SchwabQuantowerBridge.Quantower;

/// <summary>
/// Data-only Quantower vendor backed by the local Schwab workstation API.
/// </summary>
public sealed class SchwabVendor : Vendor
{
    internal const string VendorName = "Schwab";
    private Vendor? currentVendor;

    public static VendorMetaData GetVendorMetaData() => new()
    {
        VendorName = VendorName,
        VendorDescription = loc.key("Equities market data connection powered by a local Schwab bridge."),
        GetDefaultConnections = () => new List<ConnectionInfo>
        {
            CreateDefaultConnectionInfo("Schwab", VendorName, Path.Combine("SchwabVendor", "schwab.svg"))
        },
        GetConnectionParameters = () => new List<SettingItem>()
    };

    public override ConnectionResult Connect(ConnectRequestParameters connectRequestParameters)
    {
        this.currentVendor = new SchwabMarketDataVendor();
        this.currentVendor.NewMessage += this.CurrentVendor_NewMessage;
        return this.currentVendor.Connect(connectRequestParameters);
    }

    public override void Disconnect()
    {
        if (this.currentVendor != null)
        {
            this.currentVendor.NewMessage -= this.CurrentVendor_NewMessage;
            this.currentVendor.Disconnect();
        }

        this.currentVendor = null;
        base.Disconnect();
    }

    public override void OnConnected(System.Threading.CancellationToken token) => this.currentVendor?.OnConnected(token);

    public override PingResult Ping() => this.currentVendor?.Ping() ?? new PingResult { State = PingEnum.Disconnected };

    public override IList<MessageRule> GetRules(System.Threading.CancellationToken token) => this.currentVendor?.GetRules(token) ?? base.GetRules(token);

    public override IList<MessageAccount> GetAccounts(System.Threading.CancellationToken token) =>
        this.currentVendor?.GetAccounts(token) ?? new List<MessageAccount>();

    public override IList<MessageCryptoAssetBalances> GetCryptoAssetBalances(System.Threading.CancellationToken token) =>
        this.currentVendor?.GetCryptoAssetBalances(token) ?? new List<MessageCryptoAssetBalances>();

    public override IList<MessageSymbol> GetSymbols(System.Threading.CancellationToken token) => this.currentVendor?.GetSymbols(token) ?? new List<MessageSymbol>();

    public override MessageSymbolTypes GetSymbolTypes(System.Threading.CancellationToken token) => this.currentVendor?.GetSymbolTypes(token) ?? new MessageSymbolTypes();

    public override IList<MessageAsset> GetAssets(System.Threading.CancellationToken token) => this.currentVendor?.GetAssets(token) ?? new List<MessageAsset>();

    public override IList<MessageExchange> GetExchanges(System.Threading.CancellationToken token) => this.currentVendor?.GetExchanges(token) ?? new List<MessageExchange>();

    public override bool AllowNonFixedList => true;

    public override MessageSymbol GetNonFixedSymbol(GetSymbolRequestParameters requestParameters) =>
        this.currentVendor?.GetNonFixedSymbol(requestParameters) ?? base.GetNonFixedSymbol(requestParameters);

    public override IList<MessageSymbolInfo> SearchSymbols(SearchSymbolsRequestParameters requestParameters) =>
        this.currentVendor?.SearchSymbols(requestParameters) ?? new List<MessageSymbolInfo>();

    public override IList<MessageOptionSerie> GetAllOptionSeries(System.Threading.CancellationToken token) =>
        this.currentVendor?.GetAllOptionSeries(token) ?? new List<MessageOptionSerie>();

    public override IList<MessageOptionSerie> GetOptionSeries(GetOptionSeriesRequestParameters requestParameters) =>
        this.currentVendor?.GetOptionSeries(requestParameters) ?? new List<MessageOptionSerie>();

    public override IList<MessageSymbolInfo> GetStrikes(GetStrikesRequestParameters requestParameters) =>
        this.currentVendor?.GetStrikes(requestParameters) ?? new List<MessageSymbolInfo>();

    public override void SubscribeSymbol(SubscribeQuotesParameters parameters) => this.currentVendor?.SubscribeSymbol(parameters);

    public override void UnSubscribeSymbol(SubscribeQuotesParameters parameters) => this.currentVendor?.UnSubscribeSymbol(parameters);

    public override HistoryMetadata GetHistoryMetadata(System.Threading.CancellationToken cancelationToken) =>
        this.currentVendor?.GetHistoryMetadata(cancelationToken) ?? new HistoryMetadata();

    public override IList<IHistoryItem> LoadHistory(HistoryRequestParameters requestParameters) =>
        this.currentVendor?.LoadHistory(requestParameters) ?? new List<IHistoryItem>();

    public override IList<OrderType> GetAllowedOrderTypes(System.Threading.CancellationToken token) =>
        this.currentVendor?.GetAllowedOrderTypes(token) ?? new List<OrderType>();

    public override IList<MessageOpenOrder> GetPendingOrders(System.Threading.CancellationToken token) =>
        this.currentVendor?.GetPendingOrders(token) ?? new List<MessageOpenOrder>();

    public override IList<MessageOpenPosition> GetPositions(System.Threading.CancellationToken token) =>
        this.currentVendor?.GetPositions(token) ?? new List<MessageOpenPosition>();

    public override IList<MessageOrderHistory> GetOrdersHistory(OrdersHistoryRequestParameters requestParameters) =>
        this.currentVendor?.GetOrdersHistory(requestParameters) ?? new List<MessageOrderHistory>();

    public override IList<MessageTrade> GetTrades(TradesHistoryRequestParameters requestParameters) =>
        this.currentVendor?.GetTrades(requestParameters) ?? new List<MessageTrade>();

    public override void GetTrades(TradesHistoryRequestParameters requestParameters, TradingPlatform.BusinessLayer.Integration.AccountTradesLoadingCallback callback) =>
        this.currentVendor?.GetTrades(requestParameters, callback);

    public override TradesHistoryMetadata GetTradesMetadata() =>
        this.currentVendor?.GetTradesMetadata() ?? new TradesHistoryMetadata();

    public override PnL CalculatePnL(PnLRequestParameters parameters) =>
        this.currentVendor?.CalculatePnL(parameters) ?? base.CalculatePnL(parameters);

    public override TradingOperationResult PlaceOrder(PlaceOrderRequestParameters request) =>
        this.currentVendor?.PlaceOrder(request) ?? TradingOperationResult.CreateError(request.RequestId, "Schwab bridge is not connected.");

    public override TradingOperationResult ModifyOrder(ModifyOrderRequestParameters request) =>
        this.currentVendor?.ModifyOrder(request) ?? TradingOperationResult.CreateError(request.RequestId, "Schwab bridge is not connected.");

    public override TradingOperationResult CancelOrder(CancelOrderRequestParameters request) =>
        this.currentVendor?.CancelOrder(request) ?? TradingOperationResult.CreateError(request.RequestId, "Schwab bridge is not connected.");

    private void CurrentVendor_NewMessage(object? sender, VendorEventArgs e) => this.PushMessage(e.Message);
}
