using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Threading;
using TradingPlatform.BusinessLayer;
using TradingPlatform.BusinessLayer.Integration;
using TradingPlatform.BusinessLayer.Native;
using TradingPlatform.PresentationLayer.Plugins;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking.EventArgs;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking.Models.Scopes;
using TradingPlatform.PresentationLayer.Renderers;

namespace DOMImbalanceMonitor;

public sealed class DOMImbalanceMonitor : Plugin, ILinkable
{
    private readonly object stateLock = new();
    private readonly DomMonitorState state = new();
    private DomMonitorRenderer? renderer;
    private System.Threading.Timer? redrawTimer;
    private LinkingPluginService? linkingService;
    private Symbol? subscribedDxFeedSymbol;

    public static PluginInfo GetInfo()
    {
        return new PluginInfo
        {
            Name = "DOMImbalanceMonitor",
            Title = loc.key("DOM Imbalance Monitor"),
            Group = PluginGroup.Analytics,
            ShortName = "IMB",
            SortIndex = 36,
            AllowSettings = true,
            WindowParameters = new NativeWindowParameters(NativeWindowParameters.Panel)
            {
                BrowserUsageType = BrowserUsageType.None,
                BindingBehaviour = BindingBehaviour.Bindable,
                HeaderVisible = true,
                AllowActionsButton = true,
                AllowCloseButton = true,
                AllowFullScreenButton = false,
                AllowMaximizeButton = true,
                StickingEnabled = StickyWindowBehavior.AllowSticking
            },
            CustomProperties = new Dictionary<string, object>
            {
                { PluginInfo.Const.ALLOW_MANUAL_CREATION, true }
            }
        };
    }

    public override Size DefaultSize => new(this.UnitSize.Width * 2, this.UnitSize.Height);

    public override Symbol CurrentSymbol
    {
        get => this.state.LinkedSymbol!;
        set => this.SetLinkedSymbol(value);
    }

    public LinkingState LinkingState => this.linkingService?.LinkingState ?? new LinkingState(default!, [], false);

    public event EventHandler<LinkingEntityEventArgs>? LinkingStateChanged
    {
        add
        {
            if (this.linkingService != null)
                this.linkingService.LinkingStateChanged += value;
        }
        remove
        {
            if (this.linkingService != null)
                this.linkingService.LinkingStateChanged -= value;
        }
    }

    public override void Initialize()
    {
        base.Initialize();
        this.RegisterService<LinkingPluginService>(service =>
        {
            service.AllowedScopes = [LinkingScope.Symbol];
            service.Initialize(this);
            this.linkingService = service;
        });

        this.renderer = new DomMonitorRenderer(this.Window.CreateRenderingControl("DOMImbalanceMonitorRenderer"), this.ReadPaintSnapshot);
        this.redrawTimer = new System.Threading.Timer(_ => this.renderer?.Redraw(), null, 250, 250);
    }

    public override void Populate(PluginParameters? args = null)
    {
        base.Populate(args);
        if (this.CurrentSymbol != null)
            this.SetLinkedSymbol(this.CurrentSymbol);
    }

    public override void Dispose()
    {
        this.redrawTimer?.Dispose();
        this.Unsubscribe();
        this.renderer?.Dispose();
        this.linkingService?.Dispose();
        base.Dispose();
    }

    public override IList<SettingItem> Settings
    {
        get
        {
            var settings = base.Settings;
            settings.Add(new SettingItemInteger("SessionStartHHMM", this.state.SessionStartHHMM, 0) { Text = "Session start (HHMM)" });
            settings.Add(new SettingItemInteger("LevelDepth", this.state.LevelDepth, 1) { Text = "Level depth" });
            settings.Add(new SettingItemDouble("ImbalanceThreshold", this.state.ImbalanceThreshold, 2) { Text = "Legacy imbalance multiplier (unused)" });
            settings.Add(new SettingItemDouble("ImbalancePercentileThresholdPct", this.state.ImbalancePercentileThresholdPct, 3) { Text = "Imbalance percentile threshold %" });
            settings.Add(new SettingItemDouble("WithdrawalThresholdPct", this.state.WithdrawalThresholdPct, 4) { Text = "Warm-up withdrawal fallback %" });
            settings.Add(new SettingItemInteger("SweepWindowSeconds", this.state.SweepWindowSeconds, 5) { Text = "Adaptive sweep max window (seconds)" });
            settings.Add(new SettingItemInteger("SpoofWindowSeconds", this.state.SpoofWindowSeconds, 6) { Text = "Adaptive spoof max window (seconds)" });
            settings.Add(new SettingItemBoolean("ShowDiagnostics", this.state.ShowDiagnostics, 7) { Text = "Show diagnostics" });
            settings.Add(new SettingItemInteger("AlertFontSize", this.state.AlertFontSize, 8) { Text = "Alert font size" });

            if (this.linkingService != null)
            {
                foreach (var linkingSetting in this.linkingService.Settings)
                    settings.Add(linkingSetting);
            }

            return settings;
        }
        set
        {
            base.Settings = value;
            if (this.linkingService != null)
                this.linkingService.Settings = value;

            var holder = new SettingsHolder(value);
            lock (this.stateLock)
            {
                if (holder.TryGetValue("SessionStartHHMM", out var item) && item.Value is int sessionStart)
                    this.state.SessionStartHHMM = Math.Clamp(sessionStart, 0, 2359);
                if (holder.TryGetValue("LevelDepth", out item) && item.Value is int depth)
                    this.state.LevelDepth = Math.Clamp(depth, 3, 10);

                // Legacy workspace compatibility only. This key is no longer used by alert logic.
                if (holder.TryGetValue("ImbalanceThreshold", out item) && TryReadDouble(item.Value, out var legacyImbalance))
                    this.state.ImbalanceThreshold = Math.Clamp(legacyImbalance, 1.5d, 5d);

                if (holder.TryGetValue("ImbalancePercentileThresholdPct", out item) && TryReadDouble(item.Value, out var imbalancePercentile))
                    this.state.ImbalancePercentileThresholdPct = Math.Clamp(imbalancePercentile, 50d, 99d);

                if (holder.TryGetValue("WithdrawalThresholdPct", out item) && TryReadDouble(item.Value, out var withdrawal))
                    this.state.WithdrawalThresholdPct = Math.Clamp(withdrawal, 20d, 80d);
                if (holder.TryGetValue("SweepWindowSeconds", out item) && item.Value is int sweepWindow)
                    this.state.SweepWindowSeconds = Math.Clamp(sweepWindow, 1, 5);
                if (holder.TryGetValue("SpoofWindowSeconds", out item) && item.Value is int spoofWindow)
                    this.state.SpoofWindowSeconds = Math.Clamp(spoofWindow, 1, 10);
                if (holder.TryGetValue("ShowDiagnostics", out item) && item.Value is bool diagnostics)
                    this.state.ShowDiagnostics = diagnostics;
                if (holder.TryGetValue("AlertFontSize", out item) && item.Value is int fontSize)
                    this.state.AlertFontSize = Math.Clamp(fontSize, 10, 32);
            }

            this.renderer?.Redraw();
        }
    }

    protected override void OnLayoutUpdated()
    {
        base.OnLayoutUpdated();
        if (this.renderer != null)
            this.renderer.Layout.Margin = this.NonClientMargin;
    }

    private void SetLinkedSymbol(Symbol? linkedSymbol)
    {
        lock (this.stateLock)
        {
            this.state.LinkedSymbol = linkedSymbol;
            this.state.DisplaySymbol = linkedSymbol?.Name ?? string.Empty;
            this.state.StatusNote = linkedSymbol == null ? "No linked symbol" : "Resolving dxFeed symbol";
        }

        this.Unsubscribe();
        if (linkedSymbol == null)
            return;

        var dxFeedSymbol = ResolveDxFeedSymbol(linkedSymbol);
        if (dxFeedSymbol == null)
        {
            lock (this.stateLock)
                this.state.StatusNote = "dxFeed symbol not found";
            this.renderer?.Redraw();
            return;
        }

        lock (this.stateLock)
        {
            this.state.ResetForSymbol(dxFeedSymbol);
            this.state.LinkedSymbol = linkedSymbol;
            this.state.DisplaySymbol = linkedSymbol.Name;
            this.state.StatusNote = $"dxFeed: {dxFeedSymbol.Name}";
        }

        this.subscribedDxFeedSymbol = dxFeedSymbol;
        this.subscribedDxFeedSymbol.NewQuote += this.OnNewQuote;
        this.subscribedDxFeedSymbol.NewLast += this.OnNewLast;
        this.subscribedDxFeedSymbol.NewLevel2 += this.OnNewLevel2;

        try
        {
            var domSnapshot = this.subscribedDxFeedSymbol.DepthOfMarket.GetDepthOfMarketAggregatedCollections(
                new GetDepthOfMarketParameters
                {
                    GetLevel2ItemsParameters = new GetLevel2ItemsParameters
                    {
                        AggregateMethod = AggregateMethod.ByPriceLVL
                    }
                });

            lock (this.stateLock)
                this.state.SeedOrderBook(domSnapshot, DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
        }

        this.renderer?.Redraw();
    }

    private void Unsubscribe()
    {
        if (this.subscribedDxFeedSymbol == null)
            return;

        this.subscribedDxFeedSymbol.NewQuote -= this.OnNewQuote;
        this.subscribedDxFeedSymbol.NewLast -= this.OnNewLast;
        this.subscribedDxFeedSymbol.NewLevel2 -= this.OnNewLevel2;
        this.subscribedDxFeedSymbol = null;
    }

    private static Symbol? ResolveDxFeedSymbol(Symbol linkedSymbol)
    {
        bool IsDxFeed(Symbol symbol)
        {
            var connectionText = $"{symbol.ConnectionId} {symbol.Connection?.VendorName} {symbol.Connection?.Name}";
            return connectionText.Contains("dxFeed", StringComparison.OrdinalIgnoreCase);
        }

        if (IsDxFeed(linkedSymbol))
            return linkedSymbol;

        return Core.Instance.Symbols.FirstOrDefault(symbol =>
            IsDxFeed(symbol) &&
            (string.Equals(symbol.Name, linkedSymbol.Name, StringComparison.OrdinalIgnoreCase) ||
             string.Equals(symbol.Id, linkedSymbol.Name, StringComparison.OrdinalIgnoreCase) ||
             string.Equals(symbol.Name, linkedSymbol.Id, StringComparison.OrdinalIgnoreCase)));
    }

    private void OnNewQuote(Symbol symbol, Quote quote)
    {
        lock (this.stateLock)
            this.state.OnQuote(quote, DateTime.UtcNow);
    }

    private void OnNewLast(Symbol symbol, Last last)
    {
        lock (this.stateLock)
            this.state.OnLast(last, DateTime.UtcNow);
    }

    private void OnNewLevel2(Symbol symbol, Level2Quote level2, DOMQuote dom)
    {
        lock (this.stateLock)
            this.state.OnLevel2(level2, dom, DateTime.UtcNow);
    }

    private DomMonitorPaintSnapshot ReadPaintSnapshot()
    {
        lock (this.stateLock)
            return this.state.ToPaintSnapshot();
    }

    private static bool TryReadDouble(object? value, out double result)
    {
        switch (value)
        {
            case double doubleValue:
                result = doubleValue;
                return true;
            case float floatValue:
                result = floatValue;
                return true;
            case int intValue:
                result = intValue;
                return true;
            case string text:
                return double.TryParse(text, out result);
            default:
                result = 0d;
                return false;
        }
    }
}

internal sealed class DomMonitorRenderer : Renderer
{
    private readonly Func<DomMonitorPaintSnapshot> snapshotProvider;
    private readonly BufferedGraphic bufferedGraphic;
    private Font alertFont = new("Segoe UI", 16, FontStyle.Bold, GraphicsUnit.Pixel);
    private readonly Font diagFont = new("Consolas", 10, FontStyle.Regular, GraphicsUnit.Pixel);
    private int currentAlertFontSize = 16;

    public DomMonitorRenderer(IRenderingNativeControl native, Func<DomMonitorPaintSnapshot> snapshotProvider)
        : base(native)
    {
        this.snapshotProvider = snapshotProvider;
        this.bufferedGraphic = new BufferedGraphic(this.Draw, this.Refresh, native.DisposeImage, native.IsDisplayed, BufferedGraphicRequiredThreadType.LowPriority);
    }

    public void Redraw()
    {
        this.bufferedGraphic.IsDirty = true;
    }

    public override IntPtr Render() => this.bufferedGraphic.CurrentImage;

    public override void OnResize()
    {
        base.OnResize();
        var bounds = this.Bounds;
        if (bounds.Width <= 0 || bounds.Height <= 0)
            return;

        try
        {
            this.bufferedGraphic.Resize(bounds.Width, bounds.Height);
            this.bufferedGraphic.IsDirty = true;
        }
        catch
        {
        }
    }

    public override void Dispose()
    {
        this.alertFont.Dispose();
        this.diagFont.Dispose();
        this.bufferedGraphic.Dispose();
        base.Dispose();
    }

    private void Draw(Graphics graphics)
    {
        var snapshot = this.snapshotProvider();
        var bounds = this.Bounds;
        graphics.Clear(Color.FromArgb(12, 24, 34));
        graphics.TextRenderingHint = System.Drawing.Text.TextRenderingHint.ClearTypeGridFit;

        using var border = new Pen(Color.FromArgb(60, 110, 130, 150), 1f);
        using var panel = new SolidBrush(Color.FromArgb(18, 34, 46));
        using var alertBrush = new SolidBrush(snapshot.AlertColor);
        using var neutralBrush = new SolidBrush(Color.FromArgb(170, 185, 195));
        using var diagBrush = new SolidBrush(Color.FromArgb(205, 215, 220));
        if (snapshot.AlertFontSize != this.currentAlertFontSize)
        {
            this.alertFont.Dispose();
            this.currentAlertFontSize = snapshot.AlertFontSize;
            this.alertFont = new Font("Segoe UI", this.currentAlertFontSize, FontStyle.Bold, GraphicsUnit.Pixel);
        }

        var rect = new RectangleF(6, 6, Math.Max(40, bounds.Width - 12), Math.Max(40, bounds.Height - 12));
        graphics.FillRectangle(panel, rect);
        graphics.DrawRectangle(border, rect.X, rect.Y, rect.Width, rect.Height);

        var textRect = snapshot.ShowDiagnostics
            ? new RectangleF(rect.Left + 8, rect.Top + 8, rect.Width - 16, 36)
            : new RectangleF(rect.Left + 8, rect.Top + 8, rect.Width - 16, rect.Height - 16);

        using var centered = new StringFormat
        {
            Alignment = StringAlignment.Center,
            LineAlignment = StringAlignment.Center,
            Trimming = StringTrimming.EllipsisCharacter
        };

        graphics.DrawString(snapshot.AlertText, this.alertFont, alertBrush, textRect, centered);

        if (!snapshot.ShowDiagnostics)
            return;

        var y = rect.Top + 48;
        foreach (var line in snapshot.Diagnostics)
        {
            graphics.DrawString(line, this.diagFont, diagBrush, rect.Left + 12, y);
            y += 14;
            if (y > rect.Bottom - 16)
                break;
        }
    }
}

internal sealed class DomMonitorState
{
    private enum TapeSide
    {
        Unknown,
        BidHit,
        OfferLifted
    }

    private enum AlertKind
    {
        Monitoring,
        SweepDetected,
        LiquidityWithdrawal,
        RealBidSupport,
        RealAskResistance,
        SpoofingBid,
        SpoofingAsk
    }

    private const double TradeRateEwmaSeconds = 20.0;
    private const double BookRateEwmaSeconds = 20.0;
    private const double DepthEwmaSeconds = 60.0;
    private const double RealizedVolatilityEwmaSeconds = 45.0;
    private const double SweepEventBudget = 3.0;
    private const double SweepWindowMinSeconds = 0.05;
    private const double SpoofBookUpdateBudget = 6.0;
    private const double SpoofWindowMinSeconds = 0.25;
    private const int DepthChangePercentileSamples = 1200;
    private const int WithdrawalWarmupSamples = 60;
    private const double WithdrawalLowerPercentile = 0.05;
    private const int ImbalancePercentileSamples = 1200;
    private const int ImbalanceWarmupSamples = 60;
    private const double ImbalanceWarmupAbsoluteThreshold = 0.25;
    private const int DisplayedSizeRetentionLevels = 50;

    private readonly SortedDictionary<double, double> bids = new(Comparer<double>.Create((a, b) => b.CompareTo(a)));
    private readonly SortedDictionary<double, double> asks = new();
    private readonly Queue<(DateTime Utc, double Price, TapeSide Side, double Size)> recentPrints = new();
    private readonly SpoofCandidate bidSpoof = new();
    private readonly SpoofCandidate askSpoof = new();
    private readonly RollingDoubleWindow bidTop3ChangePctWindow = new(DepthChangePercentileSamples);
    private readonly RollingDoubleWindow askTop3ChangePctWindow = new(DepthChangePercentileSamples);
    private readonly RollingDoubleWindow imbalanceWindow = new(ImbalancePercentileSamples);
    private readonly Dictionary<double, double> displayedBidSizeByPrice = new();
    private readonly Dictionary<double, double> displayedAskSizeByPrice = new();
    private readonly Dictionary<double, IcebergCandidate> bidIcebergByPrice = new();
    private readonly Dictionary<double, IcebergCandidate> askIcebergByPrice = new();

    private double lastBid;
    private double lastAsk;
    private double lastTradePrice;
    private TapeSide lastSweepSide = TapeSide.Unknown;
    private int sweepLevelCount;
    private DateTime lastSweepUtc;
    private double bid25Ewma;
    private double ask25Ewma;
    private double bidL1Ewma;
    private double askL1Ewma;
    private double tradeRateEwma;
    private double bookUpdateRateEwma;
    private double realizedVolatilityEwma;
    private long sampleCount;
    private long lastWithdrawalSampleCount;
    private DateTime lastSampleUtc;
    private DateTime lastTradeRateUtc;
    private DateTime lastBookUpdateRateUtc;
    private DateTime lastVolatilityUtc;
    private double previousTop3Bid;
    private double previousTop3Ask;
    private List<(double Price, double Size)> previousBidTop3 = new(3);
    private List<(double Price, double Size)> previousAskTop3 = new(3);
    private double lastBid25;
    private double lastAsk25;
    private double lastBidL1;
    private double lastAskL1;
    private double lastBidSupportDepth;
    private double lastAskResistanceDepth;
    private double lastDepthImbalance;
    private double cachedImbalanceUpperThreshold = ImbalanceWarmupAbsoluteThreshold;
    private double cachedImbalanceLowerThreshold = -ImbalanceWarmupAbsoluteThreshold;
    private int recentBidHitCount;
    private int recentOfferLiftCount;
    private AlertKind activeAlert = AlertKind.Monitoring;
    private string activeDirection = string.Empty;

    public Symbol? LinkedSymbol { get; set; }

    public Symbol? DataSymbol { get; set; }

    public string DisplaySymbol { get; set; } = string.Empty;

    public string StatusNote { get; set; } = "Waiting for linked DOM symbol";

    public int SessionStartHHMM { get; set; } = 930;

    public int LevelDepth { get; set; } = 5;

    // Legacy workspace compatibility field. Preserved and displayed, but not used by alert logic.
    public double ImbalanceThreshold { get; set; } = 2.0;

    public double ImbalancePercentileThresholdPct { get; set; } = 95.0;

    public double WithdrawalThresholdPct { get; set; } = 50.0;

    public int SweepWindowSeconds { get; set; } = 2;

    public int SpoofWindowSeconds { get; set; } = 3;

    public bool ShowDiagnostics { get; set; }

    public int AlertFontSize { get; set; } = 16;

    public void ResetForSymbol(Symbol symbol)
    {
        this.DataSymbol = symbol;
        this.bids.Clear();
        this.asks.Clear();
        this.recentPrints.Clear();
        this.displayedBidSizeByPrice.Clear();
        this.displayedAskSizeByPrice.Clear();
        this.bidIcebergByPrice.Clear();
        this.askIcebergByPrice.Clear();
        this.lastBid = symbol.Bid;
        this.lastAsk = symbol.Ask;
        this.lastTradePrice = 0;
        this.lastSweepSide = TapeSide.Unknown;
        this.sweepLevelCount = 0;
        this.lastSweepUtc = default;
        this.bid25Ewma = 0;
        this.ask25Ewma = 0;
        this.bidL1Ewma = 0;
        this.askL1Ewma = 0;
        this.tradeRateEwma = 0;
        this.bookUpdateRateEwma = 0;
        this.realizedVolatilityEwma = 0;
        this.sampleCount = 0;
        this.lastWithdrawalSampleCount = 0;
        this.lastSampleUtc = default;
        this.lastTradeRateUtc = default;
        this.lastBookUpdateRateUtc = default;
        this.lastVolatilityUtc = default;
        this.previousTop3Bid = 0;
        this.previousTop3Ask = 0;
        this.previousBidTop3.Clear();
        this.previousAskTop3.Clear();
        this.lastBid25 = 0;
        this.lastAsk25 = 0;
        this.lastBidL1 = 0;
        this.lastAskL1 = 0;
        this.lastBidSupportDepth = 0;
        this.lastAskResistanceDepth = 0;
        this.lastDepthImbalance = 0;
        this.cachedImbalanceUpperThreshold = ImbalanceWarmupAbsoluteThreshold;
        this.cachedImbalanceLowerThreshold = -ImbalanceWarmupAbsoluteThreshold;
        this.recentBidHitCount = 0;
        this.recentOfferLiftCount = 0;
        this.bidTop3ChangePctWindow.Clear();
        this.askTop3ChangePctWindow.Clear();
        this.imbalanceWindow.Clear();
        this.bidSpoof.Reset();
        this.askSpoof.Reset();
        this.activeAlert = AlertKind.Monitoring;
        this.activeDirection = string.Empty;
    }

    public void OnQuote(Quote quote, DateTime now)
    {
        this.lastBid = quote.Bid;
        this.lastAsk = quote.Ask;
        this.Evaluate(now);
    }

    public void OnLast(Last last, DateTime now)
    {
        var side = this.ClassifyPrint(last);
        this.UpdateTradeRate(now);
        this.UpdateRealizedVolatility(last.Price, now);
        this.lastTradePrice = last.Price;
        this.recentPrints.Enqueue((now, last.Price, side, Math.Max(last.Size, 0)));
        this.PrunePrints(now);
        this.CleanupIcebergCandidates(now);
        this.UpdateIcebergCandidate(last, side, now);
        this.MarkSpoofExecution(last.Price);
        this.Evaluate(now);
    }

    public void OnLevel2(Level2Quote level2, DOMQuote dom, DateTime now)
    {
        this.UpdateBookUpdateRate(now);

        if (dom != null)
        {
            this.RebuildBook(this.bids, dom.Bids, this.LevelDepth);
            this.RebuildBook(this.asks, dom.Asks, this.LevelDepth);
            RebuildDisplayedSizes(this.displayedBidSizeByPrice, dom.Bids);
            RebuildDisplayedSizes(this.displayedAskSizeByPrice, dom.Asks);
        }
        else if (level2 != null)
        {
            var book = level2.PriceType == QuotePriceType.Bid ? this.bids : this.asks;
            var displayedSizes = level2.PriceType == QuotePriceType.Bid
                ? this.displayedBidSizeByPrice
                : this.displayedAskSizeByPrice;
            if (level2.Closed || level2.Size <= 0)
            {
                book.Remove(level2.Price);
                displayedSizes.Remove(level2.Price);
            }
            else
            {
                book[level2.Price] = level2.Size;
                displayedSizes[level2.Price] = Math.Max(0, level2.Size - level2.ImpliedSize);
            }

            this.PruneIncrementalBookState();
        }

        this.SampleBook(now);
        this.Evaluate(now);
    }

    public void SeedOrderBook(DepthOfMarketAggregatedCollections snapshot, DateTime now)
    {
        var bids = snapshot.Bids ?? Array.Empty<Level2Item>();
        var asks = snapshot.Asks ?? Array.Empty<Level2Item>();
        var depth = Math.Clamp(this.LevelDepth, 3, 10);

        this.bids.Clear();
        foreach (var level in bids.Take(depth))
        {
            if (level.Size > 0)
                this.bids[level.Price] = level.Size;
        }

        this.asks.Clear();
        foreach (var level in asks.Take(depth))
        {
            if (level.Size > 0)
                this.asks[level.Price] = level.Size;
        }

        // NOTE: Level2Item (the snapshot type returned by GetDepthOfMarketAggregatedCollections)
        // has no ImpliedSize member - confirmed via reflection against the installed v1.146.12
        // assembly. Unlike the live Level2Quote path (RebuildDisplayedSizes / OnLevel2 incremental),
        // seed-time displayed sizes cannot be implied-size-corrected here. This is a disclosed,
        // self-healing limitation: these values are overwritten with corrected sizes on the first
        // live NewLevel2 event after link (RebuildDisplayedSizes, dom != null branch), so the
        // uncorrected window is limited to the brief gap between link and the first live update.
        this.displayedBidSizeByPrice.Clear();
        foreach (var level in bids)
        {
            if (level.Size > 0)
                this.displayedBidSizeByPrice[level.Price] = level.Size;
        }

        this.displayedAskSizeByPrice.Clear();
        foreach (var level in asks)
        {
            if (level.Size > 0)
                this.displayedAskSizeByPrice[level.Price] = level.Size;
        }

        this.SampleBook(now);
        this.Evaluate(now);
    }

    public DomMonitorPaintSnapshot ToPaintSnapshot()
    {
        var diagnostics = this.ShowDiagnostics ? this.BuildDiagnostics() : Array.Empty<string>();
        return new DomMonitorPaintSnapshot(
            this.DisplaySymbol,
            this.AlertText(),
            this.AlertColor(),
            this.StatusNote,
            this.ShowDiagnostics,
            this.AlertFontSize,
            diagnostics);
    }

    private void RebuildBook(SortedDictionary<double, double> book, IEnumerable<Level2Quote> levels, int depth)
    {
        book.Clear();
        foreach (var level in levels.Take(Math.Clamp(depth, 3, 10)))
        {
            if (!level.Closed && level.Size > 0)
                book[level.Price] = level.Size;
        }
    }

    private static void RebuildDisplayedSizes(Dictionary<double, double> displayedSizes, IEnumerable<Level2Quote> levels)
    {
        displayedSizes.Clear();
        foreach (var level in levels)
        {
            if (!level.Closed && level.Size > 0)
                displayedSizes[level.Price] = Math.Max(0, level.Size - level.ImpliedSize);
        }
    }

    private static void PruneToDepth(SortedDictionary<double, double> book, int maxLevels)
    {
        if (book.Count <= maxLevels)
            return;

        foreach (var price in book.Keys.Skip(maxLevels).ToList())
            book.Remove(price);
    }

    private void PruneDisplayedSizesByDistance(Dictionary<double, double> displayedSizes, double touchPrice)
    {
        if (displayedSizes.Count <= DisplayedSizeRetentionLevels || touchPrice <= 0)
            return;

        var tolerance = this.TickSizeTolerance();
        var maxDistance = tolerance > 0 ? tolerance * 2 * (DisplayedSizeRetentionLevels * 2) : double.MaxValue;

        foreach (var price in displayedSizes.Keys.ToList())
        {
            if (Math.Abs(price - touchPrice) > maxDistance)
                displayedSizes.Remove(price);
        }
    }

    private void PruneIncrementalBookState()
    {
        var maxLevels = Math.Clamp(this.LevelDepth, 3, 10);
        PruneToDepth(this.bids, maxLevels);
        PruneToDepth(this.asks, maxLevels);
        this.PruneDisplayedSizesByDistance(this.displayedBidSizeByPrice, this.lastBid);
        this.PruneDisplayedSizesByDistance(this.displayedAskSizeByPrice, this.lastAsk);
    }

    private TapeSide ClassifyPrint(Last last)
    {
        if (last.AggressorFlag == AggressorFlag.Buy)
            return TapeSide.OfferLifted;
        if (last.AggressorFlag == AggressorFlag.Sell)
            return TapeSide.BidHit;
        if (this.lastAsk > 0 && last.Price >= this.lastAsk)
            return TapeSide.OfferLifted;
        if (this.lastBid > 0 && last.Price <= this.lastBid)
            return TapeSide.BidHit;
        if (this.lastTradePrice > 0 && last.Price > this.lastTradePrice)
            return TapeSide.OfferLifted;
        if (this.lastTradePrice > 0 && last.Price < this.lastTradePrice)
            return TapeSide.BidHit;

        return TapeSide.Unknown;
    }

    private void UpdateIcebergCandidate(Last last, TapeSide side, DateTime now)
    {
        if (side == TapeSide.Unknown)
            return;

        var displayedSizes = side == TapeSide.BidHit
            ? this.displayedBidSizeByPrice
            : this.displayedAskSizeByPrice;
        var candidates = side == TapeSide.BidHit
            ? this.bidIcebergByPrice
            : this.askIcebergByPrice;

        if (!displayedSizes.TryGetValue(last.Price, out var displayedSize) || displayedSize <= 0 || last.Size <= displayedSize)
            return;

        var windowSeconds = this.AdaptiveSweepWindowSeconds();
        if (!candidates.TryGetValue(last.Price, out var candidate) ||
            (now - candidate.FirstTradeUtc).TotalSeconds > windowSeconds)
        {
            candidate = new IcebergCandidate(displayedSize, last.Size, now, 1);
        }
        else
        {
            candidate = candidate with
            {
                ExecutedVolume = candidate.ExecutedVolume + last.Size,
                TrancheCount = candidate.TrancheCount + 1
            };
        }

        candidates[last.Price] = candidate;
        if (candidate.TrancheCount < 2)
            return;

        this.sweepLevelCount = candidate.TrancheCount;
        this.lastSweepSide = side;
        this.lastSweepUtc = now;
    }

    private void CleanupIcebergCandidates(DateTime now)
    {
        var maxAgeSeconds = Math.Max(1, this.SweepWindowSeconds) * 2.0;
        RemoveExpiredIcebergCandidates(this.bidIcebergByPrice, now, maxAgeSeconds);
        RemoveExpiredIcebergCandidates(this.askIcebergByPrice, now, maxAgeSeconds);
    }

    private static void RemoveExpiredIcebergCandidates(
        Dictionary<double, IcebergCandidate> candidates,
        DateTime now,
        double maxAgeSeconds)
    {
        foreach (var price in candidates
                     .Where(item => (now - item.Value.FirstTradeUtc).TotalSeconds > maxAgeSeconds)
                     .Select(item => item.Key)
                     .ToArray())
        {
            candidates.Remove(price);
        }
    }

    private void MarkSpoofExecution(double price)
    {
        var tolerance = this.TickSizeTolerance();
        if (this.bidSpoof.Active && Math.Abs(price - this.bidSpoof.Price) <= tolerance)
            this.bidSpoof.Executed = true;
        if (this.askSpoof.Active && Math.Abs(price - this.askSpoof.Price) <= tolerance)
            this.askSpoof.Executed = true;
    }

    private void SampleBook(DateTime now)
    {
        if (this.sampleCount > 0 && (now - this.lastSampleUtc).TotalMilliseconds < 250)
            return;

        var elapsedSeconds = this.sampleCount > 0
            ? Math.Max((now - this.lastSampleUtc).TotalSeconds, 0.001)
            : 0.001;

        this.lastSampleUtc = now;
        this.lastBid25 = SumLevels(this.bids, 2, this.LevelDepth);
        this.lastAsk25 = SumLevels(this.asks, 2, this.LevelDepth);
        this.lastBidL1 = FirstLevelSize(this.bids);
        this.lastAskL1 = FirstLevelSize(this.asks);
        this.lastBidSupportDepth = SumLevels(this.bids, 1, this.LevelDepth);
        this.lastAskResistanceDepth = SumLevels(this.asks, 1, this.LevelDepth);
        this.lastDepthImbalance = NormalizedDepthImbalance(this.lastBidSupportDepth, this.lastAskResistanceDepth);

        if (this.sampleCount == 0)
        {
            this.bid25Ewma = this.lastBid25;
            this.ask25Ewma = this.lastAsk25;
            this.bidL1Ewma = this.lastBidL1;
            this.askL1Ewma = this.lastAskL1;
        }
        else
        {
            UpdateEwma(ref this.bid25Ewma, this.lastBid25, elapsedSeconds, DepthEwmaSeconds);
            UpdateEwma(ref this.ask25Ewma, this.lastAsk25, elapsedSeconds, DepthEwmaSeconds);
            UpdateEwma(ref this.bidL1Ewma, this.lastBidL1, elapsedSeconds, DepthEwmaSeconds);
            UpdateEwma(ref this.askL1Ewma, this.lastAskL1, elapsedSeconds, DepthEwmaSeconds);
        }

        this.sampleCount++;
    }

    private void Evaluate(DateTime now)
    {
        this.PrunePrints(now);
        this.recentBidHitCount = this.CountRecent(TapeSide.BidHit, now, 3);
        this.recentOfferLiftCount = this.CountRecent(TapeSide.OfferLifted, now, 3);

        var bidTop3 = SumLevels(this.bids, 1, 3);
        var askTop3 = SumLevels(this.asks, 1, 3);
        var currentBidTop3 = TopLevels(this.bids, 3);
        var currentAskTop3 = TopLevels(this.asks, 3);
        var hasNewBookSample = this.sampleCount > this.lastWithdrawalSampleCount;
        var bidTop3ChangePct = this.previousTop3Bid > 0 ? (bidTop3 - this.previousTop3Bid) / this.previousTop3Bid : 0;
        var askTop3ChangePct = this.previousTop3Ask > 0 ? (askTop3 - this.previousTop3Ask) / this.previousTop3Ask : 0;
        var withdrawal = this.DetectWithdrawal(
            bidTop3,
            askTop3,
            currentBidTop3,
            currentAskTop3,
            hasNewBookSample,
            bidTop3ChangePct,
            askTop3ChangePct,
            now,
            out var withdrawalDirection);
        var spoof = this.DetectSpoof(now);
        var adaptiveSweepWindowSeconds = this.AdaptiveSweepWindowSeconds();
        if (hasNewBookSample)
        {
            this.cachedImbalanceUpperThreshold = this.AdaptiveImbalanceUpperThreshold();
            this.cachedImbalanceLowerThreshold = this.AdaptiveImbalanceLowerThreshold();
        }

        var imbalanceUpperThreshold = this.cachedImbalanceUpperThreshold;
        var imbalanceLowerThreshold = this.cachedImbalanceLowerThreshold;

        if (this.sweepLevelCount >= 2 && (now - this.lastSweepUtc).TotalSeconds <= adaptiveSweepWindowSeconds)
        {
            this.activeAlert = AlertKind.SweepDetected;
            this.activeDirection = this.lastSweepSide == TapeSide.BidHit ? "BID" : "ASK";
        }
        else if (withdrawal)
        {
            this.activeAlert = AlertKind.LiquidityWithdrawal;
            this.activeDirection = withdrawalDirection;
        }
        else if (this.sampleCount >= 5 && this.lastDepthImbalance >= imbalanceUpperThreshold && this.recentBidHitCount > 0)
        {
            this.activeAlert = AlertKind.RealBidSupport;
            this.activeDirection = string.Empty;
        }
        else if (this.sampleCount >= 5 && this.lastDepthImbalance <= imbalanceLowerThreshold && this.recentOfferLiftCount > 0)
        {
            this.activeAlert = AlertKind.RealAskResistance;
            this.activeDirection = string.Empty;
        }
        else if (spoof != AlertKind.Monitoring)
        {
            this.activeAlert = spoof;
            this.activeDirection = string.Empty;
        }
        else
        {
            this.activeAlert = AlertKind.Monitoring;
            this.activeDirection = string.Empty;
        }

        if (hasNewBookSample)
        {
            if (this.previousTop3Bid > 0)
                this.bidTop3ChangePctWindow.Add(bidTop3ChangePct);
            if (this.previousTop3Ask > 0)
                this.askTop3ChangePctWindow.Add(askTop3ChangePct);
            if (this.lastBidSupportDepth + this.lastAskResistanceDepth > 0)
                this.imbalanceWindow.Add(this.lastDepthImbalance);

            this.lastWithdrawalSampleCount = this.sampleCount;
        }

        this.previousTop3Bid = bidTop3;
        this.previousTop3Ask = askTop3;
        this.previousBidTop3 = currentBidTop3;
        this.previousAskTop3 = currentAskTop3;
    }

    private bool DetectWithdrawal(
        double bidTop3,
        double askTop3,
        List<(double Price, double Size)> currentBidTop3,
        List<(double Price, double Size)> currentAskTop3,
        bool hasNewBookSample,
        double bidTop3ChangePct,
        double askTop3ChangePct,
        DateTime now,
        out string direction)
    {
        direction = string.Empty;
        if (!hasNewBookSample)
            return false;

        var bidThreshold = this.AdaptiveWithdrawalChangeThreshold(this.bidTop3ChangePctWindow);
        var askThreshold = this.AdaptiveWithdrawalChangeThreshold(this.askTop3ChangePctWindow);
        var bidDrop = this.previousTop3Bid > 0 && bidTop3ChangePct <= bidThreshold;
        var askDrop = this.previousTop3Ask > 0 && askTop3ChangePct <= askThreshold;

        var bidWithdrawnLevels = bidDrop
            ? FindWithdrawnLevels(this.previousBidTop3, currentBidTop3)
            : new List<double>();

        var askWithdrawnLevels = askDrop
            ? FindWithdrawnLevels(this.previousAskTop3, currentAskTop3)
            : new List<double>();

        var bidQualifies = bidDrop && !this.HasRecentExecutionAtTopLevels(bidWithdrawnLevels, now);
        var askQualifies = askDrop && !this.HasRecentExecutionAtTopLevels(askWithdrawnLevels, now);

        if (!bidQualifies && !askQualifies)
            return false;

        direction = bidQualifies ? "BID" : "ASK";
        return true;
    }

    private AlertKind DetectSpoof(DateTime now)
    {
        this.UpdateSpoofCandidate(this.bidSpoof, this.bids, this.bidL1Ewma, now);
        this.UpdateSpoofCandidate(this.askSpoof, this.asks, this.askL1Ewma, now);

        if (this.SpoofResolved(this.bidSpoof, this.bids, now))
            return AlertKind.SpoofingBid;
        if (this.SpoofResolved(this.askSpoof, this.asks, now))
            return AlertKind.SpoofingAsk;

        return AlertKind.Monitoring;
    }

    private void UpdateSpoofCandidate(SpoofCandidate candidate, SortedDictionary<double, double> book, double average, DateTime now)
    {
        if (this.sampleCount < 5 || average <= 0 || book.Count == 0)
            return;

        var price = FirstLevelPrice(book);
        var size = FirstLevelSize(book);
        if (!candidate.Active && size >= average * 3.0)
        {
            candidate.Price = price;
            candidate.Size = size;
            candidate.StartedUtc = now;
            candidate.TimeoutSeconds = this.AdaptiveSpoofWindowSeconds();
            candidate.Executed = false;
            candidate.Active = true;
        }
    }

    private bool SpoofResolved(SpoofCandidate candidate, SortedDictionary<double, double> book, DateTime now)
    {
        if (!candidate.Active)
            return false;

        var expired = (now - candidate.StartedUtc).TotalSeconds >= candidate.TimeoutSeconds;
        var currentAtPrice = book.TryGetValue(candidate.Price, out var currentSize) ? currentSize : 0;
        var vanished = currentAtPrice <= candidate.Size * 0.2;
        if (expired && !candidate.Executed && vanished)
        {
            candidate.Active = false;
            return true;
        }

        if (candidate.Executed || (expired && !vanished))
            candidate.Active = false;

        return false;
    }

    private bool HasRecentExecutionAtTopLevels(List<double> withdrawnLevels, DateTime now)
    {
        if (withdrawnLevels.Count == 0)
            return false;

        var tolerance = this.TickSizeTolerance();
        foreach (var print in this.recentPrints)
        {
            if ((now - print.Utc).TotalSeconds > 2)
                continue;
            if (withdrawnLevels.Any(price => Math.Abs(price - print.Price) <= tolerance))
                return true;
        }

        return false;
    }

    private static List<double> FindWithdrawnLevels(List<(double Price, double Size)> previous, List<(double Price, double Size)> current)
    {
        var withdrawn = new List<double>(previous.Count);
        foreach (var prior in previous)
        {
            var currentSize = 0d;
            foreach (var now in current)
            {
                if (Math.Abs(now.Price - prior.Price) < 0.0000001)
                {
                    currentSize = now.Size;
                    break;
                }
            }

            if (currentSize < prior.Size)
                withdrawn.Add(prior.Price);
        }

        return withdrawn;
    }

    private int CountRecent(TapeSide side, DateTime now, int seconds)
    {
        var count = 0;
        foreach (var print in this.recentPrints)
        {
            if ((now - print.Utc).TotalSeconds <= seconds && print.Side == side)
                count++;
        }

        return count;
    }

    private void PrunePrints(DateTime now)
    {
        while (this.recentPrints.Count > 0 && (now - this.recentPrints.Peek().Utc).TotalSeconds > 10)
            this.recentPrints.Dequeue();
    }

    private string AlertText() => this.activeAlert switch
    {
        AlertKind.SweepDetected => $"{this.activeDirection} SWEEP",
        AlertKind.LiquidityWithdrawal => $"{this.activeDirection} LIQUIDITY WITHDRAWAL",
        AlertKind.RealBidSupport => "BID SUPPORT",
        AlertKind.RealAskResistance => "ASK RESISTANCE",
        AlertKind.SpoofingBid => "BID PULL",
        AlertKind.SpoofingAsk => "ASK PULL",
        _ => "MONITORING"
    };

    private Color AlertColor() => this.activeAlert switch
    {
        AlertKind.SweepDetected => Color.FromArgb(255, 55, 55),
        AlertKind.LiquidityWithdrawal => Color.FromArgb(255, 150, 35),
        AlertKind.RealBidSupport => Color.FromArgb(0, 220, 90),
        AlertKind.RealAskResistance => Color.FromArgb(255, 70, 70),
        AlertKind.SpoofingBid => Color.FromArgb(245, 220, 35),
        AlertKind.SpoofingAsk => Color.FromArgb(245, 220, 35),
        _ => Color.FromArgb(170, 170, 170)
    };

    private string[] BuildDiagnostics()
    {
        var lines = new List<string>
        {
            $"DATA: dxFeed only",
            $"BID L1-{this.LevelDepth}: {this.lastBidSupportDepth:N0}  IMB: {this.lastDepthImbalance:0.000}",
            $"ASK L1-{this.LevelDepth}: {this.lastAskResistanceDepth:N0}  UP/DN: {this.cachedImbalanceUpperThreshold:0.000}/{this.cachedImbalanceLowerThreshold:0.000}",
            $"BID L2-5: {this.lastBid25:N0}  EWMA: {this.bid25Ewma:N0}",
            $"ASK L2-5: {this.lastAsk25:N0}  EWMA: {this.ask25Ewma:N0}",
            $"L1 BID: {this.lastBidL1:N0}  EWMA: {this.bidL1Ewma:N0}",
            $"L1 ASK: {this.lastAskL1:N0}  EWMA: {this.askL1Ewma:N0}",
            $"RATES T/S: {this.tradeRateEwma:0.00}/s  BOOK: {this.bookUpdateRateEwma:0.00}/s",
            $"RV EWMA: {this.realizedVolatilityEwma:0.000000}",
            $"T&S BID HIT: {this.recentBidHitCount}  OFFER LIFT: {this.recentOfferLiftCount}",
            $"ICEBERG TRANCHES: {this.sweepLevelCount}",
            $"SAMPLES: {this.sampleCount}"
        };

        var depth = Math.Clamp(this.LevelDepth, 3, 10);
        var bidLevels = TopLevels(this.bids, depth);
        var askLevels = TopLevels(this.asks, depth);
        for (var i = 0; i < depth; i++)
        {
            var bid = i < bidLevels.Count ? $"{bidLevels[i].Price:0.####}/{bidLevels[i].Size:N0}" : "--";
            var ask = i < askLevels.Count ? $"{askLevels[i].Price:0.####}/{askLevels[i].Size:N0}" : "--";
            lines.Add($"L{i + 1}: B {bid}  A {ask}");
        }

        return lines.ToArray();
    }

    private void UpdateTradeRate(DateTime now)
    {
        if (this.lastTradeRateUtc != default)
        {
            var elapsedSeconds = Math.Max((now - this.lastTradeRateUtc).TotalSeconds, 0.001);
            var eventsPerSecond = 1.0 / elapsedSeconds;
            UpdateEwma(ref this.tradeRateEwma, eventsPerSecond, elapsedSeconds, TradeRateEwmaSeconds);
        }

        this.lastTradeRateUtc = now;
    }

    private void UpdateBookUpdateRate(DateTime now)
    {
        if (this.lastBookUpdateRateUtc != default)
        {
            var elapsedSeconds = Math.Max((now - this.lastBookUpdateRateUtc).TotalSeconds, 0.001);
            var eventsPerSecond = 1.0 / elapsedSeconds;
            UpdateEwma(ref this.bookUpdateRateEwma, eventsPerSecond, elapsedSeconds, BookRateEwmaSeconds);
        }

        this.lastBookUpdateRateUtc = now;
    }

    private void UpdateRealizedVolatility(double price, DateTime now)
    {
        if (price <= 0 || this.lastTradePrice <= 0)
            return;

        var elapsedSeconds = this.lastVolatilityUtc != default
            ? Math.Max((now - this.lastVolatilityUtc).TotalSeconds, 0.001)
            : 0.001;
        var absoluteLogReturn = Math.Abs(Math.Log(price / this.lastTradePrice));
        UpdateEwma(ref this.realizedVolatilityEwma, absoluteLogReturn, elapsedSeconds, RealizedVolatilityEwmaSeconds);
        this.lastVolatilityUtc = now;
    }

    private double AdaptiveSweepWindowSeconds()
    {
        var lambda = Math.Max(this.tradeRateEwma, 0.01);
        var adaptive = SweepEventBudget / lambda;
        var maxSeconds = Math.Max(1.0, this.SweepWindowSeconds);
        return Math.Clamp(adaptive, SweepWindowMinSeconds, maxSeconds);
    }

    private double AdaptiveSpoofWindowSeconds()
    {
        var lambda = Math.Max(this.bookUpdateRateEwma, 0.01);
        var adaptive = SpoofBookUpdateBudget / lambda;
        var maxSeconds = Math.Max(1.0, this.SpoofWindowSeconds);
        return Math.Clamp(adaptive, SpoofWindowMinSeconds, maxSeconds);
    }

    private double AdaptiveWithdrawalChangeThreshold(RollingDoubleWindow changes)
    {
        if (changes.Count < WithdrawalWarmupSamples)
            return -Math.Clamp(this.WithdrawalThresholdPct, 20.0, 80.0) / 100.0;

        return changes.Percentile(WithdrawalLowerPercentile);
    }

    private double AdaptiveImbalanceUpperThreshold()
    {
        var percentile = Math.Clamp(this.ImbalancePercentileThresholdPct, 50.0, 99.0) / 100.0;
        if (this.imbalanceWindow.Count < ImbalanceWarmupSamples)
            return ImbalanceWarmupAbsoluteThreshold;

        return Math.Max(0.0, this.imbalanceWindow.Percentile(percentile));
    }

    private double AdaptiveImbalanceLowerThreshold()
    {
        var lowerPercentile = 1.0 - (Math.Clamp(this.ImbalancePercentileThresholdPct, 50.0, 99.0) / 100.0);
        if (this.imbalanceWindow.Count < ImbalanceWarmupSamples)
            return -ImbalanceWarmupAbsoluteThreshold;

        return Math.Min(0.0, this.imbalanceWindow.Percentile(lowerPercentile));
    }

    private static void UpdateEwma(ref double ewma, double observation, double elapsedSeconds, double ewmaSeconds)
    {
        if (ewma <= 0)
        {
            ewma = observation;
            return;
        }

        var alpha = 1.0 - Math.Exp(-Math.Max(elapsedSeconds, 0.001) / Math.Max(ewmaSeconds, 0.001));
        ewma += alpha * (observation - ewma);
    }

    private static double NormalizedDepthImbalance(double bidDepth, double askDepth)
    {
        var denominator = bidDepth + askDepth;
        if (denominator <= 0)
            return 0;

        return (bidDepth - askDepth) / denominator;
    }

    private double TickSizeTolerance()
    {
        var tickSize = this.DataSymbol?.TickSize ?? 0.01;
        return tickSize > 0 ? tickSize * 0.5 : 0.005;
    }

    private static double SumLevels(SortedDictionary<double, double> book, int startLevel, int endLevel)
    {
        var sum = 0d;
        var level = 1;
        foreach (var kvp in book)
        {
            if (level >= startLevel && level <= endLevel)
                sum += kvp.Value;
            if (level > endLevel)
                break;
            level++;
        }

        return sum;
    }

    private static double FirstLevelSize(SortedDictionary<double, double> book)
    {
        foreach (var kvp in book)
            return kvp.Value;
        return 0;
    }

    private static double FirstLevelPrice(SortedDictionary<double, double> book)
    {
        foreach (var kvp in book)
            return kvp.Key;
        return 0;
    }

    private static List<(double Price, double Size)> TopLevels(SortedDictionary<double, double> book, int depth)
    {
        var result = new List<(double Price, double Size)>(depth);
        foreach (var kvp in book)
        {
            result.Add((kvp.Key, kvp.Value));
            if (result.Count >= depth)
                break;
        }

        return result;
    }

    private sealed record IcebergCandidate(
        double DisplayedSize,
        double ExecutedVolume,
        DateTime FirstTradeUtc,
        int TrancheCount);

    private sealed class RollingDoubleWindow
    {
        private readonly double[] values;
        private int nextIndex;

        public RollingDoubleWindow(int capacity)
        {
            this.values = new double[Math.Max(1, capacity)];
        }

        public int Count { get; private set; }

        public void Add(double value)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
                return;

            this.values[this.nextIndex] = value;
            this.nextIndex = (this.nextIndex + 1) % this.values.Length;
            if (this.Count < this.values.Length)
                this.Count++;
        }

        public void Clear()
        {
            Array.Clear(this.values, 0, this.values.Length);
            this.nextIndex = 0;
            this.Count = 0;
        }

        public double Percentile(double percentile)
        {
            if (this.Count == 0)
                return 0;

            var copy = new double[this.Count];
            Array.Copy(this.values, copy, this.Count);
            Array.Sort(copy);

            var clamped = Math.Clamp(percentile, 0.0, 1.0);
            var index = (int)Math.Round((copy.Length - 1) * clamped, MidpointRounding.AwayFromZero);
            return copy[Math.Clamp(index, 0, copy.Length - 1)];
        }
    }

    private sealed class SpoofCandidate
    {
        public double Price;
        public double Size;
        public DateTime StartedUtc;
        public double TimeoutSeconds;
        public bool Executed;
        public bool Active;

        public void Reset()
        {
            this.Price = 0;
            this.Size = 0;
            this.StartedUtc = default;
            this.TimeoutSeconds = 0;
            this.Executed = false;
            this.Active = false;
        }
    }
}

internal readonly record struct DomMonitorPaintSnapshot(
    string Symbol,
    string AlertText,
    Color AlertColor,
    string StatusNote,
    bool ShowDiagnostics,
    int AlertFontSize,
    string[] Diagnostics);
