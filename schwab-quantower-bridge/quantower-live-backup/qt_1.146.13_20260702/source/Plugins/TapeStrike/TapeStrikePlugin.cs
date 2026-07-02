using System.Drawing;
using System.Media;
using System.Security.Cryptography;
using System.Text;
using TradingPlatform.BusinessLayer;
using TradingPlatform.PresentationLayer.Plugins;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking.EventArgs;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking.Models.Scopes;
using TradingPlatform.PresentationLayer.Renderers.Table;

namespace TapeStrike;

public sealed class TapeStrikePlugin : TablePlugin, ILinkable, ILinkingArgumentHandler, ILinkingArgumentProvider
{
    private readonly object stateLock = new();
    private readonly object soundLock = new();
    private readonly Queue<DateTime> recentBurstPrints = new();
    private LinkingPluginService? linkingService;
    private System.Threading.Timer? uiTimer;
    private Symbol? subscribedSymbol;
    private readonly Dictionary<string, SoundPlayer> soundPlayers = new(StringComparer.OrdinalIgnoreCase);
    private DateTime lastTapeUtc;
    private DateTime lastTickSoundUtc;
    private DateTime lastBurstSoundUtc;
    private DateTime lastLargeTradeSoundUtc;
    private int printsLastCheck;
    private double tapeSpeedPerSecond;
    private double lastPrintSize;
    private double lastAudiblePrintSize;
    private double lastAudiblePrintPrice;
    private DateTime lastAudiblePrintUtc;
    private long totalPrints;
    private long audiblePrints;
    private bool brokerAlertTranslatorRegistered;
    private DateTime lastIbkrOrderUtc;
    private DateTime lastBrokerAlertUtc;
    private string lastIbkrOrderSymbol = string.Empty;
    private string lastBrokerAlertKey = string.Empty;
    private string status = "Set symbol or link panel to Time & Sales symbol.";
    private TapeStrikeSoundKind statusKind = TapeStrikeSoundKind.None;
    private DateTime statusHoldUntilUtc;

    public static PluginInfo GetInfo() => new()
    {
        Name = "TapeStrike",
        Title = loc.key("TapeStrike"),
        Group = PluginGroup.Analytics,
        ShortName = "TStr",
        WindowParameters = new NativeWindowParameters(NativeWindowParameters.Panel)
        {
            BrowserUsageType = BrowserUsageType.None
        },
        CustomProperties = new Dictionary<string, object>
        {
            { PluginInfo.Const.ALLOW_MANUAL_CREATION, true }
        }
    };

    protected override TableItem AssociatedTableItem => new TapeStrikeTableItem();

    public override Size DefaultSize => new(this.UnitSize.Width * 2, this.UnitSize.Height);

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

    private string SymbolName { get; set; } = "INTC";

    private bool EnableTickAudio { get; set; }

    private string TickSoundFile { get; set; } = string.Empty;

    private int TickMinimumTradeSize { get; set; } = 10;

    private int TickSpacingMs { get; set; } = 0;

    private int BurstSpeedWindowMs { get; set; } = 1000;

    private bool EnableBurstAudio { get; set; }

    private int BurstCleanSpacingMs { get; set; } = 25;

    private double BurstPrintsPerSecond { get; set; } = 8.0;

    private int BurstMinimumTradeSize { get; set; } = 1;

    private string BurstSoundFile { get; set; } = string.Empty;

    private int TickAudioVolumePercent { get; set; } = 100;

    private int BurstAudioVolumePercent { get; set; } = 60;

    private bool EnableLargeTradeAudio { get; set; }

    private int LargeTradeSize { get; set; } = 100;

    private int LargeTradeSpacingMs { get; set; } = 0;

    private string LargeTradeSoundFile { get; set; } = string.Empty;

    private int LargeTradeAudioVolumePercent { get; set; } = 60;

    private bool PlayTestTickSound { get; set; }

    private bool PlayTestBurstSound { get; set; }

    private bool PlayTestLargeTradeSound { get; set; }

    private int SymbolColumnWidth { get; set; } = 120;

    private int AudioColumnWidth { get; set; } = 120;

    private int StatusColumnWidth { get; set; } = 360;

    public override IList<SettingItem> Settings
    {
        get
        {
            var settings = base.Settings;
            settings.Add(new SettingItemString("SymbolName", this.SymbolName, 0) { Text = "Symbol" });

            var normalGroup = new SettingItemSeparatorGroup("TICK AUDIO", 100) { DefaultExpandedState = true };
            var burstGroup = new SettingItemSeparatorGroup("MID TICK AUDIO", 200) { DefaultExpandedState = true };
            var largeGroup = new SettingItemSeparatorGroup("LARGE-PRINT AUDIO", 300) { DefaultExpandedState = true };
            var columnsGroup = new SettingItemSeparatorGroup("COLUMN WIDTHS", 400) { DefaultExpandedState = false };

            AddGrouped(settings, new SettingItemBoolean("EnableTickAudio", this.EnableTickAudio, 101) { Text = "Enable audio" }, normalGroup);
            AddGrouped(settings, new SettingItemLabel("TickSoundFileLabel", "Sound file (.wav)", 102), normalGroup);
            AddGrouped(settings, new SettingItemFile("TickSoundFile", this.TickSoundFile, "wav files (.wav)|*.wav", 103) { Text = "Sound file (.wav)" }, normalGroup);
            AddGrouped(settings, new SettingItemInteger("TickAudioVolumePercent", this.TickAudioVolumePercent, 104) { Text = "Audio volume %" }, normalGroup);
            AddGrouped(settings, new SettingItemInteger("TickMinimumTradeSize", this.TickMinimumTradeSize, 105) { Text = "Trigger minimum trade size" }, normalGroup);
            AddGrouped(settings, new SettingItemInteger("TickSpacingMs", this.TickSpacingMs, 106) { Text = "Spacing ms" }, normalGroup);
            AddGrouped(settings, new SettingItemBoolean("PlayTestTickSound", this.PlayTestTickSound, 107) { Text = "Play test sound" }, normalGroup);

            AddGrouped(settings, new SettingItemBoolean("EnableBurstAudio", this.EnableBurstAudio, 201) { Text = "Enable audio" }, burstGroup);
            AddGrouped(settings, new SettingItemLabel("BurstSoundFileLabel", "Sound file (.wav)", 202), burstGroup);
            AddGrouped(settings, new SettingItemFile("BurstSoundFile", this.BurstSoundFile, "wav files (.wav)|*.wav", 203) { Text = "Sound file (.wav)" }, burstGroup);
            AddGrouped(settings, new SettingItemInteger("BurstAudioVolumePercent", this.BurstAudioVolumePercent, 204) { Text = "Audio volume %" }, burstGroup);
            AddGrouped(settings, new SettingItemInteger("BurstMinimumTradeSize", this.BurstMinimumTradeSize, 205) { Text = "Trigger minimum trade size" }, burstGroup);
            AddGrouped(settings, new SettingItemDouble("BurstPrintsPerSecond", this.BurstPrintsPerSecond, 206) { Text = "Trigger prints/sec" }, burstGroup);
            AddGrouped(settings, new SettingItemInteger("BurstCleanSpacingMs", this.BurstCleanSpacingMs, 207) { Text = "Spacing ms" }, burstGroup);
            AddGrouped(settings, new SettingItemBoolean("PlayTestBurstSound", this.PlayTestBurstSound, 208) { Text = "Play test sound" }, burstGroup);

            AddGrouped(settings, new SettingItemBoolean("EnableLargeTradeAudio", this.EnableLargeTradeAudio, 301) { Text = "Enable audio" }, largeGroup);
            AddGrouped(settings, new SettingItemLabel("LargeTradeSoundFileLabel", "Sound file (.wav)", 302), largeGroup);
            AddGrouped(settings, new SettingItemFile("LargeTradeSoundFile", this.LargeTradeSoundFile, "wav files (.wav)|*.wav", 303) { Text = "Sound file (.wav)" }, largeGroup);
            AddGrouped(settings, new SettingItemInteger("LargeTradeAudioVolumePercent", this.LargeTradeAudioVolumePercent, 304) { Text = "Audio volume %" }, largeGroup);
            AddGrouped(settings, new SettingItemInteger("LargeTradeSize", this.LargeTradeSize, 305) { Text = "Trigger trade size" }, largeGroup);
            AddGrouped(settings, new SettingItemInteger("LargeTradeSpacingMs", this.LargeTradeSpacingMs, 306) { Text = "Spacing ms" }, largeGroup);
            AddGrouped(settings, new SettingItemBoolean("PlayTestLargeTradeSound", this.PlayTestLargeTradeSound, 307) { Text = "Play test sound" }, largeGroup);

            AddGrouped(settings, new SettingItemInteger("SymbolColumnWidth", this.SymbolColumnWidth, 401) { Text = "Symbol width" }, columnsGroup);
            AddGrouped(settings, new SettingItemInteger("AudioColumnWidth", this.AudioColumnWidth, 402) { Text = "Audio width" }, columnsGroup);
            AddGrouped(settings, new SettingItemInteger("StatusColumnWidth", this.StatusColumnWidth, 403) { Text = "Status width" }, columnsGroup);

            if (this.linkingService != null)
            {
                foreach (var linkingSetting in this.linkingService.Settings)
                    settings.Add(linkingSetting);
            }

            return settings;
        }
        set
        {
            var holder = new SettingsHolder(value);
            var previousSymbolName = this.SymbolName;
            base.Settings = value;
            if (this.linkingService != null)
                this.linkingService.Settings = value;

            if (holder.TryGetValue("SymbolName", out var item) && item.Value is string symbol)
                this.SymbolName = symbol.Trim();

            if (holder.TryGetValue("EnableTickAudio", out item) && item.Value is bool enableTickAudio)
                this.EnableTickAudio = enableTickAudio;
            else if (holder.TryGetValue("EnableAudio", out item) && item.Value is bool legacyEnableAudio)
                this.EnableTickAudio = legacyEnableAudio;

            if (holder.TryGetValue("TickSoundFile", out item) && item.Value is string tickSoundFile)
                this.TickSoundFile = tickSoundFile.Trim();
            else if (holder.TryGetValue("SoundFile", out item) && item.Value is string legacySoundFile)
                this.TickSoundFile = legacySoundFile.Trim();

            if (holder.TryGetValue("EnableBurstAudio", out item) && item.Value is bool enableBurstAudio)
                this.EnableBurstAudio = enableBurstAudio;
            else if (holder.TryGetValue("EnableBurstCleanSpacing", out item) && item.Value is bool legacyEnableBurstAudio)
                this.EnableBurstAudio = legacyEnableBurstAudio;

            if (holder.TryGetValue("BurstSoundFile", out item) && item.Value is string burstSoundFile)
                this.BurstSoundFile = burstSoundFile.Trim();

            if (holder.TryGetValue("BurstAudioVolumePercent", out item) && item.Value is int burstAudioVolumePercent)
                this.BurstAudioVolumePercent = Math.Clamp(burstAudioVolumePercent, 1, 100);

            if (holder.TryGetValue("BurstMinimumTradeSize", out item) && item.Value is int burstMinimumTradeSize)
                this.BurstMinimumTradeSize = Math.Clamp(burstMinimumTradeSize, 1, 10_000_000);

            if (holder.TryGetValue("BurstPrintsPerSecond", out item) && item.Value is double burstPrintsPerSecond)
                this.BurstPrintsPerSecond = Math.Clamp(burstPrintsPerSecond, 1d, 100d);

            if (holder.TryGetValue("BurstSpeedWindowMs", out item) && item.Value is int burstSpeedWindowMs)
                this.BurstSpeedWindowMs = Math.Clamp(burstSpeedWindowMs, 250, 5000);
            else if (holder.TryGetValue("SpeedWindowMs", out item) && item.Value is int legacySpeedWindowMs)
                this.BurstSpeedWindowMs = Math.Clamp(legacySpeedWindowMs, 250, 5000);

            if (holder.TryGetValue("BurstCleanSpacingMs", out item) && item.Value is int burstCleanSpacingMs)
                this.BurstCleanSpacingMs = Math.Clamp(burstCleanSpacingMs, 0, 250);

            if (holder.TryGetValue("TickAudioVolumePercent", out item) && item.Value is int tickAudioVolumePercent)
                this.TickAudioVolumePercent = Math.Clamp(tickAudioVolumePercent, 1, 100);
            else if (holder.TryGetValue("AudioVolumePercent", out item) && item.Value is int legacyAudioVolumePercent)
                this.TickAudioVolumePercent = Math.Clamp(legacyAudioVolumePercent, 1, 100);

            if (holder.TryGetValue("TickMinimumTradeSize", out item) && item.Value is int tickMinimumTradeSize)
                this.TickMinimumTradeSize = Math.Clamp(tickMinimumTradeSize, 1, 10_000_000);
            else if (holder.TryGetValue("MinimumTradeSize", out item) && item.Value is int legacyMinimumTradeSize)
                this.TickMinimumTradeSize = Math.Clamp(legacyMinimumTradeSize, 1, 10_000_000);

            if (holder.TryGetValue("TickSpacingMs", out item) && item.Value is int tickSpacingMs)
                this.TickSpacingMs = Math.Clamp(tickSpacingMs, 0, 250);

            if (holder.TryGetValue("EnableLargeTradeAudio", out item) && item.Value is bool enableLargeTradeAudio)
                this.EnableLargeTradeAudio = enableLargeTradeAudio;

            if (holder.TryGetValue("LargeTradeSoundFile", out item) && item.Value is string largeTradeSoundFile)
                this.LargeTradeSoundFile = largeTradeSoundFile.Trim();

            if (holder.TryGetValue("LargeTradeAudioVolumePercent", out item) && item.Value is int largeTradeAudioVolumePercent)
                this.LargeTradeAudioVolumePercent = Math.Clamp(largeTradeAudioVolumePercent, 1, 100);

            if (holder.TryGetValue("LargeTradeSize", out item) && item.Value is int largeTradeSize)
                this.LargeTradeSize = Math.Clamp(largeTradeSize, 1, 10_000_000);

            if (holder.TryGetValue("LargeTradeSpacingMs", out item) && item.Value is int largeTradeSpacingMs)
                this.LargeTradeSpacingMs = Math.Clamp(largeTradeSpacingMs, 0, 1000);

            if (holder.TryGetValue("SymbolColumnWidth", out item) && item.Value is int symbolColumnWidth)
                this.SymbolColumnWidth = Math.Clamp(symbolColumnWidth, 40, 600);

            if (holder.TryGetValue("AudioColumnWidth", out item) && item.Value is int audioColumnWidth)
                this.AudioColumnWidth = Math.Clamp(audioColumnWidth, 40, 600);

            if (holder.TryGetValue("StatusColumnWidth", out item) && item.Value is int statusColumnWidth)
                this.StatusColumnWidth = Math.Clamp(statusColumnWidth, 80, 1200);

            TapeStrikeTableItem.SetColumnWidths(this.SymbolColumnWidth, this.AudioColumnWidth, this.StatusColumnWidth);

            if (holder.TryGetValue("PlayTestTickSound", out item) && item.Value is bool playTestTickSound)
            {
                this.PlayTestTickSound = playTestTickSound;
                if (playTestTickSound)
                {
                    this.PlayConfiguredSoundOnce(TapeStrikeSoundKind.Tick);
                    this.PlayTestTickSound = false;
                }
            }

            if (holder.TryGetValue("PlayTestBurstSound", out item) && item.Value is bool playTestBurstSound)
            {
                this.PlayTestBurstSound = playTestBurstSound;
                if (playTestBurstSound)
                {
                    this.PlayConfiguredSoundOnce(TapeStrikeSoundKind.Burst);
                    this.PlayTestBurstSound = false;
                }
            }

            if (holder.TryGetValue("PlayTestLargeTradeSound", out item) && item.Value is bool playTestLargeTradeSound)
            {
                this.PlayTestLargeTradeSound = playTestLargeTradeSound;
                if (playTestLargeTradeSound)
                {
                    this.PlayConfiguredSoundOnce(TapeStrikeSoundKind.LargeTrade);
                    this.PlayTestLargeTradeSound = false;
                }
            }

            if (!string.Equals(previousSymbolName, this.SymbolName, StringComparison.OrdinalIgnoreCase))
                this.SubscribeConfiguredSymbol();
        }
    }

    private static void AddGrouped(IList<SettingItem> settings, SettingItem item, SettingItemSeparatorGroup group)
    {
        item.SeparatorGroup = group;
        settings.Add(item);
    }

    public override void Initialize()
    {
        base.Initialize();
        this.AlertsAllowed = false;
        this.AllowDataExport = false;
        this.CustomPluginTitle = new TablePluginTitle(this.table);
        this.ApplyFactorySettings();
        TapeStrikeTableItem.SetColumnWidths(this.SymbolColumnWidth, this.AudioColumnWidth, this.StatusColumnWidth);

        this.RegisterService<LinkingPluginService>(service =>
        {
            service.AllowedScopes = [LinkingScope.Symbol];
            service.Initialize(this);
            service.UseTableAsSource();
            this.linkingService = service;
        });

        this.uiTimer = new System.Threading.Timer(_ => this.RefreshTable(), null, 250, 500);
        this.RegisterBrokerAlertTranslator();
        this.SubscribeConfiguredSymbol();
    }

    public override void Populate(PluginParameters? args = null)
    {
        this.SubscribeConfiguredSymbol();
        this.RefreshTable();
    }

    public override void Dispose()
    {
        this.UnregisterBrokerAlertTranslator();
        this.uiTimer?.Dispose();
        this.Unsubscribe();
        lock (this.soundLock)
        {
            foreach (var player in this.soundPlayers.Values)
                player.Dispose();
            this.soundPlayers.Clear();
        }

        this.linkingService?.Dispose();
        base.Dispose();
    }

    private void RegisterBrokerAlertTranslator()
    {
        if (this.brokerAlertTranslatorRegistered)
            return;

        Core.Instance.Loggers.NewLog += this.HandleQtLog;
        this.brokerAlertTranslatorRegistered = true;
    }

    private void UnregisterBrokerAlertTranslator()
    {
        if (!this.brokerAlertTranslatorRegistered)
            return;

        Core.Instance.Loggers.NewLog -= this.HandleQtLog;
        this.brokerAlertTranslatorRegistered = false;
    }

    private void HandleQtLog(ApplicationLoggerEvent logEvent)
    {
        try
        {
            var connection = logEvent.ConnectionName ?? string.Empty;
            var eventText = logEvent.Event ?? string.Empty;
            var message = logEvent.Message ?? string.Empty;
            var combined = $"{eventText} {message}";

            if (!IsIbkrConnection(connection, combined))
                return;

            var orderSymbol = TryExtractSymbol(combined);
            if (!string.IsNullOrWhiteSpace(orderSymbol))
            {
                this.lastIbkrOrderSymbol = orderSymbol;
                this.lastIbkrOrderUtc = DateTime.UtcNow;
            }

            if (ContainsIgnoreCase(combined, "Order held while securities are located"))
            {
                var symbol = this.GetRecentIbkrOrderSymbol();
                this.PostBrokerAlert(
                    $"IBKR: NO SHARES AVAILABLE TO SHORT{FormatSymbolSuffix(symbol)} - LOCATE REQUIRED",
                    symbol,
                    "NO_SHORT_LOCATE");
                return;
            }

            if (ContainsIgnoreCase(combined, "Account and symbol must be from same connection"))
            {
                this.PostBrokerAlert(
                    "QT: SYMBOL/ACCOUNT CONNECTION MISMATCH - USE BROKER SYMBOL WITH BROKER ACCOUNT",
                    this.SymbolName,
                    "CONNECTION_MISMATCH");
            }
        }
        catch
        {
            // Never let alert translation interfere with TapeStrike or QT's trading event pipeline.
        }
    }

    private static bool IsIbkrConnection(string connection, string text) =>
        ContainsIgnoreCase(connection, "IBKR") ||
        ContainsIgnoreCase(connection, "Interactive Brokers") ||
        ContainsIgnoreCase(text, "Connection: IBKR") ||
        ContainsIgnoreCase(text, "Interactive Brokers");

    private string GetRecentIbkrOrderSymbol()
    {
        if (!string.IsNullOrWhiteSpace(this.lastIbkrOrderSymbol) &&
            (DateTime.UtcNow - this.lastIbkrOrderUtc).TotalSeconds <= 10)
        {
            return this.lastIbkrOrderSymbol;
        }

        return this.SymbolName;
    }

    private void PostBrokerAlert(string text, string symbolName, string key)
    {
        var now = DateTime.UtcNow;
        var alertKey = $"{key}:{symbolName}";
        if (string.Equals(alertKey, this.lastBrokerAlertKey, StringComparison.OrdinalIgnoreCase) &&
            (now - this.lastBrokerAlertUtc).TotalSeconds < 5)
        {
            return;
        }

        this.lastBrokerAlertKey = alertKey;
        this.lastBrokerAlertUtc = now;
        Core.Instance.Alert(text, symbolName, "IBKR", static () => { }, "IBKR Alert Translator");
    }

    private static string FormatSymbolSuffix(string symbolName) =>
        string.IsNullOrWhiteSpace(symbolName) ? string.Empty : $" - {symbolName}";

    private static string TryExtractSymbol(string text)
    {
        const string marker = "Symbol:";
        var start = text.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (start < 0)
            return string.Empty;

        start += marker.Length;
        while (start < text.Length && char.IsWhiteSpace(text[start]))
            start++;

        var end = text.IndexOf(';', start);
        if (end < 0)
            end = text.IndexOf(',', start);
        if (end < 0)
            end = text.Length;

        return text[start..end].Trim();
    }

    private static bool ContainsIgnoreCase(string text, string value) =>
        text.Contains(value, StringComparison.OrdinalIgnoreCase);

    public bool HandleLinkingArgument(LinkingScope scope, object argument)
    {
        if (!scope.Equals(LinkingScope.Symbol))
            return false;

        var symbol = ResolveLinkedSymbol(argument);
        if (symbol == null)
            return false;

        this.SetSymbol(symbol, publishLink: false);
        return true;
    }

    public bool TryGetLinkingArgument(LinkingScope scope, out object argument)
    {
        argument = this.subscribedSymbol ?? (object)this.SymbolName;
        return scope.Equals(LinkingScope.Symbol) && argument != null;
    }

    private void SubscribeConfiguredSymbol()
    {
        if (string.IsNullOrWhiteSpace(this.SymbolName))
        {
            this.status = "No symbol configured.";
            return;
        }

        var symbol = Core.Instance.Symbols.FirstOrDefault(s =>
            string.Equals(s.Name, this.SymbolName, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(s.Id, this.SymbolName, StringComparison.OrdinalIgnoreCase));

        if (symbol == null)
        {
            this.status = $"Symbol not found: {this.SymbolName}";
            return;
        }

        this.SetSymbol(symbol, publishLink: true);
    }

    private void SetSymbol(Symbol symbol, bool publishLink)
    {
        if (ReferenceEquals(this.subscribedSymbol, symbol))
            return;

        this.Unsubscribe();
        this.SymbolName = symbol.Name;
        this.subscribedSymbol = symbol;
        this.subscribedSymbol.NewLast += this.OnNewLast;
        lock (this.stateLock)
        {
            this.lastTapeUtc = default;
            this.lastTickSoundUtc = default;
            this.lastBurstSoundUtc = default;
            this.lastLargeTradeSoundUtc = default;
            this.printsLastCheck = 0;
            this.tapeSpeedPerSecond = 0;
            this.lastPrintSize = 0;
            this.lastAudiblePrintSize = 0;
            this.totalPrints = 0;
            this.audiblePrints = 0;
            this.statusKind = TapeStrikeSoundKind.None;
            this.statusHoldUntilUtc = default;
            this.recentBurstPrints.Clear();
        }

        this.status = $"Ready: awaiting qualifying prints for {symbol.Name}.";
        if (publishLink)
            this.PublishLinking(LinkingScope.Symbol, symbol);
    }

    private void Unsubscribe()
    {
        if (this.subscribedSymbol == null)
            return;

        this.subscribedSymbol.NewLast -= this.OnNewLast;
        this.subscribedSymbol = null;
    }

    private static Symbol? ResolveLinkedSymbol(object argument)
    {
        if (argument is Symbol symbol)
            return symbol;

        if (argument is string symbolName)
            return FindSymbol(symbolName);

        var symbolProperty = argument.GetType().GetProperty("Symbol");
        if (symbolProperty?.GetValue(argument) is Symbol linkedSymbol)
            return linkedSymbol;

        if (symbolProperty?.GetValue(argument) is string linkedSymbolName)
            return FindSymbol(linkedSymbolName);

        var nameProperty = argument.GetType().GetProperty("Name");
        if (nameProperty?.GetValue(argument) is string name)
            return FindSymbol(name);

        return null;
    }

    private static Symbol? FindSymbol(string symbolName) =>
        Core.Instance.Symbols.FirstOrDefault(s =>
            string.Equals(s.Name, symbolName, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(s.Id, symbolName, StringComparison.OrdinalIgnoreCase));

    private void OnNewLast(Symbol symbol, Last last)
    {
        var tradeSize = Math.Max(0d, last.Size);
        TapeStrikeSoundKind soundKind;
        double speed;

        lock (this.stateLock)
        {
            var now = DateTime.UtcNow;
            this.lastTapeUtc = last.Time == default ? now : last.Time.ToUniversalTime();
            if (tradeSize >= Math.Max(this.BurstMinimumTradeSize, 1))
                this.recentBurstPrints.Enqueue(now);

            this.PruneRecentPrints(now);
            this.printsLastCheck = this.recentBurstPrints.Count;
            this.tapeSpeedPerSecond = this.CalculateTapeSpeed();
            speed = this.tapeSpeedPerSecond;
            this.lastPrintSize = tradeSize;
            this.totalPrints++;
            soundKind = this.ResolveSoundKind(tradeSize, speed);
            if (soundKind != TapeStrikeSoundKind.None)
            {
                this.lastAudiblePrintSize = tradeSize;
                this.lastAudiblePrintPrice = last.Price;
                this.lastAudiblePrintUtc = this.lastTapeUtc;
                this.audiblePrints++;
            }
        }

        if (soundKind != TapeStrikeSoundKind.None)
            this.TryPlayTapeSoundImmediate(DateTime.UtcNow, soundKind, tradeSize, last.Price, this.lastTapeUtc, last.AggressorFlag);

    }

    private void PruneRecentPrints(DateTime now)
    {
        var window = TimeSpan.FromMilliseconds(Math.Max(this.BurstSpeedWindowMs, 250));
        while (this.recentBurstPrints.Count > 0 && now - this.recentBurstPrints.Peek() > window)
            this.recentBurstPrints.Dequeue();
    }

    private double CalculateTapeSpeed()
    {
        var windowSeconds = Math.Max(this.BurstSpeedWindowMs, 250) / 1000d;
        return this.recentBurstPrints.Count / windowSeconds;
    }

    private SoundPlayer? ResolveSoundPlayer(TapeStrikeSoundKind kind)
    {
        var configuredPath = this.GetSoundFile(kind);
        if (string.IsNullOrWhiteSpace(configuredPath) || !File.Exists(configuredPath))
        {
            if (kind != TapeStrikeSoundKind.Tick)
            {
                configuredPath = this.TickSoundFile?.Trim();
                if (string.IsNullOrWhiteSpace(configuredPath) || !File.Exists(configuredPath))
                    return null;
            }
            else
            {
                return null;
            }
        }

        var volume = this.GetVolumePercent(kind);

        lock (this.soundLock)
        {
            var path = this.ResolvePlayableSoundPath(configuredPath, volume);
            if (this.soundPlayers.TryGetValue(path, out var cachedPlayer))
                return cachedPlayer;

            var player = new SoundPlayer(path);
            try
            {
                player.LoadAsync();
                this.soundPlayers[path] = player;
                return player;
            }
            catch
            {
                player.Dispose();
                return null;
            }
        }
    }

    private string GetSoundFile(TapeStrikeSoundKind kind)
    {
        var configured = kind switch
        {
            TapeStrikeSoundKind.Burst => this.BurstSoundFile,
            TapeStrikeSoundKind.LargeTrade => this.LargeTradeSoundFile,
            _ => this.TickSoundFile
        };

        if (!string.IsNullOrWhiteSpace(configured))
            return configured.Trim();

        return this.TickSoundFile?.Trim() ?? string.Empty;
    }

    private int GetVolumePercent(TapeStrikeSoundKind kind) =>
        kind switch
        {
            TapeStrikeSoundKind.Burst => this.BurstAudioVolumePercent,
            TapeStrikeSoundKind.LargeTrade => this.LargeTradeAudioVolumePercent,
            _ => this.TickAudioVolumePercent
        };

    private string ResolvePlayableSoundPath(string configuredPath, int volumePercent)
    {
        var volume = Math.Clamp(volumePercent, 1, 100);
        if (volume >= 100)
            return configuredPath;

        try
        {
            return CreateVolumeAdjustedWav(configuredPath, volume);
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
            return configuredPath;
        }
    }

    private static string CreateVolumeAdjustedWav(string sourcePath, int volumePercent)
    {
        var sourceInfo = new FileInfo(sourcePath);
        var cacheKey = $"{sourceInfo.FullName}|{sourceInfo.Length}|{sourceInfo.LastWriteTimeUtc.Ticks}|{volumePercent}";
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(cacheKey)))[..16];
        var cacheDir = Path.Combine(Path.GetTempPath(), "TapeStrikeAudio");
        Directory.CreateDirectory(cacheDir);

        var targetPath = Path.Combine(
            cacheDir,
            $"{Path.GetFileNameWithoutExtension(sourceInfo.Name)}-{volumePercent}pct-{hash}.wav");

        if (File.Exists(targetPath))
            return targetPath;

        var bytes = File.ReadAllBytes(sourceInfo.FullName);
        if (!IsRiffWave(bytes))
            return sourceInfo.FullName;

        var fmtOffset = -1;
        var fmtSize = 0;
        var dataOffset = -1;
        var dataSize = 0;

        var cursor = 12;
        while (cursor + 8 <= bytes.Length)
        {
            var chunkSize = BitConverter.ToInt32(bytes, cursor + 4);
            if (chunkSize < 0 || cursor + 8 + chunkSize > bytes.Length)
                break;

            if (bytes[cursor] == (byte)'f' && bytes[cursor + 1] == (byte)'m' && bytes[cursor + 2] == (byte)'t' && bytes[cursor + 3] == (byte)' ')
            {
                fmtOffset = cursor + 8;
                fmtSize = chunkSize;
            }
            else if (bytes[cursor] == (byte)'d' && bytes[cursor + 1] == (byte)'a' && bytes[cursor + 2] == (byte)'t' && bytes[cursor + 3] == (byte)'a')
            {
                dataOffset = cursor + 8;
                dataSize = chunkSize;
            }

            cursor += 8 + chunkSize + (chunkSize & 1);
        }

        if (fmtOffset < 0 || fmtSize < 16 || dataOffset < 0 || dataSize <= 0)
            return sourceInfo.FullName;

        var audioFormat = BitConverter.ToInt16(bytes, fmtOffset);
        var bitsPerSample = BitConverter.ToInt16(bytes, fmtOffset + 14);
        if (audioFormat != 1 || bitsPerSample is not (8 or 16 or 24 or 32))
            return sourceInfo.FullName;

        var scaled = (byte[])bytes.Clone();
        ScalePcmSamples(scaled, dataOffset, dataSize, bitsPerSample, volumePercent / 100d);
        File.WriteAllBytes(targetPath, scaled);
        return targetPath;
    }

    private static bool IsRiffWave(byte[] bytes) =>
        bytes.Length > 12 &&
        bytes[0] == (byte)'R' && bytes[1] == (byte)'I' && bytes[2] == (byte)'F' && bytes[3] == (byte)'F' &&
        bytes[8] == (byte)'W' && bytes[9] == (byte)'A' && bytes[10] == (byte)'V' && bytes[11] == (byte)'E';

    private static void ScalePcmSamples(byte[] bytes, int dataOffset, int dataSize, int bitsPerSample, double factor)
    {
        var bytesPerSample = bitsPerSample / 8;
        var end = Math.Min(bytes.Length, dataOffset + dataSize);
        for (var i = dataOffset; i + bytesPerSample <= end; i += bytesPerSample)
        {
            switch (bitsPerSample)
            {
                case 8:
                    var unsigned = bytes[i] - 128;
                    bytes[i] = (byte)(Math.Clamp((int)Math.Round(unsigned * factor), -128, 127) + 128);
                    break;
                case 16:
                    var sample16 = BitConverter.ToInt16(bytes, i);
                    var scaled16 = (short)Math.Clamp((int)Math.Round(sample16 * factor), short.MinValue, short.MaxValue);
                    var scaled16Bytes = BitConverter.GetBytes(scaled16);
                    bytes[i] = scaled16Bytes[0];
                    bytes[i + 1] = scaled16Bytes[1];
                    break;
                case 24:
                    var sample24 = bytes[i] | (bytes[i + 1] << 8) | (bytes[i + 2] << 16);
                    if ((sample24 & 0x800000) != 0)
                        sample24 |= unchecked((int)0xFF000000);
                    var scaled24 = Math.Clamp((int)Math.Round(sample24 * factor), -8_388_608, 8_388_607);
                    bytes[i] = (byte)(scaled24 & 0xFF);
                    bytes[i + 1] = (byte)((scaled24 >> 8) & 0xFF);
                    bytes[i + 2] = (byte)((scaled24 >> 16) & 0xFF);
                    break;
                case 32:
                    var sample32 = BitConverter.ToInt32(bytes, i);
                    var scaled32 = (int)Math.Clamp(Math.Round(sample32 * factor), int.MinValue, int.MaxValue);
                    var scaled32Bytes = BitConverter.GetBytes(scaled32);
                    bytes[i] = scaled32Bytes[0];
                    bytes[i + 1] = scaled32Bytes[1];
                    bytes[i + 2] = scaled32Bytes[2];
                    bytes[i + 3] = scaled32Bytes[3];
                    break;
            }
        }
    }

    private void PlayConfiguredSoundOnce(TapeStrikeSoundKind kind)
    {
        try
        {
            var player = this.ResolveSoundPlayer(kind);
            if (player == null)
            {
                this.status = $"Test failed: no valid {GetSoundLabel(kind)} .wav file is configured.";
                return;
            }

            player.Play();
            this.status = $"Test played: {GetSoundLabel(kind)}.";
        }
        catch (Exception ex)
        {
            this.status = $"Test failed: {ex.Message}";
            Core.Instance.Loggers.Log(ex);
        }
    }

    private void TryPlayTapeSoundImmediate(DateTime now, TapeStrikeSoundKind kind, double tradeSize, double tradePrice, DateTime tradeTimeUtc, AggressorFlag side)
    {
        if (!this.IsAudioEnabled(kind))
            return;

        var cleanSpacing = this.GetSpacingMs(kind);
        var lastSoundUtc = this.GetLastSoundUtc(kind);
        if (cleanSpacing > 0 && lastSoundUtc != default && now - lastSoundUtc < TimeSpan.FromMilliseconds(cleanSpacing))
            return;

        try
        {
            var player = this.ResolveSoundPlayer(kind);
            if (player == null)
            {
                this.status = $"{GetSoundLabel(kind)} blocked: no valid .wav file configured.";
                return;
            }

            player.Play();
            this.SetLastSoundUtc(kind, now);
            this.UpdateStatus(kind, side, tradeSize, tradePrice, tradeTimeUtc, now);
        }
        catch
        {
            this.status = $"{GetSoundLabel(kind)} playback failed.";
        }
    }

    private void UpdateStatus(TapeStrikeSoundKind kind, AggressorFlag side, double tradeSize, double tradePrice, DateTime tradeTimeUtc, DateTime now)
    {
        lock (this.stateLock)
        {
            if (this.statusKind == TapeStrikeSoundKind.LargeTrade &&
                kind != TapeStrikeSoundKind.LargeTrade &&
                now < this.statusHoldUntilUtc)
                return;

            this.status = FormatStatus(kind, side, tradeSize, tradePrice, tradeTimeUtc);
            this.statusKind = kind;
            this.statusHoldUntilUtc = kind == TapeStrikeSoundKind.LargeTrade
                ? now.AddMilliseconds(1500)
                : now;
        }
    }

    private TapeStrikeSoundKind ResolveSoundKind(double tradeSize, double speed)
    {
        if (this.EnableLargeTradeAudio && tradeSize >= Math.Max(this.LargeTradeSize, 1))
            return TapeStrikeSoundKind.LargeTrade;

        if (this.EnableBurstAudio &&
            tradeSize >= Math.Max(this.BurstMinimumTradeSize, 1) &&
            speed >= Math.Max(this.BurstPrintsPerSecond, 1d))
            return TapeStrikeSoundKind.Burst;

        if (this.EnableTickAudio && tradeSize >= Math.Max(this.TickMinimumTradeSize, 1))
            return TapeStrikeSoundKind.Tick;

        return TapeStrikeSoundKind.None;
    }

    private bool IsAudioEnabled(TapeStrikeSoundKind kind) =>
        kind switch
        {
            TapeStrikeSoundKind.Burst => this.EnableBurstAudio,
            TapeStrikeSoundKind.LargeTrade => this.EnableLargeTradeAudio,
            TapeStrikeSoundKind.Tick => this.EnableTickAudio,
            _ => false
        };

    private int GetSpacingMs(TapeStrikeSoundKind kind) =>
        kind switch
        {
            TapeStrikeSoundKind.Burst => Math.Max(this.BurstCleanSpacingMs, 0),
            TapeStrikeSoundKind.LargeTrade => Math.Max(this.LargeTradeSpacingMs, 0),
            TapeStrikeSoundKind.Tick => Math.Max(this.TickSpacingMs, 0),
            _ => 0
        };

    private DateTime GetLastSoundUtc(TapeStrikeSoundKind kind) =>
        kind switch
        {
            TapeStrikeSoundKind.Burst => this.lastBurstSoundUtc,
            TapeStrikeSoundKind.LargeTrade => this.lastLargeTradeSoundUtc,
            TapeStrikeSoundKind.Tick => this.lastTickSoundUtc,
            _ => default
        };

    private void SetLastSoundUtc(TapeStrikeSoundKind kind, DateTime timestamp)
    {
        switch (kind)
        {
            case TapeStrikeSoundKind.Burst:
                this.lastBurstSoundUtc = timestamp;
                break;
            case TapeStrikeSoundKind.LargeTrade:
                this.lastLargeTradeSoundUtc = timestamp;
                break;
            case TapeStrikeSoundKind.Tick:
                this.lastTickSoundUtc = timestamp;
                break;
        }
    }

    private static string GetSoundLabel(TapeStrikeSoundKind kind) =>
        kind switch
        {
            TapeStrikeSoundKind.Burst => "BURST",
            TapeStrikeSoundKind.LargeTrade => "LARGE PRINT",
            TapeStrikeSoundKind.Tick => "TICK",
            _ => "OFF"
        };

    private static string GetAlertTypeLabel(TapeStrikeSoundKind kind) =>
        kind switch
        {
            TapeStrikeSoundKind.Burst => "Mid",
            TapeStrikeSoundKind.LargeTrade => "Block",
            TapeStrikeSoundKind.Tick => "Tick",
            _ => "Off"
        };

    private static string FormatStatus(TapeStrikeSoundKind kind, AggressorFlag side, double tradeSize, double tradePrice, DateTime tradeTimeUtc)
    {
        var localTime = tradeTimeUtc == default ? DateTime.Now : tradeTimeUtc.ToLocalTime();
        var displaySize = tradeSize * 100d;
        return $"{GetAlertTypeLabel(kind)} | {FormatSide(side)} {displaySize:0} {tradePrice:0.00} {localTime:H:mm:ss}";
    }

    private static string FormatSide(AggressorFlag side) =>
        side switch
        {
            AggressorFlag.Buy => "A",
            AggressorFlag.Sell => "B",
            _ => "--"
        };

    private void RefreshTable()
    {
        try
        {
            TapeStrikeStatus snapshot;
            lock (this.stateLock)
            {
                snapshot = new TapeStrikeStatus(
                    this.SymbolName,
                    this.EnableTickAudio,
                    this.EnableBurstAudio,
                    this.EnableLargeTradeAudio,
                    this.status,
                    this.printsLastCheck,
                    this.totalPrints,
                    this.audiblePrints,
                    this.lastTapeUtc,
                    this.tapeSpeedPerSecond,
                    this.lastPrintSize,
                    this.lastAudiblePrintSize,
                    this.lastAudiblePrintPrice,
                    this.lastAudiblePrintUtc,
                    this.TickMinimumTradeSize,
                    this.BurstMinimumTradeSize,
                    this.LargeTradeSize,
                    DateTime.UtcNow);
            }

            this.table.SuspendDrawing = true;
            this.table.ClearAll();
            this.table.AddItem(new TapeStrikeTableItem(snapshot));
            this.table.SuspendDrawing = false;
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
        }
    }
}

internal sealed record TapeStrikeStatus(
    string Symbol,
    bool TickAudioEnabled,
    bool BurstAudioEnabled,
    bool LargeTradeAudioEnabled,
    string Status,
    int PrintsLastCheck,
    long TotalPrints,
    long AudiblePrints,
    DateTime LastTapeUtc,
    double TapeSpeedPerSecond,
    double LastPrintSize,
    double LastAudiblePrintSize,
    double LastAudiblePrintPrice,
    DateTime LastAudiblePrintUtc,
    int TickMinimumTradeSize,
    int BurstMinimumTradeSize,
    int LargeTradeSize,
    DateTime UpdatedUtc);

internal enum TapeStrikeSoundKind
{
    None,
    Tick,
    Burst,
    LargeTrade
}
