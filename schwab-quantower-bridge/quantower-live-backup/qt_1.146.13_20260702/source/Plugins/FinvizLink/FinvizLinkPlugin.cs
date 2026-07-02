using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using TradingPlatform.BusinessLayer;
using TradingPlatform.PresentationLayer.Plugins;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking.EventArgs;
using TradingPlatform.PresentationLayer.Plugins.Services.Linking.Models.Scopes;
using TradingPlatform.PresentationLayer.Renderers.Table;

namespace FinvizLink;

[SupportedOSPlatform("windows6.1")]
public sealed class FinvizLinkPlugin : TablePlugin, ILinkable, ILinkingArgumentHandler, ILinkingArgumentProvider
{
    private const int HotkeyId = 0x4B46;
    private const uint ModNone = 0x0000;
    private const uint VkF8 = 0x77;
    private static readonly Regex SymbolCleanup = new(@"[^A-Z0-9.\-]", RegexOptions.Compiled);

    private readonly object stateLock = new();
    private LinkingPluginService? linkingService;
    private System.Threading.Timer? uiTimer;
    private HotkeyMessageWindow? hotkeyWindow;
    private string symbolName = "ROKU";
    private string status = "Map this panel with the QT Watchlist or DOM symbol link. Press F8 to open Finviz.";
    private string lastOpenedSymbol = string.Empty;
    private DateTime lastOpenUtc;

    private string UrlTemplate { get; set; } = "https://finviz.com/stock?t={SYMBOL}&p=d";

    private string SenderPath { get; set; } =
        @"C:\Users\Owner\.codex\worktrees\1023\Claude Code\qt_finviz_symbol_link\bin\Release\net10.0-windows\QtFinvizSymbolLink.exe";

    private bool UseFinvizSender { get; set; } = true;

    private int SearchDelayMs { get; set; } = 1200;

    private bool AutoOpenOnSymbolChange { get; set; }

    private bool EnableF8Hotkey { get; set; } = true;

    private int OpenCooldownMs { get; set; } = 750;

    private bool OpenNow { get; set; }

    public static PluginInfo GetInfo() => new()
    {
        Name = "Finviz Link",
        Title = loc.key("Finviz Link"),
        Group = PluginGroup.Analytics,
        ShortName = "Finviz",
        WindowParameters = new NativeWindowParameters(NativeWindowParameters.Panel)
        {
            BrowserUsageType = BrowserUsageType.None
        },
        CustomProperties = new Dictionary<string, object>
        {
            { PluginInfo.Const.ALLOW_MANUAL_CREATION, true }
        }
    };

    protected override TableItem AssociatedTableItem => new FinvizLinkTableItem();

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

    public override IList<SettingItem> Settings
    {
        get
        {
            var settings = base.Settings;
            settings.Add(new SettingItemString("SymbolName", this.symbolName, 0) { Text = "Symbol" });
            settings.Add(new SettingItemString("UrlTemplate", this.UrlTemplate, 1) { Text = "Finviz URL template" });
            settings.Add(new SettingItemBoolean("UseFinvizSender", this.UseFinvizSender, 2) { Text = "Use Finviz sender" });
            settings.Add(new SettingItemFile("SenderPath", this.SenderPath, "Executable (.exe)|*.exe", 3) { Text = "Finviz sender executable" });
            settings.Add(new SettingItemInteger("SearchDelayMs", this.SearchDelayMs, 4) { Text = "Browser delay ms" });
            settings.Add(new SettingItemBoolean("EnableF8Hotkey", this.EnableF8Hotkey, 5) { Text = "Enable F8 hotkey" });
            settings.Add(new SettingItemBoolean("AutoOpenOnSymbolChange", this.AutoOpenOnSymbolChange, 6) { Text = "Auto-open linked symbol" });
            settings.Add(new SettingItemInteger("OpenCooldownMs", this.OpenCooldownMs, 7) { Text = "Open cooldown ms" });
            settings.Add(new SettingItemBoolean("OpenNow", this.OpenNow, 8) { Text = "Open current symbol now" });

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
            base.Settings = value;
            if (this.linkingService != null)
                this.linkingService.Settings = value;

            if (holder.TryGetValue("SymbolName", out var item) && item.Value is string symbol)
                this.symbolName = NormalizeSymbol(symbol);

            if (holder.TryGetValue("UrlTemplate", out item) && item.Value is string urlTemplate)
                this.UrlTemplate = string.IsNullOrWhiteSpace(urlTemplate) ? this.UrlTemplate : urlTemplate.Trim();

            if (holder.TryGetValue("UseFinvizSender", out item) && item.Value is bool useFinvizSender)
                this.UseFinvizSender = useFinvizSender;

            if (holder.TryGetValue("SenderPath", out item) && item.Value is string senderPath)
                this.SenderPath = senderPath.Trim();

            if (holder.TryGetValue("SearchDelayMs", out item) && item.Value is int searchDelayMs)
                this.SearchDelayMs = Math.Clamp(searchDelayMs, 100, 10_000);

            if (holder.TryGetValue("EnableF8Hotkey", out item) && item.Value is bool enableHotkey)
            {
                this.EnableF8Hotkey = enableHotkey;
                this.RefreshHotkeyRegistration();
            }

            if (holder.TryGetValue("AutoOpenOnSymbolChange", out item) && item.Value is bool autoOpen)
                this.AutoOpenOnSymbolChange = autoOpen;

            if (holder.TryGetValue("OpenCooldownMs", out item) && item.Value is int cooldownMs)
                this.OpenCooldownMs = Math.Clamp(cooldownMs, 0, 10_000);

            if (holder.TryGetValue("OpenNow", out item) && item.Value is bool openNow)
            {
                this.OpenNow = openNow;
                if (openNow)
                {
                    this.OpenFinviz(this.symbolName, force: true);
                    this.OpenNow = false;
                }
            }
        }
    }

    public override void Initialize()
    {
        base.Initialize();
        this.AlertsAllowed = false;
        this.AllowDataExport = false;
        this.CustomPluginTitle = new TablePluginTitle(this.table);

        this.RegisterService<LinkingPluginService>(service =>
        {
            service.AllowedScopes = [LinkingScope.Symbol];
            service.Initialize(this);
            service.UseTableAsSource();
            this.linkingService = service;
        });

        this.hotkeyWindow = new HotkeyMessageWindow(() => this.OpenFinviz(this.symbolName, force: true));
        this.RefreshHotkeyRegistration();
        this.uiTimer = new System.Threading.Timer(_ => this.RefreshTable(), null, 250, 500);
    }

    public override void Populate(PluginParameters? args = null) => this.RefreshTable();

    public override void Dispose()
    {
        this.uiTimer?.Dispose();
        this.UnregisterHotkey();
        this.hotkeyWindow?.DestroyHandle();
        this.hotkeyWindow = null;
        this.linkingService?.Dispose();
        base.Dispose();
    }

    public bool HandleLinkingArgument(LinkingScope scope, object argument)
    {
        if (!scope.Equals(LinkingScope.Symbol))
            return false;

        var linkedSymbol = ResolveSymbolName(argument);
        if (string.IsNullOrWhiteSpace(linkedSymbol))
            return false;

        linkedSymbol = NormalizeSymbol(linkedSymbol);
        lock (this.stateLock)
        {
            this.symbolName = linkedSymbol;
            this.status = $"Linked {linkedSymbol}. Press F8.";
        }

        if (this.AutoOpenOnSymbolChange)
            this.OpenFinviz(linkedSymbol, force: false);

        return true;
    }

    public bool TryGetLinkingArgument(LinkingScope scope, out object argument)
    {
        argument = this.symbolName;
        return scope.Equals(LinkingScope.Symbol) && !string.IsNullOrWhiteSpace(this.symbolName);
    }

    private void OpenFinviz(string symbol, bool force)
    {
        symbol = NormalizeSymbol(symbol);
        if (string.IsNullOrWhiteSpace(symbol))
            return;

        var now = DateTime.UtcNow;
        lock (this.stateLock)
        {
            if (!force &&
                string.Equals(symbol, this.lastOpenedSymbol, StringComparison.OrdinalIgnoreCase) &&
                (now - this.lastOpenUtc).TotalMilliseconds < Math.Max(this.OpenCooldownMs, 0))
            {
                return;
            }

            this.lastOpenedSymbol = symbol;
            this.lastOpenUtc = now;
        }

        try
        {
            if (this.UseFinvizSender)
            {
                if (!File.Exists(this.SenderPath))
                {
                    lock (this.stateLock)
                        this.status = "Finviz sender not found.";
                    return;
                }

                Process.Start(new ProcessStartInfo
                {
                    FileName = this.SenderPath,
                    Arguments = $"{symbol} {this.SearchDelayMs}",
                    UseShellExecute = false,
                    CreateNoWindow = true
                });
            }
            else
            {
                var url = BuildUrl(symbol);
                Process.Start(new ProcessStartInfo
                {
                    FileName = url,
                    UseShellExecute = true
                });
            }

            lock (this.stateLock)
                this.status = $"Opened {symbol} in Finviz";
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
            lock (this.stateLock)
                this.status = "Open failed.";
        }
    }

    private string BuildUrl(string symbol)
    {
        var encoded = Uri.EscapeDataString(symbol);
        var template = string.IsNullOrWhiteSpace(this.UrlTemplate)
            ? "https://finviz.com/stock?t={SYMBOL}&p=d"
            : this.UrlTemplate;

        return template
            .Replace("{SYMBOL}", encoded, StringComparison.OrdinalIgnoreCase)
            .Replace("{symbol}", encoded, StringComparison.OrdinalIgnoreCase);
    }

    private void RefreshHotkeyRegistration()
    {
        this.UnregisterHotkey();
        if (!this.EnableF8Hotkey || this.hotkeyWindow == null)
            return;

        if (!RegisterHotKey(this.hotkeyWindow.Handle, HotkeyId, ModNone, VkF8))
        {
            lock (this.stateLock)
                this.status = "F8 hotkey unavailable.";
        }
    }

    private void UnregisterHotkey()
    {
        if (this.hotkeyWindow != null)
            UnregisterHotKey(this.hotkeyWindow.Handle, HotkeyId);
    }

    private void RefreshTable()
    {
        try
        {
            FinvizLinkStatus snapshot;
            lock (this.stateLock)
                snapshot = new FinvizLinkStatus(this.symbolName, this.status);

            this.table.SuspendDrawing = true;
            this.table.ClearAll();
            this.table.AddItem(new FinvizLinkTableItem(snapshot));
            this.table.SuspendDrawing = false;
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
        }
    }

    private static string ResolveSymbolName(object argument)
    {
        if (argument is Symbol symbol)
            return symbol.Name;

        if (argument is string symbolName)
            return symbolName;

        var symbolProperty = argument.GetType().GetProperty("Symbol");
        var symbolValue = symbolProperty?.GetValue(argument);
        if (symbolValue is Symbol linkedSymbol)
            return linkedSymbol.Name;
        if (symbolValue is string linkedSymbolName)
            return linkedSymbolName;

        var nameProperty = argument.GetType().GetProperty("Name");
        return nameProperty?.GetValue(argument) as string ?? string.Empty;
    }

    private static string NormalizeSymbol(string symbol)
    {
        var text = symbol.Trim();
        var space = text.IndexOf(' ');
        if (space > 0)
            text = text[..space];

        text = SymbolCleanup.Replace(text.ToUpperInvariant(), string.Empty);
        return text;
    }

    [DllImport("user32.dll", SetLastError = true)]
    private static extern bool RegisterHotKey(IntPtr hWnd, int id, uint fsModifiers, uint vk);

    [DllImport("user32.dll", SetLastError = true)]
    private static extern bool UnregisterHotKey(IntPtr hWnd, int id);

    private sealed class HotkeyMessageWindow : NativeWindow
    {
        private const int WmHotkey = 0x0312;
        private readonly Action onHotkey;

        public HotkeyMessageWindow(Action onHotkey)
        {
            this.onHotkey = onHotkey;
            this.CreateHandle(new CreateParams());
        }

        protected override void WndProc(ref Message m)
        {
            if (m.Msg == WmHotkey && m.WParam.ToInt32() == HotkeyId)
            {
                this.onHotkey();
                return;
            }

            base.WndProc(ref m);
        }
    }
}
