using System;
using System.Drawing;
using System.Globalization;
using System.IO;
using TradingPlatform.BusinessLayer;

namespace WickRejectionIndicator;

public sealed class WickRejectionIndicator : Indicator, IVolumeAnalysisIndicator
{
    private const int BullishSeriesIndex = 0;
    private const int BearishSeriesIndex = 1;
    private const int BullishPreviewSeriesIndex = 2;
    private const int BearishPreviewSeriesIndex = 3;
    private const int RejectionSuppressionWindowBars = 3;
    private const int RejectionSuppressionToleranceTicks = 2;
    private readonly HashSet<long> processedBarTicks = new();
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> logQueue = new();
    private System.Threading.Timer? logFlushTimer;
    private int logFlushActive;
    private double lastRejectionPrice = double.NaN;
    private int lastRejectionBarIndex = -1;
    private DateTime sessionDate = DateTime.MinValue;
    private DateTime currentBarTimeUtc = DateTime.MinValue;
    // (EWMA volume baseline removed — replaced by live aggressor-delta gate)
    private string diagnostics = string.Empty;
    private bool bootstrapped;
    private bool pendingVaRebuild;

    [InputParameter("Wick ratio threshold", 0)]
    public double WickRatioThreshold { get; set; } = 0.60;

    [InputParameter("Volume multiplier", 1)]
    public double VolumeMultiplier { get; set; } = 1.5;

    [InputParameter("Session start (HHMM)", 2)]
    public int SessionStartHHMM { get; set; } = 930;

    [InputParameter("Bullish rejection color", 4)]
    public Color BullishRejectionColor { get; set; } = Color.FromArgb(0, 220, 90);

    [InputParameter("Bearish rejection color", 5)]
    public Color BearishRejectionColor { get; set; } = Color.FromArgb(220, 55, 55);

    [InputParameter("Diamond offset (px from candle)", 6)]
    public int DiamondOffsetPx { get; set; } = 4;

    [InputParameter("Show diagnostics", 7)]
    public bool ShowDiagnostics { get; set; }

    [InputParameter("Provisional opacity %", 8)]
    public int ProvisionalOpacityPct { get; set; } = 40;

    [InputParameter("Bullish icon size (px)", 9)]
    public int BullishSizePx { get; set; } = 8;

    [InputParameter("Bearish icon size (px)", 10)]
    public int BearishSizePx { get; set; } = 8;

    [InputParameter("Bullish preview color", 11)]
    public Color BullishPreviewColor { get; set; } = Color.FromArgb(0, 220, 90);

    [InputParameter("Bearish preview color", 12)]
    public Color BearishPreviewColor { get; set; } = Color.FromArgb(220, 55, 55);

    [InputParameter("Volume EWMA bars", 14)]
    public int VolumeEwmaBars { get; set; } = 10;

    [InputParameter("Enable aggressor side gate", 15)]
    public bool EnableAggressorGate { get; set; } = true;

    [InputParameter("Aggressor side % gate", 16)]
    public double AggressorSidePct { get; set; } = 60;

    [InputParameter("Session end (HHMM)", 17)]
    public int SessionEndHHMM { get; set; } = 1600;

    public bool IsRequirePriceLevelsCalculation => false;

    public void VolumeAnalysisData_Loaded()
    {
        this.pendingVaRebuild = true;
    }

    public WickRejectionIndicator()
    {
        this.Name = "Wick Rejection";
        this.Description = "Detects bar-close wick rejection with session-volume confirmation.";
        this.SeparateWindow = false;
        this.UpdateType = IndicatorUpdateType.OnTick;
        this.AddLineSeries("Bullish signal", Color.Transparent, 1, LineStyle.Points);
        this.AddLineSeries("Bearish signal", Color.Transparent, 1, LineStyle.Points);
        this.AddLineSeries("Bullish preview", Color.Transparent, 1, LineStyle.Points);
        this.AddLineSeries("Bearish preview", Color.Transparent, 1, LineStyle.Points);
        this.LinesSeries[BullishSeriesIndex].Visible = false;
        this.LinesSeries[BearishSeriesIndex].Visible = false;
        this.LinesSeries[BullishPreviewSeriesIndex].Visible = false;
        this.LinesSeries[BearishPreviewSeriesIndex].Visible = false;
    }

    protected override void OnInit()
    {
        base.OnInit();
        this.logFlushTimer?.Dispose();
        this.logFlushTimer = new System.Threading.Timer(
            _ => this.FlushLogQueue(),
            null,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(5));
        this.ResetSession(DateTime.MinValue);
        this.bootstrapped = false;
    }

    protected override void OnUpdate(UpdateArgs args)
    {
        if (this.HistoricalData == null || this.HistoricalData.Count == 0)
            return;

        if (!this.bootstrapped)
            this.BootstrapCurrentSession();

        if (this.pendingVaRebuild)
        {
            this.ResetSession(DateTime.MinValue);
            this.bootstrapped = false;
            this.pendingVaRebuild = false;
            this.BootstrapCurrentSession();
        }

        if (this.HistoricalData[0] is not HistoryItemBar bar)
            return;

        var barTimeUtc = CoerceUtc(bar.TimeLeft);
        if (this.currentBarTimeUtc == DateTime.MinValue)
            this.currentBarTimeUtc = barTimeUtc;

        if (barTimeUtc != this.currentBarTimeUtc)
        {
            if (this.HistoricalData.Count > 1 && this.HistoricalData[1] is HistoryItemBar closedBar)
                this.ProcessClosedBar(closedBar, 1, allowRender: true, allowLog: this.bootstrapped);

            this.currentBarTimeUtc = barTimeUtc;
        }

        this.ProcessPreviewBar(bar);
    }

    public override void OnPaintChart(PaintChartEventArgs args)
    {
        base.OnPaintChart(args);
        var gr = args.Graphics;
        var chart = this.CurrentChart;
        if (chart == null)
            return;

        var converter = chart.MainWindow.CoordinatesConverter;
        var halfBull = Math.Max(2, this.BullishSizePx / 2);
        var halfBear = Math.Max(2, this.BearishSizePx / 2);

        using var bullFill = new SolidBrush(this.BullishRejectionColor);
        using var bearFill = new SolidBrush(this.BearishRejectionColor);
        using var bullPreviewFill = new SolidBrush(WithOpacity(this.BullishPreviewColor, this.ProvisionalOpacityPct));
        using var bearPreviewFill = new SolidBrush(WithOpacity(this.BearishPreviewColor, this.ProvisionalOpacityPct));
        using var borderPen = new Pen(Color.FromArgb(180, Color.Black), 1f);

        var historicalData = this.HistoricalData;
        if (historicalData == null)
            return;

        var totalCount = historicalData.Count;
        var leftIdx = Math.Max(0, args.LeftVisibleBarIndex - 2);
        var rightIdx = Math.Min(totalCount - 1, args.RightVisibleBarIndex + 2);

        for (var i = leftIdx; i <= rightIdx; i++)
        {
            try
            {
                if (i >= historicalData.Count)
                    break;

                if (historicalData[i, SeekOriginHistory.Begin] is not HistoryItemBar bar)
                    continue;

                var cx = (float)converter.GetChartX(bar.TimeLeft);

                var bullPrice = this.GetValue(i, BullishSeriesIndex, SeekOriginHistory.Begin);
                if (!double.IsNaN(bullPrice) && bullPrice > 0)
                {
                    var cy = (float)converter.GetChartY(bullPrice) + this.DiamondOffsetPx;
                    DrawDiamond(gr, bullFill, borderPen, cx, cy, halfBull);
                }

                var bearPrice = this.GetValue(i, BearishSeriesIndex, SeekOriginHistory.Begin);
                if (!double.IsNaN(bearPrice) && bearPrice > 0)
                {
                    var cy = (float)converter.GetChartY(bearPrice) - this.DiamondOffsetPx;
                    DrawDiamond(gr, bearFill, borderPen, cx, cy, halfBear);
                }

                var bullPreview = this.GetValue(i, BullishPreviewSeriesIndex, SeekOriginHistory.Begin);
                if (!double.IsNaN(bullPreview) && bullPreview > 0)
                {
                    var cy = (float)converter.GetChartY(bullPreview) + this.DiamondOffsetPx;
                    DrawDiamond(gr, bullPreviewFill, borderPen, cx, cy, halfBull);
                }

                var bearPreview = this.GetValue(i, BearishPreviewSeriesIndex, SeekOriginHistory.Begin);
                if (!double.IsNaN(bearPreview) && bearPreview > 0)
                {
                    var cy = (float)converter.GetChartY(bearPreview) - this.DiamondOffsetPx;
                    DrawDiamond(gr, bearPreviewFill, borderPen, cx, cy, halfBear);
                }
            }
            catch (ArgumentOutOfRangeException)
            {
                break;
            }
        }

        if (this.ShowDiagnostics && !string.IsNullOrWhiteSpace(this.diagnostics))
        {
            using var font = new Font("Segoe UI", 10, FontStyle.Bold, GraphicsUnit.Pixel);
            using var back = new SolidBrush(Color.FromArgb(210, 12, 24, 34));
            using var border2 = new Pen(Color.FromArgb(160, 120, 140, 155), 1f);
            using var text = new SolidBrush(Color.WhiteSmoke);
            var rect = new RectangleF(args.Rectangle.Left + 8, args.Rectangle.Top + 8, 500, 30);
            gr.FillRectangle(back, rect);
            gr.DrawRectangle(border2, rect.X, rect.Y, rect.Width, rect.Height);
            gr.DrawString(this.diagnostics, font, text, rect.Left + 8, rect.Top + 8);
        }
    }

    private static void DrawDiamond(Graphics g, Brush fill, Pen border, float cx, float cy, int halfSize)
    {
        var points = new PointF[]
        {
            new(cx, cy - halfSize),
            new(cx + halfSize, cy),
            new(cx, cy + halfSize),
            new(cx - halfSize, cy)
        };
        g.FillPolygon(fill, points);
        g.DrawPolygon(border, points);
    }

    private void BootstrapCurrentSession()
    {
        if (this.HistoricalData == null || this.HistoricalData.Count == 0)
        {
            this.bootstrapped = true;
            return;
        }

        this.ResetSession(DateTime.MinValue);
        var nowUtc = DateTime.UtcNow;
        var count = this.HistoricalData.Count;

        for (var offset = count - 1; offset >= 1; offset--)
        {
            try
            {
                if (offset >= this.HistoricalData.Count)
                    break;

                if (this.HistoricalData[offset] is not HistoryItemBar bar)
                    continue;

                var barTimeUtc = CoerceUtc(bar.TimeRight == default ? bar.TimeLeft : bar.TimeRight);
                if (barTimeUtc > nowUtc)
                    continue;

                var local = this.ToChartLocalTime(barTimeUtc);
                if (local.Date != this.ToChartLocalTime(nowUtc).Date || !this.IsInSession(local))
                    continue;

                this.ProcessClosedBar(bar, offset, allowRender: true, allowLog: false);
            }
            catch (ArgumentOutOfRangeException)
            {
                break;
            }
        }

        this.bootstrapped = true;
    }

    private void ProcessClosedBar(HistoryItemBar bar, int offset, bool allowRender, bool allowLog)
    {
        var barTimeUtc = CoerceUtc(bar.TimeRight == default ? bar.TimeLeft : bar.TimeRight);
        var barTimeLocal = this.ToChartLocalTime(barTimeUtc);
        if (!this.IsInSession(barTimeLocal))
        {
            this.SetValue(double.NaN, BullishSeriesIndex, offset);
            this.SetValue(double.NaN, BearishSeriesIndex, offset);
            this.SetValue(double.NaN, BullishPreviewSeriesIndex, offset);
            this.SetValue(double.NaN, BearishPreviewSeriesIndex, offset);
            this.diagnostics = "Outside configured session.";
            return;
        }

        if (this.sessionDate != barTimeLocal.Date)
            this.ResetSession(barTimeLocal.Date);

        var barKey = bar.TimeRight == default ? bar.TimeLeft.Ticks : bar.TimeRight.Ticks;
        if (!this.processedBarTicks.Add(barKey))
            return;
        var currentBarIndex = this.processedBarTicks.Count;

        if (!this.TryEvaluateBar(bar, out var result))
        {
            this.SetValue(double.NaN, BullishSeriesIndex, offset);
            this.SetValue(double.NaN, BearishSeriesIndex, offset);
            this.SetValue(double.NaN, BullishPreviewSeriesIndex, offset);
            this.SetValue(double.NaN, BearishPreviewSeriesIndex, offset);
            return;
        }

        this.SetValue(double.NaN, BullishPreviewSeriesIndex, offset);
        this.SetValue(double.NaN, BearishPreviewSeriesIndex, offset);
        this.SetValue(double.NaN, BullishSeriesIndex, offset);
        this.SetValue(double.NaN, BearishSeriesIndex, offset);
        if (allowRender)
        {
            if (result.Bullish)
            {
                var rejectionPrice = bar.Low;
                if (!this.ShouldSuppressRejection(rejectionPrice, currentBarIndex))
                {
                    this.SetValue(rejectionPrice, BullishSeriesIndex, offset);
                    this.TrackRejection(rejectionPrice, currentBarIndex);
                    if (allowLog)
                        this.LogSignal(barTimeUtc, "BULLISH", result.LowerRatio, result.VolumeMultiple, bar);
                }
            }

            if (result.Bearish)
            {
                var rejectionPrice = bar.High;
                if (!this.ShouldSuppressRejection(rejectionPrice, currentBarIndex))
                {
                    this.SetValue(rejectionPrice, BearishSeriesIndex, offset);
                    this.TrackRejection(rejectionPrice, currentBarIndex);
                    if (allowLog)
                        this.LogSignal(barTimeUtc, "BEARISH", result.UpperRatio, result.VolumeMultiple, bar);
                }
            }
        }

        this.diagnostics =
            $"Upper: {result.UpperRatio:0.00} Lower: {result.LowerRatio:0.00} Flow: {result.VolumeMultiple:P0} AggB: {result.AggressorBullish} AggBr: {result.AggressorBearish}";
    }

    private void ProcessPreviewBar(HistoryItemBar bar)
    {
        this.SetValue(double.NaN, BullishPreviewSeriesIndex);
        this.SetValue(double.NaN, BearishPreviewSeriesIndex);
        if (this.sessionDate != this.ToChartLocalTime(CoerceUtc(bar.TimeLeft)).Date)
            return;

        if (!this.TryEvaluateBar(bar, out var result))
            return;

        if (result.Bullish)
        {
            this.SetValue(bar.Low, BullishPreviewSeriesIndex);
        }

        if (result.Bearish)
        {
            this.SetValue(bar.High, BearishPreviewSeriesIndex);
        }

        this.diagnostics =
            $"PREVIEW Upper: {result.UpperRatio:0.00} Lower: {result.LowerRatio:0.00} Flow: {result.VolumeMultiple:P0} AggB: {result.AggressorBullish} AggBr: {result.AggressorBearish}";
    }

    private bool ShouldSuppressRejection(double rejectionPrice, int currentBarIndex)
    {
        if (double.IsNaN(this.lastRejectionPrice) || this.lastRejectionBarIndex < 0)
            return false;

        var barsSinceLastSignal = currentBarIndex - this.lastRejectionBarIndex;
        if (barsSinceLastSignal >= RejectionSuppressionWindowBars)
            return false;

        return Math.Abs(rejectionPrice - this.lastRejectionPrice) <= this.RejectionSuppressionTolerance();
    }

    private void TrackRejection(double rejectionPrice, int currentBarIndex)
    {
        this.lastRejectionPrice = rejectionPrice;
        this.lastRejectionBarIndex = currentBarIndex;
    }

    private double RejectionSuppressionTolerance()
    {
        var tickSize = this.Symbol?.TickSize ?? 0.01;
        return tickSize > 0
            ? tickSize * RejectionSuppressionToleranceTicks
            : 0.02;
    }

    private bool TryEvaluateBar(HistoryItemBar bar, out RejectionResult result)
    {
        result = default;
        var range = bar.High - bar.Low;
        if (range <= 0)
        {
            this.diagnostics = "Skipped zero-range bar.";
            return false;
        }

        var upperWick = bar.High - Math.Max(bar.Open, bar.Close);
        var lowerWick = Math.Min(bar.Open, bar.Close) - bar.Low;
        var upperRatio = upperWick / range;
        var lowerRatio = lowerWick / range;
        // Live aggressor-delta gate — no EWMA, no trailing-window smoothing.
        // Uses THIS bar's own buy/sell split from order-flow volume analysis.
        var aggressorBullish = false;
        var aggressorBearish = false;
        var flowDominance = 0d;   // 0..1 dominant-side fraction (replaces the old volumeMultiple)

        var va = bar.VolumeAnalysisData?.Total;
        if (va != null && va.Volume > 0)
        {
            var gate      = this.AggressorSidePct / 100.0;
            var buyShare  = va.BuyVolume  / va.Volume;
            var sellShare = va.SellVolume / va.Volume;
            aggressorBullish = buyShare  >= gate;
            aggressorBearish = sellShare >= gate;
            flowDominance    = Math.Max(buyShare, sellShare);
        }

        var closeLocation = (bar.Close - bar.Low) / range;
        var bullishStructure = lowerRatio >= this.WickRatioThreshold && closeLocation >= 0.60;
        var bearishStructure = upperRatio >= this.WickRatioThreshold && closeLocation <= 0.40;
        // Gate = wick structure + dominant aggressor flow on the SAME side (zero-lag).
        var bullishQualified = bullishStructure && aggressorBullish;
        var bearishQualified = bearishStructure && aggressorBearish;

        result = new RejectionResult(
            bullishQualified,
            bearishQualified,
            lowerRatio,
            upperRatio,
            flowDominance,
            aggressorBullish,
            aggressorBearish);
        return true;
    }

    private void ResetSession(DateTime date)
    {
        this.sessionDate = date;
        this.diagnostics = string.Empty;
        this.processedBarTicks.Clear();
        this.lastRejectionPrice = double.NaN;
        this.lastRejectionBarIndex = -1;
    }

    private bool IsInSession(DateTime localTime)
    {
        var hhmm = localTime.Hour * 100 + localTime.Minute;
        return hhmm >= this.SessionStartHHMM && hhmm < this.SessionEndHHMM;
    }

    private void LogSignal(DateTime timestampUtc, string direction, double wickRatio, double volumeMultiple, HistoryItemBar bar)
    {
        this.logQueue.Enqueue(string.Join(",",
            timestampUtc.ToString("O", CultureInfo.InvariantCulture),
            Escape(this.Symbol?.Name ?? string.Empty),
            Escape(this.GetTimeframeText()),
            direction,
            wickRatio.ToString("0.####", CultureInfo.InvariantCulture),
            volumeMultiple.ToString("0.####", CultureInfo.InvariantCulture),
            bar.High.ToString("0.####", CultureInfo.InvariantCulture),
            bar.Low.ToString("0.####", CultureInfo.InvariantCulture),
            bar.Close.ToString("0.####", CultureInfo.InvariantCulture)));
    }

    private void FlushLogQueue()
    {
        if (this.logQueue.IsEmpty)
            return;

        if (System.Threading.Interlocked.CompareExchange(ref this.logFlushActive, 1, 0) != 0)
            return;

        try
        {
            var folder = @"C:\Users\Owner\OneDrive\Elmer - Personal\Trading\Research\Quantower Signals";
            Directory.CreateDirectory(folder);
            var path = Path.Combine(folder, "WRI_signals.csv");
            var writeHeader = !File.Exists(path);
            using var writer = new StreamWriter(path, append: true);
            if (writeHeader)
                writer.WriteLine("timestamp,symbol,timeframe,direction,wick_ratio,volume_multiplier,bar_high,bar_low,bar_close");

            while (this.logQueue.TryPeek(out var line))
            {
                writer.WriteLine(line);
                writer.Flush();
                this.logQueue.TryDequeue(out _);
            }
        }
        catch (Exception ex)
        {
            Core.Instance.Loggers.Log(ex);
        }
        finally
        {
            System.Threading.Interlocked.Exchange(ref this.logFlushActive, 0);
        }
    }

    public override void Dispose()
    {
        this.logFlushTimer?.Dispose();
        this.logFlushTimer = null;
        this.FlushLogQueue();
        base.Dispose();
    }

    private string GetTimeframeText()
    {
        var aggregation = this.HistoricalData?.Aggregation;
        return aggregation?.ToString() ?? "Unknown";
    }

    private static DateTime CoerceUtc(DateTime time)
    {
        return time.Kind switch
        {
            DateTimeKind.Utc => time,
            DateTimeKind.Local => time.ToUniversalTime(),
            _ => DateTime.SpecifyKind(time, DateTimeKind.Local).ToUniversalTime()
        };
    }

    private DateTime ToChartLocalTime(DateTime utc)
    {
        var tz = this.CurrentChart?.CurrentTimeZone.TimeZoneInfo
            ?? TimeZoneInfo.Local;
        return TimeZoneInfo.ConvertTimeFromUtc(
            DateTime.SpecifyKind(utc, DateTimeKind.Utc), tz);
    }

    private static Color WithOpacity(Color color, int opacityPct)
    {
        var alpha = (int)Math.Round(Math.Clamp(opacityPct, 20, 80) / 100.0 * 255);
        return Color.FromArgb(alpha, color.R, color.G, color.B);
    }

    private static string Escape(string value)
    {
        if (!value.Contains(',') && !value.Contains('"') && !value.Contains('\n'))
            return value;

        return "\"" + value.Replace("\"", "\"\"") + "\"";
    }

    private readonly record struct RejectionResult(
        bool Bullish,
        bool Bearish,
        double LowerRatio,
        double UpperRatio,
        double VolumeMultiple,
        bool AggressorBullish,
        bool AggressorBearish);
}
