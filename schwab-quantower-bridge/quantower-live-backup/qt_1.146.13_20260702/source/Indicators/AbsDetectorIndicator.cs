using System.Drawing;
using System.Drawing.Drawing2D;
using TradingPlatform.BusinessLayer;
using TradingPlatform.BusinessLayer.Integration;

namespace AbsDetector;

// ---------------------------------------------------------------------------
//  VolumeBaseline — 20-min rolling volume rate with elevation and acceleration
//  Fix 5: NOT internally thread-safe. All callers (AddVolume, TakeRateSample,
//  CurrentRatio, IsAccelerating) must be called under the indicator's stateLock.
// ---------------------------------------------------------------------------
internal sealed class VolumeBaseline
{
    private readonly int _windowSeconds;
    private readonly Queue<(DateTime time, double volume)> _buffer = new();
    private double _bufferTotal;

    // Per-second rate snapshots for acceleration detection (last 10 samples)
    private readonly Queue<double> _rateSamples = new();
    private const int RateSampleCount = 10;

    public VolumeBaseline(int windowMinutes = 20)
    {
        _windowSeconds = windowMinutes * 60;
    }

    public void AddVolume(double volume, DateTime utcTime)
    {
        _buffer.Enqueue((utcTime, volume));
        _bufferTotal += volume;

        // Evict entries outside window
        var cutoff = utcTime.AddSeconds(-_windowSeconds);
        while (_buffer.Count > 0 && _buffer.Peek().time < cutoff)
        {
            _bufferTotal -= _buffer.Dequeue().volume;
        }
    }

    // Sample the current rate (call periodically, e.g. every second)
    public void TakeRateSample()
    {
        var rate = BaselineRatePerSecond;
        _rateSamples.Enqueue(rate);
        if (_rateSamples.Count > RateSampleCount)
            _rateSamples.Dequeue();
    }

    // Average volume per second over the rolling window
    public double BaselineRatePerSecond =>
        _windowSeconds > 0 && _bufferTotal > 0
            ? _bufferTotal / Math.Max(_windowSeconds, 1)
            : 0d;

    // Ratio of recent volume rate to baseline — > 2.0 = significant absorption
    public double CurrentRatio(double recentVolumeInZone, double recentWindowSeconds)
    {
        var baseline = BaselineRatePerSecond * Math.Max(recentWindowSeconds, 1d);
        return baseline > 0d ? recentVolumeInZone / baseline : 0d;
    }

    // True if volume rate is increasing — acceleration confirms active defense
    public bool IsAccelerating
    {
        get
        {
            if (_rateSamples.Count < 4)
                return false;
            var samples = _rateSamples.ToArray();
            var firstHalf = samples.Take(samples.Length / 2).Average();
            var secondHalf = samples.Skip(samples.Length / 2).Average();
            return secondHalf > firstHalf * 1.10d; // 10% acceleration threshold
        }
    }

    public bool HasBaseline => _bufferTotal > 0 && _buffer.Count >= 10;

    public void Reset()
    {
        _buffer.Clear();
        _rateSamples.Clear();
        _bufferTotal = 0d;
    }
}

// ---------------------------------------------------------------------------
//  AbsorptionZone — ATR-relative price zone primitive
//  Replaces single-price LevelTracker. Tracks defense across a band.
// ---------------------------------------------------------------------------
internal sealed class AbsorptionZone
{
    public double ZoneTop { get; private set; }
    public double ZoneBottom { get; private set; }
    public double ZoneMid => (ZoneTop + ZoneBottom) / 2d;
    public bool IsActive => ZoneTop > 0d && ZoneBottom >= 0d;

    // Volume accumulated inside zone since sequence started
    public double ZoneVolume { get; private set; }
    public DateTime ZoneVolumeWindowStart { get; private set; }

    // Reload tracking across all levels in zone
    private double _sizeBaseline;
    private double _sizeTrough;
    private int _reloadCount;
    private bool _droppedBelowThreshold;

    public int ReloadCount => _reloadCount;

    // Define zone around a tested price using ATR-derived half-width
    public void Define(double testedPrice, double halfWidth, int side)
    {
        // BUY ABS (side=+1): zone is below tested price — bid defense zone
        // SELL ABS (side=-1): zone is above tested price — ask defense zone
        if (side > 0)
        {
            ZoneTop = testedPrice;
            ZoneBottom = Math.Max(0d, testedPrice - halfWidth);
        }
        else
        {
            ZoneTop = testedPrice + halfWidth;
            ZoneBottom = testedPrice;
        }

        ZoneVolume = 0d;
        ZoneVolumeWindowStart = DateTime.UtcNow;
        _sizeBaseline = 0d;
        _sizeTrough = 0d;
        _reloadCount = 0;
        _droppedBelowThreshold = false;
    }

    // Returns true if price is within the zone
    public bool Contains(double price) =>
        IsActive && price >= ZoneBottom && price <= ZoneTop;

    // Add tape volume when a print occurs inside the zone
    public void AddZoneVolume(double volume) => ZoneVolume += volume;

    // Update DOM size for reload detection — price must be inside zone
    public void OnSizeUpdate(double newSize, double price)
    {
        if (!Contains(price))
            return;

        if (_sizeBaseline <= 0d)
        {
            _sizeBaseline = newSize;
            _sizeTrough = newSize;
            return;
        }

        if (newSize < _sizeTrough)
            _sizeTrough = newSize;

        if (!_droppedBelowThreshold && _sizeTrough < _sizeBaseline * 0.70d)
            _droppedBelowThreshold = true;

        if (_droppedBelowThreshold && newSize >= _sizeBaseline * 0.80d)
        {
            _reloadCount++;
            _droppedBelowThreshold = false;
            _sizeTrough = newSize;          // Fix 4: was write-only property alias — now direct
            _sizeBaseline = newSize;
        }
    }

    // Fix 1: accepts captured now — avoids DateTime.UtcNow allocation on every call
    public double GetZoneVolumeWindowSeconds(DateTime now) =>
        ZoneVolumeWindowStart == default ? 0d :
        (now - ZoneVolumeWindowStart).TotalSeconds;

    // Re-anchor zone if price drifts — keeps zone tracking current defense area
    public void ReAnchor(double newTestedPrice, double halfWidth, int side)
    {
        Define(newTestedPrice, halfWidth, side);
    }

    public void Reset()
    {
        ZoneTop = 0d;
        ZoneBottom = 0d;
        ZoneVolume = 0d;
        ZoneVolumeWindowStart = default;   // Fix 3: was never reset — latent stale window bug
        _sizeBaseline = 0d;
        _sizeTrough = 0d;
        _reloadCount = 0;
        _droppedBelowThreshold = false;
    }
}

// ---------------------------------------------------------------------------
//  AbsorptionState
// ---------------------------------------------------------------------------
internal enum AbsorptionState
{
    Idle,
    AggressionDetected,
    LevelTested,
    Defended,
    SignalFired
}

// ---------------------------------------------------------------------------
//  AbsorptionStateMachine — zone-aware, volume-gated
// ---------------------------------------------------------------------------
internal sealed class AbsorptionStateMachine
{
    private static readonly TimeSpan SequenceWindow = TimeSpan.FromSeconds(12);

    public AbsorptionState State { get; private set; } = AbsorptionState.Idle;
    public int Side { get; private set; }
    public DateTime SignalFiredUtc { get; private set; }
    public string SignalType { get; private set; } = "ABS";
    public int PrintsAtLevel { get; private set; }

    // The zone this sequence is defending
    public readonly AbsorptionZone Zone = new();

    private DateTime _sequenceStartUtc;

    public void OnAggression(int aggressionSide, double price, double aggressionScore, double zoneHalfWidth, double minAggressionScore)
    {
        if (aggressionScore < minAggressionScore)
            return;

        if (State != AbsorptionState.Idle && Side == aggressionSide)
        {
            CheckExpiry();
            return;
        }

        State = AbsorptionState.AggressionDetected;
        Side = aggressionSide;
        PrintsAtLevel = 0;
        _sequenceStartUtc = DateTime.UtcNow;
        SignalType = "ABS";
        Zone.Define(price, zoneHalfWidth, aggressionSide);
    }

    public void OnPriceTest(double lastPrice, int minPrintsRequired)
    {
        if (State != AbsorptionState.AggressionDetected)
        {
            CheckExpiry();
            return;
        }

        // Price test: must be within or at the zone boundary
        var atOrThrough = Side > 0
            ? lastPrice <= Zone.ZoneTop
            : lastPrice >= Zone.ZoneBottom;

        if (atOrThrough)
        {
            PrintsAtLevel++;
            if (PrintsAtLevel >= minPrintsRequired)
                State = AbsorptionState.LevelTested;
        }
        else
        {
            PrintsAtLevel = 0;
        }

        CheckExpiry();
    }

    public void OnZoneVolume(double volume, double price)
    {
        if (State == AbsorptionState.Idle || State == AbsorptionState.SignalFired)
            return;

        if (Zone.Contains(price))
            Zone.AddZoneVolume(volume);
    }

    public void OnDefense(
        double depthScore,
        double volumeRatio,
        bool volumeAccelerating,
        double volumeElevationThreshold,
        double footprintAligned,
        bool footprintAvailable,
        bool requireFootprintConfirmation)
    {
        if (State != AbsorptionState.LevelTested)
        {
            CheckExpiry();
            return;
        }

        // Gate 1: depth holding in zone
        var depthDefended = depthScore >= 0.55d && (Zone.ReloadCount >= 1 || depthScore >= 0.80d);
        if (!depthDefended)
        {
            CheckExpiry();
            return;
        }

        // Gate 2: volume elevated in zone vs baseline
        var volumeConfirmed = volumeRatio >= volumeElevationThreshold;
        if (!volumeConfirmed)
        {
            CheckExpiry();
            return;
        }

        // Gate 3: optional footprint confirmation
        if (requireFootprintConfirmation && footprintAvailable && footprintAligned <= 0d)
        {
            CheckExpiry();
            return;
        }

        State = AbsorptionState.Defended;

        // ICE: multiple reloads = hidden liquidity replenishing
        // ICE + acceleration = high-conviction iceberg
        SignalType = Zone.ReloadCount >= 2
            ? (volumeAccelerating ? "ICE+" : "ICE")
            : "ABS";

        AdvanceToSignal();
    }

    public void OnBreakthrough(double lastPrice)
    {
        if (State == AbsorptionState.Idle || State == AbsorptionState.SignalFired)
            return;

        var broke = Side > 0
            ? lastPrice < Zone.ZoneBottom - 1e-9
            : lastPrice > Zone.ZoneTop + 1e-9;

        if (broke)
            Reset();
    }

    public void Reset()
    {
        State = AbsorptionState.Idle;
        Side = 0;
        PrintsAtLevel = 0;
        _sequenceStartUtc = default;
        Zone.Reset();
    }

    private void AdvanceToSignal()
    {
        State = AbsorptionState.SignalFired;
        SignalFiredUtc = DateTime.UtcNow;
    }

    private void CheckExpiry()
    {
        if (State != AbsorptionState.Idle &&
            State != AbsorptionState.SignalFired &&
            _sequenceStartUtc != default &&
            DateTime.UtcNow - _sequenceStartUtc > SequenceWindow)
        {
            Reset();
        }
    }
}

// ---------------------------------------------------------------------------
//  Fired signal — immutable record held for drawing
// ---------------------------------------------------------------------------
internal sealed record FiredSignal(
    int Side,
    string Type,
    double ZoneTop,
    double ZoneBottom,
    DateTime FiredUtc,
    int BarIndexAtFire);

// ---------------------------------------------------------------------------
//  Main indicator
// ---------------------------------------------------------------------------
public sealed class AbsDetectorIndicator : Indicator, IVolumeAnalysisIndicator
{
    private readonly object stateLock = new();

    private DateTime lastDomUtc;
    private DateTime lastTapeUtc;
    private DateTime lastFootprintUtc;

    private double lastBid;
    private double lastAsk;
    private double recentBuyAggression;
    private double recentSellAggression;
    private double currentDelta;
    private double currentVolume;
    private double topBidSize;
    private double topAskSize;
    private double score;

    // ATR-derived zone half-width — recalculated on every bar close
    private double _zoneHalfWidth = 0.10d;
    private int _lastAtrBarIndex = -1;

    // Fired signals held for drawing (up to MaxSignalsDrawn)
    private readonly List<FiredSignal> _firedSignals = new();
    private const int MaxSignalsDrawn = 10;

    private DateTime lastFootprintReadUtc;
    private DateTime lastDecayUtc;
    private DateTime lastBaselineSampleUtc;

    private readonly AbsorptionStateMachine _machine = new();
    private readonly VolumeBaseline _volumeBaseline = new(20);

    // Cached GDI resources
    private Font? _cachedFont;
    private int _cachedFontSize = -1;

    private static readonly TimeSpan DecayInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan FreshnessWindow = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan BaselineSampleInterval = TimeSpan.FromSeconds(1);

    // ---------------------------------------------------------------------------
    //  Input parameters
    // ---------------------------------------------------------------------------
    [InputParameter("Label corner", 0, variants: new object[]
    {
        "Top left", 0, "Top right", 1, "Bottom left", 2, "Bottom right", 3
    })]
    public int LabelCorner { get; set; } = 0;

    [InputParameter("Label X offset", 1, 0, 500, 1, 0)]
    public int LabelXOffset { get; set; } = 12;

    [InputParameter("Label Y offset", 2, 0, 500, 1, 0)]
    public int LabelYOffset { get; set; } = 34;

    [InputParameter("Label font size", 3, 8, 24, 1, 0)]
    public int LabelFontSize { get; set; } = 11;

    [InputParameter("Label padding", 4, 2, 24, 1, 0)]
    public int LabelPadding { get; set; } = 6;

    [InputParameter("Footprint read interval ms", 5, 250, 5000, 250, 0)]
    public int FootprintReadIntervalMs { get; set; } = 1000;

    [InputParameter("Signal hold seconds", 7, 0, 60, 1, 0)]
    public int SignalHoldSeconds { get; set; } = 8;

    [InputParameter("Min prints to confirm test", 9, 1, 20, 1, 0)]
    public int MinPrintsToConfirmTest { get; set; } = 3;

    [InputParameter("Volume elevation threshold", 11, 1.0, 5.0, 0.1, 1)]
    public double VolumeElevationThreshold { get; set; } = 2.0;

    [InputParameter("Require footprint confirmation", 10)]
    public bool RequireFootprintConfirmation { get; set; } = false;

    [InputParameter("Show diagnostics", 8)]
    public bool ShowDiagnostics { get; set; } = false;

    [InputParameter("ATR period (bars)", 12, 5, 50, 1, 0)]
    public int AtrPeriod { get; set; } = 20;

    [InputParameter("ATR zone fraction", 13, 0.05, 0.50, 0.01, 2)]
    public double AtrZoneFraction { get; set; } = 0.20;

    [InputParameter("Depth saturation (shares)", 14, 200, 50000, 100, 0)]
    public double DepthSaturationShares { get; set; } = 2000;   // was hardcoded 5000

    [InputParameter("Tape saturation (shares)", 15, 200, 50000, 100, 0)]
    public double TapeSaturationShares { get; set; } = 3000;     // was hardcoded 10000

    [InputParameter("Arming score (0-1)", 16, 0.05, 0.90, 0.01, 2)]
    public double ArmingScore { get; set; } = 0.20;              // was hardcoded 0.25

    [InputParameter("Zone half-width max ($)", 17, 0.10, 5.00, 0.05, 2)]
    public double ZoneHalfWidthMax { get; set; } = 1.00;         // was hardcoded 0.50

    public AbsDetectorIndicator()
    {
        Name = "ABS Detector";
        Description = "Institutional absorption detector — ATR zone, volume-gated state machine.";
        SeparateWindow = false;
        OnBackGround = false;
        AddLineSeries("Probe", Color.Transparent, 1, LineStyle.Solid);
    }

    public bool IsRequirePriceLevelsCalculation => true;

    public void VolumeAnalysisData_Loaded()
    {
        lock (stateLock)
            lastFootprintUtc = DateTime.UtcNow;
    }

    protected override void OnInit()
    {
        base.OnInit();
        if (Symbol == null) return;
        Symbol.NewQuote += OnNewQuote;
        Symbol.NewLast += OnNewLast;
        Symbol.NewLevel2 += OnNewLevel2;
    }

    protected override void OnUpdate(UpdateArgs args)
    {
        var now = DateTime.UtcNow;

        // ATR recalc — only on new bar close
        var currentBarIndex = HistoricalData?.Count ?? 0;
        if (currentBarIndex != _lastAtrBarIndex && currentBarIndex >= AtrPeriod + 1)
        {
            _lastAtrBarIndex = currentBarIndex;
            RecalculateAtr();
        }

        // Footprint read
        var fpInterval = TimeSpan.FromMilliseconds(Math.Max(FootprintReadIntervalMs, 250));
        if (now - lastFootprintReadUtc >= fpInterval)
        {
            lastFootprintReadUtc = now;
            ReadCurrentFootprint();
        }

        // Tape pressure decay
        if (now - lastDecayUtc >= DecayInterval)
        {
            lastDecayUtc = now;
            DecayTapePressure();
        }

        // Volume baseline rate sample (once per second)
        if (now - lastBaselineSampleUtc >= BaselineSampleInterval)
        {
            lastBaselineSampleUtc = now;
            lock (stateLock)
                _volumeBaseline.TakeRateSample();
        }
    }

    public override void OnPaintChart(PaintChartEventArgs args)
    {
        base.OnPaintChart(args);

        var now = DateTime.UtcNow;
        PaintSnapshot snapshot;
        List<FiredSignal> signals;

        lock (stateLock)
        {
            snapshot = new PaintSnapshot(
                IsFresh(lastDomUtc, now),
                IsFresh(lastTapeUtc, now),
                IsFresh(lastFootprintUtc, now),
                score,
                _machine.State,
                _machine.Side,
                _machine.PrintsAtLevel,
                _machine.Zone.ZoneTop,
                _machine.Zone.ZoneBottom,
                _machine.Zone.ZoneMid,
                _zoneHalfWidth);

            signals = new List<FiredSignal>(_firedSignals);
        }

        var safeFontSize = Math.Clamp(LabelFontSize, 8, 24);
        if (_cachedFont == null || _cachedFontSize != safeFontSize)
        {
            _cachedFont?.Dispose();
            _cachedFont = new Font("Segoe UI", safeFontSize, FontStyle.Bold, GraphicsUnit.Pixel);
            _cachedFontSize = safeFontSize;
        }

        var chartWindow = CurrentChart?.MainWindow;
        var chartRect = ResolveChartRectangle();
        DrawZoneBands(args.Graphics, signals, chartRect, chartWindow?.CoordinatesConverter, now, _cachedFont, LabelPadding, SignalHoldSeconds);

        if (ShowDiagnostics)
            DrawDiagnostics(args.Graphics, snapshot, chartRect, LabelCorner, LabelXOffset, LabelYOffset, _cachedFont, LabelPadding);
    }

    protected override void OnClear()
    {
        if (Symbol != null)
        {
            Symbol.NewQuote -= OnNewQuote;
            Symbol.NewLast -= OnNewLast;
            Symbol.NewLevel2 -= OnNewLevel2;
        }
        _cachedFont?.Dispose();
        _cachedFont = null;
        base.OnClear();
    }

    // ---------------------------------------------------------------------------
    //  ATR auto-calibration
    // ---------------------------------------------------------------------------
    private void RecalculateAtr()
    {
        try
        {
            if (HistoricalData == null || HistoricalData.Count < AtrPeriod + 1)
                return;

            double atrSum = 0d;
            for (var i = 1; i <= AtrPeriod; i++)
            {
                if (HistoricalData[i] is not HistoryItemBar bar || HistoricalData[i + 1] is not HistoryItemBar previousBar)
                    return;

                var high = bar.High;
                var low = bar.Low;
                var prevClose = previousBar.Close;

                var tr = Math.Max(high - low,
                         Math.Max(Math.Abs(high - prevClose),
                                  Math.Abs(low - prevClose)));
                atrSum += tr;
            }

            var atr = atrSum / AtrPeriod;
            var raw = atr * AtrZoneFraction;

            lock (stateLock)
                _zoneHalfWidth = Math.Clamp(raw, 0.05d, ZoneHalfWidthMax);
        }
        catch (InvalidOperationException) { }
    }

    // ---------------------------------------------------------------------------
    //  Event handlers
    // ---------------------------------------------------------------------------
    private void OnNewQuote(Symbol symbol, Quote quote)
    {
        lock (stateLock)
        {
            lastBid = quote.Bid;
            lastAsk = quote.Ask;
        }
    }

    private void OnNewLast(Symbol symbol, Last last)
    {
        lock (stateLock)
        {
            lastTapeUtc = ToUtc(last.Time);
            var side = ResolveAggressor(last);

            if (side > 0) recentBuyAggression += last.Size;
            else if (side < 0) recentSellAggression += last.Size;

            _volumeBaseline.AddVolume(last.Size, DateTime.UtcNow);
            _machine.OnZoneVolume(last.Size, last.Price);

            if (side != 0)
            {
                var tapeScore = Saturate(Math.Max(recentBuyAggression, recentSellAggression), TapeSaturationShares);
                _machine.OnAggression(-side, last.Price, tapeScore, _zoneHalfWidth, ArmingScore);
                _machine.OnPriceTest(last.Price, MinPrintsToConfirmTest);
                _machine.OnBreakthrough(last.Price);
            }

            // Fix 2: check State before promoting — machine may have already been reset
            // by a prior PromoteSignal call in this same lock cycle
            if (_machine.State == AbsorptionState.SignalFired)
                PromoteSignal();

            RecalculateScore();
        }
    }

    private void OnNewLevel2(Symbol symbol, Level2Quote level2, DOMQuote dom)
    {
        lock (stateLock)
        {
            var now = DateTime.UtcNow;  // Fix 1: capture once — avoids repeated allocation in property getters
            lastDomUtc = now;

            if (dom != null)
            {
                if (dom.Bids.Count > 0)
                {
                    topBidSize = dom.Bids[0].Size;
                    if (_machine.Side > 0)
                        _machine.Zone.OnSizeUpdate(topBidSize, dom.Bids[0].Price);
                }
                if (dom.Asks.Count > 0)
                {
                    topAskSize = dom.Asks[0].Size;
                    if (_machine.Side < 0)
                        _machine.Zone.OnSizeUpdate(topAskSize, dom.Asks[0].Price);
                }
            }
            else if (level2 != null)
            {
                if (level2.PriceType == QuotePriceType.Bid)
                {
                    topBidSize = level2.Closed ? 0d : level2.Size;
                    if (_machine.Side > 0)
                        _machine.Zone.OnSizeUpdate(topBidSize, level2.Price);
                }
                else if (level2.PriceType == QuotePriceType.Ask)
                {
                    topAskSize = level2.Closed ? 0d : level2.Size;
                    if (_machine.Side < 0)
                        _machine.Zone.OnSizeUpdate(topAskSize, level2.Price);
                }
            }

            if (_machine.State == AbsorptionState.LevelTested)
            {
                var isBuyAbs = _machine.Side > 0;
                var defendingSize = isBuyAbs ? topBidSize : topAskSize;
                var depthScore = Saturate(defendingSize, DepthSaturationShares);
                var volumeRatio = _volumeBaseline.CurrentRatio(
                    _machine.Zone.ZoneVolume,
                    _machine.Zone.GetZoneVolumeWindowSeconds(now));
                var footprintAligned = isBuyAbs
                    ? Math.Max(-currentDelta, 0d)
                    : Math.Max(currentDelta, 0d);

                _machine.OnDefense(
                    depthScore,
                    volumeRatio,
                    _volumeBaseline.IsAccelerating,
                    VolumeElevationThreshold,
                    footprintAligned,
                    currentVolume > 0d,
                    RequireFootprintConfirmation);
            }

            if (_machine.State == AbsorptionState.SignalFired)
                PromoteSignal();

            RecalculateScore();
        }
    }

    private void PromoteSignal()
    {
        // Fix 2: State check at call site ensures we don't double-promote.
        // Secondary guard: 500ms window catches any edge case where SignalFiredUtc
        // is stale from a prior cycle that was not yet reset.
        if (_machine.State != AbsorptionState.SignalFired)
            return;

        if (DateTime.UtcNow - _machine.SignalFiredUtc > TimeSpan.FromMilliseconds(500))
            return;

        var barIndex = HistoricalData?.Count ?? 0;
        var signal = new FiredSignal(
            _machine.Side,
            _machine.SignalType,
            _machine.Zone.ZoneTop,
            _machine.Zone.ZoneBottom,
            _machine.SignalFiredUtc,
            barIndex);

        _firedSignals.Add(signal);
        if (_firedSignals.Count > MaxSignalsDrawn)
            _firedSignals.RemoveAt(0);

        // Reset clears State to Idle — second call from Level2 handler will no-op
        _machine.Reset();
    }

    // ---------------------------------------------------------------------------
    //  Drawing
    // ---------------------------------------------------------------------------
    private static void DrawZoneBands(
        Graphics g,
        List<FiredSignal> signals,
        RectangleF chartRect,
        TradingPlatform.BusinessLayer.Chart.IChartWindowCoordinatesConverter? coordinatesConverter,
        DateTime now,
        Font font,
        int padding,
        int holdSeconds)
    {
        foreach (var signal in signals)
        {
            var age = now - signal.FiredUtc;
            var maxAge = TimeSpan.FromSeconds(Math.Max(holdSeconds, 0));
            if (age > maxAge) continue;

            // Fade alpha over hold period
            var fadeRatio = 1.0 - (age.TotalSeconds / Math.Max(maxAge.TotalSeconds, 1));
            var alpha = (int)(fadeRatio * 200);
            if (alpha < 10) continue;

            // Resolve Y coordinates — need to map price to chart pixel Y
            // Quantower chart: higher price = lower Y
            // We resolve via the chart rectangle and visible price range
            // If chart coordinate mapping is unavailable, skip drawing
            var topY = ResolvePriceToY(signal.ZoneTop, chartRect, coordinatesConverter);
            var botY = ResolvePriceToY(signal.ZoneBottom, chartRect, coordinatesConverter);
            if (topY < 0 || botY < 0) continue;
            var bandTopY = Math.Min(topY, botY);
            var bandBottomY = Math.Max(topY, botY);

            var isBuy = signal.Side > 0;
            var fillColor = isBuy
                ? Color.FromArgb(alpha / 4, 0, 200, 80)
                : Color.FromArgb(alpha / 4, 200, 50, 50);
            var lineColor = isBuy
                ? Color.FromArgb(alpha, 0, 200, 80)
                : Color.FromArgb(alpha, 220, 60, 60);

            // Fill band
            using var fillBrush = new SolidBrush(fillColor);
            g.FillRectangle(fillBrush, chartRect.Left, bandTopY, chartRect.Width, bandBottomY - bandTopY);

            // Top dashed line
            using var linePen = new Pen(lineColor, 1.5f);
            linePen.DashStyle = DashStyle.Dash;
            g.DrawLine(linePen, chartRect.Left, topY, chartRect.Right, topY);
            g.DrawLine(linePen, chartRect.Left, botY, chartRect.Right, botY);

            // Right-anchored label on the defending boundary
            var labelPrice = isBuy ? signal.ZoneBottom : signal.ZoneTop;
            var labelY = isBuy ? botY : topY;
            var labelText = $"{signal.Type} {labelPrice:0.00}";
            var labelSize = g.MeasureString(labelText, font);
            var labelX = chartRect.Right - labelSize.Width - padding * 2f;

            using var labelBack = new SolidBrush(Color.FromArgb(alpha, 15, 18, 22));
            using var labelFore = new SolidBrush(lineColor);
            g.FillRectangle(labelBack, labelX - padding, labelY - labelSize.Height - padding, labelSize.Width + padding * 2f, labelSize.Height + padding);
            g.DrawString(labelText, font, labelFore, labelX, labelY - labelSize.Height - padding / 2f);
        }
    }

    private static float ResolvePriceToY(
        double price,
        RectangleF chartRect,
        TradingPlatform.BusinessLayer.Chart.IChartWindowCoordinatesConverter? coordinatesConverter)
    {
        if (coordinatesConverter == null || price <= 0d)
            return -1f;

        try
        {
            var y = coordinatesConverter.GetChartY(price);
            if (double.IsNaN(y) || double.IsInfinity(y))
                return -1f;

            return y < chartRect.Top - 1000d || y > chartRect.Bottom + 1000d
                ? -1f
                : (float)y;
        }
        catch
        {
            return -1f;
        }
    }

    private static void DrawDiagnostics(
        Graphics g,
        PaintSnapshot snapshot,
        RectangleF chartRect,
        int corner, int xOffset, int yOffset,
        Font font, int padding)
    {
        var safePadding = Math.Clamp(padding, 2, 24);
        var feedsOk = snapshot.DomOk && snapshot.TapeOk && snapshot.FootprintOk;
        string text;
        Color color;

        if (!feedsOk)
        {
            text = $"DOM {Flag(snapshot.DomOk)} | TAPE {Flag(snapshot.TapeOk)} | FP {Flag(snapshot.FootprintOk)}";
            color = Color.FromArgb(218, 58, 45, 25);
        }
        else if (snapshot.MachineState == AbsorptionState.Idle)
        {
            text = $"IDLE  SCORE:{snapshot.Score:0}  ZONE±{snapshot.ZoneHalfWidth:0.00}";
            color = Color.FromArgb(200, 20, 35, 55);
        }
        else
        {
            text = $"{snapshot.MachineState}  [{snapshot.ZoneBottom:0.00}–{snapshot.ZoneTop:0.00}]  P:{snapshot.PrintsAtLevel}  SCORE:{snapshot.Score:0}";
            color = Color.FromArgb(200, 20, 35, 55);
        }

        var size = g.MeasureString(text, font);
        var rect = ResolveBadgeRectangle(chartRect, corner, xOffset, yOffset, size.Width + safePadding * 3f, size.Height + safePadding * 2f);
        using var back = new SolidBrush(color);
        using var border = new Pen(Color.FromArgb(170, 230, 230, 230), 1f);
        using var fore = new SolidBrush(Color.WhiteSmoke);
        g.FillRectangle(back, rect);
        g.DrawRectangle(border, rect.X, rect.Y, rect.Width, rect.Height);
        g.DrawString(text, font, fore, rect.X + safePadding + 2f, rect.Y + safePadding);
    }

    private static RectangleF ResolveBadgeRectangle(RectangleF chart, int corner, int xOffset, int yOffset, float width, float height)
    {
        var x = corner is 1 or 3 ? chart.Right - width - Math.Max(0, xOffset) : chart.Left + Math.Max(0, xOffset);
        var y = corner is 2 or 3 ? chart.Bottom - height - Math.Max(0, yOffset) : chart.Top + Math.Max(0, yOffset);
        x = Math.Clamp(x, chart.Left, Math.Max(chart.Left, chart.Right - width));
        y = Math.Clamp(y, chart.Top, Math.Max(chart.Top, chart.Bottom - height));
        return new RectangleF(x, y, width, height);
    }

    // ---------------------------------------------------------------------------
    //  Helpers
    // ---------------------------------------------------------------------------
    private void ReadCurrentFootprint()
    {
        try
        {
            if (HistoricalData == null || HistoricalData.Count == 0) return;
            var total = HistoricalData[0].VolumeAnalysisData?.Total;
            if (total == null) return;
            lock (stateLock)
            {
                currentDelta = total.Delta;
                currentVolume = total.Volume;
                lastFootprintUtc = DateTime.UtcNow;
                RecalculateScore();
            }
        }
        catch (InvalidOperationException) { }
    }

    private void DecayTapePressure()
    {
        lock (stateLock)
        {
            recentBuyAggression *= 0.94d;
            recentSellAggression *= 0.94d;
            if (recentBuyAggression < 1d) recentBuyAggression = 0d;
            if (recentSellAggression < 1d) recentSellAggression = 0d;
            RecalculateScore();
        }
    }

    private int ResolveAggressor(Last last)
    {
        if (last.AggressorFlag == AggressorFlag.Buy) return 1;
        if (last.AggressorFlag == AggressorFlag.Sell) return -1;
        if (lastAsk > 0 && last.Price >= lastAsk) return 1;
        if (lastBid > 0 && last.Price <= lastBid) return -1;
        return 0;
    }

    private void RecalculateScore()
    {
        var buyPressure = recentBuyAggression;
        var sellPressure = recentSellAggression;
        var activeSide = buyPressure >= sellPressure ? 1 : -1;
        var aggressive = Math.Max(buyPressure, sellPressure);

        if (aggressive <= 0) { score = 0; return; }

        var defendingDepth = activeSide > 0 ? topAskSize : topBidSize;
        var depthSupport = Saturate(defendingDepth, DepthSaturationShares);
        var tapeSupport = Saturate(aggressive, TapeSaturationShares);
        var footprintAligned = activeSide > 0 ? Math.Max(currentDelta, 0d) : Math.Max(-currentDelta, 0d);
        var footprintSupport = Saturate(footprintAligned, Math.Max(currentVolume * 0.20d, 1d));
        var dominance = Math.Abs(buyPressure - sellPressure) / Math.Max(buyPressure + sellPressure, 1d);

        score = Math.Clamp(100d * (
            0.34d * tapeSupport +
            0.30d * depthSupport +
            0.24d * footprintSupport +
            0.12d * Math.Clamp(dominance, 0d, 1d)), 0d, 100d);
    }

    private RectangleF ResolveChartRectangle()
    {
        try { return CurrentChart?.MainWindow?.ClientRectangle ?? new RectangleF(0f, 0f, 800f, 600f); }
        catch { return new RectangleF(0f, 0f, 800f, 600f); }
    }

    private static bool IsFresh(DateTime t, DateTime now) =>
        t != default && now - t <= FreshnessWindow;

    private static string Flag(bool ok) => ok ? "OK" : "--";

    private static double Saturate(double value, double scale) =>
        value <= 0d || scale <= 0d ? 0d : 1d - Math.Exp(-value / scale);

    private static DateTime ToUtc(DateTime value)
    {
        if (value == default) return DateTime.UtcNow;
        return value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
    }

    // ---------------------------------------------------------------------------
    //  Snapshot structs
    // ---------------------------------------------------------------------------
    private readonly record struct PaintSnapshot(
        bool DomOk,
        bool TapeOk,
        bool FootprintOk,
        double Score,
        AbsorptionState MachineState,
        int MachineSide,
        int PrintsAtLevel,
        double ZoneTop,
        double ZoneBottom,
        double ZoneMid,
        double ZoneHalfWidth);
}
