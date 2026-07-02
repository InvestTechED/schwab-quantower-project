using System.Drawing;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using TradingPlatform.BusinessLayer;
using TradingPlatform.BusinessLayer.Integration;

namespace FlowTools;

// ---------------------------------------------------------------------------
//  AbnormalVolume — Institutional real-time volume anomaly detector
//
//  Arrow generation logic:
//    1. Volume scaling     — auto-detects dxFeed (x100) vs native (x1)
//    2. Session filter     — bars from 9:30 AM only
//    3. Rolling average    — session volume baseline
//    4. Gate 1             — scaledVol >= avgVol x MinFloorPct (floor)
//    5. Gate 2             — scaledVol >= avgVol x AvgVolMultiplier (threshold)
//    6. Real-time imbalance — tick-by-tick Lee-Ready bid/offer classification
//                            Arrow appears when offer% >= threshold (green)
//                            or bid% >= threshold (red)
//                            Arrow disappears if ratio falls back below threshold
//                            Arrow reappears if ratio crosses again — live state
//    7. Baseline Z-score   — current imbalance vs rolling 30-min session average
//                            Z >= 1.5 = standard arrow, Z >= 2.0 = significant
//
//  Research basis:
//    Lee & Ready (1991)              — tick aggressor classification via NBBO
//    Chordia, Roll & Sub. (2002)     — 60% imbalance = min statistically significant
//    Hasbrouck & Sofianos (1993)     — contested levels = bidirectional flickering
//    Admati & Pfleiderer (1988)      — session baseline normalization
// ---------------------------------------------------------------------------
public sealed class AbnormalVolumeIndicator : Indicator, IVolumeAnalysisIndicator
{
    // ── Volume baseline parameters ────────────────────────────────────────────
    [InputParameter("Min floor (% of avg volume)", 0, 0.10, 2.0, 0.05, 2)]
    public double MinFloorPct { get; set; } = 0.50;

    [InputParameter("Avg volume multiplier", 1, 1.0, 10.0, 0.25, 2)]
    public double AvgVolMultiplier { get; set; } = 2.0;

    [InputParameter("Session start (HHMM)", 2, 0, 2359, 1, 0)]
    public int SessionStartHHMM { get; set; } = 930;

    // ── Real-time imbalance parameters ────────────────────────────────────────
    // Chordia et al. (2002): 60% is the minimum statistically significant
    // bid/offer imbalance on Nasdaq. Range 55-80% exposed for calibration.
    [InputParameter("Imbalance threshold (%)", 10, 55, 80, 1, 0)]
    public int ImbalanceThresholdPct { get; set; } = 60;

    [InputParameter("Depth levels", 21, 1, 10, 1, 0)]
    public int DepthLevels { get; set; } = 5;

    // ── Baseline deviation parameters ─────────────────────────────────────────
    // Rolling window of per-minute bid/offer readings since session open.
    // Z-score >= ZScoreSignificant triggers the large outlined arrow.
    [InputParameter("Baseline window (minutes)", 11, 5, 60, 5, 0)]
    public int BaselineWindowMinutes { get; set; } = 30;

    // Event-time baseline (replaces clock-minute sampling). Tune BaselineBucketSize per symbol.
    [InputParameter("Baseline bucket size (shares)", 22, 100, 5000000, 100, 0)]
    public double BaselineBucketSize { get; set; } = 5000;

    [InputParameter("Baseline buckets (ring length)", 23, 8, 300, 1, 0)]
    public int BaselineBuckets { get; set; } = 50;

    [InputParameter("Z-score standard threshold", 12, 0.5, 3.0, 0.1, 1)]
    public double ZScoreStandard { get; set; } = 1.5;

    [InputParameter("Z-score significant threshold", 13, 1.0, 4.0, 0.1, 1)]
    public double ZScoreSignificant { get; set; } = 2.0;

    [InputParameter("Z-score institutional threshold", 16, 2.0, 4.0, 0.1, 1)]
    public double ZScoreInstitutional { get; set; } = 2.5;

    [InputParameter("Z-score regime shift threshold", 17, 2.5, 5.0, 0.1, 1)]
    public double ZScoreRegimeShift { get; set; } = 3.0;

    // ── Display parameters ────────────────────────────────────────────────────
    [InputParameter("Show upside signals", 3)]
    public bool ShowUpside { get; set; } = true;

    [InputParameter("Show downside signals", 4)]
    public bool ShowDownside { get; set; } = true;

    [InputParameter("Upside arrow color", 5)]
    public Color UpsideColor { get; set; } = Color.FromArgb(255, 0, 220, 90);

    [InputParameter("Downside arrow color", 6)]
    public Color DownsideColor { get; set; } = Color.FromArgb(255, 220, 55, 55);

    [InputParameter("Arrow size (px)", 7, 4, 20, 1, 0)]
    public int ArrowSize { get; set; } = 8;

    [InputParameter("Arrow offset (px from candle)", 8, 1, 20, 1, 0)]
    public int ArrowOffset { get; set; } = 4;

    [InputParameter("Significant arrow size (px)", 14, 6, 30, 1, 0)]
    public int SignificantArrowSize { get; set; } = 14;

    [InputParameter("Show diagnostics", 9)]
    public bool ShowDiagnostics { get; set; } = true;

    [InputParameter("Institutional arrow color", 18)]
    public Color InstitutionalArrowColor { get; set; } = Color.White;

    [InputParameter("Regime shift arrow color", 19)]
    public Color RegimeShiftArrowColor { get; set; } = Color.Gold;

    [InputParameter("Regime shift arrow size (px)", 20, 8, 20, 1, 0)]
    public int RegimeShiftArrowSize { get; set; } = 14;

    // ── Auto-scaling ──────────────────────────────────────────────────────────
    private double _volumeScale   = 1.0d;
    private bool   _scaleDetected = false;
    private string _scaleNote     = "detecting...";
    private string _scaleSource   = "";

    // ── NBBO state (updated from NewQuote) ────────────────────────────────────
    private double _bestBid;
    private double _bestAsk;

    // ── Real-time intrabar bid/offer accumulator ──────────────────────────────
    // Reset on each new bar. Updated on every tick via NewLast.
    private double _barOfferVol;    // volume classified as offer-lifted (buy aggressor)
    private double _barBidVol;      // volume classified as bid-hit (sell aggressor)
    private double _barTotalVol;    // total volume this bar (from ticks)
    private double _coreFlow;       // signed log-size weighted flow
    private int    _currentBarIndex; // tracks which bar we're accumulating for

    private readonly SortedDictionary<double, double> _bidBook = new(Comparer<double>.Create((a, b) => b.CompareTo(a)));
    private readonly SortedDictionary<double, double> _askBook = new();

    // ── Baseline circular buffer ──────────────────────────────────────────────
    // Stores per-minute (offer%, bid%) snapshots since session open.
    // Max 120 entries = 2 hours at 1 per minute (more than BaselineWindowMinutes max).
    private readonly record struct MinuteSnapshot(DateTime MinuteUtc, double AdjFlow);
    private readonly Queue<MinuteSnapshot> _baseline = new();
    private double _bucketVol;   // event-time accumulator toward the next baseline bucket
    private DateTime _lastBaselineSnapshotUtc = DateTime.MinValue;

    // ── Diagnostics ───────────────────────────────────────────────────────────
    private double _lastRawVolume;
    private double _lastScaledVolume;
    private double _lastAvgVolume;
    private int    _lastRollingCount;
    private double _lastOfferPct;
    private double _lastBidPct;
    private double _lastZScore;
    private double _lastCoreFlow;
    private double _lastDepth;
    private double _lastAdjFlow;
    private double _lastVolumeAnalysisDelta;
    private DateTime _lastLevel2CounterMinuteUtc = DateTime.MinValue;
    private int _domRebuildCountThisMinute;
    private int _level2IncrementalCountThisMinute;
    private int _lastDomRebuildsPerMinute;
    private int _lastLevel2IncrementalsPerMinute;

    // ── Signal cache ──────────────────────────────────────────────────────────
    // Byte encoding: 0=none, 1=up standard, 2=up significant,
    //                3=down standard, 4=down significant,
    //                5=up institutional, 6=down institutional,
    //                7=up regime shift, 8=down regime shift
    // Live state for current bar is stored at key = Count-1.
    // Past bars are frozen when a new bar opens.
    private readonly Dictionary<int, byte> _signals = new();
    private const int MaxSignalCache = 500;

    // ── Session tracking ──────────────────────────────────────────────────────
    private DateTime _lastKnownSessionDate = DateTime.MinValue;

    // ── Cached GDI resources ──────────────────────────────────────────────────
    private Font? _diagFont;

    // Database logging (opt-in per symbol per session)
    [InputParameter("Log signals to database", 15)]
    public bool LogToDatabase { get; set; } = false;

    private UdpClient? _udpClient;
    private const string BridgeHost = "127.0.0.1";
    private const int    BridgePort = 9103;

    // Path C - session context reconstructor
    private bool   _sessionContextActive;
    private double _sessionAvgVolume;
    private double _sessionVolumeTrend;
    private double _sessionHigh;
    private double _sessionLow;
    private double _rangePosition;
    private double _openBarMultiple;
    private string _openBarCharacter = "";
    private int    _sessionContextBarsRead;

    // Bridge payload compatibility state for this AVP version.
    private int    _chartTimeframeMinutes = 0;
    private double _lastAdaptiveThreshold;
    private int    _lastPersistenceCount;
    private double _lastLogSizeWeight;
    private int    _lastWindowsAgreeing;
    private double _lastZScore15;
    private double _lastZScore30;
    private double _lastZScore60;
    private double _lastBaselineMean15;
    private double _lastBaselineMean30;
    private double _lastBaselineMean60;
    private double _lastBaselineStddev15;
    private double _lastBaselineStddev30;
    private double _lastBaselineStddev60;
    private int    _lastBaselineN15;
    private int    _lastBaselineN30;
    private int    _lastBaselineN60;
    private int    _lastHistoricalSignalBuildCount;
    private DateTime _lastHistoricalSignalBuildDate = DateTime.MinValue;
    private readonly object _lock = new();

    public AbnormalVolumeIndicator()
    {
        Name = "Abnormal Volume";
        Description = "Real-time institutional volume anomaly detector. " +
                      "Lee-Ready tick classification. Baseline Z-score deviation. " +
                      "Arrow appears/disappears live with imbalance ratio.";
        SeparateWindow = false;
        OnBackGround = false;
        AddLineSeries("_", Color.Transparent, 1, LineStyle.Solid);
    }

    public bool IsRequirePriceLevelsCalculation => false;

    public void VolumeAnalysisData_Loaded()
    {
    }

    protected override void OnInit()
    {
        base.OnInit();
        if (Symbol != null)
        {
            Symbol.NewQuote += OnNewQuote;
            Symbol.NewLast  += OnNewLast;
            Symbol.NewLevel2 += OnNewLevel2;
        }
        ResetAll();

        // Initialize UDP client if logging enabled
        if (LogToDatabase)
            _udpClient = new UdpClient();

        // Reconstruct session context from completed bars
        ReconstructSessionContext();
    }

    protected override void OnClear()
    {
        if (Symbol != null)
        {
            Symbol.NewQuote -= OnNewQuote;
            Symbol.NewLast  -= OnNewLast;
            Symbol.NewLevel2 -= OnNewLevel2;
        }
        _diagFont?.Dispose();
        _diagFont = null;
        ResetAll();
        _udpClient?.Close();
        _udpClient?.Dispose();
        _udpClient = null;
        _sessionContextActive = false;
        base.OnClear();
    }

    // ── Feed handlers ─────────────────────────────────────────────────────────

    private void OnNewQuote(Symbol symbol, Quote quote)
    {
        lock (_lock)
        {
            _bestBid = quote.Bid;
            _bestAsk = quote.Ask;
        }
    }

    private void OnNewLast(Symbol symbol, Last last)
    {
        var size  = Math.Max(0d, last.Size);
        var price = last.Price;

        lock (_lock)
        {
            if (!_scaleDetected) return; // wait for scale detection in OnUpdate

            // ── Lee-Ready aggressor classification ───────────────────────────
            // Priority 1: dxFeed AggressorFlag (direct)
            // Priority 2: NBBO comparison (Lee & Ready 1991)
            int side;
            if (last.AggressorFlag == AggressorFlag.Buy)
                side = 1;
            else if (last.AggressorFlag == AggressorFlag.Sell)
                side = -1;
            else if (_bestAsk > 0 && price >= _bestAsk)
                side = 1;
            else if (_bestBid > 0 && price <= _bestBid)
                side = -1;
            else
                side = 0;

            var scaledSize = size * _volumeScale;
            _barTotalVol += scaledSize;
            if (side > 0)  _barOfferVol += scaledSize;
            if (side < 0)  _barBidVol   += scaledSize;
            if (side != 0)
            {
                var w = Math.Log(1d + scaledSize);
                var signedFlow = side * w;
                _coreFlow += signedFlow;
            }

            // Update live signal for current bar
            UpdateLiveSignal();

            // Baseline snapshot — one per minute
            _bucketVol += scaledSize;
            if (_barTotalVol > 0 && _bucketVol >= Math.Max(1d, BaselineBucketSize))
            {
                var adjFlow = ComputeAdjustedFlow();
                _baseline.Enqueue(new MinuteSnapshot(DateTime.UtcNow, adjFlow));
                _bucketVol = 0d;

                // Keep only the last N completed buckets (count-based ring)
                while (_baseline.Count > Math.Max(8, BaselineBuckets))
                    _baseline.Dequeue();
            }
        }
    }

    // ── Core: live signal update (called inside lock, on every tick) ──────────

    private void OnNewLevel2(Symbol symbol, Level2Quote level2, DOMQuote dom)
    {
        lock (_lock)
        {
            UpdateLevel2Diagnostics(DateTime.UtcNow);
            if (dom != null)
            {
                RebuildBook(_bidBook, dom.Bids);
                RebuildBook(_askBook, dom.Asks);
                _domRebuildCountThisMinute++;
            }
            else if (level2 != null)
            {
                var book = level2.PriceType == QuotePriceType.Bid ? _bidBook : _askBook;
                if (level2.Closed || level2.Size <= 0d)
                    book.Remove(level2.Price);
                else
                    book[level2.Price] = level2.Size;
                _level2IncrementalCountThisMinute++;
            }
        }
    }

    private void UpdateLiveSignal()
    {
        // If volume gate not yet crossed, no signal possible
        if (!_lastVolumeGatePassed) return;

        var key = _currentBarIndex;
        if (!_signals.ContainsKey(key) && _lastVolumeGatePassed == false) return;

        if (_barTotalVol <= 0)
        {
            return;
        }

        var offerPct = _barOfferVol / _barTotalVol * 100d;
        var bidPct   = _barBidVol   / _barTotalVol * 100d;
        _lastOfferPct = offerPct;
        _lastBidPct   = bidPct;

        var adjFlow = ComputeAdjustedFlow();
        _lastCoreFlow = _coreFlow;
        _lastDepth = SumTopLevels(_bidBook, DepthLevels) + SumTopLevels(_askBook, DepthLevels);
        _lastAdjFlow = adjFlow;
        _lastVolumeAnalysisDelta = ReadCurrentVolumeAnalysisDelta();

        // Z-score vs baseline
        var zScore = ComputeZScore(adjFlow);
        _lastZScore = zScore;

        // Tier ladder: highest qualifying tier wins.
        var isStandard = zScore >= ZScoreStandard;

        var threshold = (double)ImbalanceThresholdPct;

        if (ShowUpside && offerPct >= threshold)
        {
            // Green arrow — buyers dominating
            var tier = ResolveSignalTier(isUpside: true, zScore);
            if (tier != 0)
                _signals[key] = tier;
        }
        else if (ShowDownside && bidPct >= threshold)
        {
            // Red arrow — sellers dominating
            var tier = ResolveSignalTier(isUpside: false, zScore);
            if (tier != 0)
                _signals[key] = tier;
        }
        else
        {
            // Once a signal fires, keep it as a session reference point.
            // Do not erase the arrow just because the live imbalance fades.
        }

        _lastAdaptiveThreshold = threshold;
        _lastPersistenceCount = _signals.ContainsKey(key) ? 1 : 0;
        _lastLogSizeWeight = Math.Log(Math.Max(_barTotalVol, 1d)) * _barTotalVol;
        _lastWindowsAgreeing = isStandard ? 1 : 0;
        _lastZScore15 = _lastZScore30 = _lastZScore60 = zScore;
        CaptureBaselineState(adjFlow);

        // Send signal event to bridge (opt-in only, fire-and-forget)
        if (LogToDatabase && _signals.ContainsKey(_currentBarIndex))
        {
            var tier = _signals[_currentBarIndex];
            var isUp = IsUpsideTier(tier);
            SendSignalEvent(
                signalType: "FIRE",
                direction: isUp ? 1 : -1,
                tier: tier,
                offerPct: _lastOfferPct,
                bidPct: _lastBidPct,
                z15: _lastZScore15,
                z30: _lastZScore30,
                z60: _lastZScore60,
                adaptiveThresh: _lastAdaptiveThreshold,
                persistence: _lastPersistenceCount,
                logWeight: _lastLogSizeWeight,
                barHigh: GetPrice(PriceType.High, 0),
                barLow: GetPrice(PriceType.Low, 0),
                barOpen: GetPrice(PriceType.Open, 0),
                barClose: GetPrice(PriceType.Close, 0),
                barVol: (long)_lastScaledVolume,
                avgVol: _lastAvgVolume,
                windowsAgreeing: _lastWindowsAgreeing,
                baselineMean15: _lastBaselineMean15,
                baselineMean30: _lastBaselineMean30,
                baselineMean60: _lastBaselineMean60,
                baselineStddev15: _lastBaselineStddev15,
                baselineStddev30: _lastBaselineStddev30,
                baselineStddev60: _lastBaselineStddev60,
                baselineN15: _lastBaselineN15,
                baselineN30: _lastBaselineN30,
                baselineN60: _lastBaselineN60);
        }
    }

    // Track whether the current bar has passed the volume gate
    private bool _lastVolumeGatePassed;

    // ── OnUpdate: bar-level gates ─────────────────────────────────────────────

    protected override void OnUpdate(UpdateArgs args)
    {
        SetValue(double.NaN);
        if (Count < 2) return;

        var barTime = Time(0);
        var barDate = barTime.Date;
        var barHHMM = barTime.Hour * 100 + barTime.Minute;
        UpdateChartTimeframeMinutes();

        // Session reset
        if (barDate > _lastKnownSessionDate && _lastKnownSessionDate != DateTime.MinValue)
        {
            var toRemove = new List<int>();
            foreach (var k in _signals.Keys)
            {
                var offset = Count - 1 - k;
                if (offset >= 0 && offset < Count && Time(offset).Date < barDate)
                    toRemove.Add(k);
            }
            foreach (var k in toRemove) _signals.Remove(k);
            _baseline.Clear();
            _bucketVol = 0d;
        }
        _lastKnownSessionDate = barDate;

        // Deactivate session context panel once live baseline is sufficient
        if (_sessionContextActive && _lastRollingCount >= 15)
            _sessionContextActive = false;

        // Detect new bar — reset intrabar accumulator
        var newBarIndex = Count - 1;
        lock (_lock)
        {
            if (newBarIndex != _currentBarIndex)
            {
                _currentBarIndex     = newBarIndex;
                _barOfferVol         = 0;
                _barBidVol           = 0;
                _barTotalVol         = 0;
                _coreFlow            = 0;
                _lastVolumeGatePassed = false;
            }

            if (!_scaleDetected)
                TryDetectVolumeScale();
        }

        if (barHHMM < SessionStartHHMM) return;

        RebuildHistoricalSessionSignals(barDate);

        // Rolling session average
        double rollingVolume = 0;
        int rollingCount = 0;
        for (var j = 1; j < Count; j++)
        {
            var prevTime = Time(j);
            var prevHHMM = prevTime.Hour * 100 + prevTime.Minute;
            if (prevTime.Date == barDate && prevHHMM < SessionStartHHMM) break;
            if (prevTime.Date < barDate) break;
            rollingVolume += GetPrice(PriceType.Volume, j) * _volumeScale;
            rollingCount++;
        }

        var avgVolume = rollingCount > 0 ? rollingVolume / rollingCount : 0d;
        var rawVolume    = GetPrice(PriceType.Volume, 0);
        var scaledVolume = rawVolume * _volumeScale;

        _lastRawVolume    = rawVolume;
        _lastScaledVolume = scaledVolume;
        _lastAvgVolume    = avgVolume;
        _lastRollingCount = rollingCount;

        // Two volume gates
        var floor = avgVolume * MinFloorPct;
        var passed = avgVolume > 0 &&
                     scaledVolume >= floor &&
                     scaledVolume >= avgVolume * AvgVolMultiplier;

        lock (_lock)
        {
            _lastVolumeGatePassed = passed;
            // If gate just passed, force a signal evaluation with current tick data
            if (passed) UpdateLiveSignal();
        }

        // Signal cache is session-scoped and resets on session change.
        // Do not prune same-session arrows; traders need the full intraday map.
    }

    // ── Baseline Z-score ──────────────────────────────────────────────────────

    private double ComputeZScore(double currentAdjFlow)
    {
        // Called inside lock
        if (_baseline.Count < 8) return 0d; // need a minimum bucket sample

        var values = _baseline.Select(s => s.AdjFlow).ToArray();
        var mean   = values.Average();
        var stdDev = Math.Sqrt(values.Select(v => (v - mean) * (v - mean)).Average());

        if (stdDev < 0.5d) return 0d; // degenerate — avoid division by near-zero
        return (currentAdjFlow - mean) / stdDev;
    }

    private double ComputeAdjustedFlow()
    {
        var depth = SumTopLevels(_bidBook, DepthLevels) + SumTopLevels(_askBook, DepthLevels);
        return _coreFlow / Math.Sqrt(Math.Max(depth, 1d));
    }

    private void UpdateLevel2Diagnostics(DateTime nowUtc)
    {
        var minute = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, nowUtc.Minute, 0, DateTimeKind.Utc);
        if (_lastLevel2CounterMinuteUtc == minute)
            return;

        if (_lastLevel2CounterMinuteUtc != DateTime.MinValue)
        {
            _lastDomRebuildsPerMinute = _domRebuildCountThisMinute;
            _lastLevel2IncrementalsPerMinute = _level2IncrementalCountThisMinute;
        }

        _lastLevel2CounterMinuteUtc = minute;
        _domRebuildCountThisMinute = 0;
        _level2IncrementalCountThisMinute = 0;
    }

    private static void RebuildBook(SortedDictionary<double, double> book, IEnumerable<Level2Quote> levels)
    {
        book.Clear();
        foreach (var level in levels)
        {
            if (!level.Closed && level.Size > 0d)
                book[level.Price] = level.Size;
        }
    }

    private static double SumTopLevels(SortedDictionary<double, double> book, int depthLevels)
    {
        var sum = 0d;
        var count = 0;
        var depth = Math.Clamp(depthLevels, 1, 10);
        foreach (var level in book)
        {
            sum += level.Value;
            count++;
            if (count >= depth)
                break;
        }

        return sum;
    }

    private double ReadCurrentVolumeAnalysisDelta()
    {
        try
        {
            return HistoricalData[0].VolumeAnalysisData?.Total?.Delta ?? 0d;
        }
        catch (Exception)
        {
            return 0d;
        }
    }

    // ── Auto-scale detection ──────────────────────────────────────────────────

    private void TryDetectVolumeScale()
    {
        if (Symbol == null) return;
        var source = Symbol.ConnectionId ?? string.Empty;
        _scaleSource = source;

        if (source.Contains("dxFeed", StringComparison.OrdinalIgnoreCase))
        {
            _volumeScale = 100d;
            _scaleNote   = "dxFeed: x100";
        }
        else
        {
            _volumeScale = 1d;
            _scaleNote   = $"native: x1 ({source})";
        }
        _scaleDetected = true;
    }

    private void UpdateChartTimeframeMinutes()
    {
        try
        {
            if (HistoricalData == null || HistoricalData.Count < 2) return;

            var current = Time(0);
            var previous = Time(1);
            var minutes = Math.Abs((current - previous).TotalMinutes);

            if (minutes >= 1d)
                _chartTimeframeMinutes = Math.Max(1, (int)Math.Round(minutes));
        }
        catch (Exception) { }
    }

    // ── Paint ─────────────────────────────────────────────────────────────────

    private void RebuildHistoricalSessionSignals(DateTime sessionDate)
    {
        try
        {
            if (HistoricalData == null || HistoricalData.Count < 3) return;

            if (_lastHistoricalSignalBuildCount == Count &&
                _lastHistoricalSignalBuildDate == sessionDate &&
                _signals.Count > 0)
                return;

            double rollingVolume = 0d;
            var rollingCount = 0;

            lock (_lock)
            {
                for (var offset = Count - 1; offset >= 1; offset--)
                {
                    var barTime = Time(offset);
                    if (barTime.Date != sessionDate) continue;

                    var hhmm = barTime.Hour * 100 + barTime.Minute;
                    if (hhmm < SessionStartHHMM) continue;

                    var rawVolume = GetPrice(PriceType.Volume, offset);
                    var scaledVolume = rawVolume * _volumeScale;

                    if (rollingCount > 0)
                    {
                        var avgVolume = rollingVolume / rollingCount;
                        var floor = avgVolume * MinFloorPct;
                        var passed = avgVolume > 0 &&
                                     scaledVolume >= floor &&
                                     scaledVolume >= avgVolume * AvgVolMultiplier;

                        if (passed && TryResolveHistoricalSignalTier(offset, scaledVolume, avgVolume, out var tier))
                        {
                            var barIndex = Count - 1 - offset;
                            if (!_signals.ContainsKey(barIndex))
                                _signals[barIndex] = tier;
                        }
                    }

                    rollingVolume += scaledVolume;
                    rollingCount++;
                }

                _lastHistoricalSignalBuildCount = Count;
                _lastHistoricalSignalBuildDate = sessionDate;
            }
        }
        catch (Exception) { }
    }

    private bool TryResolveHistoricalSignalTier(int offset, double scaledVolume, double avgVolume, out byte tier)
    {
        tier = 0;

        var open = GetPrice(PriceType.Open, offset);
        var close = GetPrice(PriceType.Close, offset);
        var isUpside = close >= open;
        var isDownside = close < open;

        if (isUpside && !ShowUpside) return false;
        if (isDownside && !ShowDownside) return false;

        var isSignificant = avgVolume > 0 &&
                            scaledVolume >= avgVolume * Math.Max(AvgVolMultiplier * 1.75d, AvgVolMultiplier);

        if (isUpside)
            tier = isSignificant ? (byte)2 : (byte)1;
        else if (isDownside)
            tier = isSignificant ? (byte)4 : (byte)3;

        return tier != 0;
    }

    public override void OnPaintChart(PaintChartEventArgs args)
    {
        base.OnPaintChart(args);

        var gr = args.Graphics;
        var converter = CurrentChart?.MainWindow?.CoordinatesConverter;
        if (converter == null) return;

        // ── Diagnostics ───────────────────────────────────────────────────────
        if (ShowDiagnostics)
        {
            _diagFont ??= new Font("Segoe UI", 10, FontStyle.Bold, GraphicsUnit.Pixel);

            double offerPct, bidPct, zScore, rawVol, scaledVol, avgVol;
            double coreFlow, depth, adjFlow, volumeAnalysisDelta;
            int rollingCount, domRebuildsPerMinute, level2IncrementalsPerMinute;
            bool gatePassed;
            string scaleNote, scaleSource;

            lock (_lock)
            {
                offerPct     = _lastOfferPct;
                bidPct       = _lastBidPct;
                zScore       = _lastZScore;
                rawVol       = _lastRawVolume;
                scaledVol    = _lastScaledVolume;
                avgVol       = _lastAvgVolume;
                coreFlow     = _lastCoreFlow;
                depth        = _lastDepth;
                adjFlow      = _lastAdjFlow;
                volumeAnalysisDelta = _lastVolumeAnalysisDelta;
                rollingCount = _lastRollingCount;
                domRebuildsPerMinute = _domRebuildCountThisMinute;
                level2IncrementalsPerMinute = _level2IncrementalCountThisMinute;
                gatePassed   = _lastVolumeGatePassed;
                scaleNote    = _scaleNote;
                scaleSource  = _scaleSource;
            }

            var threshold  = avgVol * AvgVolMultiplier;
            var floor      = avgVol * MinFloorPct;
            var dominant   = offerPct >= bidPct ? $"OFFER {offerPct:0}%" : $"BID {bidPct:0}%";
            var imbalMet   = Math.Max(offerPct, bidPct) >= ImbalanceThresholdPct;
            var zOnlyTier  = zScore >= ZScoreStandard ? ResolveSignalTier(offerPct >= bidPct, zScore) : (byte)0;
            var zOnlyName  = zOnlyTier != 0 ? ResolveTierName(zOnlyTier) : "none";

            var lines = new[]
            {
                $"RAW VOL:   {rawVol:N0}",
                $"SCALED:    {scaledVol:N0}  ({scaleNote})",
                $"SOURCE:    {scaleSource}",
                $"AVG VOL:   {avgVol:N0}  (n={rollingCount})",
                $"THRESHOLD: {threshold:N0}  ({AvgVolMultiplier:0.##}x)",
                $"MIN FLOOR: {floor:N0}  ({MinFloorPct:0.##}x avg)",
                $"GATES:     {(gatePassed ? "PASSED" : "below threshold")}",
                $"IMBALANCE: {dominant}  (threshold {ImbalanceThresholdPct}%)",
                $"CORE FLOW: {coreFlow:0.000}",
                $"DEPTH:     {depth:N0}  (top {DepthLevels})",
                $"L2 MODE:   DOM {domRebuildsPerMinute}/min  L2 {level2IncrementalsPerMinute}/min",
                $"ADJ FLOW:  {adjFlow:0.000000}",
                $"VA DELTA:  {volumeAnalysisDelta:0.####}  (diagnostic only)",
                $"Z-SCORE:   {zScore:0.00}  (base n={_baseline.Count})",
                $"Z-ONLY:    {zOnlyName}",
                (gatePassed && imbalMet) ? "STATUS:    ARROW ACTIVE" : "STATUS:    no arrow",
            };

            var lineH = _diagFont.Height + 2;
            var rect  = new RectangleF(10f, 10f, 310f, lines.Length * lineH + 10f);

            using var back   = new SolidBrush(Color.FromArgb(210, 15, 18, 22));
            using var border = new Pen(Color.FromArgb(180, 100, 100, 100), 1f);
            using var fore   = new SolidBrush(Color.WhiteSmoke);
            using var good   = new SolidBrush(Color.FromArgb(255, 0, 220, 90));
            using var bad    = new SolidBrush(Color.FromArgb(255, 220, 55, 55));

            gr.FillRectangle(back, rect);
            gr.DrawRectangle(border, rect.X, rect.Y, rect.Width, rect.Height);

            for (var i = 0; i < lines.Length; i++)
            {
                var isStatus = i == lines.Length - 1;
                var isActive = lines[i].Contains("ACTIVE");
                Brush brush  = isStatus
                    ? (isActive ? good : (Brush)bad)
                    : fore;
                gr.DrawString(lines[i], _diagFont, brush, 16f, 15f + i * lineH);
            }
        }

        // Session context panel
        if (ShowDiagnostics && _sessionContextActive)
        {
            _diagFont ??= new Font("Segoe UI", 10, FontStyle.Bold, GraphicsUnit.Pixel);
            var trend = _sessionVolumeTrend >= 0
                ? $"↑ Accelerating +{_sessionVolumeTrend:0}%"
                : $"↓ Decelerating {_sessionVolumeTrend:0}%";
            var lines = new[]
            {
                $"SESSION CONTEXT — {Symbol?.Name ?? ""}  (reconstructed)",
                $"Bars read:     {_sessionContextBarsRead} bars since {SessionStartHHMM / 100:00}:{SessionStartHHMM % 100:00}",
                $"Avg vol/bar:   {_sessionAvgVolume:N0}",
                $"Volume trend:  {trend}",
                $"Session range: {_sessionLow:0.00} – {_sessionHigh:0.00}",
                $"Range pos:     {_rangePosition:0}%  ({(_rangePosition >= 50 ? "upper half" : "lower half")})",
                $"Open bar:      {_openBarMultiple:0.0}x avg  [{_openBarCharacter}]",
                $"Live baseline: warming up ({_lastRollingCount}/15 snapshots)",
            };

            var lineH = _diagFont.Height + 2;
            var boxW = 340f;
            var boxH = lines.Length * lineH + 10f;
            var rect = new RectangleF(10f, 10f, boxW, boxH);

            using var back = new SolidBrush(Color.FromArgb(220, 13, 30, 45));
            using var border = new Pen(Color.FromArgb(200, 0, 180, 80), 1.5f);
            using var fore = new SolidBrush(Color.WhiteSmoke);
            using var head = new SolidBrush(Color.FromArgb(255, 0, 220, 90));

            gr.FillRectangle(back, rect);
            gr.DrawRectangle(border, rect.X, rect.Y, rect.Width, rect.Height);

            for (var i = 0; i < lines.Length; i++)
            {
                var brush = i == 0 ? head : (Brush)fore;
                gr.DrawString(lines[i], _diagFont, brush, 16f, 15f + i * lineH);
            }
        }

        // ── Arrows ────────────────────────────────────────────────────────────
        Dictionary<int, byte> snapshot;
        lock (_lock) { snapshot = new Dictionary<int, byte>(_signals); }

        if (snapshot.Count == 0) return;

        foreach (var kvp in snapshot)
        {
            var offset = Count - 1 - kvp.Key;
            if (offset < 0 || offset >= Count) continue;

            var barHigh = GetPrice(PriceType.High, offset);
            var barLow  = GetPrice(PriceType.Low,  offset);
            var barTime = Time(offset);
            var xCenter = (float)converter.GetChartX(barTime);
            if (xCenter < args.Rectangle.Left - 20 || xCenter > args.Rectangle.Right + 20)
                continue;

            var tier       = kvp.Value;
            var isUpside   = IsUpsideTier(tier);
            var isSignif   = IsSignificantTier(tier);
            var size       = ResolveArrowSize(tier);
            var half       = Math.Max(size / 2, 3);
            var color      = ResolveArrowColor(tier, isUpside);

            if (isUpside)
            {
                float tipY  = (float)converter.GetChartY(barLow) + ArrowOffset;
                float baseY = tipY + size;
                var pts = new PointF[] {
                    new(xCenter, tipY),
                    new(xCenter - half, baseY),
                    new(xCenter + half, baseY)
                };
                using var b = new SolidBrush(color);
                gr.FillPolygon(b, pts);
                if (isSignif) { using var p = new Pen(Color.White, 1f); gr.DrawPolygon(p, pts); }
            }
            else
            {
                float tipY  = (float)converter.GetChartY(barHigh) - ArrowOffset;
                float baseY = tipY - size;
                var pts = new PointF[] {
                    new(xCenter, tipY),
                    new(xCenter - half, baseY),
                    new(xCenter + half, baseY)
                };
                using var b = new SolidBrush(color);
                gr.FillPolygon(b, pts);
                if (isSignif) { using var p = new Pen(Color.White, 1f); gr.DrawPolygon(p, pts); }
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private byte ResolveSignalTier(bool isUpside, double zScore)
    {
        if (zScore >= ZScoreRegimeShift)
            return isUpside ? (byte)7 : (byte)8;

        if (zScore >= ZScoreInstitutional)
            return isUpside ? (byte)5 : (byte)6;

        if (zScore >= ZScoreSignificant)
            return isUpside ? (byte)2 : (byte)4;

        if (zScore >= ZScoreStandard)
            return isUpside ? (byte)1 : (byte)3;

        return 0;
    }

    private static bool IsUpsideTier(byte tier) =>
        tier is 1 or 2 or 5 or 7;

    private static bool IsSignificantTier(byte tier) =>
        tier is 2 or 4;

    private static bool IsInstitutionalTier(byte tier) =>
        tier is 5 or 6;

    private static bool IsRegimeShiftTier(byte tier) =>
        tier is 7 or 8;

    private int ResolveArrowSize(byte tier)
    {
        if (IsRegimeShiftTier(tier))
            return RegimeShiftArrowSize;

        if (IsInstitutionalTier(tier) || IsSignificantTier(tier))
            return SignificantArrowSize;

        return ArrowSize;
    }

    private Color ResolveArrowColor(byte tier, bool isUpside)
    {
        if (IsRegimeShiftTier(tier))
            return RegimeShiftArrowColor;

        if (IsInstitutionalTier(tier))
            return InstitutionalArrowColor;

        if (IsSignificantTier(tier))
            return isUpside ? Color.FromArgb(255, 0, 255, 120) : Color.FromArgb(255, 255, 80, 80);

        return isUpside ? UpsideColor : DownsideColor;
    }

    private static string ResolveTierName(byte tier)
    {
        if (IsRegimeShiftTier(tier))
            return "REGIME";

        if (IsInstitutionalTier(tier))
            return "INSTITUTION";

        if (IsSignificantTier(tier))
            return "SIGNIFICANT";

        return "STANDARD";
    }

    private void ReconstructSessionContext()
    {
        try
        {
            if (HistoricalData == null || HistoricalData.Count < 2) return;

            var today = DateTime.Now.Date;
            var sessionBars = new List<(double high, double low, double vol)>();

            for (var i = 1; i < Math.Min(HistoricalData.Count, 500); i++)
            {
                var bar = HistoricalData[i] as HistoryItemBar;
                if (bar == null) continue;
                if (bar.TimeLeft.Date < today) break;
                var hhmm = bar.TimeLeft.Hour * 100 + bar.TimeLeft.Minute;
                if (hhmm < SessionStartHHMM) continue;

                sessionBars.Add((
                    (double)bar.High,
                    (double)bar.Low,
                    bar.Volume * _volumeScale));
            }

            if (sessionBars.Count < 2) return;

            _sessionContextBarsRead = sessionBars.Count;
            _sessionAvgVolume = sessionBars.Average(b => b.vol);
            _sessionHigh = sessionBars.Max(b => b.high);
            _sessionLow = sessionBars.Min(b => b.low);

            var currentPrice = HistoricalData[0] is HistoryItemBar curr ? (double)curr.Close : 0d;
            var range = _sessionHigh - _sessionLow;
            _rangePosition = range > 0 ? (currentPrice - _sessionLow) / range * 100d : 50d;

            if (sessionBars.Count >= 6)
            {
                var first3 = sessionBars.TakeLast(3).Average(b => b.vol);
                var last3 = sessionBars.Take(3).Average(b => b.vol);
                _sessionVolumeTrend = first3 > 0 ? (last3 - first3) / first3 * 100d : 0d;
            }

            var openVol = sessionBars.Last().vol;
            _openBarMultiple = _sessionAvgVolume > 0 ? openVol / _sessionAvgVolume : 0d;
            _openBarCharacter = _openBarMultiple >= 2.5 ? "HIGH CONVICTION"
                : _openBarMultiple >= 1.2 ? "NORMAL"
                : "LOW CONVICTION";

            _sessionContextActive = true;

            if (LogToDatabase)
                SendSessionContext();
        }
        catch (Exception) { }
    }

    private void SendSignalEvent(string signalType, int direction, byte tier,
        double offerPct, double bidPct, double z15, double z30, double z60,
        double adaptiveThresh, int persistence, double logWeight,
        double barHigh, double barLow, double barOpen, double barClose,
        long barVol, double avgVol, int windowsAgreeing,
        double baselineMean15, double baselineMean30, double baselineMean60,
        double baselineStddev15, double baselineStddev30, double baselineStddev60,
        int baselineN15, int baselineN30, int baselineN60)
    {
        try
        {
            if (_udpClient == null) return;

            UpdateChartTimeframeMinutes();
            var now = DateTime.UtcNow;
            var barTime = Time(0);

            var packet = new Dictionary<string, object?>
            {
                ["created_at_utc"] = now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                ["symbol"] = Symbol?.Name ?? "",
                ["exchange"] = Symbol?.Exchange?.ToString() ?? "",
                ["timeframe_minutes"] = _chartTimeframeMinutes,
                ["session_date"] = barTime.Date.ToString("yyyy-MM-dd"),
                ["signal_type"] = signalType,
                ["direction"] = direction > 0 ? "BUY" : "SELL",
                ["tier"] = ResolveTierName(tier),
                ["signal_price"] = barClose,
                ["bar_open"] = barOpen,
                ["bar_high"] = barHigh,
                ["bar_low"] = barLow,
                ["bar_close"] = barClose,
                ["bar_volume_scaled"] = barVol,
                ["session_avg_volume"] = (long)avgVol,
                ["volume_multiplier"] = AvgVolMultiplier,
                ["volume_floor_pct"] = MinFloorPct,
                ["gates_passed"] = _lastVolumeGatePassed ? 1 : 0,
                ["offer_pct"] = Math.Round(offerPct, 2),
                ["bid_pct"] = Math.Round(bidPct, 2),
                ["adaptive_threshold"] = Math.Round(adaptiveThresh, 2),
                ["persistence_count"] = persistence,
                ["log_size_weight"] = Math.Round(logWeight, 4),
                ["z_score_15min"] = Math.Round(z15, 3),
                ["z_score_30min"] = Math.Round(z30, 3),
                ["z_score_60min"] = Math.Round(z60, 3),
                ["baseline_mean_15"] = Math.Round(baselineMean15, 2),
                ["baseline_mean_30"] = Math.Round(baselineMean30, 2),
                ["baseline_mean_60"] = Math.Round(baselineMean60, 2),
                ["baseline_stddev_15"] = Math.Round(baselineStddev15, 2),
                ["baseline_stddev_30"] = Math.Round(baselineStddev30, 2),
                ["baseline_stddev_60"] = Math.Round(baselineStddev60, 2),
                ["baseline_n_15"] = baselineN15,
                ["baseline_n_30"] = baselineN30,
                ["baseline_n_60"] = baselineN60,
                ["windows_agreeing"] = windowsAgreeing,
                ["session_avg_vol_reconstructed"] = (long)_sessionAvgVolume,
                ["open_bar_multiple"] = Math.Round(_openBarMultiple, 2),
            };

            var json = JsonSerializer.Serialize(packet);
            var bytes = Encoding.UTF8.GetBytes(json);
            _udpClient.Send(bytes, bytes.Length, BridgeHost, BridgePort);
        }
        catch (Exception) { }
    }

    private void SendSessionContext()
    {
        try
        {
            if (_udpClient == null) return;

            UpdateChartTimeframeMinutes();
            var packet = new Dictionary<string, object?>
            {
                ["created_at_utc"] = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                ["symbol"] = Symbol?.Name ?? "",
                ["exchange"] = Symbol?.Exchange?.ToString() ?? "",
                ["timeframe_minutes"] = _chartTimeframeMinutes,
                ["session_date"] = DateTime.Now.Date.ToString("yyyy-MM-dd"),
                ["signal_type"] = "SESSION_CONTEXT",
                ["direction"] = "N/A",
                ["tier"] = "N/A",
                ["signal_price"] = GetPrice(PriceType.Close, 0),
                ["bar_open"] = 0d,
                ["bar_high"] = _sessionHigh,
                ["bar_low"] = _sessionLow,
                ["bar_close"] = 0d,
                ["bar_volume_scaled"] = 0L,
                ["session_avg_volume"] = (long)_sessionAvgVolume,
                ["volume_multiplier"] = AvgVolMultiplier,
                ["volume_floor_pct"] = MinFloorPct,
                ["gates_passed"] = 0,
                ["offer_pct"] = 0d,
                ["bid_pct"] = 0d,
                ["adaptive_threshold"] = 0d,
                ["persistence_count"] = 0,
                ["log_size_weight"] = 0d,
                ["z_score_15min"] = 0d,
                ["z_score_30min"] = 0d,
                ["z_score_60min"] = 0d,
                ["baseline_mean_15"] = 0d,
                ["baseline_mean_30"] = 0d,
                ["baseline_mean_60"] = 0d,
                ["baseline_stddev_15"] = 0d,
                ["baseline_stddev_30"] = 0d,
                ["baseline_stddev_60"] = 0d,
                ["baseline_n_15"] = 0,
                ["baseline_n_30"] = 0,
                ["baseline_n_60"] = 0,
                ["windows_agreeing"] = 0,
                ["session_avg_vol_reconstructed"] = (long)_sessionAvgVolume,
                ["open_bar_multiple"] = Math.Round(_openBarMultiple, 2),
            };

            var json = JsonSerializer.Serialize(packet);
            var bytes = Encoding.UTF8.GetBytes(json);
            _udpClient.Send(bytes, bytes.Length, BridgeHost, BridgePort);
        }
        catch (Exception) { }
    }

    private void CaptureBaselineState(double currentAdjFlow)
    {
        if (_baseline.Count == 0)
        {
            _lastBaselineMean15 = _lastBaselineMean30 = _lastBaselineMean60 = 0;
            _lastBaselineStddev15 = _lastBaselineStddev30 = _lastBaselineStddev60 = 0;
            _lastBaselineN15 = _lastBaselineN30 = _lastBaselineN60 = 0;
            return;
        }

        var values = _baseline.Select(s => s.AdjFlow).ToArray();
        var mean = values.Average();
        var stdDev = Math.Sqrt(values.Select(v => (v - mean) * (v - mean)).Average());

        _lastBaselineMean15 = _lastBaselineMean30 = _lastBaselineMean60 = mean;
        _lastBaselineStddev15 = _lastBaselineStddev30 = _lastBaselineStddev60 = stdDev;
        _lastBaselineN15 = _lastBaselineN30 = _lastBaselineN60 = values.Length;

        if (stdDev >= 0.5d)
        {
            var z = (currentAdjFlow - mean) / stdDev;
            // NOTE: multi-window (15/30/60) retired — these mirror the single event-time z-score.
            _lastZScore15 = _lastZScore30 = _lastZScore60 = z;
        }
    }

    private void ResetAll()
    {
        lock (_lock)
        {
            _signals.Clear();
            _baseline.Clear();
            _bucketVol = 0d;
            _bidBook.Clear();
            _askBook.Clear();
            _bestBid = _bestAsk = 0;
            _barOfferVol = _barBidVol = _barTotalVol = _coreFlow = 0;
            _currentBarIndex = -1;
            _lastVolumeGatePassed = false;
            _scaleDetected = false;
            _volumeScale   = 1.0d;
            _scaleNote     = "detecting...";
            _scaleSource   = "";
            _lastRawVolume = _lastScaledVolume = _lastAvgVolume = 0;
            _lastRollingCount = 0;
            _lastOfferPct = _lastBidPct = _lastZScore = 0;
            _lastCoreFlow = _lastDepth = _lastAdjFlow = _lastVolumeAnalysisDelta = 0;
            _lastLevel2CounterMinuteUtc = DateTime.MinValue;
            _domRebuildCountThisMinute = _level2IncrementalCountThisMinute = 0;
            _lastDomRebuildsPerMinute = _lastLevel2IncrementalsPerMinute = 0;
            _lastKnownSessionDate = DateTime.MinValue;
            _lastBaselineSnapshotUtc = DateTime.MinValue;
            _lastHistoricalSignalBuildCount = 0;
            _lastHistoricalSignalBuildDate = DateTime.MinValue;
        }
    }

    private new DateTime Time(int offset) => HistoricalData[offset].TimeLeft;
}
