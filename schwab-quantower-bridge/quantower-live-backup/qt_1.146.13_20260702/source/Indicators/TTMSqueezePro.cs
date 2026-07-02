using System.Drawing;
using TradingPlatform.BusinessLayer;

namespace FlowTools;

public sealed class TTMSqueezePro : Indicator
{
    [InputParameter("Period", 0, 1, 100, 1, 0)]
    public int Period { get; set; } = 20;

    [InputParameter("Bollinger Bands Multiplier", 1, 0.1, 5.0, 0.1, 2)]
    public double BbMult { get; set; } = 2.0;

    [InputParameter("High Squeeze KC Multiplier", 2, 0.1, 5.0, 0.1, 2)]
    public double KcMultHigh { get; set; } = 1.0;

    [InputParameter("Mid Squeeze KC Multiplier", 3, 0.1, 5.0, 0.1, 2)]
    public double KcMultMid { get; set; } = 1.5;

    [InputParameter("Low Squeeze KC Multiplier", 4, 0.1, 5.0, 0.1, 2)]
    public double KcMultLow { get; set; } = 2.0;

    private LineSeries? _momentumSeries;
    private LineSeries? _squeezeDotsSeries;
    private LineSeries? _zeroSeries;

    public TTMSqueezePro()
    {
        Name = "TTM Squeeze Pro";
        Description = "Multi-stage squeeze detector with color-coded momentum histogram.";
        SeparateWindow = true;

        _momentumSeries = AddLineSeries("Momentum", Color.Gray, 3, LineStyle.Histogramm);
        _squeezeDotsSeries = AddLineSeries("Squeeze Dots", Color.Green, 4, LineStyle.Points);
        _zeroSeries = AddLineSeries("Zero", Color.Gray, 1, LineStyle.Dash);
    }

    protected override void OnUpdate(UpdateArgs args)
    {
        var period = Math.Max(1, Period);
        var requiredBars = period * 2 + 1;

        if (Count < requiredBars)
        {
            SetSeriesValues(0d, 0d, Color.Gray, Color.Gray);
            return;
        }

        var basis = CalculateSma(0, period);
        var stdDev = CalculateStdDev(0, period, basis);
        var atr = CalculateAtr(0, period);

        if (double.IsNaN(basis) || double.IsNaN(stdDev) || double.IsNaN(atr) || atr <= 0d)
        {
            SetSeriesValues(0d, 0d, Color.Gray, Color.Gray);
            return;
        }

        var bbRange = BbMult * stdDev;
        var squeezeColor = GetSqueezeColor(bbRange, atr);
        var momentum = CalculateLinearRegressionMomentum(period);
        var previousMomentum = _momentumSeries?.GetValue(1, SeekOriginHistory.End) ?? 0d;
        var momentumColor = GetMomentumColor(momentum, previousMomentum);

        SetSeriesValues(momentum, 0d, momentumColor, squeezeColor);
    }

    private Color GetSqueezeColor(double bbRange, double atr)
    {
        if (bbRange < KcMultHigh * atr)
            return Color.Orange;

        if (bbRange < KcMultMid * atr)
            return Color.Red;

        if (bbRange < KcMultLow * atr)
            return Color.Black;

        return Color.LimeGreen;
    }

    private static Color GetMomentumColor(double momentum, double previousMomentum)
    {
        if (momentum >= 0d)
            return momentum >= previousMomentum ? Color.Cyan : Color.DarkBlue;

        return momentum <= previousMomentum ? Color.Red : Color.Yellow;
    }

    private double CalculateLinearRegressionMomentum(int period)
    {
        double sumX = 0d;
        double sumY = 0d;
        double sumXy = 0d;
        double sumXX = 0d;

        for (var x = 0; x < period; x++)
        {
            var offset = period - 1 - x;
            var y = CalculateMomentumDelta(offset, period);

            sumX += x;
            sumY += y;
            sumXy += x * y;
            sumXX += x * x;
        }

        var denominator = period * sumXX - sumX * sumX;
        if (denominator == 0d)
            return 0d;

        var slope = (period * sumXy - sumX * sumY) / denominator;
        var intercept = (sumY - slope * sumX) / period;

        return intercept + slope * (period - 1);
    }

    private double CalculateMomentumDelta(int offset, int period)
    {
        var highestHigh = High(offset);
        var lowestLow = Low(offset);

        for (var i = 0; i < period; i++)
        {
            var lookbackOffset = offset + i;
            highestHigh = Math.Max(highestHigh, High(lookbackOffset));
            lowestLow = Math.Min(lowestLow, Low(lookbackOffset));
        }

        var midpoint = (highestHigh + lowestLow) / 2d;
        var sma = CalculateSma(offset, period);
        return Close(offset) - ((midpoint + sma) / 2d);
    }

    private double CalculateSma(int offset, int period)
    {
        if (period <= 0 || Count < offset + period)
            return double.NaN;

        double sum = 0d;
        for (var i = 0; i < period; i++)
            sum += Close(offset + i);

        return sum / period;
    }

    private double CalculateStdDev(int offset, int period, double mean)
    {
        if (period <= 0 || Count < offset + period)
            return double.NaN;

        double sumVariance = 0d;
        for (var i = 0; i < period; i++)
        {
            var distance = Close(offset + i) - mean;
            sumVariance += distance * distance;
        }

        return Math.Sqrt(sumVariance / period);
    }

    private double CalculateAtr(int offset, int period)
    {
        if (period <= 0 || Count < offset + period + 1)
            return double.NaN;

        double sum = 0d;
        for (var i = 0; i < period; i++)
        {
            var barOffset = offset + i;
            var high = High(barOffset);
            var low = Low(barOffset);
            var previousClose = Close(barOffset + 1);
            var trueRange = Math.Max(
                high - low,
                Math.Max(Math.Abs(high - previousClose), Math.Abs(low - previousClose)));

            sum += trueRange;
        }

        return sum / period;
    }

    private void SetSeriesValues(double momentum, double dots, Color momentumColor, Color squeezeColor)
    {
        SetValue(momentum, 0);
        SetValue(dots, 1);
        SetValue(0d, 2);

        _momentumSeries?.SetValue(momentum, 0, SeekOriginHistory.End);
        _squeezeDotsSeries?.SetValue(dots, 0, SeekOriginHistory.End);
        _zeroSeries?.SetValue(0d, 0, SeekOriginHistory.End);

        _momentumSeries?.SetMarker(0, momentumColor);
        _squeezeDotsSeries?.SetMarker(0, squeezeColor);
    }
}
