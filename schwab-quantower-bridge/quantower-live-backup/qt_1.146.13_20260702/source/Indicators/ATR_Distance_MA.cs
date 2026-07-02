using System.Drawing;
using TradingPlatform.BusinessLayer;

namespace FlowTools;

public sealed class ATR_Distance_MA : Indicator
{
    [InputParameter("ATR Period", 0, 1, 999, 1, 0)]
    public int AtrPeriod { get; set; } = 14;

    [InputParameter("50 MA Period", 1, 1, 999, 1, 0)]
    public int ShortMaPeriod { get; set; } = 50;

    [InputParameter("200 MA Period", 2, 1, 999, 1, 0)]
    public int LongMaPeriod { get; set; } = 200;

    private LineSeries? _distance50Series;
    private LineSeries? _distance200Series;
    private LineSeries? _zeroLineSeries;

    public ATR_Distance_MA()
    {
        Name = "ATR Distance from MAs";
        Description = "Distance from 50 and 200 SMA expressed in ATR units. Positive = price above MA. Negative = price below MA.";
        SeparateWindow = true;

        _distance50Series = AddLineSeries("50 DMA Distance", Color.Cyan, 2, LineStyle.Solid);
        _distance200Series = AddLineSeries("200 DMA Distance", Color.Magenta, 2, LineStyle.Solid);
        _zeroLineSeries = AddLineSeries("Zero", Color.Gray, 1, LineStyle.Dash);
    }

    protected override void OnUpdate(UpdateArgs args)
    {
        var requiredBars = Math.Max(LongMaPeriod, AtrPeriod + 1);
        if (Count < requiredBars)
        {
            SetSeriesValues(0d, 0d, 0d);
            return;
        }

        var close = Close();
        var atr = CalculateAtr(AtrPeriod);
        var sma50 = CalculateSma(ShortMaPeriod);
        var sma200 = CalculateSma(LongMaPeriod);

        if (atr <= 0d || double.IsNaN(atr) || double.IsNaN(sma50) || double.IsNaN(sma200))
        {
            SetSeriesValues(0d, 0d, 0d);
            return;
        }

        var dist50 = (close - sma50) / atr;
        var dist200 = (close - sma200) / atr;

        SetSeriesValues(dist50, dist200, 0d);
    }

    private double CalculateSma(int period)
    {
        if (period <= 0 || Count < period)
            return double.NaN;

        double sum = 0d;
        for (var i = 0; i < period; i++)
            sum += Close(i);

        return sum / period;
    }

    private double CalculateAtr(int period)
    {
        if (period <= 0 || Count < period + 1)
            return double.NaN;

        double sum = 0d;
        for (var i = 0; i < period; i++)
        {
            var high = High(i);
            var low = Low(i);
            var previousClose = Close(i + 1);
            var trueRange = Math.Max(
                high - low,
                Math.Max(Math.Abs(high - previousClose), Math.Abs(low - previousClose)));

            sum += trueRange;
        }

        return sum / period;
    }

    private void SetSeriesValues(double distance50, double distance200, double zero)
    {
        SetValue(distance50, 0);
        SetValue(distance200, 1);
        SetValue(zero, 2);

        _distance50Series?.SetValue(distance50, 0, SeekOriginHistory.End);
        _distance200Series?.SetValue(distance200, 0, SeekOriginHistory.End);
        _zeroLineSeries?.SetValue(zero, 0, SeekOriginHistory.End);
    }
}
