using System;
using System.Drawing;
using TradingPlatform.BusinessLayer;

namespace SchwabQuantowerBridge.Indicators;

/// <summary>
/// Minimal diagnostic indicator used to verify what the Schwab bridge can populate
/// for Quantower volume-analysis consumers on a live chart.
/// </summary>
public sealed class SchwabDataProbe : Indicator, IVolumeAnalysisIndicator
{
    private string lastStatus = string.Empty;
    private int okBars;
    private int nullBars;
    private int zeroBars;

    public bool IsRequirePriceLevelsCalculation => true;

    public SchwabDataProbe()
    {
        Name = "Schwab Data Probe";
        Description = "Checks whether the Schwab bridge populates Quantower volume-analysis data.";
        SeparateWindow = true;

        AddLineSeries("Delta", Color.DodgerBlue, 2);
        AddLineSeries("BuyVol", Color.ForestGreen, 1);
        AddLineSeries("SellVol", Color.IndianRed, 1);

        AddLabel("probe_feed", ComparingType.String, "Feed");
        AddLabel("probe_status", ComparingType.String, "Status");
        AddLabel("probe_levels", ComparingType.Int, "Levels");
        AddLabel("probe_reason", ComparingType.String, "Reason");
    }

    protected override void OnUpdate(UpdateArgs args)
    {
        var volumeAnalysis = GetVolumeAnalysisData(0);
        if (volumeAnalysis?.Total == null)
        {
            nullBars++;
            SetOutputs(0d, 0d, 0d, healthy: false);
            PublishStatus("NO_VOLUME_ANALYSIS", 0, "No VolumeAnalysisData/Total on current bar.");
            return;
        }

        var total = volumeAnalysis.Total;
        var delta = total.Delta;
        var buyVolume = total.BuyVolume;
        var sellVolume = total.SellVolume;
        var levels = volumeAnalysis.PriceLevels?.Count ?? 0;

        SetOutputs(delta, buyVolume, sellVolume, healthy: delta != 0d || buyVolume > 0d || sellVolume > 0d || levels > 0);

        if (delta == 0d && buyVolume == 0d && sellVolume == 0d && levels == 0)
        {
            zeroBars++;
            PublishStatus("ZEROED", levels, $"Reason={args.Reason}; structure exists but totals are empty.");
            return;
        }

        okBars++;
        PublishStatus("OK", levels, $"Reason={args.Reason}; delta/totals detected.");
    }

    public void VolumeAnalysisData_Loaded()
    {
        Core.Loggers.Log($"{Name}: volume-analysis data load callback received.");
        Refresh();
    }

    private void SetOutputs(double delta, double buyVolume, double sellVolume, bool healthy)
    {
        SetValue(delta, 0);
        SetValue(buyVolume, 1);
        SetValue(sellVolume, 2);
        SetBarColor(healthy ? Color.ForestGreen : Color.IndianRed);
    }

    private void PublishStatus(string status, int levels, string reason)
    {
        SetLabelValue("probe_feed", "Schwab");
        SetLabelValue("probe_status", $"{status} | ok={okBars} null={nullBars} zero={zeroBars}");
        SetLabelValue("probe_levels", levels);
        SetLabelValue("probe_reason", reason);

        if (string.Equals(lastStatus, status, StringComparison.Ordinal))
            return;

        lastStatus = status;
        Core.Loggers.Log($"{Name}: {status}; levels={levels}; ok={okBars}; null={nullBars}; zero={zeroBars}; {reason}");
    }
}
