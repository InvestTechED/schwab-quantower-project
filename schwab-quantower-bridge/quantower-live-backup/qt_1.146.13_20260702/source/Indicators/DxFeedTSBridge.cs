using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using TradingPlatform.BusinessLayer;

// Auto-export: loaded on a DAILY (D1) chart, writes this chart's completed daily
// bars to <symbol>_daily.csv in OUTPUT_DIR. No Python, no socket. Append-only,
// deduped on date, current forming day skipped.
public sealed class DxFeedTSBridge : Indicator
{
    private const string OUTPUT_DIR = @"D:\YouTube_Scripts";

    private string _csvPath;
    private readonly HashSet<string> _writtenDates = new HashSet<string>();
    private bool _ready;

    public DxFeedTSBridge() : base()
    {
        Name = "DxFeedTSBridge";
        Description = "Auto-writes this chart's completed daily bars to <symbol>_daily.csv in " + OUTPUT_DIR + ".";
        SeparateWindow = false;
    }

    protected override void OnInit()
    {
        base.OnInit();
        _ready = false;
        _csvPath = null;
        _writtenDates.Clear();
    }

    protected override void OnUpdate(UpdateArgs args)
    {
        var hd = this.HistoricalData;
        if (hd == null || hd.Count == 0) return;

        if (!_ready && !TrySetup()) return;

        // [0] = the bar currently being calculated (replays through all history on
        // load, then tracks live). Write it once it's a completed prior session.
        if (hd[0] is not HistoryItemBar bar) return;
        if (bar.TimeLeft.Date >= DateTime.Now.Date) return;   // skip current/forming day

        string date = bar.TimeLeft.ToString("MMddyy", CultureInfo.InvariantCulture);
        if (_writtenDates.Contains(date)) return;             // dedupe

        AppendRow(date, bar);
    }

    private bool TrySetup()
    {
        try
        {
            string symbol = this.Symbol?.Name ?? "UNKNOWN";
            foreach (char c in Path.GetInvalidFileNameChars())
                symbol = symbol.Replace(c, '_');

            Directory.CreateDirectory(OUTPUT_DIR);
            _csvPath = Path.Combine(OUTPUT_DIR, symbol + "_daily.csv");

            if (File.Exists(_csvPath))
            {
                foreach (var line in File.ReadAllLines(_csvPath))
                {
                    int comma = line.IndexOf(',');
                    if (comma <= 0) continue;
                    string col0 = line.Substring(0, comma).Trim();
                    if (col0 != "date") _writtenDates.Add(col0);
                }
            }
            _ready = true;
            return true;
        }
        catch { return false; }   // retry on the next update
    }

    private void AppendRow(string date, HistoryItemBar bar)
    {
        try
        {
            bool newFile = !File.Exists(_csvPath);
            using (var w = new StreamWriter(_csvPath, append: true))
            {
                if (newFile) w.Write("date,open,high,low,close,volume\n");
                var sb = new StringBuilder();
                sb.Append(date).Append(',')
                  .Append(bar.Open.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(bar.High.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(bar.Low.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(bar.Close.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(bar.Volume.ToString(CultureInfo.InvariantCulture)).Append('\n');
                w.Write(sb.ToString());
            }
            _writtenDates.Add(date);   // mark only after a successful write
        }
        catch
        {
            // file locked (e.g., open in Excel) -> retry on a later update
        }
    }
}