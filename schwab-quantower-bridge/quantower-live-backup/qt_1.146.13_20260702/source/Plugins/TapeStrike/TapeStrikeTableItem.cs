using TradingPlatform.BusinessLayer;
using TradingPlatform.PresentationLayer.Renderers.Table;

namespace TapeStrike;

internal sealed class TapeStrikeTableItem : TableItem
{
    private const int CompactColumnWidth = 40;
    private const int SpacerColumnWidth = 16;

    private static int symbolColumnWidth = CompactColumnWidth;
    private static int audioColumnWidth = CompactColumnWidth;
    private static int statusColumnWidth = 360;

    private readonly TapeStrikeStatus? status;

    public TapeStrikeTableItem()
    {
    }

    public TapeStrikeTableItem(TapeStrikeStatus status)
    {
        this.status = status;
    }

    public static void SetColumnWidths(int symbolWidth, int audioWidth, int statusWidth)
    {
        symbolColumnWidth = CompactColumnWidth;
        audioColumnWidth = CompactColumnWidth;
        statusColumnWidth = Math.Clamp(statusWidth, 80, 1200);
    }

    public override List<TableColumnDefinition> ColumnsDefinition =>
    [
        new TableColumnDefinition(loc.key("Symbol"), TableComparingType.String, symbolColumnWidth, true, false),
        new TableColumnDefinition(loc.key("Audio"), TableComparingType.String, audioColumnWidth, true, false),
        new TableColumnDefinition(loc.key("Status"), TableComparingType.String, statusColumnWidth, true, false),
        new TableColumnDefinition(loc.key(" "), TableComparingType.String, SpacerColumnWidth, true, false)
    ];

    public override (object value, string formattedValue) GetCellValue(int columnIndex, bool requireFormattedValue = true, bool useInvariantCulture = false)
    {
        if (this.status == null)
        {
            var empty = columnIndex == 3 ? "Waiting" : string.Empty;
            return (empty, empty);
        }

        object value = columnIndex switch
        {
            0 => this.status.Symbol,
            1 => FormatAudioState(this.status),
            2 => this.status.Status,
            3 => string.Empty,
            _ => string.Empty
        };

        var formatted = value.ToString() ?? string.Empty;

        return (value, requireFormattedValue ? formatted : string.Empty);
    }

    private static string FormatAudioState(TapeStrikeStatus status)
    {
        if (status.TickAudioEnabled && status.BurstAudioEnabled && status.LargeTradeAudioEnabled)
            return "ON";

        return $"T:{OnOff(status.TickAudioEnabled)} B:{OnOff(status.BurstAudioEnabled)} L:{OnOff(status.LargeTradeAudioEnabled)}";
    }

    private static string OnOff(bool enabled) => enabled ? "ON" : "OFF";
}
