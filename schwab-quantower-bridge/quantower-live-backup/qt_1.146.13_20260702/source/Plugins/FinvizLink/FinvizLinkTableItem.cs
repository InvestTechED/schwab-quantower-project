using TradingPlatform.BusinessLayer;
using TradingPlatform.PresentationLayer.Renderers.Table;

namespace FinvizLink;

internal sealed class FinvizLinkTableItem : TableItem
{
    private readonly FinvizLinkStatus? status;

    public FinvizLinkTableItem()
    {
    }

    public FinvizLinkTableItem(FinvizLinkStatus status)
    {
        this.status = status;
    }

    public override List<TableColumnDefinition> ColumnsDefinition =>
    [
        new TableColumnDefinition(loc.key("Symbol"), TableComparingType.String, 120, true, false),
        new TableColumnDefinition(loc.key("Status"), TableComparingType.String, 420, true, false),
        new TableColumnDefinition(loc.key(" "), TableComparingType.String, 40, true, false)
    ];

    public override (object value, string formattedValue) GetCellValue(int columnIndex, bool requireFormattedValue = true, bool useInvariantCulture = false)
    {
        if (this.status == null)
            return (string.Empty, string.Empty);

        object value = columnIndex switch
        {
            0 => this.status.Symbol,
            1 => this.status.Status,
            2 => string.Empty,
            _ => string.Empty
        };

        var formatted = value.ToString() ?? string.Empty;
        return (value, requireFormattedValue ? formatted : string.Empty);
    }
}

internal sealed record FinvizLinkStatus(string Symbol, string Status);
