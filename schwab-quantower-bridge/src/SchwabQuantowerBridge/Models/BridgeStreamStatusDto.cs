using System;
using System.Collections.Generic;

namespace SchwabQuantowerBridge.Models;

public sealed class BridgeStreamStatusDto
{
    public bool Started { get; set; }

    public bool StreamTaskRunning { get; set; }

    public bool HasClient { get; set; }

    public bool QuoteInitialized { get; set; }

    public bool ChartInitialized { get; set; }

    public bool NasdaqBookInitialized { get; set; }

    public bool NyseBookInitialized { get; set; }

    public int RegisteredDisplayCount { get; set; }

    public int ActiveDisplaySymbolCount { get; set; }

    public int ActiveActualSymbolCount { get; set; }

    public int SubscribedSymbolCount { get; set; }

    public int PublishedEventCount { get; set; }

    public int DroppedEventCount { get; set; }

    public int RestartCount { get; set; }

    public int StartCount { get; set; }

    public int ShutdownCount { get; set; }

    public int QueueMaxSize { get; set; }

    public DateTimeOffset? LastEventAt { get; set; }

    public DateTimeOffset? LastStartedAt { get; set; }

    public DateTimeOffset? LastRestartedAt { get; set; }

    public DateTimeOffset? LastErrorAt { get; set; }

    public string? LastErrorMessage { get; set; }

    public List<string>? SubscribedSymbols { get; set; }
}
