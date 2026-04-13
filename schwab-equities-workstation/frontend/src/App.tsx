import { FormEvent, useEffect, useState, useTransition } from "react";

type MarketSnapshot = {
  symbol: string;
  as_of: string;
  last: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  relative_volume: number;
  trend_state: "bullish" | "bearish" | "neutral";
  vwap_bias: "above" | "below" | "flat";
};

type Signal = {
  name: string;
  score: number;
  summary: string;
};

type PriceActionReport = {
  symbol: string;
  snapshot: MarketSnapshot;
  signals: Signal[];
  conclusion: string;
};

type Bar = {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

const watchSymbols = ["AAPL", "MSFT", "NVDA", "AMD", "META", "TSLA"];

const formatNumber = (value: number) =>
  new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value);

const formatCompact = (value: number) =>
  new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 2
  }).format(value);

const toneClass = (score: number) => {
  if (score >= 70) return "positive";
  if (score >= 45) return "neutral";
  return "warning";
};

const buildPath = (bars: Bar[]) => {
  if (!bars.length) return "";

  const width = 760;
  const height = 250;
  const min = Math.min(...bars.map((bar) => bar.low));
  const max = Math.max(...bars.map((bar) => bar.high));
  const range = max - min || 1;

  return bars
    .map((bar, index) => {
      const x = (index / Math.max(bars.length - 1, 1)) * width;
      const y = height - ((bar.close - min) / range) * height;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
};

export function App() {
  const [symbolInput, setSymbolInput] = useState("AAPL");
  const [activeSymbol, setActiveSymbol] = useState("AAPL");
  const [snapshot, setSnapshot] = useState<MarketSnapshot | null>(null);
  const [report, setReport] = useState<PriceActionReport | null>(null);
  const [bars, setBars] = useState<Bar[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      try {
        setError(null);
        const [snapshotResponse, reportResponse, barsResponse] = await Promise.all([
          fetch(`/api/market/snapshot/${activeSymbol}`),
          fetch(`/api/analysis/price-action/${activeSymbol}`),
          fetch(`/api/market/bars/${activeSymbol}?limit=180`)
        ]);

        if (!snapshotResponse.ok || !reportResponse.ok || !barsResponse.ok) {
          throw new Error("Unable to load market data from backend.");
        }

        const [nextSnapshot, nextReport, nextBars] = await Promise.all([
          snapshotResponse.json() as Promise<MarketSnapshot>,
          reportResponse.json() as Promise<PriceActionReport>,
          barsResponse.json() as Promise<Bar[]>
        ]);

        if (cancelled) return;
        setSnapshot(nextSnapshot);
        setReport(nextReport);
        setBars(nextBars);
      } catch (fetchError) {
        if (cancelled) return;
        setError(fetchError instanceof Error ? fetchError.message : "Unknown error");
      }
    };

    load();
    const intervalId = window.setInterval(load, 15000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeSymbol]);

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextSymbol = symbolInput.trim().toUpperCase();
    if (!nextSymbol) return;
    startTransition(() => setActiveSymbol(nextSymbol));
  };

  const latestBar = bars[bars.length - 1];
  const path = buildPath(bars);
  const priceChange = snapshot ? snapshot.last - snapshot.close : 0;
  const percentChange = snapshot?.close ? (priceChange / snapshot.close) * 100 : 0;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <p className="eyebrow">Live Schwab Feed</p>
          <h1>Equities Workstation</h1>
          <p className="muted">
            A modern equities dashboard built around live quotes, 5-minute price
            structure, and deterministic signal analysis.
          </p>
        </div>

        <form className="panel search-panel" onSubmit={onSubmit}>
          <p className="panel-label">Symbol Lookup</p>
          <div className="search-row">
            <input
              className="symbol-input"
              value={symbolInput}
              onChange={(event) => setSymbolInput(event.target.value.toUpperCase())}
              placeholder="AAPL"
            />
            <button className="primary-button" type="submit">
              Load
            </button>
          </div>
          <p className="tiny">
            Pulls live quote, bar history, and analysis from the backend.
          </p>
        </form>

        <div className="panel">
          <p className="panel-label">Connection</p>
          <div className="status-row">
            <span className={`status-dot ${error ? "status-down" : "status-live"}`} />
            <span>{error ? "Backend issue" : "Session active"}</span>
          </div>
          <p className="tiny">
            {error
              ? error
              : isPending
                ? "Refreshing symbol state..."
                : `Monitoring ${activeSymbol} with 15s refresh.`}
          </p>
        </div>

        <div className="panel">
          <p className="panel-label">Watchlist</p>
          <div className="watchlist">
            {watchSymbols.map((symbol) => (
              <button
                className={`watchlist-row ${symbol === activeSymbol ? "selected" : ""}`}
                key={symbol}
                onClick={() => {
                  setSymbolInput(symbol);
                  startTransition(() => setActiveSymbol(symbol));
                }}
                type="button"
              >
                <div>
                  <strong>{symbol}</strong>
                  <p className="tiny">
                    {symbol === activeSymbol && snapshot ? `${formatNumber(snapshot.last)} last` : "Tap to load"}
                  </p>
                </div>
                <span className={symbol === activeSymbol && percentChange < 0 ? "down" : "up"}>
                  {symbol === activeSymbol && snapshot ? `${percentChange >= 0 ? "+" : ""}${percentChange.toFixed(2)}%` : "Live"}
                </span>
              </button>
            ))}
          </div>
        </div>
      </aside>

      <main className="dashboard">
        <section className="hero-card">
          <div>
            <p className="eyebrow">Market State</p>
            <h2>{activeSymbol} {snapshot ? formatNumber(snapshot.last) : "loading..."}</h2>
            <p className="muted">
              {report?.conclusion ?? "Waiting for live backend analysis."}
            </p>
          </div>
          <div className="hero-metrics">
            <div className="hero-stat">
              <span className="tiny">Session Change</span>
              <strong className={percentChange >= 0 ? "up" : "down"}>
                {snapshot ? `${priceChange >= 0 ? "+" : ""}${priceChange.toFixed(2)} (${percentChange.toFixed(2)}%)` : "--"}
              </strong>
            </div>
            <div className="hero-stat">
              <span className="tiny">Relative Volume</span>
              <strong>{snapshot ? `${snapshot.relative_volume.toFixed(2)}x` : "--"}</strong>
            </div>
            <div className="hero-stat">
              <span className="tiny">Bias</span>
              <strong>{snapshot ? `${snapshot.trend_state} / ${snapshot.vwap_bias}` : "--"}</strong>
            </div>
          </div>
        </section>

        <section className="grid market-grid">
          <div className="panel chart-panel">
            <div className="chart-header">
              <div>
                <p className="panel-label">Price Action</p>
                <h3>5-Minute Structure</h3>
              </div>
              <div className="chart-meta">
                <span>Open {snapshot ? formatNumber(snapshot.open) : "--"}</span>
                <span>High {snapshot ? formatNumber(snapshot.high) : "--"}</span>
                <span>Low {snapshot ? formatNumber(snapshot.low) : "--"}</span>
              </div>
            </div>

            <div className="chart-stage">
              {path ? (
                <svg viewBox="0 0 760 250" className="line-chart" preserveAspectRatio="none">
                  <defs>
                    <linearGradient id="lineFill" x1="0" x2="0" y1="0" y2="1">
                      <stop offset="0%" stopColor="rgba(78, 214, 255, 0.35)" />
                      <stop offset="100%" stopColor="rgba(78, 214, 255, 0.02)" />
                    </linearGradient>
                  </defs>
                  <path d={`${path} L 760 250 L 0 250 Z`} fill="url(#lineFill)" />
                  <path d={path} className="line-chart-stroke" />
                </svg>
              ) : (
                <div className="chart-empty">Waiting for bar data...</div>
              )}
            </div>

            <div className="metric-row">
              <div>
                <p className="tiny">Latest Bar Close</p>
                <strong>{latestBar ? formatNumber(latestBar.close) : "--"}</strong>
              </div>
              <div>
                <p className="tiny">Latest Bar Volume</p>
                <strong>{latestBar ? formatCompact(latestBar.volume) : "--"}</strong>
              </div>
              <div>
                <p className="tiny">As Of</p>
                <strong>{snapshot ? new Date(snapshot.as_of).toLocaleTimeString() : "--"}</strong>
              </div>
            </div>
          </div>

          <div className="panel ladder-panel">
            <p className="panel-label">Signal Ladder</p>
            <div className="signal-list">
              {report?.signals.map((signal) => (
                <div className={`signal-card ${toneClass(signal.score)}`} key={signal.name}>
                  <div className="signal-topline">
                    <strong>{signal.name.replaceAll("_", " ")}</strong>
                    <span>{signal.score.toFixed(1)}</span>
                  </div>
                  <p>{signal.summary}</p>
                </div>
              )) ?? <div className="chart-empty">No signals available yet.</div>}
            </div>
          </div>
        </section>

        <section className="grid lower-grid">
          <div className="panel">
            <p className="panel-label">Tape Context</p>
            <div className="stat-board">
              <div className="stat-cell">
                <span className="tiny">Volume</span>
                <strong>{snapshot ? formatCompact(snapshot.volume) : "--"}</strong>
              </div>
              <div className="stat-cell">
                <span className="tiny">Prior Close</span>
                <strong>{snapshot ? formatNumber(snapshot.close) : "--"}</strong>
              </div>
              <div className="stat-cell">
                <span className="tiny">Intraday Range</span>
                <strong>{snapshot ? formatNumber(snapshot.high - snapshot.low) : "--"}</strong>
              </div>
              <div className="stat-cell">
                <span className="tiny">State</span>
                <strong>{snapshot?.trend_state ?? "--"}</strong>
              </div>
            </div>
          </div>

          <div className="panel">
            <p className="panel-label">Operator Notes</p>
            <ul className="insight-list">
              <li>Live backend is powered by Schwab quote and bar endpoints.</li>
              <li>Analysis is deterministic and fast, with no LLM dependency required.</li>
              <li>Next upgrade: support/resistance, breakout logic, and richer volume regimes.</li>
            </ul>
          </div>
        </section>
      </main>
    </div>
  );
}
