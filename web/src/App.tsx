import { useEffect, useMemo, useState } from 'react';
import { AreaChart, type Point } from './AreaChart.js';

interface Snapshot {
  totals: { events: number; value: number; buckets: number };
  lag: number;
  top: Array<{ type: string; count: number; total: number }>;
  buckets: Array<{ bucket: string; type: string; count: number; total: number }>;
}

interface Health {
  bus: string;
  store: string;
}

/** Lag thresholds are arbitrary demo values; the point is that the state is always named. */
function lagState(lag: number): { tone: string; icon: string; word: string } {
  if (lag === 0) return { tone: 'status-good', icon: '●', word: 'caught up' };
  if (lag < 5000) return { tone: 'status-warning', icon: '▲', word: 'catching up' };
  return { tone: 'status-critical', icon: '■', word: 'falling behind' };
}

function useSnapshot() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    const source = new EventSource('/api/stream');
    source.onopen = () => setConnected(true);
    source.onerror = () => setConnected(false);
    source.onmessage = (message) => {
      setSnapshot(JSON.parse(message.data) as Snapshot);
      setConnected(true);
    };
    return () => source.close();
  }, []);

  return { snapshot, connected };
}

export function App() {
  const { snapshot, connected } = useSnapshot();
  const [health, setHealth] = useState<Health | null>(null);

  useEffect(() => {
    fetch('/health')
      .then((response) => response.json())
      .then(setHealth)
      .catch(() => setHealth(null));
  }, []);

  const series: Point[] = useMemo(() => {
    if (!snapshot) return [];
    const byBucket = new Map<string, number>();
    for (const row of snapshot.buckets) {
      byBucket.set(row.bucket, (byBucket.get(row.bucket) ?? 0) + row.count);
    }
    const ordered = [...byBucket.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([bucket, count]) => ({ bucket, count }));
    return ordered;
  }, [snapshot]);

  // The newest bucket is the minute still in progress, so it is always partially filled.
  // Plotting it makes the series end in a phantom drop; it belongs in a tile, not a trend.
  const currentMinute = series.at(-1);
  const completed = series.length > 1 ? series.slice(0, -1) : series;
  const perMinute = currentMinute?.count ?? 0;
  const lag = snapshot?.lag ?? 0;
  const state = lagState(lag);
  const topMax = Math.max(1, ...(snapshot?.top ?? []).map((row) => row.count));

  return (
    <div className="page">
      <header className="masthead">
        <h1>StreamTally</h1>
        <div className="chips">
          <span className={`chip ${connected ? 'live' : 'stale'}`}>
            {connected ? 'live' : 'disconnected'}
          </span>
          {health && (
            <>
              <span className="chip">bus: {health.bus}</span>
              <span className="chip">store: {health.store}</span>
            </>
          )}
        </div>
      </header>
      <p className="subtitle">
        Every accepted event counted exactly once, under redelivery and mid-batch failure.
      </p>

      <section className="tiles">
        <div className="tile">
          <div className="tile-label">Events counted</div>
          <div className="tile-value">{(snapshot?.totals.events ?? 0).toLocaleString()}</div>
          <div className="tile-note">deduplicated by event id</div>
        </div>
        <div className="tile">
          <div className="tile-label">Current minute</div>
          <div className="tile-value">{perMinute.toLocaleString()}</div>
          <div className="tile-note">events in the newest bucket</div>
        </div>
        <div className="tile">
          <div className="tile-label">Consumer lag</div>
          <div className="tile-value">{lag.toLocaleString()}</div>
          <div className="tile-note">
            <span className={`status-icon ${state.tone}`} aria-hidden="true">
              {state.icon}
            </span>
            <span>{state.word}</span>
          </div>
        </div>
        <div className="tile">
          <div className="tile-label">Value aggregated</div>
          <div className="tile-value">
            {Math.round(snapshot?.totals.value ?? 0).toLocaleString()}
          </div>
          <div className="tile-note">
            across {(snapshot?.totals.buckets ?? 0).toLocaleString()} minute-by-type rollups
          </div>
        </div>
      </section>

      <section className="panels">
        <div className="panel">
          <h2>Events per minute</h2>
          <p className="hint">
            Completed minutes only, bucketed by when each event occurred. The minute in progress is
            in the tile above.
          </p>
          <AreaChart points={completed} />
          {completed.length > 0 && (
            <details className="table-toggle">
              <summary>View as table</summary>
              <table className="data-view">
                <thead>
                  <tr>
                    <th>Minute</th>
                    <th>Events</th>
                  </tr>
                </thead>
                <tbody>
                  {[...completed]
                    .reverse()
                    .slice(0, 12)
                    .map((point) => (
                      <tr key={point.bucket}>
                        <td>{new Date(point.bucket).toLocaleTimeString()}</td>
                        <td>{point.count.toLocaleString()}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            </details>
          )}
        </div>

        <div className="panel">
          <h2>Top event types</h2>
          <p className="hint">All time, by count.</p>
          {(snapshot?.top ?? []).length === 0 ? (
            <p className="empty">Nothing yet.</p>
          ) : (
            (snapshot?.top ?? []).map((row) => (
              <div className="bar-row" key={row.type}>
                <span className="bar-name">{row.type}</span>
                <span className="bar-value">{row.count.toLocaleString()}</span>
                <div className="bar-track">
                  <div className="bar-fill" style={{ width: `${(row.count / topMax) * 100}%` }} />
                </div>
              </div>
            ))
          )}
        </div>
      </section>

      <footer className="note">
        Fictional demo data. Metrics at <code>/metrics</code>; run{' '}
        <code>npm run verify:exactness</code> to check the counting claim yourself.
      </footer>
    </div>
  );
}
