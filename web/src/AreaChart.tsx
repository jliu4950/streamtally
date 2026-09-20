import { useMemo, useState } from 'react';

export interface Point {
  bucket: string;
  count: number;
}

interface Props {
  points: Point[];
  height?: number;
}

const PADDING = { top: 12, right: 12, bottom: 24, left: 44 };

function niceCeiling(value: number): number {
  if (value <= 5) return 5;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  for (const step of [1, 2, 2.5, 5, 10]) {
    if (value <= step * magnitude) return step * magnitude;
  }
  return 10 * magnitude;
}

function label(bucket: string): string {
  return new Date(bucket).toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
  });
}

/**
 * Single-series area chart. One measure, one axis -- deliberately never a second y-scale,
 * which is the standard way these dashboards start lying about correlation.
 */
export function AreaChart({ points, height = 260 }: Props) {
  const [hover, setHover] = useState<number | null>(null);
  const width = 640;

  const geometry = useMemo(() => {
    const max = niceCeiling(Math.max(1, ...points.map((point) => point.count)));
    const plotWidth = width - PADDING.left - PADDING.right;
    const plotHeight = height - PADDING.top - PADDING.bottom;
    const x = (index: number) =>
      PADDING.left +
      (points.length <= 1 ? plotWidth / 2 : (index / (points.length - 1)) * plotWidth);
    const y = (value: number) => PADDING.top + plotHeight - (value / max) * plotHeight;
    return { max, plotHeight, x, y };
  }, [points, height]);

  if (points.length === 0) {
    return <p className="empty">No events yet. Send some and this fills in within a second.</p>;
  }

  const { max, x, y } = geometry;
  const line = points.map((point, index) => `${x(index)},${y(point.count)}`).join(' ');
  const area = `${PADDING.left},${y(0)} ${line} ${x(points.length - 1)},${y(0)}`;
  const ticks = [0, max / 2, max];
  const hovered = hover === null ? null : points[hover];

  return (
    <div className="chart-wrap">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        role="img"
        aria-label={`Events per minute, ${points.length} buckets, peak ${Math.max(
          ...points.map((point) => point.count),
        )}`}
        onMouseLeave={() => setHover(null)}
        onMouseMove={(event) => {
          const box = event.currentTarget.getBoundingClientRect();
          const localX = ((event.clientX - box.left) / box.width) * width;
          const ratio = (localX - PADDING.left) / (width - PADDING.left - PADDING.right);
          const index = Math.round(ratio * (points.length - 1));
          setHover(Math.min(points.length - 1, Math.max(0, index)));
        }}
      >
        <title>Events per minute</title>
        {ticks.map((tick) => (
          <g key={tick}>
            <line
              x1={PADDING.left}
              x2={width - PADDING.right}
              y1={y(tick)}
              y2={y(tick)}
              stroke="var(--grid)"
              strokeWidth="1"
            />
            <text
              x={PADDING.left - 8}
              y={y(tick) + 4}
              textAnchor="end"
              fontSize="11"
              fill="var(--text-muted)"
            >
              {tick >= 1000 ? `${Math.round(tick / 1000)}k` : Math.round(tick)}
            </text>
          </g>
        ))}

        <polyline points={area} fill="var(--series-1-fill)" stroke="none" />
        <polyline
          points={line}
          fill="none"
          stroke="var(--series-1)"
          strokeWidth="2"
          strokeLinejoin="round"
          strokeLinecap="round"
        />

        {[0, points.length - 1].map((index) => {
          const point = points[index];
          if (!point) return null;
          return (
            <text
              key={`axis-${point.bucket}`}
              x={x(index)}
              y={height - 6}
              textAnchor={index === 0 ? 'start' : 'end'}
              fontSize="11"
              fill="var(--text-muted)"
            >
              {label(point.bucket)}
            </text>
          );
        })}

        {hovered && hover !== null && (
          <g>
            <line
              x1={x(hover)}
              x2={x(hover)}
              y1={PADDING.top}
              y2={height - PADDING.bottom}
              stroke="var(--axis)"
              strokeWidth="1"
            />
            {/* 2px surface ring so the marker reads against both the fill and the line. */}
            <circle
              cx={x(hover)}
              cy={y(hovered.count)}
              r="5"
              fill="var(--series-1)"
              stroke="var(--surface-1)"
              strokeWidth="2"
            />
          </g>
        )}
      </svg>

      {hovered && hover !== null && (
        <div
          className="tooltip"
          style={{
            left: `${(x(hover) / width) * 100}%`,
            top: 0,
            transform: 'translate(-50%, -4px)',
          }}
        >
          <span className="tooltip-time">{label(hovered.bucket)}</span>
          <strong>{hovered.count.toLocaleString()}</strong> events
        </div>
      )}
    </div>
  );
}
