// The timeline: a scrubber over the run's frames, with its interesting
// moments marked (leader changes, crashes, partitions, the violation).

import type { Marker } from "./replay";
import { formatTime } from "./describe";
import type { Trace } from "./trace";

interface Props {
  trace: Trace;
  markers: Marker[];
  frame: number;
  onSeek: (frame: number) => void;
}

export function Timeline({ trace, markers, frame, onSeek }: Props) {
  const last = Math.max(1, trace.frames.length - 1);
  const pct = (f: number) => `${(f / last) * 100}%`;

  // Partition spans run from each partition to the next heal (or the end).
  const spans: { from: number; to: number; label: string }[] = [];
  let open: Marker | null = null;
  for (const m of markers) {
    if (m.kind === "partition") open = m;
    if (m.kind === "heal" && open) {
      spans.push({ from: open.frame, to: m.frame, label: `${open.label} (${formatTime(open.t)} to ${formatTime(m.t)})` });
      open = null;
    }
  }
  if (open) spans.push({ from: open.frame, to: last, label: `${open.label} (from ${formatTime(open.t)})` });
  const quiet = markers.find((m) => m.kind === "quiet");

  return (
    <div>
      <div className="track">
        <div
          className="lane"
          onClick={(e) => {
            const r = e.currentTarget.getBoundingClientRect();
            onSeek(Math.round(((e.clientX - r.left) / r.width) * last));
          }}
        />
        {quiet && <div className="quietspan" style={{ left: pct(quiet.frame), right: 0 }} title={quiet.label} />}
        {spans.map((s) => (
          <div
            key={`s${s.from}`}
            className="span"
            style={{ left: pct(s.from), width: `${((s.to - s.from) / last) * 100}%` }}
            title={s.label}
            onClick={() => onSeek(s.from)}
          />
        ))}
        {markers
          .filter((m) => m.kind !== "partition" && m.kind !== "heal" && m.kind !== "quiet")
          .map((m, i) => (
            <button
              key={`m${i}`}
              className={`mk ${m.kind}`}
              style={{ left: pct(m.frame) }}
              title={`${formatTime(m.t)} ${m.label}`}
              aria-label={m.label}
              onClick={() => onSeek(m.frame)}
            />
          ))}
        <div className="cursor" style={{ left: pct(frame) }} />
      </div>
      <input
        className="scrub"
        type="range"
        min={0}
        max={last}
        value={frame}
        onChange={(e) => onSeek(Number(e.target.value))}
        aria-label="Position in the trace"
      />
      <div className="tl-legend">
        <span><i style={{ background: "var(--leader)" }} />new leader</span>
        {markers.some((m) => m.kind === "config") && (
          <span><i style={{ background: "#8b5cf6", transform: "rotate(45deg) scale(0.8)" }} />config change</span>
        )}
        <span><i style={{ background: "var(--danger)", borderRadius: "50%" }} />crash</span>
        <span><i style={{ background: "var(--quiet)" }} />restart</span>
        <span><i style={{ background: "color-mix(in srgb, var(--warn) 35%, transparent)" }} />partition</span>
        <span><i style={{ background: "color-mix(in srgb, var(--quiet) 25%, transparent)" }} />quiet period</span>
        {trace.violation && <span><i style={{ background: "var(--danger)" }} />violation</span>}
      </div>
    </div>
  );
}
