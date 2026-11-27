// The event log around the current position, and details of whatever is
// selected.

import { useEffect, useMemo, useRef } from "react";
import { formatTime, nodeName, summarize, summarizeMessage } from "./describe";
import { lowerBound } from "./replay";
import type { Rec, Sent, Trace } from "./trace";

const CLIENT_KINDS = new Set(["ClientRequest", "ClientResponse"]);

interface EventsProps {
  trace: Trace;
  frame: number;
  showClients: boolean;
  showDeliveries: boolean;
  onPick: (r: Rec, index: number) => void;
}

export function Events({ trace, frame, showClients, showDeliveries, onPick }: EventsProps) {
  // Indexes of the records that pass the filters, in order.
  const shown = useMemo(() => {
    const out: number[] = [];
    trace.records.forEach((r, i) => {
      if (!showClients && (CLIENT_KINDS.has(r.kind ?? "") || (r.node ?? 0) > trace.header.nodes)) return;
      if (!showDeliveries && r.type === "deliver") return;
      out.push(i);
    });
    return out;
  }, [trace, showClients, showDeliveries]);

  const f = trace.frames[frame];
  const end = f ? lowerBound(shown.length, (k) => shown[k] >= f.end) : 0;
  const from = Math.max(0, end - 80);
  const to = Math.min(shown.length, end + 20);
  const list = useRef<HTMLDivElement | null>(null);
  const current = useRef<HTMLDivElement | null>(null);
  // Centre the current frame's first event in the list. (scrollIntoView
  // would also scroll the page, under the sticky timeline.)
  useEffect(() => {
    const c = list.current;
    const r = current.current;
    if (c && r) c.scrollTop = r.offsetTop - c.clientHeight / 2 + r.offsetHeight / 2;
  }, [frame, showClients, showDeliveries]);

  // The row to keep in view: the current frame's first shown event, or if
  // the filters hide them all, the last event before it.
  const startK = f ? lowerBound(shown.length, (k) => shown[k] >= f.start) : 0;
  const anchor = startK < end ? startK : end - 1;
  const rows = [];
  for (let k = from; k < to; k++) {
    const i = shown[k];
    const r = trace.records[i];
    const isCurrent = !!f && i >= f.start && i < f.end;
    const clickable = r.id !== undefined || r.type === "state" || r.type === "violation";
    const ref = k === anchor ? current : undefined;
    rows.push(
      <div
        key={i}
        ref={ref}
        className={`ev ${r.type} ${isCurrent ? "current" : ""} ${clickable ? "clickable" : ""}`}
        onClick={clickable ? () => onPick(r, i) : undefined}
      >
        <span className="t">{formatTime(r.t)}</span>
        <span className="type">{r.type}</span>
        <span className="what" title={summarize(trace, r)}>
          {summarize(trace, r)}
        </span>
      </div>,
    );
  }
  return (
    <div className="events" ref={list}>
      {rows.length === 0 ? <span className="muted">No events.</span> : rows}
    </div>
  );
}

export type Selection = { kind: "message"; id: number; copy: number } | { kind: "record"; index: number } | null;

export function Details({ trace, sel, sendByID }: { trace: Trace; sel: Selection; sendByID: Map<number, Sent> }) {
  if (!sel) {
    return <div className="details muted">Click a message dot, a marker or an event to see its details.</div>;
  }
  if (sel.kind === "message") {
    const s = sendByID.get(sel.id);
    if (!s) return null;
    const r = s.rec;
    return (
      <div className="details">
        <div>
          <b>{r.kind}</b> {nodeName(trace, r.from)}→{nodeName(trace, r.to)} <span className="muted">message {s.id}</span>
        </div>
        <div>{summarizeMessage(r.kind, r.msg)}</div>
        <div className="muted">
          sent at {formatTime(r.t)} (step {r.step})
          {s.arrivals.length === 0 && "; still in flight when the run ended"}
          {s.arrivals.map((a, i) => (
            <span key={i}>
              ; {s.arrivals.length > 1 ? `copy ${i + 1} ` : ""}
              {a.type === "deliver" ? "delivered" : `dropped (${a.reason})`} at {formatTime(a.t)}
            </span>
          ))}
        </div>
        <pre>{JSON.stringify(r.msg, null, 2)}</pre>
      </div>
    );
  }
  const r = trace.records[sel.index];
  return (
    <div className="details">
      <div>
        <b>{r.type}</b> <span className="muted">at {formatTime(r.t)}, step {r.step}</span>
      </div>
      <div>{summarize(trace, r)}</div>
      <pre>{JSON.stringify(r, null, 2)}</pre>
    </div>
  );
}
