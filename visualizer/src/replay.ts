// Rebuilding the cluster as it was at any point of a trace.
//
// The trace only records changes, so the state at frame f is every record
// of frames 0..f applied in order. Replaying from the start on every scrub
// would be slow for long traces, so the replayer keeps a checkpoint every
// `every` frames and replays forward from the nearest one.

import { isRaftState } from "./raft";
import type { Arrival, Link, Rec, Sent, Trace } from "./trace";

export interface NodeView {
  id: number;
  up: boolean;
  state: unknown; // the last traced state (kept while the node is down)
  crash?: { kind: string; t: number };
}

export interface ClusterView {
  frame: number;
  nodes: Map<number, NodeView>;
  partition: { kind: string; links: Link[] } | null;
  quiet: boolean;
  violation: Rec | null;
}

interface Checkpoint {
  nodes: Map<number, NodeView>;
  partition: ClusterView["partition"];
  quiet: boolean;
  violation: Rec | null;
}

export class Replayer {
  private readonly checkpoints: Checkpoint[] = [];
  private readonly trace: Trace;
  private readonly every: number;

  constructor(trace: Trace, every = 256) {
    this.trace = trace;
    this.every = every;
    let cp: Checkpoint = { nodes: new Map(), partition: null, quiet: false, violation: null };
    this.checkpoints.push(cp);
    for (let f = 0; f < trace.frames.length; f++) {
      if (f > 0 && f % every === 0) {
        this.checkpoints.push(cp);
      }
      cp = this.apply(clone(cp), f);
    }
  }

  // at returns the cluster after every record of frame `frame` is applied.
  at(frame: number): ClusterView {
    const last = this.trace.frames.length - 1;
    frame = Math.max(0, Math.min(frame, last));
    const k = Math.floor(frame / this.every);
    let s = clone(this.checkpoints[k]);
    for (let f = k * this.every; f <= frame; f++) s = this.apply(s, f);
    return { frame, ...s };
  }

  private apply(s: Checkpoint, frame: number): Checkpoint {
    const f = this.trace.frames[frame];
    if (!f) return s;
    for (let i = f.start; i < f.end; i++) {
      const r = this.trace.records[i];
      switch (r.type) {
        case "start": {
          const prev = s.nodes.get(r.node!);
          s.nodes.set(r.node!, { id: r.node!, up: true, state: prev?.state ?? null });
          break;
        }
        case "crash": {
          const prev = s.nodes.get(r.node!);
          s.nodes.set(r.node!, { id: r.node!, up: false, state: prev?.state ?? null, crash: { kind: r.kind ?? "", t: r.t } });
          break;
        }
        case "state": {
          const prev = s.nodes.get(r.node!);
          s.nodes.set(r.node!, { ...(prev ?? { id: r.node!, up: true }), state: r.state });
          break;
        }
        case "partition":
          s.partition = { kind: r.kind ?? "", links: r.links ?? [] };
          break;
        case "heal":
          s.partition = null;
          break;
        case "quiet":
          s.quiet = true;
          break;
        case "violation":
          s.violation = r;
          break;
      }
    }
    return s;
  }
}

// clone copies a checkpoint's containers. NodeViews are replaced, never
// mutated, so a shallow copy of the map is enough.
function clone(c: Checkpoint): Checkpoint {
  return { ...c, nodes: new Map(c.nodes) };
}

// A copy of a message on the wire at some moment.
export interface InFlight {
  sent: Sent;
  copy: number; // which delivery attempt (duplicates have several)
  arrival: Arrival | null; // null: still in flight when the run ended
  progress: number; // 0 at the sender, 1 at the receiver
}

export interface DropMark {
  sent: Sent;
  arrival: Arrival;
}

// MessageIndex answers "what is on the wire at frame f" without scanning
// every message: a copy is in flight from its send's frame until its
// arrival's frame, and no copy is in flight longer than maxSpan frames.
export class MessageIndex {
  private readonly trace: Trace;
  private readonly maxSpan: number;
  private readonly unarrived: Sent[];
  private readonly drops: DropMark[];

  constructor(trace: Trace) {
    this.trace = trace;
    let span = 0;
    this.unarrived = [];
    this.drops = [];
    for (const s of trace.sends) {
      if (s.arrivals.length === 0) this.unarrived.push(s);
      for (const a of s.arrivals) {
        span = Math.max(span, a.frame - s.frame);
        if (a.type === "drop") this.drops.push({ sent: s, arrival: a });
      }
    }
    this.drops.sort((a, b) => a.arrival.frame - b.arrival.frame);
    this.maxSpan = span;
  }

  // inFlight lists the copies on the wire after frame `frame`, positioned
  // for virtual time t (which may lie between frames while playing).
  inFlight(frame: number, t: number): InFlight[] {
    const out: InFlight[] = [];
    const sends = this.trace.sends;
    const first = lowerBound(sends.length, (i) => sends[i].frame >= frame - this.maxSpan);
    for (let i = first; i < sends.length && sends[i].frame <= frame; i++) {
      const s = sends[i];
      s.arrivals.forEach((a, copy) => {
        if (a.frame > frame) out.push({ sent: s, copy, arrival: a, progress: progress(s.rec.t, a.t, t) });
      });
    }
    const end = this.trace.frames[this.trace.frames.length - 1]?.t ?? t;
    for (const s of this.unarrived) {
      if (s.frame <= frame) out.push({ sent: s, copy: 0, arrival: null, progress: progress(s.rec.t, end + 1, t) });
    }
    return out;
  }

  // recentDrops lists the copies dropped in the `window` microseconds up to
  // t, at or before frame `frame`, so the view can flash them briefly.
  recentDrops(frame: number, t: number, window: number): DropMark[] {
    const d = this.drops;
    const last = lowerBound(d.length, (i) => d[i].arrival.frame > frame);
    const out: DropMark[] = [];
    for (let i = last - 1; i >= 0 && d[i].arrival.t > t - window; i--) out.push(d[i]);
    return out.reverse();
  }
}

function progress(sentAt: number, arriveAt: number, t: number): number {
  if (arriveAt <= sentAt) return 1;
  return Math.max(0, Math.min(1, (t - sentAt) / (arriveAt - sentAt)));
}

// lowerBound is the first i in [0, n) with pred(i) true, for a predicate
// that is false then true; n if it is never true.
export function lowerBound(n: number, pred: (i: number) => boolean): number {
  let lo = 0;
  let hi = n;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (pred(mid)) hi = mid;
    else lo = mid + 1;
  }
  return lo;
}

// Things worth marking on the timeline.
export type MarkerKind = "leader" | "config" | "crash" | "restart" | "partition" | "heal" | "quiet" | "violation";

export interface Marker {
  frame: number;
  t: number;
  kind: MarkerKind;
  node?: number;
  label: string;
}

export function markers(trace: Trace): Marker[] {
  const out: Marker[] = [];
  const leaderOf = new Map<number, number>(); // node -> the term it last led
  const started = new Set<number>();
  let config = ""; // the last configuration a leader reported
  trace.frames.forEach((f, frame) => {
    for (let i = f.start; i < f.end; i++) {
      const r = trace.records[i];
      const m = (kind: MarkerKind, label: string, node?: number) => out.push({ frame, t: r.t, kind, node, label });
      switch (r.type) {
        case "state":
          if (isRaftState(r.state) && r.state.role === "leader" && leaderOf.get(r.node!) !== r.state.term) {
            leaderOf.set(r.node!, r.state.term);
            m("leader", `S${r.node} leads term ${r.state.term}`, r.node);
          }
          // A configuration change shows up first in the leader's state:
          // it takes effect as soon as the leader appends it (§4.1).
          if (isRaftState(r.state) && r.state.role === "leader" && r.state.voters) {
            const c = r.state.voters.join(",");
            if (config !== "" && c !== config) m("config", `config: ${r.state.voters.map((v) => `S${v}`).join(" ")}`, r.node);
            config = c;
          }
          break;
        case "crash":
          m("crash", `S${r.node} crashed (${r.kind})`, r.node);
          leaderOf.delete(r.node!);
          break;
        case "start":
          if (started.has(r.node!)) m("restart", `S${r.node} restarted`, r.node);
          started.add(r.node!);
          break;
        case "partition":
          m("partition", `partition: ${r.kind}`);
          break;
        case "heal":
          m("heal", "partition healed");
          break;
        case "quiet":
          m("quiet", "quiet period: no more faults");
          break;
        case "violation":
          m("violation", `${r.kind}: ${r.reason}`);
          break;
      }
    }
  });
  return out;
}
