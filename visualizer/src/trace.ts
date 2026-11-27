// Parsing simulator traces (sim/trace.go). A trace is JSON Lines: a header,
// then one record per line in the order the event loop produced them.

export const SUPPORTED_VERSION = 2;

export interface Link {
  from: number;
  to: number;
}

export interface Header {
  type: "header";
  version: number;
  seed: number;
  nodes: number;
  clients?: number;
  duration: number; // virtual microseconds
  tick_interval: number;
  profile: {
    name: string;
    net?: { min_delay: number; max_delay: number; drop_prob: number; duplicate_prob: number };
    [k: string]: unknown;
  };
}

export type RecordType =
  | "start"
  | "crash"
  | "send"
  | "deliver"
  | "drop"
  | "state"
  | "partition"
  | "heal"
  | "quiet"
  | "violation";

// One trace record. Which fields are set depends on type, as in sim.Record.
export interface Rec {
  step: number;
  t: number; // virtual microseconds
  type: RecordType;
  id?: number; // message ID: send, deliver, drop
  node?: number;
  from?: number;
  to?: number;
  kind?: string;
  msg?: Record<string, unknown>;
  links?: Link[];
  state?: unknown;
  reason?: string;
}

// The records produced by one simulator event share a step; a frame is one
// step: the unit the timeline moves in.
export interface Frame {
  step: number;
  t: number;
  start: number; // index of the frame's first record
  end: number; // one past its last record
}

// What happened to one copy of a sent message.
export interface Arrival {
  frame: number;
  t: number;
  type: "deliver" | "drop";
  reason?: string; // for drops: "lost", "partition" or "node down"
}

export interface Sent {
  id: number;
  rec: Rec;
  frame: number;
  arrivals: Arrival[]; // empty if the run ended with the message in flight
}

export interface Trace {
  header: Header;
  records: Rec[];
  frames: Frame[];
  servers: number[]; // IDs 1..header.nodes: the nodes running Raft (or the toy)
  clients: number[]; // IDs above that: simulated clients
  sends: Sent[]; // in send order
  violation: { frame: number; rec: Rec } | null;
}

export class TraceError extends Error {}

export function parseTrace(text: string): Trace {
  const lines = text.split("\n");
  let header: Header | null = null;
  const records: Rec[] = [];
  lines.forEach((line, i) => {
    if (line.trim() === "") return;
    let v: unknown;
    try {
      v = JSON.parse(line);
    } catch {
      throw new TraceError(`line ${i + 1}: not valid JSON`);
    }
    if (typeof v !== "object" || v === null) throw new TraceError(`line ${i + 1}: not a JSON object`);
    if (header === null) {
      const h = v as Header;
      if (h.type !== "header") throw new TraceError("line 1: the trace must start with a header record");
      if (h.version !== SUPPORTED_VERSION) {
        throw new TraceError(
          `trace version ${h.version} is not supported (this visualizer reads version ${SUPPORTED_VERSION}); re-run the seed with the current simulator`,
        );
      }
      header = h;
      return;
    }
    const r = v as Rec;
    if (typeof r.step !== "number" || typeof r.t !== "number" || typeof r.type !== "string") {
      throw new TraceError(`line ${i + 1}: a record needs step, t and type`);
    }
    records.push(r);
  });
  if (header === null) throw new TraceError("the trace is empty");
  return index(header, records);
}

function index(header: Header, records: Rec[]): Trace {
  const frames: Frame[] = [];
  const ids = new Set<number>();
  const byID = new Map<number, Sent>();
  const sends: Sent[] = [];
  let violation: Trace["violation"] = null;

  records.forEach((r, i) => {
    let f = frames[frames.length - 1];
    if (f === undefined || f.step !== r.step) {
      f = { step: r.step, t: r.t, start: i, end: i };
      frames.push(f);
    }
    f.end = i + 1;
    const frame = frames.length - 1;
    for (const id of [r.node, r.from, r.to]) if (id !== undefined) ids.add(id);
    switch (r.type) {
      case "send": {
        const s: Sent = { id: r.id!, rec: r, frame, arrivals: [] };
        byID.set(s.id, s);
        sends.push(s);
        break;
      }
      case "deliver":
      case "drop": {
        const s = byID.get(r.id!);
        if (s) s.arrivals.push({ frame, t: r.t, type: r.type, reason: r.reason });
        break;
      }
      case "violation":
        violation = { frame, rec: r };
        break;
    }
  });
  for (let id = 1; id <= header.nodes; id++) ids.add(id);
  const all = [...ids].sort((a, b) => a - b);
  return {
    header,
    records,
    frames,
    servers: all.filter((id) => id <= header.nodes),
    clients: all.filter((id) => id > header.nodes),
    sends,
    violation,
  };
}

// frameAt returns the last frame at or before virtual time t (0 if t is
// before the first frame).
export function frameAt(trace: Trace, t: number): number {
  const fs = trace.frames;
  let lo = 0;
  let hi = fs.length - 1;
  if (hi < 0 || t < fs[0].t) return 0;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (fs[mid].t <= t) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}
