import { describe, expect, it } from "vitest";
import { termAt, type RaftState } from "./raft";
import { MessageIndex, Replayer, lowerBound, markers } from "./replay";
import { TraceError, frameAt, parseTrace } from "./trace";

const header = `{"type":"header","version":2,"seed":7,"nodes":2,"duration":1000000,"tick_interval":10000,"profile":{"name":"test"}}`;

function raft(id: number, role: string, term: number, extra: Partial<RaftState> = {}): string {
  return JSON.stringify({ id, role, term, vote: 0, leader: 0, last_index: 0, last_term: 0, commit: 0, voters: [1, 2], applied: 0, log: [], ...extra });
}

// A small hand-written trace: two servers start, 1 sends to 2 twice (one
// copy duplicated, one lost), 2 becomes leader, a partition comes and goes,
// 1 crashes and restarts, then a violation.
const lines = [
  header,
  `{"step":0,"t":0,"type":"start","node":1}`,
  `{"step":0,"t":0,"type":"state","node":1,"state":${raft(1, "follower", 0)}}`,
  `{"step":0,"t":0,"type":"start","node":2}`,
  `{"step":0,"t":0,"type":"state","node":2,"state":${raft(2, "follower", 0)}}`,
  `{"step":1,"t":100,"type":"send","id":1,"from":1,"to":2,"kind":"RequestVote","msg":{"term":1}}`,
  `{"step":1,"t":100,"type":"send","id":2,"from":1,"to":2,"kind":"RequestVote","msg":{"term":1}}`,
  `{"step":1,"t":100,"type":"drop","id":2,"from":1,"to":2,"kind":"RequestVote","reason":"lost"}`,
  `{"step":2,"t":300,"type":"deliver","id":1,"from":1,"to":2,"kind":"RequestVote"}`,
  `{"step":2,"t":300,"type":"state","node":2,"state":${raft(2, "leader", 1)}}`,
  `{"step":3,"t":500,"type":"deliver","id":1,"from":1,"to":2,"kind":"RequestVote"}`,
  `{"step":4,"t":600,"type":"partition","kind":"isolate","links":[{"from":1,"to":2},{"from":2,"to":1}]}`,
  `{"step":5,"t":700,"type":"heal"}`,
  `{"step":6,"t":800,"type":"crash","node":1,"kind":"machine","reason":"injected"}`,
  `{"step":7,"t":900,"type":"start","node":1}`,
  `{"step":7,"t":900,"type":"state","node":1,"state":${raft(1, "follower", 1)}}`,
  `{"step":8,"t":950,"type":"quiet"}`,
  `{"step":9,"t":990,"type":"violation","kind":"election-safety","reason":"two leaders"}`,
];
const text = lines.join("\n") + "\n";

describe("parseTrace", () => {
  it("indexes records into frames, servers and messages", () => {
    const tr = parseTrace(text);
    expect(tr.header.seed).toBe(7);
    expect(tr.records).toHaveLength(lines.length - 1);
    expect(tr.frames.map((f) => f.step)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(tr.frames[1]).toMatchObject({ t: 100, start: 4, end: 7 });
    expect(tr.servers).toEqual([1, 2]);
    expect(tr.clients).toEqual([]);
    expect(tr.sends.map((s) => s.arrivals.map((a) => [a.frame, a.type]))).toEqual([
      [
        [2, "deliver"],
        [3, "deliver"],
      ],
      [[1, "drop"]],
    ]);
    expect(tr.violation?.frame).toBe(9);
  });

  it("rejects bad input with a useful message", () => {
    expect(() => parseTrace("")).toThrow(TraceError);
    expect(() => parseTrace(`{"step":0}`)).toThrow(/header/);
    expect(() => parseTrace(header.replace(`"version":2`, `"version":1`))).toThrow(/version 1 is not supported/);
    expect(() => parseTrace(header + "\n{nope")).toThrow(/line 2/);
    expect(() => parseTrace(header + `\n{"step":0}`)).toThrow(/line 2/);
  });

  it("finds the frame for a time", () => {
    const tr = parseTrace(text);
    expect(frameAt(tr, -5)).toBe(0);
    expect(frameAt(tr, 100)).toBe(1);
    expect(frameAt(tr, 299)).toBe(1);
    expect(frameAt(tr, 5000)).toBe(9);
  });
});

describe("Replayer", () => {
  it("rebuilds nodes, partitions, quiet and violations", () => {
    const tr = parseTrace(text);
    const r = new Replayer(tr, 2); // tiny checkpoint interval, to exercise checkpoints
    const role = (f: number, n: number) => (r.at(f).nodes.get(n)?.state as RaftState).role;
    expect(role(1, 2)).toBe("follower");
    expect(role(2, 2)).toBe("leader");
    expect(r.at(3).partition).toBeNull();
    expect(r.at(4).partition?.kind).toBe("isolate");
    expect(r.at(5).partition).toBeNull();
    expect(r.at(6).nodes.get(1)).toMatchObject({ up: false, crash: { kind: "machine", t: 800 } });
    // A crashed node keeps its last state for display.
    expect((r.at(6).nodes.get(1)?.state as RaftState).term).toBe(0);
    expect(r.at(7).nodes.get(1)).toMatchObject({ up: true });
    expect((r.at(7).nodes.get(1)?.state as RaftState).term).toBe(1);
    expect(r.at(7).quiet).toBe(false);
    expect(r.at(8).quiet).toBe(true);
    expect(r.at(9).violation?.kind).toBe("election-safety");
    expect(r.at(99).frame).toBe(9); // clamped
  });
});

describe("MessageIndex", () => {
  it("tracks each copy from send to arrival", () => {
    const tr = parseTrace(text);
    const mi = new MessageIndex(tr);
    expect(mi.inFlight(0, 0)).toHaveLength(0);
    // Frame 1: message 1 is on the wire twice (it gets duplicated); message 2
    // was lost as it was sent, so it never is.
    const f1 = mi.inFlight(1, 200);
    expect(f1.map((m) => [m.sent.id, m.copy])).toEqual([
      [1, 0],
      [1, 1],
    ]);
    expect(f1[0].progress).toBeCloseTo(0.5); // halfway from t=100 to t=300
    expect(f1[1].progress).toBeCloseTo(0.25); // a quarter of t=100 to t=500
    expect(mi.inFlight(2, 300).map((m) => m.copy)).toEqual([1]);
    expect(mi.inFlight(3, 500)).toHaveLength(0);
  });

  it("lists recent drops", () => {
    const tr = parseTrace(text);
    const mi = new MessageIndex(tr);
    expect(mi.recentDrops(0, 0, 1000)).toHaveLength(0);
    expect(mi.recentDrops(1, 100, 50).map((d) => d.sent.id)).toEqual([2]);
    expect(mi.recentDrops(3, 500, 50)).toHaveLength(0); // too long ago
  });
});

describe("markers", () => {
  it("marks leaders, faults, quiet and the violation", () => {
    const ms = markers(parseTrace(text));
    expect(ms.map((m) => [m.frame, m.kind])).toEqual([
      [2, "leader"],
      [4, "partition"],
      [5, "heal"],
      [6, "crash"],
      [7, "restart"],
      [8, "quiet"],
      [9, "violation"],
    ]);
  });
});

it("marks configuration changes seen by leaders", () => {
  const t = [
    header,
    `{"step":0,"t":0,"type":"state","node":1,"state":${raft(1, "leader", 1, { voters: [1, 2] })}}`,
    `{"step":1,"t":5,"type":"state","node":1,"state":${raft(1, "leader", 1, { voters: [1, 2], commit: 3 })}}`,
    `{"step":2,"t":9,"type":"state","node":2,"state":${raft(2, "follower", 1, { voters: [1] })}}`,
    `{"step":3,"t":10,"type":"state","node":1,"state":${raft(1, "leader", 1, { voters: [1] })}}`,
  ].join("\n");
  const ms = markers(parseTrace(t)).filter((m) => m.kind === "config");
  expect(ms.map((m) => [m.frame, m.label])).toEqual([[3, "config: S1"]]);
});

describe("termAt", () => {
  const s = JSON.parse(
    raft(1, "follower", 3, {
      snap_index: 4,
      snap_term: 1,
      last_index: 9,
      log: [
        { from: 5, term: 1, count: 2 },
        { from: 7, term: 3, count: 3 },
      ],
    }),
  ) as RaftState;
  it("looks up terms through the runs", () => {
    expect([3, 4, 5, 6, 7, 9, 10].map((i) => termAt(s, i))).toEqual([undefined, 1, 1, 1, 3, 3, undefined]);
  });
});

it("lowerBound", () => {
  const a = [1, 3, 3, 7];
  expect(lowerBound(a.length, (i) => a[i] >= 3)).toBe(1);
  expect(lowerBound(a.length, (i) => a[i] >= 8)).toBe(4);
  expect(lowerBound(0, () => true)).toBe(0);
});

// Every example trace shipped with the site parses, and the checkpointed
// replayer agrees with replaying from the start at every frame.
describe("example traces", () => {
  const examples = import.meta.glob<string>("../public/traces/*.jsonl", { query: "?raw", import: "default", eager: true });
  const files = Object.keys(examples);
  it("exist", () => expect(files.length).toBeGreaterThan(0));
  it.each(files)("%s replays consistently", (file) => {
    const tr = parseTrace(examples[file]);
    const fast = new Replayer(tr, 64);
    const slow = new Replayer(tr, Number.MAX_SAFE_INTEGER); // one checkpoint: always from the start
    const n = tr.frames.length;
    for (const f of [0, 1, 63, 64, 65, Math.floor(n / 3), Math.floor(n / 2), n - 2, n - 1]) {
      if (f < 0 || f >= n) continue;
      expect(fast.at(f)).toEqual(slow.at(f));
    }
    const mi = new MessageIndex(tr);
    for (const f of [Math.floor(n / 4), Math.floor(n / 2)]) {
      // Brute force: a copy is in flight if sent by f and arriving after it.
      const want = tr.sends.flatMap((s) => (s.frame <= f ? s.arrivals.filter((a) => a.frame > f).length + (s.arrivals.length === 0 ? 1 : 0) : 0));
      expect(mi.inFlight(f, tr.frames[f].t)).toHaveLength(want.reduce((a, b) => a + b, 0));
    }
  });
});
