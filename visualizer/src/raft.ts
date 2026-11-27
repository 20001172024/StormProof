// The state a Raft server writes into the trace (sim/raftsim: raft.Status
// plus the applied index and the log, run-length encoded by term).

export type Role = "follower" | "pre-candidate" | "candidate" | "leader";

export interface TermRun {
  from: number; // index of the run's first entry
  term: number;
  count: number;
}

export interface RaftState {
  id: number;
  role: Role;
  term: number;
  vote: number;
  leader: number;
  last_index: number;
  last_term: number;
  commit: number;
  snap_index?: number;
  snap_term?: number;
  voters: number[] | null;
  applied: number;
  log: TermRun[] | null;
}

export function isRaftState(s: unknown): s is RaftState {
  return typeof s === "object" && s !== null && "role" in s && "term" in s && "commit" in s;
}

// termAt is the term of the entry at index in this server's log: the
// snapshot's term for the snapshot's last index, undefined for an index the
// log doesn't hold (compacted away, or past its end).
export function termAt(s: RaftState, index: number): number | undefined {
  const runs = s.log ?? [];
  if (index === (s.snap_index ?? 0) && index > 0) return s.snap_term;
  let lo = 0;
  let hi = runs.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const r = runs[mid];
    if (index < r.from) hi = mid - 1;
    else if (index >= r.from + r.count) lo = mid + 1;
    else return r.term;
  }
  return undefined;
}

// termColor gives each term its own hue, spread by the golden angle so
// consecutive terms look clearly different.
export function termColor(term: number): string {
  const hue = (term * 137.508 + 200) % 360;
  return `hsl(${hue.toFixed(0)} 62% 52%)`;
}
