// Short human-readable summaries of trace records and messages.

import type { Rec, Trace } from "./trace";

export function nodeName(trace: Trace, id: number | undefined): string {
  if (id === undefined) return "?";
  return id <= trace.header.nodes ? `S${id}` : `C${id}`;
}

export function formatTime(us: number): string {
  return `${(us / 1e6).toFixed(3)}s`;
}

type Msg = Record<string, unknown>;

const num = (v: unknown) => (typeof v === "number" ? v : 0);

// summarizeMessage describes a message body in the terms of Figure 2 of
// the Raft paper (or the KV client protocol).
export function summarizeMessage(kind: string | undefined, msg: Msg | undefined): string {
  const m = msg ?? {};
  switch (kind) {
    case "RequestVote":
      return `term ${num(m.term)}, last log ${num(m.last_log_index)}/${num(m.last_log_term)}`;
    case "RequestVoteResponse":
      return `term ${num(m.term)}, ${m.vote_granted ? "granted" : "denied"}`;
    case "PreVote":
      return `would campaign in term ${num(m.term)}, last log ${num(m.last_log_index)}/${num(m.last_log_term)}`;
    case "PreVoteResponse":
      return m.vote_granted ? `would vote in term ${num(m.term)}` : `no (in term ${num(m.term)})`;
    case "AppendEntries": {
      const n = Array.isArray(m.entries) ? m.entries.length : 0;
      const what = n === 0 ? "heartbeat" : `${n} entr${n === 1 ? "y" : "ies"}`;
      return `term ${num(m.term)}, ${what}, prev ${num(m.prev_log_index)}/${num(m.prev_log_term)}, commit ${num(m.leader_commit)}`;
    }
    case "AppendEntriesResponse":
      if (m.success) return `term ${num(m.term)}, ok, match ${num(m.match_index)}`;
      return `term ${num(m.term)}, rejected` + (m.conflict_index ? `, conflict ${num(m.conflict_index)}/${num(m.conflict_term)}` : "");
    case "InstallSnapshot": {
      const s = (m.snapshot ?? {}) as Msg;
      return `term ${num(m.term)}, snapshot up to ${num(s.index)}/${num(s.term)}`;
    }
    case "ClientRequest": {
      const c = (m.cmd ?? {}) as Msg;
      return `${String(c.op ?? "?")} ${String(c.key ?? "")}`.trim() + (c.request ? ` (req ${num(c.request)})` : "");
    }
    case "ClientResponse":
      return `${String(m.status ?? "?")}` + (m.request ? ` (req ${num(m.request)})` : "");
  }
  return msg ? JSON.stringify(msg) : "";
}

export function summarize(trace: Trace, r: Rec): string {
  const n = (id?: number) => nodeName(trace, id);
  switch (r.type) {
    case "start":
      return `${n(r.node)} started`;
    case "crash":
      return `${n(r.node)} crashed (${r.kind === "machine" ? "machine: unsynced writes lost" : "process"})`;
    case "send":
      return `${n(r.from)}→${n(r.to)} ${r.kind}: ${summarizeMessage(r.kind, r.msg)}`;
    case "deliver":
      return `${n(r.from)}→${n(r.to)} ${r.kind} delivered`;
    case "drop":
      return `${n(r.from)}→${n(r.to)} ${r.kind} dropped: ${r.reason}`;
    case "state": {
      const s = r.state as Msg | null;
      if (s && "role" in s) {
        return `${n(r.node)} ${String(s.role)}, term ${num(s.term)}, commit ${num(s.commit)}, last ${num(s.last_index)}`;
      }
      return `${n(r.node)} ${JSON.stringify(s)}`;
    }
    case "partition":
      return `partition (${r.kind}): ${(r.links ?? []).map((l) => `${n(l.from)}→${n(l.to)}`).join(", ")} cut`;
    case "heal":
      return "partition healed";
    case "quiet":
      return "quiet period: no new faults from here";
    case "violation":
      return `${r.kind}: ${r.reason}`;
  }
  return JSON.stringify(r);
}
