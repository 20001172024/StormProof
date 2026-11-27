// The cluster drawn as a network: servers on a circle, clients along the
// bottom, messages as dots moving along the links, partitions in red.

import type { ReactElement } from "react";
import { nodeName, summarizeMessage } from "./describe";
import { isRaftState, type RaftState } from "./raft";
import type { ClusterView, DropMark, InFlight } from "./replay";
import type { Trace } from "./trace";

const W = 640;
const H = 440;
const CX = W / 2;
const CY = 190;
const R = 140;
const NODE_R = 32;

export const KIND_COLORS: Record<string, string> = {
  PreVote: "#d946ef",
  PreVoteResponse: "#d946ef",
  RequestVote: "#8b5cf6",
  RequestVoteResponse: "#8b5cf6",
  AppendEntries: "#2563eb",
  AppendEntriesResponse: "#2563eb",
  InstallSnapshot: "#f59e0b",
  ClientRequest: "#8a94a6",
  ClientResponse: "#8a94a6",
};

const isClientKind = (k?: string) => k === "ClientRequest" || k === "ClientResponse";
const isResponse = (k?: string) => !!k && (k.endsWith("Response") || k === "ClientResponse");

type Pt = { x: number; y: number };

export function layout(trace: Trace): Map<number, Pt> {
  const pos = new Map<number, Pt>();
  const n = trace.servers.length;
  trace.servers.forEach((id, i) => {
    const a = -Math.PI / 2 + (2 * Math.PI * i) / n;
    pos.set(id, { x: CX + R * Math.cos(a), y: CY + R * Math.sin(a) });
  });
  const m = trace.clients.length;
  trace.clients.forEach((id, i) => {
    pos.set(id, { x: W / 2 + (i - (m - 1) / 2) * 70, y: H - 22 });
  });
  return pos;
}

// The configuration to draw: the current leader's view if there is one,
// otherwise the view of the server with the highest term.
function currentVoters(view: ClusterView): Set<number> | null {
  let best: RaftState | null = null;
  for (const nv of view.nodes.values()) {
    const s = nv.state;
    if (!nv.up || !isRaftState(s) || !s.voters) continue;
    if (!best || (s.role === "leader") > (best.role === "leader") || (s.role === best.role && s.term > best.term)) best = s;
  }
  return best ? new Set(best.voters) : null;
}

// offset moves a point on the segment a→b sideways, so a request and its
// response (opposite directions) travel on opposite sides of the link.
function along(a: Pt, b: Pt, p: number, side: number): Pt {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  return { x: a.x + dx * p + (-dy / len) * side, y: a.y + dy * p + (dx / len) * side };
}

// trim shortens a→b so it starts and ends at the node circles' edges.
function trim(a: Pt, b: Pt, ra: number, rb: number): [Pt, Pt] {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  return [
    { x: a.x + (dx / len) * ra, y: a.y + (dy / len) * ra },
    { x: b.x - (dx / len) * rb, y: b.y - (dy / len) * rb },
  ];
}

interface Props {
  trace: Trace;
  view: ClusterView;
  flights: InFlight[];
  drops: DropMark[];
  t: number;
  dropWindow: number;
  showClients: boolean;
  selected: { id: number; copy: number } | null;
  onSelect: (id: number, copy: number) => void;
}

export function Network({ trace, view, flights, drops, t, dropWindow, showClients, selected, onSelect }: Props) {
  const pos = layout(trace);
  const voters = currentVoters(view);
  const radius = (id: number) => (id <= trace.header.nodes ? NODE_R : 14);
  const blocked = new Set((view.partition?.links ?? []).map((l) => `${l.from}-${l.to}`));
  const visible = (k?: string) => showClients || !isClientKind(k);
  const height = showClients && trace.clients.length > 0 ? H : H - 60;

  const edges: ReactElement[] = [];
  const servers = trace.servers;
  for (let i = 0; i < servers.length; i++) {
    for (let j = i + 1; j < servers.length; j++) {
      const a = servers[i];
      const b = servers[j];
      const [p, q] = trim(pos.get(a)!, pos.get(b)!, NODE_R, NODE_R);
      edges.push(<line key={`e${a}-${b}`} x1={p.x} y1={p.y} x2={q.x} y2={q.y} stroke="var(--edge)" strokeWidth={1.5} />);
    }
  }
  // Each blocked direction gets its own red dashed arrow, so one-way
  // partitions are visible as such.
  const cuts = (view.partition?.links ?? []).map((l) => {
    const a = pos.get(l.from);
    const b = pos.get(l.to);
    if (!a || !b) return null;
    const [p, q] = trim(a, b, radius(l.from) + 4, radius(l.to) + 8);
    const s = along(p, q, 0, 5);
    const e = along(p, q, 1, 5);
    return (
      <line
        key={`cut${l.from}-${l.to}`}
        x1={s.x}
        y1={s.y}
        x2={e.x}
        y2={e.y}
        stroke="var(--danger)"
        strokeWidth={2}
        strokeDasharray="5 4"
        markerEnd="url(#cut-arrow)"
      >
        <title>{`${nodeName(trace, l.from)}→${nodeName(trace, l.to)} cut by the partition`}</title>
      </line>
    );
  });

  const dots = flights
    .filter((f) => visible(f.sent.rec.kind))
    .map((f) => {
      const r = f.sent.rec;
      const a = pos.get(r.from!);
      const b = pos.get(r.to!);
      if (!a || !b) return null;
      const [p, q] = trim(a, b, radius(r.from!), radius(r.to!));
      const at = along(p, q, f.progress, 6);
      const color = KIND_COLORS[r.kind ?? ""] ?? "#888";
      const entries = Array.isArray(r.msg?.entries) ? (r.msg!.entries as unknown[]).length : 0;
      const size = r.kind === "InstallSnapshot" ? 7 : entries > 0 ? 6 : isClientKind(r.kind) ? 4 : 5;
      const sel = selected?.id === f.sent.id && selected.copy === f.copy;
      const blockedLink = blocked.has(`${r.from}-${r.to}`);
      return (
        <g key={`m${f.sent.id}-${f.copy}`} onClick={() => onSelect(f.sent.id, f.copy)} style={{ cursor: "pointer" }}>
          <circle
            cx={at.x}
            cy={at.y}
            r={size}
            fill={isResponse(r.kind) ? "var(--panel)" : color}
            stroke={color}
            strokeWidth={isResponse(r.kind) ? 2.5 : 1}
            opacity={blockedLink ? 0.55 : 1}
          />
          {sel && <circle cx={at.x} cy={at.y} r={size + 5} fill="none" stroke="var(--text)" strokeWidth={1.5} />}
          <title>{`${nodeName(trace, r.from)}→${nodeName(trace, r.to)} ${r.kind}: ${summarizeMessage(r.kind, r.msg)}${blockedLink ? " (will be dropped: partition)" : ""}`}</title>
        </g>
      );
    });

  // Drops flash as a red cross where the copy died, fading over dropWindow:
  // near the sender if the network lost it, at the receiver if it arrived
  // at a cut link or a down node.
  const crosses = drops
    .filter((d) => visible(d.sent.rec.kind))
    .map((d) => {
      const r = d.sent.rec;
      const a = pos.get(r.from!);
      const b = pos.get(r.to!);
      if (!a || !b) return null;
      const [p, q] = trim(a, b, radius(r.from!), radius(r.to!));
      const at = along(p, q, d.arrival.reason === "lost" ? 0.25 : 0.88, 6);
      const age = Math.max(0, t - d.arrival.t) / dropWindow;
      const k = 5;
      return (
        <g key={`d${d.sent.id}-${d.arrival.frame}-${d.arrival.t}`} opacity={Math.max(0.15, 1 - age)} stroke="var(--danger)" strokeWidth={2.5}>
          <line x1={at.x - k} y1={at.y - k} x2={at.x + k} y2={at.y + k} />
          <line x1={at.x - k} y1={at.y + k} x2={at.x + k} y2={at.y - k} />
          <title>{`${r.kind} ${nodeName(trace, r.from)}→${nodeName(trace, r.to)} dropped: ${d.arrival.reason}`}</title>
        </g>
      );
    });

  const nodes = [...servers, ...(showClients ? trace.clients : [])].map((id) => {
    const p = pos.get(id)!;
    const nv = view.nodes.get(id);
    const name = nodeName(trace, id);
    if (id > trace.header.nodes) {
      return (
        <g key={`n${id}`}>
          <rect x={p.x - 14} y={p.y - 14} width={28} height={28} rx={6} fill="var(--panel)" stroke="var(--muted)" strokeWidth={1.5} />
          <text x={p.x} y={p.y + 4} textAnchor="middle" fontSize={11}>
            {name}
          </text>
        </g>
      );
    }
    const s = nv?.state;
    const raft = isRaftState(s) ? s : null;
    const up = nv?.up ?? false;
    const member = !voters || voters.has(id);
    const role = raft?.role ?? "follower";
    const fill = !up
      ? "var(--down)"
      : role === "leader"
        ? "var(--leader)"
        : role === "candidate"
          ? "var(--candidate)"
          : role === "pre-candidate"
            ? "var(--precandidate)"
            : "var(--follower)";
    return (
      <g key={`n${id}`} opacity={member ? 1 : 0.45}>
        <circle
          cx={p.x}
          cy={p.y}
          r={NODE_R}
          fill={fill}
          fillOpacity={up ? 0.9 : 0.35}
          stroke={up ? "none" : "var(--down)"}
          strokeWidth={2}
          strokeDasharray={up ? undefined : "4 3"}
        />
        {view.violation && view.violation.reason?.includes(`node ${id} `) && (
          <circle cx={p.x} cy={p.y} r={NODE_R + 6} fill="none" stroke="var(--danger)" strokeWidth={3} />
        )}
        <text x={p.x} y={p.y - 3} textAnchor="middle" fontSize={15} fontWeight={600} style={{ fill: "white" }}>
          {name}
        </text>
        <text x={p.x} y={p.y + 14} textAnchor="middle" fontSize={11.5} style={{ fill: "white" }}>
          {raft ? `term ${raft.term}` : s ? JSON.stringify(s).slice(0, 12) : ""}
        </text>
        <text x={p.x} y={p.y + NODE_R + 15} textAnchor="middle" fontSize={11.5} style={{ fill: "var(--muted)" }}>
          {!up ? `crashed${nv?.crash ? ` (${nv.crash.kind})` : ""}` : raft ? role + (member ? "" : " · not a voter") : ""}
        </text>
        {raft && up && role === "leader" && (
          <text x={p.x} y={p.y - NODE_R - 6} textAnchor="middle" fontSize={16} style={{ fill: "var(--leader)" }}>
            ★
          </text>
        )}
      </g>
    );
  });

  return (
    <div>
      <svg className="net" viewBox={`0 0 ${W} ${height}`} role="img" aria-label="Cluster network">
        <defs>
          <marker id="cut-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
            <path d="M0,0 L10,5 L0,10 z" fill="var(--danger)" />
          </marker>
        </defs>
        {edges}
        {cuts}
        {nodes}
        {dots}
        {crosses}
      </svg>
      <div className="legend">
        <span><i style={{ background: KIND_COLORS.PreVote }} />PreVote</span>
        <span><i style={{ background: KIND_COLORS.RequestVote }} />RequestVote</span>
        <span><i style={{ background: KIND_COLORS.AppendEntries }} />AppendEntries</span>
        <span><i style={{ background: KIND_COLORS.InstallSnapshot }} />InstallSnapshot</span>
        {showClients && <span><i style={{ background: KIND_COLORS.ClientRequest }} />client</span>}
        <span><i style={{ border: "2px solid var(--muted)", background: "transparent" }} />hollow: response</span>
        <span style={{ color: "var(--danger)" }}>✕ dropped</span>
        <span style={{ color: "var(--danger)" }}>- - → cut link</span>
      </div>
    </div>
  );
}
