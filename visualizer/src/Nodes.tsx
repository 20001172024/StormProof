// Per-server views: a card with each server's Raft state, and a grid of
// the log tails side by side, so diverging logs are easy to spot.

import { nodeName } from "./describe";
import { isRaftState, termAt, termColor, type RaftState } from "./raft";
import type { ClusterView, NodeView } from "./replay";
import type { Trace } from "./trace";

function raftOf(nv: NodeView | undefined): RaftState | null {
  return nv && isRaftState(nv.state) ? nv.state : null;
}

// LogBar draws a whole log to scale (the same scale for every server, so
// lengths compare): compacted entries hatched, then one block per run of a
// term, with ticks for the commit and applied indexes.
function LogBar({ s, maxIndex }: { s: RaftState; maxIndex: number }) {
  const W = 200;
  const x = (i: number) => (maxIndex === 0 ? 0 : (i / maxIndex) * W);
  const snap = s.snap_index ?? 0;
  return (
    <svg className="logbar" viewBox={`0 0 ${W} 14`} preserveAspectRatio="none" aria-label="log">
      <rect x={0} y={2} width={W} height={10} fill="var(--bg)" />
      {snap > 0 && <rect x={0} y={2} width={x(snap)} height={10} fill="var(--edge)" />}
      {(s.log ?? []).map((r) => (
        <rect key={r.from} x={x(r.from - 1)} y={2} width={Math.max(0.8, x(r.count))} height={10} fill={termColor(r.term)}>
          <title>{`entries ${r.from}-${r.from + r.count - 1}: term ${r.term}`}</title>
        </rect>
      ))}
      <rect x={x(s.commit) - 0.75} y={0} width={1.5} height={14} fill="var(--text)">
        <title>{`commit index ${s.commit}`}</title>
      </rect>
    </svg>
  );
}

export function NodeCards({ trace, view }: { trace: Trace; view: ClusterView }) {
  const states = trace.servers.map((id) => raftOf(view.nodes.get(id)));
  const maxIndex = Math.max(1, ...states.map((s) => s?.last_index ?? 0));
  return (
    <div className="cards">
      {trace.servers.map((id, i) => {
        const nv = view.nodes.get(id);
        const s = states[i];
        const up = nv?.up ?? false;
        if (!s) {
          return (
            <div key={id} className={`card ${up ? "follower" : "down"}`}>
              <div className="name">{nodeName(trace, id)}</div>
              <pre className="mono">{JSON.stringify(nv?.state ?? null)}</pre>
            </div>
          );
        }
        const voter = !s.voters || s.voters.includes(id);
        return (
          <div key={id} className={`card ${up ? s.role : "down"} ${voter ? "" : "nonvoter"}`}>
            <div className="name">
              {nodeName(trace, id)}
              <span className="role">
                {up ? s.role : `crashed (${nv?.crash?.kind ?? "?"})`}
                {voter ? "" : " · not a voter"}
              </span>
            </div>
            <dl>
              <dt>term</dt>
              <dd>
                {s.term} <span className="muted">vote {s.vote ? nodeName(trace, s.vote) : "–"}</span>
              </dd>
              <dt>leader</dt>
              <dd>{s.leader ? nodeName(trace, s.leader) : "–"}</dd>
              <dt>log</dt>
              <dd>
                last {s.last_index}/{s.last_term}
              </dd>
              <dt>commit</dt>
              <dd>
                {s.commit} <span className="muted">applied {s.applied}</span>
              </dd>
              {(s.snap_index ?? 0) > 0 && (
                <>
                  <dt>snapshot</dt>
                  <dd>
                    {s.snap_index}/{s.snap_term}
                  </dd>
                </>
              )}
              <dt>config</dt>
              <dd>{(s.voters ?? []).map((v) => nodeName(trace, v)).join(" ")}</dd>
            </dl>
            <LogBar s={s} maxIndex={maxIndex} />
          </div>
        );
      })}
    </div>
  );
}

// LogGrid shows the last `width` log indexes of every server, one row per
// server: each cell is an entry colored and labelled by its term. Dashed
// cells are not yet committed (as far as that server knows); hatched ones
// are compacted into a snapshot.
export function LogGrid({ trace, view, width = 18 }: { trace: Trace; view: ClusterView; width?: number }) {
  const rows = trace.servers.map((id) => ({ id, nv: view.nodes.get(id), s: raftOf(view.nodes.get(id)) }));
  if (rows.every((r) => !r.s)) return null;
  const last = Math.max(0, ...rows.map((r) => r.s?.last_index ?? 0));
  const first = Math.max(1, last - width + 1);
  const idx: number[] = [];
  for (let i = first; i <= last; i++) idx.push(i);
  return (
    <div className="loggrid">
      <table>
        <thead>
          <tr>
            <th />
            {idx.map((i) => (
              <th key={i}>{i}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map(({ id, nv, s }) => (
            <tr key={id} style={{ opacity: nv?.up ? 1 : 0.5 }}>
              <th className="row">{nodeName(trace, id)}</th>
              {idx.map((i) => {
                if (!s || i > s.last_index) return <td key={i} className="empty" />;
                if (i <= (s.snap_index ?? 0)) {
                  return (
                    <td key={i} className="snap" title={`entry ${i}: in the snapshot`}>
                      ·
                    </td>
                  );
                }
                const term = termAt(s, i);
                return (
                  <td
                    key={i}
                    className={i > s.commit ? "uncommitted" : ""}
                    style={{ background: term === undefined ? "var(--edge)" : termColor(term) }}
                    title={`entry ${i}: term ${term ?? "?"}${i > s.commit ? ", not committed" : ", committed"}`}
                  >
                    {term ?? "?"}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {last === 0 ? <div className="note">No log entries yet.</div> : null}
    </div>
  );
}
