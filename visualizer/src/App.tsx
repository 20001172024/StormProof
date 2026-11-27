import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { formatTime } from "./describe";
import { Details, Events, type Selection } from "./Events";
import { Network } from "./Network";
import { LogGrid, NodeCards } from "./Nodes";
import { MessageIndex, Replayer, lowerBound, markers as findMarkers } from "./replay";
import { Timeline } from "./Timeline";
import { frameAt, parseTrace, type Sent, type Trace } from "./trace";

interface Example {
  file: string;
  title: string;
  description: string;
}

// Playback speeds, in virtual microseconds per real second.
const SPEEDS = [
  { label: "0.01×", us: 10_000 },
  { label: "0.05×", us: 50_000 },
  { label: "0.2×", us: 200_000 },
  { label: "1×", us: 1_000_000 },
];

const EXAMPLE_NAME = /^[a-z0-9-]+\.jsonl$/;

async function readFile(file: File): Promise<string> {
  // CI uploads traces gzipped; read those directly.
  if (file.name.endsWith(".gz")) {
    const stream = file.stream().pipeThrough(new DecompressionStream("gzip"));
    return new Response(stream).text();
  }
  return file.text();
}

export default function App() {
  const [examples, setExamples] = useState<Example[]>([]);
  const [loaded, setLoaded] = useState<{ trace: Trace; name: string; example: boolean } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [dragging, setDragging] = useState(false);
  const initialStep = useRef<number | null>(null);
  const fileInput = useRef<HTMLInputElement | null>(null);

  const open = useCallback((text: string, name: string, example: boolean) => {
    try {
      const trace = parseTrace(text);
      setLoaded({ trace, name, example });
      setError(null);
    } catch (e) {
      setError(`${name}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }, []);

  const openExample = useCallback(
    async (file: string) => {
      if (!EXAMPLE_NAME.test(file)) return;
      setLoading(true);
      try {
        const res = await fetch(`traces/${file}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        open(await res.text(), file, true);
      } catch (e) {
        setError(`${file}: could not load (${e instanceof Error ? e.message : String(e)})`);
      } finally {
        setLoading(false);
      }
    },
    [open],
  );

  const openFile = useCallback(
    async (file: File) => {
      setLoading(true);
      try {
        open(await readFile(file), file.name, false);
      } catch (e) {
        setError(`${file.name}: could not read (${e instanceof Error ? e.message : String(e)})`);
      } finally {
        setLoading(false);
      }
    },
    [open],
  );

  useEffect(() => {
    fetch("traces/index.json")
      .then((r) => (r.ok ? r.json() : []))
      .then((x: Example[]) => setExamples(x))
      .catch(() => setExamples([]));
    const q = new URLSearchParams(location.search);
    const step = q.get("step");
    if (step !== null && /^\d+$/.test(step)) initialStep.current = Number(step);
    const t = q.get("trace");
    if (t) void openExample(t);
  }, [openExample]);

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const f = e.dataTransfer.files[0];
    if (f) void openFile(f);
  };

  return (
    <div
      className={`app ${dragging ? "dragging" : ""}`}
      onDragOver={(e) => {
        e.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={onDrop}
    >
      <header className="top">
        <h1>
          StormProof <span>visualizer</span>
        </h1>
        {examples.length > 0 && (
          <select
            aria-label="Example trace"
            value={loaded?.example ? loaded.name : ""}
            onChange={(e) => e.target.value && void openExample(e.target.value)}
          >
            <option value="">Example traces…</option>
            {examples.map((x) => (
              <option key={x.file} value={x.file}>
                {x.title}
              </option>
            ))}
          </select>
        )}
        <input
          ref={fileInput}
          type="file"
          accept=".jsonl,.json,.gz,.txt"
          hidden
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) void openFile(f);
            e.target.value = ""; // so choosing the same file again reloads it
          }}
        />
        <button type="button" onClick={() => fileInput.current?.click()}>
          Open trace file…
        </button>
        {loaded && (
          <span className="info">
            {loaded.name} · seed {loaded.trace.header.seed} · {loaded.trace.header.profile.name} · {loaded.trace.servers.length} servers
            {loaded.trace.clients.length > 0 ? `, ${loaded.trace.clients.length} clients` : ""} · {formatTime(loaded.trace.header.duration)}
          </span>
        )}
        <span className="grow" />
        {loading && <span className="info">loading…</span>}
      </header>
      {error && <div className="error" style={{ margin: "10px 16px" }}>{error}</div>}
      {loaded ? (
        <Player key={loaded.name + loaded.trace.records.length} trace={loaded.trace} name={loaded.name} example={loaded.example} initialStep={initialStep} />
      ) : (
        <Welcome examples={examples} onExample={(f) => void openExample(f)} />
      )}
    </div>
  );
}

function Welcome({ examples, onExample }: { examples: Example[]; onExample: (file: string) => void }) {
  return (
    <div className="empty">
      <h2>Step through a simulated Raft cluster</h2>
      <p>
        StormProof's simulator runs a whole Raft cluster in one process with a fake clock, network and disk, injects crashes,
        partitions and message loss, and records every event to a trace. Load a trace to scrub through it: elections, log
        replication, commits, and the faults that hit them.
      </p>
      {examples.length > 0 && (
        <>
          <h3>Examples</h3>
          <div className="examples">
            {examples.map((x) => (
              <button key={x.file} className="example" onClick={() => onExample(x.file)}>
                <b>{x.title}</b>
                <span>{x.description}</span>
              </button>
            ))}
          </div>
        </>
      )}
      <div className="drop-hint">
        Or drop a trace file here: a <code>.jsonl</code> (or gzipped <code>.jsonl.gz</code>), such as one from an unzipped CI
        failing-traces artifact. Make one with
        <br />
        <code>go run ./sim/cmd/stormproof-sim -seed 1 -trace run.jsonl</code>
      </div>
      <p className="muted">
        Keys: <kbd>Space</kbd> play/pause · <kbd>←</kbd> <kbd>→</kbd> step · <kbd>Shift</kbd>+<kbd>←</kbd> <kbd>→</kbd> previous/next
        marker · <kbd>Home</kbd> <kbd>End</kbd> · <kbd>V</kbd> jump to violation
      </p>
    </div>
  );
}

interface PlayerProps {
  trace: Trace;
  name: string;
  example: boolean;
  initialStep: React.MutableRefObject<number | null>;
}

function Player({ trace, name, example, initialStep }: PlayerProps) {
  const replayer = useMemo(() => new Replayer(trace), [trace]);
  const msgs = useMemo(() => new MessageIndex(trace), [trace]);
  const marks = useMemo(() => findMarkers(trace), [trace]);
  const sendByID = useMemo(() => new Map<number, Sent>(trace.sends.map((s) => [s.id, s])), [trace]);
  const last = trace.frames.length - 1;
  const endT = trace.frames[last]?.t ?? 0;

  const [pos, setPos] = useState(() => {
    let f = 0;
    if (initialStep.current !== null) {
      const want = initialStep.current;
      f = Math.max(0, trace.frames.findIndex((x) => x.step >= want));
      initialStep.current = null;
    }
    return { frame: f, t: trace.frames[f]?.t ?? 0 };
  });
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [showClients, setShowClients] = useState(false);
  const [showDeliveries, setShowDeliveries] = useState(false);
  const [sel, setSel] = useState<Selection>(null);

  const seek = useCallback(
    (frame: number) => {
      const f = Math.max(0, Math.min(last, frame));
      setPos({ frame: f, t: trace.frames[f]?.t ?? 0 });
    },
    [last, trace],
  );

  // Playback advances virtual time smoothly, so messages glide between
  // frames instead of jumping.
  useEffect(() => {
    if (!playing) return;
    let raf = 0;
    let prev = performance.now();
    const step = (now: number) => {
      const dt = (now - prev) / 1000;
      prev = now;
      let stop = false;
      setPos((p) => {
        const t = p.t + dt * SPEEDS[speed].us;
        if (t >= endT) {
          stop = true;
          return { frame: last, t: endT };
        }
        return { frame: frameAt(trace, t), t };
      });
      if (stop) setPlaying(false);
      else raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [playing, speed, trace, endT, last]);

  const nextMarker = useCallback(
    (dir: 1 | -1) => {
      const ms = dir > 0 ? marks.filter((m) => m.frame > pos.frame) : marks.filter((m) => m.frame < pos.frame).reverse();
      if (ms[0]) seek(ms[0].frame);
    },
    [marks, pos.frame, seek],
  );
  const toViolation = useCallback(() => {
    if (trace.violation) {
      setPlaying(false);
      seek(trace.violation.frame);
      const i = trace.frames[trace.violation.frame].end - 1;
      setSel({ kind: "record", index: i });
    }
  }, [trace, seek]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      // Leave keys to the controls that use them: selects and the scrubber
      // (which steps by itself), and Space/Enter on a focused button.
      const el = e.target as HTMLElement;
      if (el.tagName === "SELECT" || (el.tagName === "INPUT" && (el as HTMLInputElement).type !== "checkbox")) return;
      if (el.tagName === "BUTTON" && (e.key === " " || e.key === "Enter")) return;
      if (e.key === " ") {
        e.preventDefault();
        setPlaying((p) => !p && pos.frame < last);
      } else if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
        e.preventDefault();
        setPlaying(false);
        const dir = e.key === "ArrowRight" ? 1 : -1;
        if (e.shiftKey) nextMarker(dir);
        else seek(pos.frame + dir);
      } else if (e.key === "Home") {
        seek(0);
      } else if (e.key === "End") {
        seek(last);
      } else if (e.key === "v" || e.key === "V") {
        toViolation();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [pos.frame, last, seek, nextMarker, toViolation]);

  // Keep the URL pointing at this trace and step while paused, so a moment
  // can be shared as a link.
  useEffect(() => {
    if (playing) return;
    const q = new URLSearchParams(location.search);
    if (example) q.set("trace", name);
    else q.delete("trace");
    q.set("step", String(trace.frames[pos.frame]?.step ?? 0));
    history.replaceState(null, "", `?${q.toString()}`);
  }, [playing, pos.frame, example, name, trace]);

  const view = useMemo(() => replayer.at(pos.frame), [replayer, pos.frame]);
  const dropWindow = Math.max(10_000, 2 * (trace.header.profile.net?.max_delay ?? 5_000));
  const flights = msgs.inFlight(pos.frame, pos.t);
  const drops = msgs.recentDrops(pos.frame, pos.t, dropWindow);
  const frame = trace.frames[pos.frame];
  const v = trace.violation;
  const selMsg = sel?.kind === "message" ? { id: sel.id, copy: sel.copy } : null;

  return (
    <>
      {v && (
        <div className="banner">
          <span>
            <b>Invariant violated: {v.rec.kind}</b> — {v.rec.reason} <span>(at {formatTime(v.rec.t)}, step {v.rec.step})</span>
          </span>
          <button className="danger" onClick={toViolation}>
            Jump to violation
          </button>
        </div>
      )}
      <main className="body">
        <div className="col">
          <section className="panel">
            <h2>
              Cluster
              <span className="grow" />
              {view.partition && <span style={{ color: "var(--warn)", textTransform: "none" }}>partition: {view.partition.kind}</span>}
              {view.quiet && <span style={{ color: "var(--quiet)", textTransform: "none" }}>quiet period</span>}
              <label>
                <input type="checkbox" checked={showClients} onChange={(e) => setShowClients(e.target.checked)} /> clients
              </label>
            </h2>
            <Network
              trace={trace}
              view={view}
              flights={flights}
              drops={drops}
              t={pos.t}
              dropWindow={dropWindow}
              showClients={showClients}
              selected={selMsg}
              onSelect={(id, copy) => {
                setPlaying(false);
                setSel({ kind: "message", id, copy });
              }}
            />
          </section>
          <section className="panel">
            <h2>Log tails (cell = entry, labelled with its term)</h2>
            <LogGrid trace={trace} view={view} />
          </section>
        </div>
        <div className="col">
          <section className="panel">
            <h2>Servers</h2>
            <NodeCards trace={trace} view={view} />
          </section>
          <section className="panel">
            <h2>Selected</h2>
            <Details trace={trace} sel={sel} sendByID={sendByID} />
          </section>
          <section className="panel">
            <h2>
              Events
              <span className="grow" />
              <label>
                <input type="checkbox" checked={showDeliveries} onChange={(e) => setShowDeliveries(e.target.checked)} /> deliveries
              </label>
            </h2>
            <Events
              trace={trace}
              frame={pos.frame}
              showClients={showClients}
              showDeliveries={showDeliveries}
              onPick={(r, i) => {
                setPlaying(false);
                seek(lowerBound(trace.frames.length, (k) => trace.frames[k].end > i));
                setSel(r.id !== undefined ? { kind: "message", id: r.id, copy: 0 } : { kind: "record", index: i });
              }}
            />
          </section>
        </div>
      </main>
      <footer className="timeline">
        <div className="controls">
          <button onClick={() => seek(0)} title="Start (Home)" aria-label="Start">
            ⏮
          </button>
          <button onClick={() => nextMarker(-1)} title="Previous marker (Shift+←)" aria-label="Previous marker">
            ⇤
          </button>
          <button onClick={() => seek(pos.frame - 1)} title="Previous step (←)" aria-label="Previous step">
            ◀
          </button>
          <button className="primary" onClick={() => setPlaying((p) => !p && pos.frame < last)} title="Play/pause (Space)" style={{ minWidth: 70 }}>
            {playing ? "⏸ Pause" : "▶ Play"}
          </button>
          <button onClick={() => seek(pos.frame + 1)} title="Next step (→)" aria-label="Next step">
            ▶
          </button>
          <button onClick={() => nextMarker(1)} title="Next marker (Shift+→)" aria-label="Next marker">
            ⇥
          </button>
          <button onClick={() => seek(last)} title="End (End)" aria-label="End">
            ⏭
          </button>
          <select value={speed} onChange={(e) => setSpeed(Number(e.target.value))} aria-label="Playback speed" title="Virtual time per real second">
            {SPEEDS.map((s, i) => (
              <option key={s.label} value={i}>
                {s.label}
              </option>
            ))}
          </select>
          <span className="pos">
            t={formatTime(pos.t)} · step {frame?.step ?? 0} · {pos.frame + 1}/{trace.frames.length}
          </span>
          <span className="grow" />
          {v && (
            <button className="danger" onClick={toViolation} title="Jump to violation (V)">
              Jump to violation
            </button>
          )}
        </div>
        <Timeline trace={trace} markers={marks} frame={pos.frame} onSeek={(f) => (setPlaying(false), seek(f))} />
      </footer>
    </>
  );
}
