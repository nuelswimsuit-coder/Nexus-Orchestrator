"use client";

/**
 * DeployTerminal — Phase 19: Nexus-Push Sync UI
 *
 * Terminal-style panel with a [🚀 SYNC CLUSTER] button.
 * Shows live per-file upload progress and self-healing output:
 *
 *   [04:12:29] [NEXUS-PUSH]      Triggered — yadmin@10.100.102.20
 *   [04:12:30] [CONNECTING]      SSH → yadmin@10.100.102.20
 *   [04:12:31] [UPLOADING]       [1/312] nexus/__init__.py  (0%)
 *   [04:12:38] [UPLOADING]       [156/312] nexus/worker/tasks/auto_scrape.py  (50%)
 *   [04:12:45] [UPLOADING]       312 files synced
 *   [04:12:45] [INSTALLING DEPS] pip install -r requirements.txt
 *   [04:13:10] [RESTARTING]      start_worker.py launched in background
 *   [04:13:10] [DONE]            Worker Live ✓
 *
 * The SSE stream is opened BEFORE the API call so no events are missed.
 * Shares deploy phase with the Header button via NexusContext.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { API_BASE, apiSseBase, getDeployStatus, triggerSync } from "@/lib/api";
import { useNexus } from "@/lib/nexus-context";
import type { DeployPhase } from "@/lib/nexus-context";
import type { DeployProgressEvent } from "@/lib/api";

// ── Step → display config ─────────────────────────────────────────────────────

const STEP_CFG: Record<string, { tag: string; color: string }> = {
  connecting:      { tag: "CONNECTING",      color: "#60a5fa" },
  stopping_worker: { tag: "STOPPING",        color: "#f59e0b" },
  uploading:       { tag: "UPLOADING",       color: "#a78bfa" },
  installing_deps: { tag: "INSTALLING DEPS", color: "#34d399" },
  bootstrapping:   { tag: "INSTALLING DEPS", color: "#34d399" },
  restarting:      { tag: "RESTARTING",      color: "#f472b6" },
  skipped:         { tag: "SKIPPED",         color: "#94a3b8" },
  done:            { tag: "DONE",            color: "#22c55e" },
  error:           { tag: "ERROR",           color: "#ef4444" },
};

const NEXUS_PUSH_SYNC_NODE_IDS = ["worker_linux", "worker_windows"] as const;

function deployEventTerminal(ev: DeployProgressEvent): boolean {
  return (
    ev.status === "error" ||
    ev.step === "error" ||
    (ev.step === "done" && ev.status === "done") ||
    (ev.step === "skipped" && ev.status === "done")
  );
}

function deployEventOk(ev: DeployProgressEvent): boolean {
  return ev.step === "done" && ev.status === "done";
}

// ── Upload progress extraction ────────────────────────────────────────────────

interface UploadProgress {
  done: number;
  total: number;
  file: string;
}

function parseUploadProgress(detail: string): UploadProgress | null {
  // Matches "[156/312] nexus/worker/tasks/auto_scrape.py  (50%)"
  const m = detail.match(/^\[(\d+)\/(\d+)\]\s+(.+?)\s+\(\d+%\)$/);
  if (!m) return null;
  return { done: parseInt(m[1], 10), total: parseInt(m[2], 10), file: m[3] };
}

// ── Log line ──────────────────────────────────────────────────────────────────

interface LogLine {
  id: number;
  ts: string;
  tag: string;
  color: string;
  text: string;
  upload?: UploadProgress;
}

let _lineId = 0;
function nowHMS() {
  return new Date().toLocaleTimeString("en-GB", { hour12: false });
}

function makeLine(tag: string, color: string, text: string): LogLine {
  return { id: ++_lineId, ts: nowHMS(), tag, color, text,
           upload: parseUploadProgress(text) ?? undefined };
}

function evToLine(ev: DeployProgressEvent): LogLine {
  const cfg = STEP_CFG[ev.step] ?? { tag: ev.step.toUpperCase(), color: "#94a3b8" };
  const text = ev.detail || (ev as DeployProgressEvent & { label?: string }).label || cfg.tag;
  return makeLine(cfg.tag, cfg.color, text);
}

// ── Upload progress bar ───────────────────────────────────────────────────────

function UploadBar({ done, total, file }: UploadProgress) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  return (
    <div style={{ marginTop: "2px", marginBottom: "2px" }}>
      <div style={{ display: "flex", justifyContent: "space-between",
                    fontFamily: "var(--font-mono)", fontSize: "0.6rem",
                    color: "#475569", marginBottom: "2px" }}>
        <span style={{ overflow: "hidden", textOverflow: "ellipsis",
                       whiteSpace: "nowrap", maxWidth: "70%" }}>
          {file}
        </span>
        <span>{done}/{total} ({pct}%)</span>
      </div>
      <div style={{ height: "3px", background: "#1e293b", borderRadius: "2px",
                    overflow: "hidden" }}>
        <div style={{
          height: "100%", width: `${pct}%`, borderRadius: "2px",
          background: "linear-gradient(90deg, #7c3aed, #a855f7)",
          transition: "width 0.3s ease",
          boxShadow: "0 0 6px #a855f766",
        }} />
      </div>
    </div>
  );
}

// ── DeployTerminal ────────────────────────────────────────────────────────────

export default function DeployTerminal() {
  const { setDeployingNode, setDeployPhase } = useNexus();

  const [phaseLocal, setPhaseLocal] = useState<DeployPhase>("idle");
  const [lines, setLines]           = useState<LogLine[]>([]);
  const [uploadProg, setUploadProg] = useState<UploadProgress | null>(null);
  const [errMsg, setErrMsg]         = useState("");

  const termRef  = useRef<HTMLDivElement>(null);
  const streamsRef = useRef<Map<string, EventSource>>(new Map());
  const phaseRef = useRef<DeployPhase>("idle");
  const statusPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const deploySettledRef = useRef(false);

  // Keep context + local ref in sync
  const setPhase = useCallback((p: DeployPhase) => {
    setPhaseLocal(p);
    setDeployPhase(p);
    phaseRef.current = p;
  }, [setDeployPhase]);

  const addLine = useCallback((line: LogLine) => {
    setLines(prev => [...prev, line]);
    if (line.upload) setUploadProg(line.upload);
  }, []);

  // Auto-scroll
  useEffect(() => {
    if (termRef.current) termRef.current.scrollTop = termRef.current.scrollHeight;
  }, [lines]);

  // Cleanup on unmount
  useEffect(
    () => () => {
      streamsRef.current.forEach(es => es.close());
      streamsRef.current.clear();
      if (statusPollRef.current) clearInterval(statusPollRef.current);
    },
    [],
  );

  const openDeployStream = useCallback(
    (nodeId: string) => {
      if (streamsRef.current.has(nodeId)) return;
      setDeployingNode(nodeId, true);
      const es = new EventSource(
        `${apiSseBase()}/api/deploy/progress/${encodeURIComponent(nodeId)}`,
      );
      streamsRef.current.set(nodeId, es);

      es.onmessage = (e) => {
        try {
          const ev = JSON.parse(e.data) as DeployProgressEvent;
          addLine(evToLine(ev));

          const failed = ev.status === "error" || ev.step === "error";
          const succeeded = ev.step === "done" && ev.status === "done";
          const skipped = ev.step === "skipped" && ev.status === "done";
          if (failed || succeeded || skipped) {
            es.close();
            streamsRef.current.delete(nodeId);
            setUploadProg(null);
            setTimeout(() => setDeployingNode(nodeId, false), 4000);
          }
        } catch {}
      };

      es.onerror = () => {
        if (phaseRef.current === "running") {
          es.close();
          streamsRef.current.delete(nodeId);
          setDeployingNode(nodeId, false);
        }
      };
    },
    [addLine, setDeployingNode],
  );

  const handleSync = useCallback(async () => {
    if (phaseRef.current === "running") return;

    if (statusPollRef.current) {
      clearInterval(statusPollRef.current);
      statusPollRef.current = null;
    }

    // Reset state
    setPhase("running");
    setLines([]);
    setUploadProg(null);
    setErrMsg("");
    deploySettledRef.current = false;

    streamsRef.current.forEach(es => es.close());
    streamsRef.current.clear();

    addLine(
      makeLine(
        "NEXUS-PUSH",
        "#a855f7",
        `Triggered — ${API_BASE.replace("http://", "")} (worker_linux + worker_windows)`,
      ),
    );
    addLine(
      makeLine("API", "#475569", "POST /api/deploy/sync (timeout 30s)…"),
    );

    try {
      const res = await triggerSync();
      addLine(makeLine("JOB", "#475569", res.job_id));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setErrMsg(msg);
      setPhase("error");
      addLine(makeLine("ERROR", "#ef4444", msg));
      streamsRef.current.forEach(es => es.close());
      streamsRef.current.clear();
      return;
    }

    openDeployStream("worker_linux");
    setTimeout(() => openDeployStream("worker_windows"), 400);

    let pollCount = 0;
    statusPollRef.current = setInterval(async () => {
        pollCount++;
        try {
          const st = await getDeployStatus();
          const { nodes } = st;
          for (const id of NEXUS_PUSH_SYNC_NODE_IDS) {
            const ev = nodes[id];
            if (!ev || !deployEventTerminal(ev)) return;
          }
          if (deploySettledRef.current) return;
          if (phaseRef.current !== "running") return;
          deploySettledRef.current = true;
          if (statusPollRef.current) {
            clearInterval(statusPollRef.current);
            statusPollRef.current = null;
          }
          streamsRef.current.forEach(es => es.close());
          streamsRef.current.clear();
          setUploadProg(null);
          for (const id of NEXUS_PUSH_SYNC_NODE_IDS) {
            setDeployingNode(id, false);
          }
          const anyOk = NEXUS_PUSH_SYNC_NODE_IDS.some(
            id => nodes[id] && deployEventOk(nodes[id]!),
          );
          if (anyOk) {
            const failed = NEXUS_PUSH_SYNC_NODE_IDS.filter(
              id => nodes[id] && !deployEventOk(nodes[id]!),
            );
            const hint =
              failed.length > 0
                ? ` — ${failed.join(", ")} failed or skipped`
                : "";
            addLine(
              makeLine(
                "DONE",
                "#22c55e",
                `Deployment complete (at least one worker) ✓${hint}`,
              ),
            );
            setPhase("done");
          } else {
            const parts = NEXUS_PUSH_SYNC_NODE_IDS.map(id => {
              const ev = nodes[id];
              return ev ? `${id}: ${ev.detail || "failed"}` : `${id}: (no status)`;
            });
            addLine(makeLine("ERROR", "#ef4444", parts.join(" | ")));
            setPhase("error");
          }
        } catch {
          /* ignore */
        }
        if (pollCount >= 360 && statusPollRef.current) {
          clearInterval(statusPollRef.current);
          statusPollRef.current = null;
          if (phaseRef.current === "running" && !deploySettledRef.current) {
            deploySettledRef.current = true;
            streamsRef.current.forEach(es => es.close());
            streamsRef.current.clear();
            setUploadProg(null);
            for (const id of NEXUS_PUSH_SYNC_NODE_IDS) {
              setDeployingNode(id, false);
            }
            addLine(
              makeLine(
                "ERROR",
                "#ef4444",
                "Deploy status poll timed out (~12 min) — check server logs and worker connectivity.",
              ),
            );
            setPhase("error");
          }
        }
      }, 2_000);
  }, [addLine, openDeployStream, setPhase]);

  const isRunning = phaseLocal === "running";
  const isDone    = phaseLocal === "done";
  const isError   = phaseLocal === "error";

  const borderColor = isRunning ? "#a855f755" : isDone ? "#22c55e33"
                    : isError   ? "#ef444433" : "#1e293b";

  return (
    <div
      data-deploy-terminal
      style={{
        background: "linear-gradient(160deg, #080d18 0%, #050912 100%)",
        border: `1px solid ${borderColor}`,
        borderRadius: "12px",
        overflow: "hidden",
        boxShadow: isRunning ? "0 0 28px #a855f714" : "none",
        transition: "border-color 0.3s, box-shadow 0.3s",
      }}
    >

      {/* ── Title bar ── */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0.6rem 1rem",
        background: "rgba(255,255,255,0.02)",
        borderBottom: "1px solid #0f172a",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.45rem" }}>
          {/* Traffic-light dots */}
          {(["#ef4444", "#f59e0b", "#22c55e"] as const).map((c, i) => (
            <span key={i} style={{
              width: 10, height: 10, borderRadius: "50%",
              display: "inline-block",
              background: isRunning && i === 2 ? c : "#1e293b",
              boxShadow: isRunning && i === 2 ? `0 0 6px ${c}` : "none",
              animation: isRunning && i === 2 ? "dp-pulse 1s infinite" : "none",
            }} />
          ))}
          <span style={{
            fontFamily: "var(--font-mono)", fontSize: "0.65rem", fontWeight: 700,
            color: "#475569", letterSpacing: "0.1em", marginLeft: "0.2rem",
          }}>
            nexus-push — deploy terminal
          </span>
          {isRunning && (
            <span style={{
              fontFamily: "var(--font-mono)", fontSize: "0.58rem",
              color: "#a855f7", marginLeft: "0.3rem",
              animation: "dp-fade 1.2s ease-in-out infinite",
            }}>
              ● SYNCING
            </span>
          )}
          {isDone && (
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.58rem",
                           color: "#22c55e", marginLeft: "0.3rem" }}>
              ✓ DONE
            </span>
          )}
          {isError && (
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.58rem",
                           color: "#ef4444", marginLeft: "0.3rem" }}>
              ✗ ERROR
            </span>
          )}
        </div>

        {/* Target info */}
        <span style={{
          fontFamily: "var(--font-mono)", fontSize: "0.55rem", color: "#334155",
        }}>
          worker_linux
        </span>
      </div>

      {/* ── Upload progress bar (visible while uploading) ── */}
      {isRunning && uploadProg && (
        <div style={{ padding: "0.4rem 1rem 0", borderBottom: "1px solid #0a0f1e" }}>
          <UploadBar {...uploadProg} />
        </div>
      )}

      {/* ── Terminal log ── */}
      <div
        ref={termRef}
        style={{
          minHeight: "200px",
          maxHeight: "340px",
          overflowY: "auto",
          padding: "0.7rem 1rem",
          fontFamily: "var(--font-mono)",
          fontSize: "0.71rem",
          lineHeight: "1.65",
          background: "#020617",
        }}
      >
        {lines.length === 0 ? (
          <span style={{ color: "#1e293b", userSelect: "none" }}>
            {"> Ready. Click [🚀 SYNC CLUSTER] to push code to the Linux worker."}
          </span>
        ) : (
          lines.map(ln => {
            // Skip intermediate upload lines — only show every 20th or the last one
            // to avoid flooding the terminal with 300 file lines
            if (ln.upload && ln.upload.done % 20 !== 0 &&
                ln.upload.done !== ln.upload.total) {
              return null;
            }
            return (
              <div key={ln.id} style={{ display: "flex", gap: "0.55rem",
                                        alignItems: "baseline" }}>
                <span style={{ color: "#1e3a5f", flexShrink: 0,
                               fontSize: "0.63rem" }}>
                  [{ln.ts}]
                </span>
                <span style={{
                  color: ln.color, fontWeight: 700, flexShrink: 0,
                  minWidth: "136px",
                  textShadow: `0 0 8px ${ln.color}44`,
                }}>
                  [{ln.tag}]
                </span>
                <span style={{ color: "#64748b", wordBreak: "break-all",
                               overflow: "hidden", textOverflow: "ellipsis",
                               whiteSpace: "nowrap", maxWidth: "520px" }}>
                  {ln.upload
                    ? `${ln.upload.file}  (${ln.upload.done}/${ln.upload.total})`
                    : ln.text}
                </span>
              </div>
            );
          })
        )}

        {/* Blinking cursor */}
        {isRunning && (
          <div style={{ display: "flex", gap: "0.55rem", alignItems: "baseline",
                        marginTop: "2px" }}>
            <span style={{ color: "#1e3a5f", fontSize: "0.63rem" }}>
              [{nowHMS()}]
            </span>
            <span style={{ color: "#a855f7", fontWeight: 700,
                           animation: "dp-blink 1s step-end infinite" }}>
              ▋
            </span>
          </div>
        )}
      </div>

      {/* ── Action bar ── */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        padding: "0.6rem 1rem",
        borderTop: "1px solid #0a0f1e",
        background: "rgba(255,255,255,0.012)",
      }}>
        {/* Status */}
        <span style={{
          fontFamily: "var(--font-mono)", fontSize: "0.62rem",
          color: isError ? "#ef4444" : isDone ? "#22c55e"
               : isRunning ? "#a855f7" : "#334155",
          animation: isRunning ? "dp-fade 1.4s ease-in-out infinite" : "none",
        }}>
          {isRunning ? "● Syncing to worker_linux…"
           : isDone  ? "✓ Worker Live — deployment complete"
           : isError ? `✗ ${errMsg || "Deployment failed"}`
           : "○ Idle — ready to deploy"}
        </span>

        {/* Button */}
        <button
          onClick={handleSync}
          disabled={isRunning}
          style={{
            display: "flex", alignItems: "center", gap: "0.45rem",
            padding: "6px 18px", borderRadius: "7px",
            border: `1px solid ${
              isRunning ? "#a855f766" : isDone ? "#22c55e55"
            : isError  ? "#ef444455" : "#a855f788"}`,
            background: isRunning ? "rgba(168,85,247,0.14)"
                       : isDone   ? "rgba(34,197,94,0.09)"
                       : isError  ? "rgba(239,68,68,0.09)"
                       : "rgba(168,85,247,0.12)",
            cursor: isRunning ? "not-allowed" : "pointer",
            opacity: isRunning ? 0.7 : 1,
            transition: "all 0.2s",
            boxShadow: !isRunning && !isDone && !isError
              ? "0 0 16px #a855f722" : "none",
          }}
        >
          <span style={{ fontSize: "0.9rem", lineHeight: 1 }}>
            {isRunning ? "⚙️" : isDone ? "✅" : isError ? "🔄" : "🚀"}
          </span>
          <span style={{
            fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700,
            letterSpacing: "0.06em",
            color: isRunning ? "#c084fc" : isDone ? "#22c55e"
                 : isError   ? "#ef4444" : "#e879f9",
            textShadow: !isRunning && !isDone && !isError
              ? "0 0 10px #e879f966" : "none",
          }}>
            {isRunning ? "SYNCING…"
             : isDone  ? "WORKER LIVE"
             : isError ? "RETRY SYNC"
             : "🚀 SYNC CLUSTER"}
          </span>
          {isRunning && (
            <span style={{
              width: 6, height: 6, borderRadius: "50%", background: "#a855f7",
              display: "inline-block", flexShrink: 0,
              boxShadow: "0 0 7px #a855f7",
              animation: "dp-pulse 0.8s infinite",
            }} />
          )}
        </button>
      </div>

      <style>{`
        @keyframes dp-blink {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0; }
        }
        @keyframes dp-fade {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.4; }
        }
        @keyframes dp-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </div>
  );
}
