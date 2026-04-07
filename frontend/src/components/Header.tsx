"use client";

import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { usePathname } from "next/navigation";
import useSWR from "swr";
import {
  swrFetcher,
  API_BASE,
  openDeployProgressStream,
  getDeployStatus,
  triggerPanic,
  triggerSync,
} from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { useNexus } from "@/lib/nexus-context";
import { LanguageToggle, useI18n, type TranslationKey } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import type { DeployProgressEvent } from "@/lib/api";
import type { DeployPhase } from "@/lib/nexus-context";
import type {
  ChatOpsStatusResponse,
  ClusterStatusResponse,
  HitlPendingResponse,
  PanicStateResponse,
} from "@/lib/api";

/** POST /api/deploy/sync — wait for both legs before settling the modal. */
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

// ─────────────────────────────────────────────────────────────────────────────
// ChatOps status dot
// ─────────────────────────────────────────────────────────────────────────────

function ChatOpsDot({
  name,
  connected,
  detail,
  stealth,
}: {
  name: string;
  connected: boolean;
  detail: string;
  stealth: boolean;
}) {
  const c = connected ? "#22c55e" : "#ef4444";
  const icon = name === "whatsapp" ? "💬" : "✈️";

  return (
    <span
      title={`${name.toUpperCase()}: ${detail}`}
      className="flex items-center gap-1 cursor-default"
      style={{ opacity: stealth ? 0.3 : 1 }}
    >
      <span style={{ fontSize: "0.7rem" }}>{icon}</span>
      <span
        className="rounded-full"
        style={{
          width: 6,
          height: 6,
          background: stealth ? "#334155" : c,
          display: "inline-block",
          boxShadow: stealth ? "none" : `0 0 5px ${c}`,
          animation: connected && !stealth ? "rgb-pulse 2s infinite" : "none",
        }}
      />
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Header
// ─────────────────────────────────────────────────────────────────────────────

// ── System health breadcrumb ──────────────────────────────────────────────────
function SystemHealthBreadcrumb({ stealth }: { stealth: boolean }) {
  const { data: cluster } = useSWR<ClusterStatusResponse>(
    "/api/cluster/status", swrFetcher<ClusterStatusResponse>, { refreshInterval: 10_000 }
  );
  const { data: hitl } = useSWR<HitlPendingResponse>(
    "/api/hitl/pending", swrFetcher<HitlPendingResponse>, { refreshInterval: 4_000 }
  );

  // #region agent log
  fetch("http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Debug-Session-Id": "fd2053" },
    body: JSON.stringify({
      sessionId: "fd2053",
      location: "Header.tsx:SystemHealthBreadcrumb",
      message: "cluster shape before derive",
      data: {
        hypothesisId: "H1-H2",
        hasCluster: cluster != null,
        nodesType: typeof cluster?.nodes,
        nodesIsArray: Array.isArray(cluster?.nodes),
      },
      timestamp: Date.now(),
      runId: "post-fix-verify",
    }),
  }).catch(() => {});
  // #endregion

  const masterOnline = cluster?.nodes?.some(n => n.role === "master" && n.online) ?? false;
  const workers =
    (cluster?.nodes?.filter(n => n.role === "worker" && n.online))?.length ?? 0;
  const hitlCount    = hitl?.total ?? 0;
  const statusC      = stealth ? "#334155" : (masterOnline ? "#22c55e" : "#ef4444");

  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
      {/* Master dot */}
      <span
        title={masterOnline ? "Master Online" : "Master Offline"}
        style={{ display: "flex", alignItems: "center", gap: "0.3rem", cursor: "default" }}
      >
        <span style={{
          width: 6, height: 6, borderRadius: "50%",
          background: statusC,
          display: "inline-block",
          boxShadow: masterOnline && !stealth ? `0 0 5px ${statusC}` : "none",
          animation: masterOnline && !stealth ? "rgb-pulse 2s infinite" : "none",
        }} />
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: stealth ? "#1e293b" : "#334155" }}>
          {workers}W
        </span>
      </span>

      {/* HITL badge */}
      {hitlCount > 0 && (
        <Link href="/dashboard" style={{ textDecoration: "none" }}>
          <span style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            fontWeight: 700,
            padding: "1px 6px",
            borderRadius: "999px",
            background: stealth ? "transparent" : "#f59e0b",
            color: stealth ? "#334155" : "#000",
            border: stealth ? "1px solid #1e293b" : "none",
            animation: stealth ? "none" : "rgb-pulse 1s infinite",
          }}>
            {hitlCount} HITL
          </span>
        </Link>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// SYNC & RESTART CLUSTER — Phase 18
// Big neon-purple header button that opens a full-screen modal terminal.
// ─────────────────────────────────────────────────────────────────────────────

const MODAL_STEP_TAG: Record<string, string> = {
  connecting:      "CONNECTING",
  stopping_worker: "STOPPING",
  uploading:       "UPLOADING",
  installing_deps: "INSTALLING DEPS",
  bootstrapping:   "INSTALLING DEPS",
  restarting:      "RESTARTING",
  skipped:         "SKIPPED",
  done:            "DONE",
  error:           "ERROR",
};

const MODAL_STEP_COLOR: Record<string, string> = {
  connecting:      "#60a5fa",
  stopping_worker: "#f59e0b",
  uploading:       "#a78bfa",
  installing_deps: "#34d399",
  bootstrapping:   "#34d399",
  restarting:      "#f472b6",
  skipped:         "#94a3b8",
  done:            "#22c55e",
  error:           "#ef4444",
};

interface ModalLogLine {
  ts: string;
  tag: string;
  text: string;
  color: string;
}

function nowHMS() {
  return new Date().toLocaleTimeString("en-GB", { hour12: false });
}

function SyncClusterButton({ stealth }: { stealth: boolean }) {
  const { deployPhase, setDeployPhase, setDeployingNode } = useNexus();

  const [modalOpen, setModalOpen] = useState(false);
  const [phase, setPhaseLocal]    = useState<DeployPhase>("idle");
  const [lines, setLines]         = useState<ModalLogLine[]>([]);
  const [errMsg, setErrMsg]       = useState("");
  const [headerMounted, setHeaderMounted] = useState(false);

  const streamsRef  = useRef<Map<string, EventSource>>(new Map());
  const termRef     = useRef<HTMLDivElement>(null);
  const phaseRef    = useRef<DeployPhase>("idle");
  /** Prevents double DONE/ERROR when both SSE and status poll see the same terminal state. */
  const deploySettledRef = useRef(false);

  useEffect(() => { setHeaderMounted(true); }, []);

  // Keep global context in sync
  const setPhase = useCallback((p: DeployPhase) => {
    phaseRef.current = p;
    setPhaseLocal(p);
    setDeployPhase(p);
  }, [setDeployPhase]);

  const addLine = useCallback((tag: string, text: string, color: string) => {
    setLines(prev => [...prev, { ts: nowHMS(), tag, text, color }]);
  }, []);

  // Auto-scroll terminal
  useEffect(() => {
    if (termRef.current) termRef.current.scrollTop = termRef.current.scrollHeight;
  }, [lines]);

  // Close modal on Escape
  useEffect(() => {
    if (!modalOpen) return;
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") setModalOpen(false); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [modalOpen]);

  // Cleanup streams on unmount
  useEffect(() => () => { streamsRef.current.forEach(es => es.close()); }, []);

  const openStream = useCallback((node_id: string) => {
    if (streamsRef.current.has(node_id)) return;
    setDeployingNode(node_id, true);
    const es = openDeployProgressStream(node_id);
    streamsRef.current.set(node_id, es);

    // Track max file progress seen so far for this stream
    let _maxFile = 0, _totFile = 0;

    es.onmessage = (e) => {
      try {
        const ev = JSON.parse(e.data) as DeployProgressEvent;
        const tag   = MODAL_STEP_TAG[ev.step]   ?? ev.step.toUpperCase();
        const color = MODAL_STEP_COLOR[ev.step] ?? "#94a3b8";
        const text  = ev.detail || (ev as DeployProgressEvent & { label?: string }).label || tag;

        // For individual file upload events, track max and collapse into one updating line
        if (ev.step === "uploading" && ev.status === "running" && text.match(/^\[(\d+)\/(\d+)\]/)) {
          const m = text.match(/^\[(\d+)\/(\d+)\]/);
          const cur = parseInt(m![1]), tot = parseInt(m![2]);
          if (cur > _maxFile) { _maxFile = cur; _totFile = tot; }
          const pct = Math.round(_maxFile / _totFile * 100);
          const bar = "▓".repeat(Math.round(pct / 5)) + "░".repeat(20 - Math.round(pct / 5));
          setLines(prev => {
            const last = prev[prev.length - 1];
            const uploadLine = { ts: nowHMS(), tag: "UPLOADING", text: `[${_maxFile}/${_totFile}] ${pct}%  ${bar}`, color: "#a855f7" };
            if (last?.tag === "UPLOADING" && last.text.startsWith("[")) {
              return [...prev.slice(0, -1), uploadLine];
            }
            return [...prev, uploadLine];
          });
        } else {
          // Force progress bar to 100% when upload step completes
          if (ev.step === "uploading" && ev.status === "done") {
            const tot = _totFile > 0 ? _totFile : 173;
            const bar = "▓".repeat(20);
            setLines(prev => {
              const last = prev[prev.length - 1];
              const uploadLine = { ts: nowHMS(), tag: "UPLOADING", text: `[${tot}/${tot}] 100%  ${bar}`, color: "#a855f7" };
              if (last?.tag === "UPLOADING" && last.text.startsWith("[")) {
                return [...prev.slice(0, -1), uploadLine];
              }
              return [...prev, uploadLine];
            });
          }
          if (
            ev.status === "running"
            || ev.status === "error"
            || ev.step === "done"
            || ev.step === "error"
            || ev.status === "done"
          ) {
            addLine(tag, text, color);
          }
        }

        const failed = ev.status === "error" || ev.step === "error";
        const succeeded = ev.step === "done" && ev.status === "done";
        const skipped = ev.step === "skipped" && ev.status === "done";
        if (failed || succeeded || skipped) {
          es.close();
          streamsRef.current.delete(node_id);
          setTimeout(() => setDeployingNode(node_id, false), 4000);
          // Final success/error comes from the multi-node status poll (Nexus-Push).
        }
      } catch {}
    };
    es.onerror = () => {
      es.close();
      streamsRef.current.delete(node_id);
      setDeployingNode(node_id, false);
    };
  }, [addLine, setDeployingNode]);

  const handleSync = useCallback(async () => {
    if (phase === "running") return;

    setPhase("running");
    setLines([]);
    setErrMsg("");
    deploySettledRef.current = false;
    setModalOpen(true);
    streamsRef.current.forEach(es => es.close());
    streamsRef.current.clear();

    addLine(
      "NEXUS-PUSH",
      `Targeting ${API_BASE} → worker_linux + worker_windows (continues if one is down)`,
      "#a855f7",
    );
    addLine(
      "API",
      "POST /api/deploy/sync (timeout 30s)…",
      "#64748b",
    );

    try {
      const data = await triggerSync();
      addLine("JOB", data.job_id ?? "sync", "#64748b");

      // Poll for node discovery, then open SSE stream
      const poll = setInterval(async () => {
        try {
          const st = await getDeployStatus();
          const ids = Object.keys(st.nodes);
          if (ids.length > 0) {
            ids.forEach(openStream);
            clearInterval(poll);
          }
        } catch {}
      }, 800);
      setTimeout(() => clearInterval(poll), 15_000);

      // Open SSE for both Nexus-Push targets (Linux may fail fast while Windows continues)
      setTimeout(() => openStream("worker_linux"), 1200);
      setTimeout(() => openStream("worker_windows"), 1400);

      // Poll /api/deploy/status every 2s — settle only when every target has a terminal event
      let _pollCount = 0;
      const statusPoll = setInterval(async () => {
        _pollCount++;
        try {
          const st = await getDeployStatus();
          const { nodes } = st;
          for (const id of NEXUS_PUSH_SYNC_NODE_IDS) {
            const ev = nodes[id];
            if (!ev || !deployEventTerminal(ev)) return;
          }
          if (deploySettledRef.current) return;
          deploySettledRef.current = true;
          clearInterval(statusPoll);
          if (streamsRef.current.size > 0) {
            streamsRef.current.forEach(s => s.close());
            streamsRef.current.clear();
          }
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
                ? ` — ${failed.join(", ")} failed or skipped (see log above)`
                : "";
            addLine(
              "DONE",
              `Deployment complete (at least one worker) ✓${hint}`,
              "#22c55e",
            );
            setPhase("done");
          } else {
            const parts = NEXUS_PUSH_SYNC_NODE_IDS.map(id => {
              const ev = nodes[id];
              return ev ? `${id}: ${ev.detail || "failed"}` : `${id}: (no status)`;
            });
            addLine("ERROR", parts.join(" | "), "#ef4444");
            setPhase("error");
          }
        } catch {}
        if (_pollCount >= 360) {
          clearInterval(statusPoll);
          if (phaseRef.current === "running" && !deploySettledRef.current) {
            deploySettledRef.current = true;
            streamsRef.current.forEach(s => s.close());
            streamsRef.current.clear();
            for (const id of NEXUS_PUSH_SYNC_NODE_IDS) {
              setDeployingNode(id, false);
            }
            addLine(
              "ERROR",
              "Deploy status poll timed out (~12 min) — check server logs and worker connectivity.",
              "#ef4444",
            );
            setPhase("error");
          }
        }
      }, 2_000);

    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      setErrMsg(msg);
      setPhase("error");
      addLine("ERROR", msg, "#ef4444");
    }
  }, [phase, openStream, addLine, setPhase]);

  if (stealth) return null;

  const isRunning = phase === "running";
  const isDone    = phase === "done";
  const isError   = phase === "error";

  const btnBorder = isRunning ? "#a855f7aa" : isDone ? "#22c55e66" : isError ? "#ef444466" : "#a855f766";
  const btnBg     = isRunning ? "rgba(168,85,247,0.18)" : isDone ? "rgba(34,197,94,0.10)" : isError ? "rgba(239,68,68,0.10)" : "rgba(168,85,247,0.10)";
  const btnColor  = isRunning ? "#c084fc" : isDone ? "#22c55e" : isError ? "#ef4444" : "#e879f9";

  return (
    <>
      {/* ── The big neon-purple header button ── */}
      <button
        onClick={() => isRunning ? setModalOpen(true) : handleSync()}
        title="סנכרון והפעלה מחדש — העברת קוד לעובד Linux"
        style={{
          display: "flex", alignItems: "center", gap: "0.5rem",
          padding: "6px 16px", borderRadius: "8px",
          border: `1px solid ${btnBorder}`,
          background: btnBg, cursor: "pointer",
          transition: "all 0.2s",
          boxShadow: isRunning
            ? "0 0 18px #a855f733, 0 0 6px #a855f755"
            : !isDone && !isError
            ? "0 0 10px #a855f722"
            : "none",
        }}
      >
        <span style={{ fontSize: "0.85rem", lineHeight: 1 }}>
          {isRunning ? "⚙️" : isDone ? "✅" : isError ? "❌" : "🚀"}
        </span>
        <span style={{
          fontFamily: "'Inter', var(--font-sans), sans-serif",
          fontSize: "0.72rem", fontWeight: 700,
          letterSpacing: "0.04em", color: btnColor, whiteSpace: "nowrap",
          textShadow: !isDone && !isError ? `0 0 10px ${btnColor}88` : "none",
        }}>
          {isRunning ? "מסנכרן..." : isDone ? "פועל" : isError ? "כשל בסנכרון" : "סנכרון והפעלה מחדש"}
        </span>
        {isRunning && (
          <span style={{
            width: 7, height: 7, borderRadius: "50%", background: "#e879f9",
            display: "inline-block", flexShrink: 0,
            boxShadow: "0 0 8px #e879f9", animation: "rgb-pulse 0.7s infinite",
          }} />
        )}
      </button>

      {/* ── Full-screen modal terminal ── */}
      {headerMounted && modalOpen && createPortal(
        <div
          onClick={(e) => { if (e.target === e.currentTarget) setModalOpen(false); }}
          style={{
            position: "fixed", inset: 0, zIndex: 1000,
            background: "rgba(2,6,23,0.92)",
            backdropFilter: "blur(8px)",
            display: "flex", alignItems: "center", justifyContent: "center",
            padding: "1.5rem",
          }}
        >
          <div style={{
            width: "100%", maxWidth: "860px",
            background: "linear-gradient(160deg, #080d18 0%, #050912 100%)",
            border: `1px solid ${isRunning ? "#a855f755" : isDone ? "#22c55e44" : isError ? "#ef444444" : "#1e293b"}`,
            borderRadius: "14px",
            overflow: "hidden",
            boxShadow: isRunning
              ? "0 0 60px #a855f722, 0 24px 80px rgba(0,0,0,0.7)"
              : "0 24px 80px rgba(0,0,0,0.7)",
            display: "flex", flexDirection: "column",
          }}>

            {/* Modal title bar */}
            <div style={{
              display: "flex", alignItems: "center", justifyContent: "space-between",
              padding: "0.75rem 1.25rem",
              background: "rgba(255,255,255,0.025)",
              borderBottom: "1px solid #0f172a",
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
                {/* Traffic lights */}
                {(["#ef4444", "#f59e0b", "#22c55e"] as const).map((c, i) => (
                  <span key={i} style={{
                    width: 11, height: 11, borderRadius: "50%",
                    background: isRunning && i === 2 ? c : "#1e293b",
                    display: "inline-block",
                    boxShadow: isRunning && i === 2 ? `0 0 7px ${c}` : "none",
                    animation: isRunning && i === 2 ? "rgb-pulse 1s infinite" : "none",
                  }} />
                ))}
                <span style={{
                  fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700,
                  color: "#475569", letterSpacing: "0.12em", marginLeft: "0.3rem",
                }}>
                  nexus-push — deploy terminal
                </span>
                <span style={{
                  fontFamily: "var(--font-mono)", fontSize: "0.6rem",
                  color: isRunning ? "#a855f7" : isDone ? "#22c55e" : isError ? "#ef4444" : "#334155",
                  marginLeft: "0.5rem",
                  animation: isRunning ? "ticker-fade 1.2s ease-in-out infinite" : "none",
                }}>
                  {isRunning ? "● מסנכרן" : isDone ? "✓ הושלם" : isError ? "✗ שגיאה" : "○ ממתין"}
                </span>
              </div>
              <button
                onClick={() => setModalOpen(false)}
                style={{
                  background: "none", border: "1px solid #1e293b",
                  borderRadius: "5px", color: "#475569",
                  cursor: "pointer", padding: "2px 8px",
                  fontFamily: "var(--font-mono)", fontSize: "0.65rem",
                }}
              >
                  ESC ✕
                </button>
            </div>

            {/* לוג טרמינל */}
            <div
              ref={termRef}
              style={{
                flex: 1, minHeight: "360px", maxHeight: "60vh",
                overflowY: "auto", padding: "1rem 1.25rem",
                fontFamily: "var(--font-mono)", fontSize: "0.75rem", lineHeight: "1.75",
                background: "#020617",
              }}
            >
              {lines.length === 0 ? (
                <span style={{ color: "#1e293b" }}>
                  {"> ממתין לתחילת הפריסה..."}
                </span>
              ) : (
                lines.map((ln, i) => (
                  <div key={i} style={{ display: "flex", gap: "0.75rem", alignItems: "baseline" }}>
                    <span style={{ color: "#1e3a5f", flexShrink: 0, fontSize: "0.68rem" }}>
                      [{ln.ts}]
                    </span>
                    <span style={{
                      color: ln.color, fontWeight: 700, flexShrink: 0,
                      minWidth: "148px",
                      textShadow: `0 0 10px ${ln.color}55`,
                    }}>
                      [{ln.tag}]
                    </span>
                    <span style={{ color: "#64748b", wordBreak: "break-all" }}>
                      {ln.text}
                    </span>
                  </div>
                ))
              )}
              {isRunning && (
                <div style={{ display: "flex", gap: "0.75rem", alignItems: "baseline", marginTop: "2px" }}>
                  <span style={{ color: "#1e3a5f", fontSize: "0.68rem" }}>[{nowHMS()}]</span>
                  <span style={{ color: "#a855f7", fontWeight: 700, animation: "terminal-blink 1s step-end infinite" }}>▋</span>
                </div>
              )}
            </div>

            {/* Action bar */}
            <div style={{
              display: "flex", alignItems: "center", justifyContent: "space-between",
              padding: "0.75rem 1.25rem",
              borderTop: "1px solid #0f172a",
              background: "rgba(255,255,255,0.015)",
            }}>
              <span style={{
                fontFamily: "var(--font-mono)", fontSize: "0.65rem",
                color: isError ? "#ef4444" : isDone ? "#22c55e" : isRunning ? "#a855f7" : "#334155",
                animation: isRunning ? "ticker-fade 1.4s ease-in-out infinite" : "none",
              }}>
                {isRunning ? "● פריסה בתהליך..."
                 : isDone  ? "✓ עובד פעיל — פריסה הושלמה"
                 : isError ? `✗ ${errMsg || "הפריסה נכשלה"}`
                 : "○ מוכן"}
              </span>
              <div style={{ display: "flex", gap: "0.5rem" }}>
                {(isDone || isError) && (
                  <button
                    onClick={handleSync}
                    style={{
                      fontFamily: "'Inter', var(--font-sans), sans-serif",
                      fontSize: "0.72rem", fontWeight: 600,
                      padding: "5px 14px", borderRadius: "6px",
                      border: "1px solid rgba(168,85,247,0.4)",
                      background: "rgba(168,85,247,0.12)",
                      color: "#c084fc", cursor: "pointer",
                      display: "flex", alignItems: "center", gap: "0.3rem",
                    }}
                  >
                    <span>🔄</span><span>סנכרן מחדש</span>
                  </button>
                )}
                <button
                  onClick={() => setModalOpen(false)}
                  style={{
                    fontFamily: "'Inter', var(--font-sans), sans-serif",
                    fontSize: "0.72rem", fontWeight: 500,
                    padding: "5px 14px", borderRadius: "6px",
                    border: "1px solid rgba(71,85,105,0.5)",
                    background: "transparent", color: "#475569", cursor: "pointer",
                  }}
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}

      <style>{`
        @keyframes terminal-blink {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0; }
        }
        @keyframes ticker-fade {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.4; }
        }
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Color Mode Toggle — bilingual segmented control (COLOR MODE)
// ─────────────────────────────────────────────────────────────────────────────

function ColorModeToggle({ stealth }: { stealth: boolean }) {
  const { colorMode, setColorMode, isHighContrast } = useTheme();
  const { t } = useI18n();

  const modes: Array<{ id: "standard" | "high-contrast"; labelKey: TranslationKey; icon: string }> = [
    { id: "standard",      labelKey: "theme_standard",      icon: "◑" },
    { id: "high-contrast", labelKey: "theme_high_contrast", icon: "◉" },
  ];

  if (stealth) return null;

  const borderColor = isHighContrast ? "#374151" : "rgba(14,165,233,0.15)";
  const wrapBg      = isHighContrast ? "rgba(248,249,250,0.97)" : "rgba(14,165,233,0.04)";

  return (
    <div
      title={t("color_mode")}
      style={{
        display: "flex",
        alignItems: "center",
        background: wrapBg,
        border: `1px solid ${borderColor}`,
        borderRadius: "8px",
        padding: "2px",
        gap: "1px",
        flexShrink: 0,
      }}
    >
      {modes.map(({ id, labelKey, icon }) => {
        const isActive = colorMode === id;
        const activeBg    = isHighContrast ? "#E8F0FE" : "rgba(14,165,233,0.18)";
        const activeColor = isHighContrast ? "#0055CC" : "#0ea5e9";
        const inactiveColor = isHighContrast ? "#374151" : "#6b8fab";

        return (
          <button
            key={id}
            onClick={() => setColorMode(id)}
            title={t(labelKey)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              padding: "3px 8px",
              borderRadius: "6px",
              border: "none",
              cursor: "pointer",
              background: isActive ? activeBg : "transparent",
              boxShadow:
                isActive && !isHighContrast
                  ? "0 0 8px rgba(14,165,233,0.2)"
                  : "none",
              transition: "all 0.18s ease",
            }}
          >
            <span
              style={{
                fontSize: "0.75rem",
                lineHeight: 1,
                color: isActive ? activeColor : inactiveColor,
              }}
            >
              {icon}
            </span>
            <span
              style={{
                fontFamily: "var(--font-sans)",
                fontSize: "0.58rem",
                fontWeight: isActive ? 600 : 400,
                color: isActive ? activeColor : inactiveColor,
                letterSpacing: "0.04em",
                transition: "color 0.18s ease",
                userSelect: "none",
                whiteSpace: "nowrap",
              }}
            >
              {id === "standard" ? "STD" : "HC"}
            </span>
          </button>
        );
      })}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// PANIC BUTTON — Global Kill-Switch
// Big, red, glowing. Double-confirmation modal in Hebrew.
// Syncs dashboard theme to "Restricted Mode" (gray/red) on activation.
// ─────────────────────────────────────────────────────────────────────────────

function PanicButton() {
  const { colorMode, setColorMode } = useTheme();
  const [modalStep, setModalStep]   = useState<"closed" | "confirm" | "executing" | "done">("closed");
  const [errMsg, setErrMsg]         = useState("");
  const [result, setResult]         = useState<{ workers: number; elapsed_ms: number } | null>(null);
  const isPanic                     = colorMode === "panic";

  // Poll panic state (every 3 s) so the UI auto-recovers when backend resets
  const { data: panicState } = useSWR<PanicStateResponse>(
    "/api/system/panic/state",
    swrFetcher<PanicStateResponse>,
    { refreshInterval: 3_000 },
  );

  // Sync theme with backend panic state
  useEffect(() => {
    if (panicState?.panic && colorMode !== "panic") {
      setColorMode("panic");
    } else if (!panicState?.panic && colorMode === "panic") {
      // Restore to standard when panic is cleared from outside (e.g. settings reset)
      setColorMode("standard");
    }
  }, [panicState?.panic]); // eslint-disable-line react-hooks/exhaustive-deps

  // Close modal on Escape
  useEffect(() => {
    if (modalStep === "closed") return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape" && modalStep !== "executing") setModalStep("closed");
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [modalStep]);

  const handleConfirm = useCallback(async () => {
    setModalStep("executing");
    setErrMsg("");
    try {
      const res = await triggerPanic();
      setResult({ workers: res.workers_terminated.length, elapsed_ms: res.elapsed_ms });
      setColorMode("panic");
      setModalStep("done");
    } catch (err: unknown) {
      setErrMsg(err instanceof Error ? err.message : String(err));
      setModalStep("confirm");
    }
  }, [setColorMode]);

  // ── PANIC ACTIVE banner (shown instead of button when in panic mode) ──────
  if (isPanic) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          padding: "5px 14px",
          borderRadius: "8px",
          border: "1px solid rgba(204,34,51,0.7)",
          background: "rgba(204,34,51,0.15)",
          animation: "panic-glow-pulse 1.2s ease-in-out infinite",
          cursor: "default",
          flexShrink: 0,
        }}
        title="System is in PANIC / Emergency mode — go to Settings to reset"
      >
        <span style={{ fontSize: "0.85rem" }}>🛑</span>
        <span style={{
          fontFamily: "'Inter', var(--font-sans), sans-serif",
          fontSize: "0.7rem",
          fontWeight: 800,
          letterSpacing: "0.06em",
          color: "#ff3344",
          textShadow: "0 0 12px #ff334488",
          whiteSpace: "nowrap",
        }}>
          PANIC ACTIVE
        </span>
        <span style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: "#ff3344",
          display: "inline-block",
          boxShadow: "0 0 10px #ff3344",
          animation: "panic-glow-pulse 0.6s ease-in-out infinite",
        }} />
        <style>{`
          @keyframes panic-glow-pulse {
            0%, 100% { opacity: 1; }
            50%       { opacity: 0.45; }
          }
        `}</style>
      </div>
    );
  }

  return (
    <>
      {/* ── The big red PANIC button ── */}
      <button
        onClick={() => setModalStep("confirm")}
        title="עצירת חירום (PANIC) — Emergency system kill-switch"
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.45rem",
          padding: "6px 14px",
          borderRadius: "8px",
          border: "1px solid rgba(220,38,38,0.65)",
          background: "rgba(220,38,38,0.13)",
          cursor: "pointer",
          flexShrink: 0,
          transition: "all 0.18s ease",
          boxShadow: "0 0 14px rgba(220,38,38,0.25), 0 0 4px rgba(220,38,38,0.15)",
          animation: "panic-btn-breathe 3s ease-in-out infinite",
        }}
      >
        <span style={{ fontSize: "0.9rem", lineHeight: 1 }}>🚨</span>
        <span style={{
          fontFamily: "'Inter', var(--font-sans), sans-serif",
          fontSize: "0.72rem",
          fontWeight: 800,
          letterSpacing: "0.05em",
          color: "#ef4444",
          textShadow: "0 0 10px rgba(239,68,68,0.55)",
          whiteSpace: "nowrap",
        }}>
          PANIC
        </span>
        <span style={{
          fontFamily: "var(--font-assistant), var(--font-sans), sans-serif",
          fontSize: "0.62rem",
          fontWeight: 600,
          color: "rgba(239,68,68,0.75)",
          whiteSpace: "nowrap",
          direction: "rtl",
        }}>
          עצירת חירום
        </span>
      </button>

      {/* ── Double-confirmation modal ── */}
      {modalStep !== "closed" && (
        <div
          onClick={(e) => {
            if (e.target === e.currentTarget && modalStep !== "executing") setModalStep("closed");
          }}
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 2000,
            background: "rgba(5,0,2,0.94)",
            backdropFilter: "blur(10px)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "1.5rem",
          }}
        >
          <div style={{
            width: "100%",
            maxWidth: "520px",
            background: "linear-gradient(160deg, #110305 0%, #0a0203 100%)",
            border: `1px solid ${modalStep === "done" ? "rgba(239,68,68,0.8)" : "rgba(239,68,68,0.45)"}`,
            borderRadius: "16px",
            overflow: "hidden",
            boxShadow: "0 0 80px rgba(220,38,38,0.18), 0 24px 80px rgba(0,0,0,0.85)",
          }}>

            {/* Title bar */}
            <div style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "0.9rem 1.4rem",
              background: "rgba(220,38,38,0.07)",
              borderBottom: "1px solid rgba(220,38,38,0.2)",
            }}>
              <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
                <span style={{ fontSize: "1.1rem" }}>🚨</span>
                <span style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.72rem",
                  fontWeight: 700,
                  letterSpacing: "0.12em",
                  color: "#ef4444",
                  textShadow: "0 0 12px rgba(239,68,68,0.5)",
                }}>
                  PANIC / עצירת חירום
                </span>
              </div>
              {modalStep !== "executing" && (
                <button
                  onClick={() => setModalStep("closed")}
                  style={{
                    background: "none",
                    border: "1px solid rgba(220,38,38,0.25)",
                    borderRadius: "5px",
                    color: "#6b7280",
                    cursor: "pointer",
                    padding: "2px 8px",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.65rem",
                  }}
                >
                  ESC ✕
                </button>
              )}
            </div>

            {/* Body */}
            <div style={{ padding: "1.75rem 1.5rem" }}>
              {modalStep === "done" ? (
                /* ── Done state ── */
                <div style={{ textAlign: "center" as const }}>
                  <div style={{ fontSize: "2.5rem", marginBottom: "1rem" }}>🛑</div>
                  <p style={{
                    fontFamily: "var(--font-assistant), var(--font-sans), sans-serif",
                    fontSize: "1.1rem",
                    fontWeight: 700,
                    color: "#ef4444",
                    marginBottom: "0.5rem",
                    direction: "rtl",
                  }}>
                    המערכת הופסקה
                  </p>
                  <p style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.72rem",
                    color: "#6b7280",
                    marginBottom: "1.5rem",
                  }}>
                    System halted. {result?.workers ?? 0} worker(s) terminated in {result?.elapsed_ms ?? 0} ms.
                  </p>
                  <div style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.65rem",
                    color: "rgba(239,68,68,0.65)",
                    background: "rgba(220,38,38,0.06)",
                    border: "1px solid rgba(220,38,38,0.15)",
                    borderRadius: "8px",
                    padding: "0.6rem 1rem",
                  }}>
                    Telegram notification sent • Workers TERMINATED • Trading halted
                  </div>
                  <p style={{
                    fontFamily: "var(--font-assistant), var(--font-sans), sans-serif",
                    fontSize: "0.75rem",
                    color: "#4b5563",
                    marginTop: "1rem",
                    direction: "rtl",
                  }}>
                    לאיפוס המערכת: הגדרות → Reset System / איפוס מערכת
                  </p>
                </div>
              ) : (
                /* ── Confirmation state ── */
                <>
                  <div style={{ textAlign: "center" as const, marginBottom: "1.5rem" }}>
                    <div style={{
                      fontSize: "2.5rem",
                      marginBottom: "0.75rem",
                      animation: modalStep === "executing" ? "panic-glow-pulse 0.5s infinite" : "none",
                    }}>
                      ⚠️
                    </div>

                    {/* Hebrew confirmation question */}
                    <p style={{
                      fontFamily: "var(--font-assistant), var(--font-sans), sans-serif",
                      fontSize: "1.05rem",
                      fontWeight: 700,
                      color: "#f1f5f9",
                      lineHeight: 1.5,
                      direction: "rtl",
                      marginBottom: "0.75rem",
                    }}>
                      האם אתה בטוח שברצונך לעצור את כל המערכת מיידית?
                    </p>

                    <p style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.68rem",
                      color: "#6b7280",
                      lineHeight: 1.7,
                    }}>
                      This will immediately:<br />
                      • Set global kill-switch in Redis<br />
                      • Broadcast TERMINATE to all workers<br />
                      • Halt all active trading tasks<br />
                      • Send urgent Telegram notification
                    </p>
                  </div>

                  {errMsg && (
                    <div style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.65rem",
                      color: "#ef4444",
                      background: "rgba(239,68,68,0.06)",
                      border: "1px solid rgba(239,68,68,0.2)",
                      borderRadius: "6px",
                      padding: "0.5rem 0.75rem",
                      marginBottom: "1rem",
                    }}>
                      ✗ {errMsg}
                    </div>
                  )}
                </>
              )}
            </div>

            {/* Action bar */}
            {modalStep !== "done" && (
              <div style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "flex-end",
                gap: "0.6rem",
                padding: "0.9rem 1.4rem",
                borderTop: "1px solid rgba(220,38,38,0.12)",
                background: "rgba(0,0,0,0.2)",
              }}>
                <button
                  onClick={() => setModalStep("closed")}
                  disabled={modalStep === "executing"}
                  style={{
                    fontFamily: "'Inter', var(--font-sans), sans-serif",
                    fontSize: "0.72rem",
                    fontWeight: 500,
                    padding: "7px 18px",
                    borderRadius: "7px",
                    border: "1px solid rgba(71,85,105,0.5)",
                    background: "transparent",
                    color: "#6b7280",
                    cursor: modalStep === "executing" ? "not-allowed" : "pointer",
                    opacity: modalStep === "executing" ? 0.4 : 1,
                  }}
                >
                  ביטול / Cancel
                </button>
                <button
                  onClick={handleConfirm}
                  disabled={modalStep === "executing"}
                  style={{
                    fontFamily: "'Inter', var(--font-sans), sans-serif",
                    fontSize: "0.72rem",
                    fontWeight: 800,
                    padding: "7px 22px",
                    borderRadius: "7px",
                    border: "1px solid rgba(220,38,38,0.7)",
                    background: modalStep === "executing"
                      ? "rgba(220,38,38,0.25)"
                      : "rgba(220,38,38,0.22)",
                    color: "#ef4444",
                    cursor: modalStep === "executing" ? "not-allowed" : "pointer",
                    textShadow: "0 0 10px rgba(239,68,68,0.6)",
                    boxShadow: modalStep !== "executing" ? "0 0 16px rgba(220,38,38,0.3)" : "none",
                    letterSpacing: "0.04em",
                    animation: modalStep !== "executing" ? "panic-btn-breathe 1.5s ease-in-out infinite" : "none",
                  }}
                >
                  {modalStep === "executing" ? "⏳ מפסיק…" : "🛑 כן, עצור הכל"}
                </button>
              </div>
            )}

            {modalStep === "done" && (
              <div style={{
                display: "flex",
                justifyContent: "center",
                padding: "0.9rem 1.4rem",
                borderTop: "1px solid rgba(220,38,38,0.12)",
              }}>
                <button
                  onClick={() => setModalStep("closed")}
                  style={{
                    fontFamily: "'Inter', var(--font-sans), sans-serif",
                    fontSize: "0.72rem",
                    fontWeight: 500,
                    padding: "7px 20px",
                    borderRadius: "7px",
                    border: "1px solid rgba(71,85,105,0.5)",
                    background: "transparent",
                    color: "#6b7280",
                    cursor: "pointer",
                  }}
                >
                  סגור / Close
                </button>
              </div>
            )}
          </div>
        </div>
      )}

      <style>{`
        @keyframes panic-btn-breathe {
          0%, 100% { box-shadow: 0 0 14px rgba(220,38,38,0.25), 0 0 4px rgba(220,38,38,0.15); }
          50%       { box-shadow: 0 0 22px rgba(220,38,38,0.45), 0 0 8px rgba(220,38,38,0.30); }
        }
        @keyframes panic-glow-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.45; }
        }
      `}</style>
    </>
  );
}


export default function Header() {
  const pathname = usePathname();
  const { stealth, toggleStealth, stealthOverride, toggleOverride } = useStealth();
  const { t, isRTL } = useI18n();
  const { isHighContrast, tokens } = useTheme();

  const { data: chatOpsData } = useSWR<ChatOpsStatusResponse>(
    "/api/notifications/status",
    swrFetcher<ChatOpsStatusResponse>,
    { refreshInterval: 30_000 }
  );

  const PAGE_KEY_MAP: Record<string, string> = {
    "/dashboard":          "nav.dashboard",
    "/operations":         "nav.operations",
    "/fleet":              "nav.fleet",
    "/projects":           "nav.projects",
    "/treasury":           "nav.treasury",
    "/automation":         "nav.automation",
    "/incubator":          "nav.incubator",
    "/niche-lab":          "nav.niche-lab",
    "/settings":           "nav.settings",
    "/about":              "nav.about",
    "/sessions":           "nav.sessions",
    "/wallet-ops":         "nav.wallet-ops",
    "/swarm-control":      "nav.swarm-control",
    "/sentinel-seo":       "nav.sentinel-seo",
    "/vault":              "nav.vault",
    "/market-intel":       "nav.market-intel",
    "/polymarket-deck":    "nav.polymarket-deck",
    "/modules":            "nav.modules",
    "/nexus-os":           "nav.nexus-os",
    "/logs-raw":           "nav.logs-raw",
    "/ai-evolution":       "nav.ai-evolution",
    "/evolution":          "nav.evolution",
    "/bot-farm":           "nav.bot-farm",
    "/strategy-lab":       "nav.strategy-lab",
    "/bot-factory":        "nav.bot-factory",
    "/scrape-browser":     "nav.scrape-browser",
    "/group-infiltration": "nav.group-infiltration",
  };
  const pageLabel = pathname && PAGE_KEY_MAP[pathname] ? t(PAGE_KEY_MAP[pathname] as TranslationKey) : "";

  return (
    <header
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        zIndex: 50,
        height: "56px",
        background: isHighContrast
          ? "rgba(255,255,255,0.98)"
          : "rgba(15, 17, 26, 0.88)",
        backdropFilter: isHighContrast ? "none" : "blur(18px)",
        WebkitBackdropFilter: isHighContrast ? "none" : "blur(18px)",
        borderBottom: isHighContrast
          ? "1px solid #374151"
          : `1px solid ${stealth ? "#141824" : "rgba(14,165,233,0.1)"}`,
        display: "flex",
        alignItems: "center",
        flexDirection: isRTL ? "row-reverse" : "row",
        paddingInline: "1.5rem",
        gap: "0.75rem",
        transition: "background 0.25s, border-color 0.25s",
        boxShadow: isHighContrast ? "0 1px 4px rgba(0,0,0,0.12)" : "none",
      }}
    >
      {/* Accent bar */}
      <span
        style={{
          display: "block",
          width: "3px",
          height: "22px",
          borderRadius: "2px",
          backgroundColor: stealth ? "#21293d" : tokens.accent,
          flexShrink: 0,
          transition: "background-color 0.3s",
          boxShadow: stealth || isHighContrast ? "none" : `0 0 8px ${tokens.accentDim}`,
        }}
      />

      {/* TeleFix Branding */}
      <span
        style={{
          fontFamily: "var(--font-sans)",
          fontWeight: 700,
          fontSize: "0.92rem",
          letterSpacing: "0.1em",
          color: stealth ? "#2a3450" : tokens.accent,
          textTransform: "uppercase",
          transition: "color 0.3s",
        }}
      >
        TeleFix OS
      </span>

      {/* Page breadcrumb */}
      {pageLabel && (
        <span
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.65rem",
            color: isHighContrast
              ? tokens.textMuted
              : stealth ? "#141824" : "#2a3450",
            letterSpacing: "0.04em",
          }}
        >
          {isRTL ? `${pageLabel} /` : `/ ${pageLabel}`}
        </span>
      )}

      <span style={{ flex: 1 }} />

      {/* System Health */}
      <SystemHealthBreadcrumb stealth={stealth} />

      {/* ChatOps status */}
      {chatOpsData && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.5rem",
            padding: "3px 8px",
            borderRadius: "6px",
            border: isHighContrast
              ? "1px solid #9CA3AF"
              : `1px solid ${stealth ? "#141824" : "rgba(14,165,233,0.12)"}`,
            background: "transparent",
          }}
          title="ממשק פקודות (ChatOps) — סטטוס חיבור"
        >
          <span
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.6rem",
              letterSpacing: "0.1em",
              color: isHighContrast
                ? tokens.textMuted
                : stealth ? "#141824" : "#2a3450",
            }}
          >
            {t("header.chatops")}
          </span>
          {chatOpsData.providers.map((p) => (
            <ChatOpsDot
              key={p.name}
              name={p.name}
              connected={p.connected}
              detail={p.detail}
              stealth={stealth}
            />
          ))}
        </div>
      )}

      {/* Language Toggle */}
      <LanguageToggle stealth={stealth} isHighContrast={isHighContrast} />

      {/* Color Mode Toggle */}
      <ColorModeToggle stealth={stealth} />

      {/* PANIC / Emergency Kill-Switch */}
      {!stealth && <PanicButton />}

      {/* SYNC & RESTART CLUSTER */}
      <SyncClusterButton stealth={stealth} />

      {/* Stealth Override (stealth mode only) */}
      {stealth && (
        <button
          onClick={toggleOverride}
          title={
            stealthOverride
              ? "Stealth Override ON — scraper runs at full CPU priority"
              : "Enable Stealth Override"
          }
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.4rem",
            padding: "3px 8px",
            borderRadius: "5px",
            border: `1px solid ${stealthOverride ? "rgba(245,158,11,0.4)" : "#21293d"}`,
            background: stealthOverride ? "rgba(245,158,11,0.08)" : "transparent",
            cursor: "pointer",
            transition: "all 0.2s",
          }}
        >
          <span style={{ fontSize: "0.7rem" }}>⚡</span>
          <span
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.6rem",
              fontWeight: 600,
              letterSpacing: "0.08em",
              color: stealthOverride ? "#f59e0b" : "#2a3450",
              transition: "color 0.2s",
            }}
          >
            {stealthOverride ? t("header.override_on") : t("header.override")}
          </span>
        </button>
      )}

      {/* Stealth toggle — hidden in High Contrast (accessibility concern) */}
      {!isHighContrast && (
        <button
          onClick={toggleStealth}
          title={stealth ? "Stealth Mode: ON" : "Stealth Mode: OFF"}
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.5rem",
            padding: "4px 10px",
            borderRadius: "6px",
            border: `1px solid ${stealth ? "rgba(34,197,94,0.3)" : "rgba(42,52,80,0.6)"}`,
            background: stealth ? "rgba(34,197,94,0.06)" : "transparent",
            cursor: "pointer",
            transition: "all 0.2s",
          }}
        >
          <span
            style={{
              position: "relative",
              display: "inline-block",
              width: "32px",
              height: "16px",
              borderRadius: "8px",
              background: stealth ? "#22c55e" : "#21293d",
              border: `1px solid ${stealth ? "#22c55e" : "#2a3450"}`,
              transition: "background 0.25s, border-color 0.25s",
              flexShrink: 0,
              boxShadow: stealth ? "0 0 8px rgba(34,197,94,0.5)" : "none",
            }}
          >
            <span
              style={{
                position: "absolute",
                top: "2px",
                left: stealth ? "16px" : "2px",
                width: "10px",
                height: "10px",
                borderRadius: "50%",
                background: stealth ? "#fff" : "#6b8fab",
                transition: "left 0.25s, background 0.25s",
              }}
            />
          </span>
          <span
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.65rem",
              fontWeight: 600,
              letterSpacing: "0.08em",
              color: stealth ? "#22c55e" : "#6b8fab",
              transition: "color 0.25s",
            }}
          >
            {stealth ? t("header.stealth") : t("header.tactical")}
          </span>
        </button>
      )}

      {/* Live indicator */}
      {!stealth && (
        <span
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.4rem",
            fontFamily: "var(--font-sans)",
            fontSize: "0.65rem",
            fontWeight: 600,
            letterSpacing: "0.08em",
            color: isHighContrast ? tokens.textMuted : "#6b8fab",
          }}
        >
          <span
            style={{
              width: "7px",
              height: "7px",
              borderRadius: "50%",
              backgroundColor: tokens.success,
              boxShadow: isHighContrast ? "none" : `0 0 6px ${tokens.success}`,
              animation: isHighContrast ? "none" : "rgb-pulse 2s infinite",
              display: "block",
            }}
          />
          {t("header.live")}
        </span>
      )}

      <style>{`
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.4; }
        }
      `}</style>
    </header>
  );
}
