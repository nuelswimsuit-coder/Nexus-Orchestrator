"use client";

import { useEffect, useRef, useState } from "react";
import useSWR from "swr";
import { swrFetcher, resetSupervisorWorker } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { SectionHeader } from "@/components/ClusterStatus";
import type {
  AgentLogEntry,
  AgentLogResponse,
  DecisionsResponse,
  EngineStateResponse,
  EngineStateValue,
  SupervisorStatusResponse,
  SupervisorWorkerStatus,
} from "@/lib/api";

// ─────────────────────────────────────────────────────────────────────────────
// Engine state → terminal glow colour
// ─────────────────────────────────────────────────────────────────────────────

const STATE_GLOW: Record<EngineStateValue, string> = {
  idle:        "#00ff8833",
  calculating: "#6366f155",
  dispatching: "#f59e0b55",
  warning:     "#ef444455",
};

const STATE_BORDER: Record<EngineStateValue, string> = {
  idle:        "#1e293b",
  calculating: "#6366f144",
  dispatching: "#f59e0b44",
  warning:     "#ef444444",
};

const STATE_LABEL: Record<EngineStateValue, string> = {
  idle:        "IDLE",
  calculating: "CALCULATING",
  dispatching: "DISPATCHING",
  warning:     "AWAITING APPROVAL",
};

const STATE_COLOR: Record<EngineStateValue, string> = {
  idle:        "#334155",
  calculating: "#6366f1",
  dispatching: "#f59e0b",
  warning:     "#ef4444",
};

// ─────────────────────────────────────────────────────────────────────────────
// Level colours and icons
// ─────────────────────────────────────────────────────────────────────────────

interface LevelStyle { color: string; icon: string; }

const LEVEL_STYLE: Record<string, LevelStyle> = {
  info:     { color: "#64748b", icon: "›" },
  decision: { color: "#00ff88", icon: "◆" },
  warning:  { color: "#f59e0b", icon: "⚠" },
  action:   { color: "#f59e0b", icon: "⚡" },
  error:    { color: "#ef4444", icon: "✗" },
};

function levelStyle(level: string, engineState: EngineStateValue, stealth: boolean): LevelStyle {
  if (stealth) return { color: "#334155", icon: "›" };
  // When dispatching, action entries glow gold
  if (level === "action" && engineState === "dispatching") {
    return { color: "#f59e0b", icon: "⚡" };
  }
  // When calculating, decision entries glow indigo
  if (level === "decision" && engineState === "calculating") {
    return { color: "#6366f1", icon: "◆" };
  }
  return LEVEL_STYLE[level] ?? LEVEL_STYLE.info;
}

// ─────────────────────────────────────────────────────────────────────────────
// Log line
// ─────────────────────────────────────────────────────────────────────────────

function LogLine({
  entry,
  engineState,
  stealth,
}: {
  entry: AgentLogEntry;
  engineState: EngineStateValue;
  stealth: boolean;
}) {
  const { color, icon } = levelStyle(entry.level, engineState, stealth);
  const ts = entry.ts
    ? new Date(entry.ts).toLocaleTimeString("en-US", { hour12: false })
    : "--:--:--";

  return (
    <div
      className="flex items-start gap-2 font-mono text-[11px] leading-relaxed"
      style={{ fontFamily: "var(--font-mono), 'Courier New', monospace" }}
    >
      <span className="shrink-0 select-none" style={{ color: "#1e293b" }}>
        {ts}
      </span>
      <span className="shrink-0 w-3 text-center select-none" style={{ color }}>
        {icon}
      </span>
      <span
        style={{
          color,
          textShadow: stealth ? "none" : `0 0 8px ${color}44`,
          wordBreak: "break-word",
        }}
      >
        {entry.message}
      </span>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Synthesise log entries from decisions when no real log exists
// ─────────────────────────────────────────────────────────────────────────────

function decisionsToLog(decisions: DecisionsResponse | undefined): AgentLogEntry[] {
  if (!decisions?.decisions.length) return [];
  const now = new Date().toISOString();
  return [
    {
      ts: now,
      level: "info",
      message: `Decision Engine ran — ${decisions.decisions.length} action(s) scored`,
      metadata: {},
    },
    ...decisions.decisions.slice(0, 5).map((d) => ({
      ts: d.created_at,
      level: d.requires_approval ? "warning" : "decision",
      message: `[${d.confidence}% conf] ${d.title} — ${d.roi_impact}`,
      metadata: { decision_type: d.decision_type },
    })),
  ];
}

// ─────────────────────────────────────────────────────────────────────────────
// Engine state badge
// ─────────────────────────────────────────────────────────────────────────────

function EngineStateBadge({
  engineState,
  stealth,
}: {
  engineState: EngineStateValue;
  stealth: boolean;
}) {
  const c = stealth ? "#334155" : STATE_COLOR[engineState];
  const isActive = engineState !== "idle";

  return (
    <span
      className="inline-flex items-center gap-1.5 font-mono text-[9px] font-bold tracking-widest px-2 py-1 rounded-full"
      style={{
        color: c,
        background: stealth ? "transparent" : `${c}12`,
        border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
      }}
    >
      <span
        className="rounded-full shrink-0"
        style={{
          width: 5,
          height: 5,
          background: c,
          display: "inline-block",
          boxShadow: stealth ? "none" : `0 0 5px ${c}`,
          animation: isActive && !stealth ? "rgb-pulse 0.8s infinite" : "none",
        }}
      />
      {STATE_LABEL[engineState]}
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Recovery status colours
// ─────────────────────────────────────────────────────────────────────────────

const RECOVERY_COLOR: Record<string, string> = {
  healthy:    "#22c55e",
  recovering: "#f59e0b",
  critical:   "#ef4444",
};

const RECOVERY_LABEL: Record<string, string> = {
  healthy:    "HEALTHY",
  recovering: "RECOVERING",
  critical:   "CRITICAL",
};

// ─────────────────────────────────────────────────────────────────────────────
// Recovery banner — shown when any worker is recovering or critical
// ─────────────────────────────────────────────────────────────────────────────

function RecoveryBanner({
  workers,
  stealth,
  onReset,
}: {
  workers:  SupervisorWorkerStatus[];
  stealth:  boolean;
  onReset:  (name: string) => Promise<void>;
}) {
  const [resetting, setResetting] = useState<string | null>(null);

  const abnormal = workers.filter((w) => w.status !== "healthy");
  if (abnormal.length === 0) return null;

  async function handleReset(name: string) {
    setResetting(name);
    try {
      await onReset(name);
    } finally {
      setResetting(null);
    }
  }

  return (
    <div
      className="rounded-lg px-3 py-2 mb-3 flex flex-col gap-1.5"
      style={{
        background:  stealth ? "transparent" : "#0d1117",
        border:      `1px solid ${stealth ? "#1e293b" : "#f59e0b44"}`,
        boxShadow:   stealth ? "none" : "0 0 12px #f59e0b22",
      }}
    >
      {abnormal.map((w) => {
        const color = stealth ? "#334155" : RECOVERY_COLOR[w.status] ?? "#64748b";
        const isCritical = w.status === "critical";

        return (
          <div key={w.name} className="flex items-center justify-between gap-2 flex-wrap">
            {/* Left: status badge + worker name */}
            <div className="flex items-center gap-2">
              <span
                className="rounded-full shrink-0"
                style={{
                  width:      6,
                  height:     6,
                  background: color,
                  display:    "inline-block",
                  boxShadow:  stealth ? "none" : `0 0 6px ${color}`,
                  animation:  !stealth ? "rgb-pulse 0.8s infinite" : "none",
                }}
              />
              <span
                className="font-mono text-[9px] font-bold tracking-widest"
                style={{ color }}
              >
                {RECOVERY_LABEL[w.status] ?? w.status.toUpperCase()}
              </span>
              <span
                className="font-mono text-[10px]"
                style={{ color: stealth ? "#1e293b" : "#94a3b8" }}
              >
                {w.name}
              </span>
              {w.strike_count > 0 && (
                <span
                  className="font-mono text-[9px] px-1.5 py-0.5 rounded"
                  style={{
                    color:      color,
                    background: `${color}18`,
                    border:     `1px solid ${color}44`,
                  }}
                >
                  {w.strike_count}/3 קריסות
                </span>
              )}
            </div>

            {/* Right: Manual Reset button — only when CRITICAL */}
            {isCritical && !stealth && (
              <button
                onClick={() => handleReset(w.name)}
                disabled={resetting === w.name}
                className="font-mono text-[9px] font-bold px-2 py-1 rounded transition-all"
                style={{
                  color:      resetting === w.name ? "#64748b" : "#ef4444",
                  background: resetting === w.name ? "#0f172a" : "#ef444418",
                  border:     `1px solid ${resetting === w.name ? "#1e293b" : "#ef444455"}`,
                  cursor:     resetting === w.name ? "not-allowed" : "pointer",
                  boxShadow:  resetting === w.name ? "none" : "0 0 8px #ef444422",
                }}
              >
                {resetting === w.name ? "⏳ RESETTING..." : "🔄 MANUAL RESET"}
              </button>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────────────────────────────────

export default function AgentThinkingLog() {
  const { stealth } = useStealth();
  const scrollRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [resetError, setResetError] = useState<string | null>(null);

  const { data: logData } = useSWR<AgentLogResponse>(
    "/api/business/agent-log",
    swrFetcher<AgentLogResponse>,
    { refreshInterval: 5_000 }
  );

  const { data: decisions } = useSWR<DecisionsResponse>(
    "/api/business/decisions",
    swrFetcher<DecisionsResponse>,
    { refreshInterval: 30_000 }
  );

  const { data: engineStateData } = useSWR<EngineStateResponse>(
    "/api/business/engine-state",
    swrFetcher<EngineStateResponse>,
    { refreshInterval: 3_000 }
  );

  const { data: supervisorData, mutate: mutateSupervisor } =
    useSWR<SupervisorStatusResponse>(
      "/api/business/supervisor-status",
      swrFetcher<SupervisorStatusResponse>,
      { refreshInterval: 5_000 }
    );

  const engineState: EngineStateValue = engineStateData?.state ?? "idle";
  const supervisorWorkers: SupervisorWorkerStatus[] =
    supervisorData?.workers ?? [];

  async function handleManualReset(workerName: string) {
    setResetError(null);
    try {
      await resetSupervisorWorker(workerName);
      await mutateSupervisor();
    } catch (err) {
      setResetError(
        err instanceof Error ? err.message : "Reset failed — check logs."
      );
    }
  }

  // Merge real + synthetic entries
  const realEntries = logData?.entries ?? [];
  const syntheticEntries = realEntries.length === 0 ? decisionsToLog(decisions) : [];
  const allEntries = [...syntheticEntries, ...realEntries].slice(0, 100);

  // Auto-scroll to bottom on new entries
  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [allEntries.length, autoScroll]);

  const terminalGlow = stealth ? "none" : STATE_GLOW[engineState];
  const terminalBorder = stealth ? "#1e293b" : STATE_BORDER[engineState];

  return (
    <section>
      {/* Header */}
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <SectionHeader
          title="Nexus Agent Logic"
          subtitle="Autonomous orchestrator reasoning — live stream"
        />

        <div className="flex items-center gap-3">
          <EngineStateBadge engineState={engineState} stealth={stealth} />

          {/* Auto-scroll toggle */}
          <button
            onClick={() => setAutoScroll((v) => !v)}
            className="font-mono text-[9px] px-2 py-1 rounded"
            style={{
              color: autoScroll ? "#6366f1" : "#334155",
              border: `1px solid ${autoScroll ? "#6366f144" : "#1e293b"}`,
              background: "transparent",
              cursor: "pointer",
            }}
          >
            {autoScroll ? "⏬ AUTO" : "⏸ PAUSED"}
          </button>
        </div>
      </div>

      {/* Recovery banner — only when at least one worker is not healthy */}
      {supervisorWorkers.some((w) => w.status !== "healthy") && (
        <RecoveryBanner
          workers={supervisorWorkers}
          stealth={stealth}
          onReset={handleManualReset}
        />
      )}

      {/* Reset error toast */}
      {resetError && (
        <div
          className="font-mono text-[10px] px-3 py-1.5 rounded mb-2"
          style={{
            color:      "#ef4444",
            background: "#ef444418",
            border:     "1px solid #ef444444",
          }}
        >
          ✗ {resetError}
        </div>
      )}

      {/* Terminal window */}
      <div
        className="rounded-xl overflow-hidden transition-all duration-500"
        style={{
          border: `1px solid ${terminalBorder}`,
          boxShadow: terminalGlow === "none"
            ? "0 0 20px #00000066"
            : `0 0 20px #00000066, 0 0 40px ${terminalGlow}`,
          transition: "box-shadow 0.5s ease, border-color 0.5s ease",
        }}
      >
        {/* Title bar */}
        <div
          className="flex items-center gap-2 px-3 py-2"
          style={{
            background: "#0a0e1a",
            borderBottom: `1px solid ${terminalBorder}`,
            transition: "border-color 0.5s ease",
          }}
        >
          {/* Traffic lights */}
          {["#ef4444", "#f59e0b", "#22c55e"].map((dotColor, i) => (
            <span
              key={i}
              className="rounded-full"
              style={{
                width: 8,
                height: 8,
                background: stealth ? "#1e293b" : dotColor,
                display: "inline-block",
              }}
            />
          ))}

          {/* Title */}
          <span
            className="font-mono text-[9px] ml-2 tracking-widest"
            style={{ color: stealth ? "#1e293b" : STATE_COLOR[engineState] }}
          >
            nexus-agent — {STATE_LABEL[engineState].toLowerCase()}
          </span>

          {/* Right: entry count */}
          <span className="ml-auto font-mono text-[8px]" style={{ color: "#1e293b" }}>
            {allEntries.length} entries
          </span>
        </div>

        {/* Log content */}
        <div
          ref={scrollRef}
          onScroll={(e) => {
            const el = e.currentTarget;
            const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 20;
            setAutoScroll(atBottom);
          }}
          className="flex flex-col gap-0.5 p-4 overflow-y-auto"
          style={{
            background: "#030810",
            height: "320px",
            fontFamily: "var(--font-mono), 'Courier New', monospace",
          }}
        >
          {/* Welcome line */}
          <div
            className="font-mono text-[10px] mb-2 pb-2"
            style={{
              color: stealth ? "#1e293b" : STATE_COLOR[engineState],
              borderBottom: `1px solid #0f172a`,
              textShadow: stealth ? "none" : `0 0 8px ${STATE_COLOR[engineState]}44`,
            }}
          >
            {`> Nexus Autonomous Orchestrator v2.0 — ${new Date().toLocaleDateString()}`}
          </div>

          {allEntries.length === 0 ? (
            <span className="font-mono text-[11px]" style={{ color: "#1e293b" }}>
              Waiting for agent activity... (runs every 5 minutes)
            </span>
          ) : (
            allEntries.map((entry, i) => (
              <LogLine
                key={i}
                entry={entry}
                engineState={engineState}
                stealth={stealth}
              />
            ))
          )}

          {/* Blinking cursor */}
          <span
            className="font-mono text-[11px] select-none"
            style={{
              color: stealth ? "#1e293b" : STATE_COLOR[engineState],
              animation: "rgb-pulse 1s step-end infinite",
            }}
          >
            ▮
          </span>
        </div>

        {/* Status bar */}
        <div
          className="flex items-center justify-between px-3 py-1.5 flex-wrap gap-1"
          style={{
            background: "#0a0e1a",
            borderTop: `1px solid ${terminalBorder}`,
          }}
        >
          <span className="font-mono text-[8px]" style={{ color: "#1e293b" }}>
            INTERVAL: 5 min
          </span>
          <span className="font-mono text-[8px]" style={{ color: "#1e293b" }}>
            HITL THRESHOLD: 70%
          </span>

          {/* Supervisor recovery indicator */}
          {!stealth && supervisorWorkers.length > 0 && (() => {
            const anyRecovering = supervisorWorkers.some((w) => w.status === "recovering");
            const anyCritical   = supervisorWorkers.some((w) => w.status === "critical");
            if (!anyRecovering && !anyCritical) return null;
            const color = anyCritical ? "#ef4444" : "#f59e0b";
            const label = anyCritical ? "CRITICAL" : "RECOVERY";
            return (
              <span
                className="font-mono text-[8px] font-bold px-1.5 py-0.5 rounded"
                style={{
                  color,
                  background: `${color}18`,
                  border:     `1px solid ${color}44`,
                  animation:  "rgb-pulse 0.8s infinite",
                }}
              >
                ⚠ SUPERVISOR: {label}
              </span>
            );
          })()}

          <span
            className="font-mono text-[8px]"
            style={{ color: stealth ? "#1e293b" : STATE_COLOR[engineState] }}
          >
            {engineStateData?.updated_at
              ? `LAST: ${new Date(engineStateData.updated_at).toLocaleTimeString()}`
              : "LAST: —"}
          </span>
        </div>
      </div>

      <style>{`
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </section>
  );
}
