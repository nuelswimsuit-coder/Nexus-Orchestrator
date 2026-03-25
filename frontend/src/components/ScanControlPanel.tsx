"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import useSWR from "swr";
import {
  API_BASE,
  swrFetcher,
  triggerScan,
  type ScanLogLine,
  type ScanStatusResponse,
  type ScanHistoryEntry,
  type ScanHistoryResponse,
} from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

function formatTs(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString("he-IL", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}

function formatAgo(iso: string | null): string {
  if (!iso) return "—";
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 60) return `${Math.round(diff)}s ago`;
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  return `${Math.round(diff / 3600)}h ago`;
}

const LEVEL_COLOR: Record<string, string> = {
  ok: "#4ade80",
  info: "#94a3b8",
  warn: "#fbbf24",
  error: "#f87171",
};

const LEVEL_PREFIX: Record<string, string> = {
  ok: "✓",
  info: "›",
  warn: "⚠",
  error: "✗",
};

function LogLine({ line }: { line: ScanLogLine }) {
  const color = LEVEL_COLOR[line.level] ?? "#94a3b8";
  const prefix = LEVEL_PREFIX[line.level] ?? "·";
  return (
    <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.18rem", lineHeight: 1.4 }}>
      <span style={{ color: "#334155", minWidth: 64, flexShrink: 0, fontSize: "0.6rem" }}>
        {formatTs(line.ts)}
      </span>
      <span style={{ color, minWidth: 12, flexShrink: 0 }}>{prefix}</span>
      <span style={{ color, flex: 1 }}>
        {line.msg}
        {line.detail && (
          <span style={{ color: "#475569", fontSize: "0.62rem", marginLeft: "0.4rem" }}>
            {line.detail}
          </span>
        )}
      </span>
    </div>
  );
}

export default function ScanControlPanel() {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";

  const [lines, setLines] = useState<ScanLogLine[]>([]);
  const [running, setRunning] = useState(false);
  const [runMsg, setRunMsg] = useState<string | null>(null);
  const [showHistory, setShowHistory] = useState(false);
  const [sseActive, setSseActive] = useState(false);
  const logRef = useRef<HTMLDivElement>(null);
  const esRef = useRef<EventSource | null>(null);

  const { data: status, mutate: mutateStatus } = useSWR<ScanStatusResponse>(
    "/api/scan/status",
    swrFetcher<ScanStatusResponse>,
    { refreshInterval: running ? 1500 : 5000 },
  );

  const { data: histData } = useSWR<ScanHistoryResponse>(
    showHistory ? "/api/scan/history?limit=10" : null,
    swrFetcher<ScanHistoryResponse>,
    { refreshInterval: 15_000 },
  );

  // Auto-scroll log to bottom
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [lines]);

  // Sync running state from status
  useEffect(() => {
    if (status?.phase === "running") {
      setRunning(true);
    } else if (status?.phase === "done" || status?.phase === "error" || status?.phase === "idle") {
      setRunning(false);
    }
  }, [status?.phase]);

  const openSseStream = useCallback(() => {
    if (esRef.current) {
      esRef.current.close();
    }
    setLines([]);
    const es = new EventSource(`${API_BASE}/api/scan/stream`);
    esRef.current = es;
    setSseActive(true);

    es.onmessage = (ev) => {
      try {
        const line = JSON.parse(ev.data) as ScanLogLine;
        setLines((prev) => [...prev.slice(-200), line]);
      } catch {
        // keep-alive ping
      }
    };

    es.onerror = () => {
      setSseActive(false);
    };
  }, []);

  const closeSseStream = useCallback(() => {
    esRef.current?.close();
    esRef.current = null;
    setSseActive(false);
  }, []);

  // Cleanup on unmount
  useEffect(() => () => { esRef.current?.close(); }, []);

  const handleRun = async () => {
    setRunning(true);
    setRunMsg(null);
    openSseStream();
    try {
      const r = await triggerScan({ force: true });
      setRunMsg(r.message);
      await mutateStatus();
    } catch (e) {
      setRunMsg(he ? "שגיאה בהפעלת הסריקה" : "Failed to start scan");
      setRunning(false);
    }
  };

  const phase = status?.phase ?? "idle";
  const isRunning = phase === "running";

  const border = isHighContrast ? tokens.borderDefault : "#1e293b";
  const bg = isHighContrast ? tokens.surface1 : "linear-gradient(145deg, #060d1a, #080f1c)";
  const mono = "var(--font-mono)";
  const cyan = "#22d3ee";
  const green = "#4ade80";
  const amber = "#fbbf24";
  const red = "#f87171";
  const muted = isHighContrast ? tokens.textMuted : "#475569";
  const text = isHighContrast ? tokens.textPrimary : "#e2e8f0";

  const phaseColor =
    phase === "running" ? amber :
    phase === "done" ? green :
    phase === "error" ? red :
    muted;

  const phaseLabel =
    phase === "running" ? (he ? "רץ…" : "RUNNING…") :
    phase === "done" ? (he ? "הושלם" : "DONE") :
    phase === "error" ? (he ? "שגיאה" : "ERROR") :
    (he ? "מוכן" : "IDLE");

  return (
    <div
      style={{
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 14,
        overflow: "hidden",
        boxShadow: "0 8px 32px #00000066",
        fontFamily: mono,
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: "0.55rem 1rem",
          borderBottom: `1px solid ${border}`,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          background: isHighContrast ? tokens.surface2 : "#0a1628",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
          <span
            style={{
              fontSize: "0.6rem",
              color: cyan,
              textTransform: "uppercase",
              letterSpacing: "0.18em",
              fontWeight: 700,
            }}
          >
            {he ? "מרכז שליטה — סריקה ואוטומציה" : "SCAN & AUTOMATION"}
          </span>
          <span
            style={{
              fontSize: "0.58rem",
              padding: "1px 7px",
              borderRadius: 4,
              background: phase === "running" ? "#451a03" : phase === "done" ? "#0e4429" : "#0f172a",
              color: phaseColor,
              border: `1px solid ${phaseColor}44`,
              animation: phase === "running" ? "pulse 1.2s infinite" : "none",
            }}
          >
            {phaseLabel}
          </span>
        </div>
        <div style={{ display: "flex", gap: "0.4rem" }}>
          <button
            type="button"
            onClick={() => setShowHistory((v) => !v)}
            style={{
              fontFamily: mono,
              fontSize: "0.6rem",
              padding: "3px 8px",
              borderRadius: 5,
              border: `1px solid ${border}`,
              background: showHistory ? "#1e293b" : "transparent",
              color: muted,
              cursor: "pointer",
            }}
          >
            {he ? "היסטוריה" : "HISTORY"}
          </button>
          {sseActive ? (
            <button
              type="button"
              onClick={closeSseStream}
              style={{
                fontFamily: mono,
                fontSize: "0.6rem",
                padding: "3px 8px",
                borderRadius: 5,
                border: `1px solid ${green}44`,
                background: "#0e2d1a",
                color: green,
                cursor: "pointer",
              }}
            >
              ● LIVE
            </button>
          ) : (
            <button
              type="button"
              onClick={openSseStream}
              style={{
                fontFamily: mono,
                fontSize: "0.6rem",
                padding: "3px 8px",
                borderRadius: 5,
                border: `1px solid ${border}`,
                background: "transparent",
                color: muted,
                cursor: "pointer",
              }}
            >
              ○ {he ? "חבר לוג" : "CONNECT"}
            </button>
          )}
        </div>
      </div>

      {/* Stats row */}
      {status && (
        <div
          style={{
            padding: "0.6rem 1rem",
            borderBottom: `1px solid ${border}`,
            display: "flex",
            gap: "1.5rem",
            flexWrap: "wrap",
            background: isHighContrast ? tokens.accentSubtle : "#070e1c",
          }}
        >
          <StatChip
            label={he ? "נודים מחוברים" : "Nodes online"}
            value={`${status.nodes_online} / ${status.nodes_found}`}
            color={status.nodes_online > 0 ? green : amber}
          />
          <StatChip
            label={he ? "משימות הורצו" : "Tasks queued"}
            value={String(status.tasks_enqueued)}
            color={cyan}
          />
          <StatChip
            label={he ? "שגיאות" : "Errors"}
            value={String(status.tasks_failed)}
            color={status.tasks_failed > 0 ? red : muted}
          />
          <StatChip
            label={he ? "תור ARQ" : "ARQ queue"}
            value={String(status.queue_depth)}
            color={text}
          />
          {status.started_at && (
            <StatChip
              label={he ? "הסריקה האחרונה" : "Last run"}
              value={formatAgo(status.started_at)}
              color={muted}
            />
          )}
        </div>
      )}

      {/* Terminal log */}
      <div
        ref={logRef}
        style={{
          padding: "0.65rem 0.9rem",
          minHeight: 180,
          maxHeight: 320,
          overflowY: "auto",
          fontSize: "0.68rem",
          fontFamily: mono,
          background: isHighContrast ? tokens.surface1 : "#04080f",
          lineHeight: 1.5,
        }}
      >
        {lines.length === 0 && !isRunning && (
          <div style={{ color: muted }}>
            {he
              ? "לחץ RUN SCAN כדי להתחיל סריקה מלאה של המערכת…"
              : "Press RUN SCAN to start a full system scan…"}
          </div>
        )}
        {lines.map((line, i) => (
          <LogLine key={i} line={line} />
        ))}
        {isRunning && lines.length === 0 && (
          <div style={{ color: amber }}>
            <span style={{ animation: "pulse 1s infinite" }}>⟳</span>
            {" "}{he ? "מתחבר לזרם הלוג…" : "Connecting to log stream…"}
          </div>
        )}
      </div>

      {/* Errors */}
      {status?.errors && status.errors.length > 0 && (
        <div
          style={{
            padding: "0.5rem 0.9rem",
            borderTop: `1px solid #7f1d1d`,
            background: "#1c0a0a",
          }}
        >
          {status.errors.map((e, i) => (
            <div key={i} style={{ fontSize: "0.65rem", color: red, marginBottom: "0.15rem" }}>
              ✗ {e}
            </div>
          ))}
        </div>
      )}

      {/* Action bar */}
      <div
        style={{
          padding: "0.65rem 1rem",
          borderTop: `1px solid ${border}`,
          display: "flex",
          alignItems: "center",
          gap: "0.75rem",
          flexWrap: "wrap",
          background: isHighContrast ? tokens.surface2 : "#070e1c",
        }}
      >
        <button
          type="button"
          disabled={isRunning}
          onClick={() => void handleRun()}
          style={{
            fontFamily: mono,
            fontSize: "0.72rem",
            fontWeight: 700,
            padding: "8px 20px",
            borderRadius: 8,
            border: `1px solid ${isRunning ? amber + "44" : cyan + "66"}`,
            background: isRunning
              ? `${amber}12`
              : `${cyan}18`,
            color: isRunning ? amber : cyan,
            cursor: isRunning ? "wait" : "pointer",
            letterSpacing: "0.1em",
            transition: "all 0.15s",
          }}
        >
          {isRunning
            ? (he ? "⟳ רץ…" : "⟳ RUNNING…")
            : (he ? "▶ הרץ סריקה מלאה" : "▶ RUN FULL SCAN")}
        </button>

        <button
          type="button"
          onClick={() => { setLines([]); setRunMsg(null); }}
          style={{
            fontFamily: mono,
            fontSize: "0.62rem",
            padding: "6px 12px",
            borderRadius: 7,
            border: `1px solid ${border}`,
            background: "transparent",
            color: muted,
            cursor: "pointer",
          }}
        >
          {he ? "נקה לוג" : "CLEAR"}
        </button>

        {runMsg && (
          <span style={{ fontSize: "0.65rem", color: muted, flex: 1 }}>
            {runMsg}
          </span>
        )}
      </div>

      {/* History panel */}
      {showHistory && (
        <div
          style={{
            borderTop: `1px solid ${border}`,
            padding: "0.75rem 1rem",
            background: isHighContrast ? tokens.surface1 : "#060c18",
          }}
        >
          <div
            style={{
              fontSize: "0.58rem",
              color: muted,
              textTransform: "uppercase",
              letterSpacing: "0.14em",
              marginBottom: "0.5rem",
            }}
          >
            {he ? "היסטוריית סריקות" : "SCAN HISTORY"}
          </div>

          {!histData || histData.runs.length === 0 ? (
            <div style={{ fontSize: "0.68rem", color: muted }}>
              {he ? "אין היסטוריה עדיין" : "No history yet"}
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
              {histData.runs.map((run) => (
                <HistoryRow key={run.run_id} run={run} he={he} muted={muted} green={green} red={red} amber={amber} text={text} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function StatChip({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color: string;
}) {
  return (
    <div>
      <div
        style={{
          fontSize: "0.52rem",
          color: "#334155",
          textTransform: "uppercase",
          letterSpacing: "0.1em",
          marginBottom: 2,
        }}
      >
        {label}
      </div>
      <div style={{ fontSize: "0.78rem", fontWeight: 700, color }}>{value}</div>
    </div>
  );
}

function HistoryRow({
  run,
  he,
  muted,
  green,
  red,
  amber,
  text,
}: {
  run: ScanHistoryEntry;
  he: boolean;
  muted: string;
  green: string;
  red: string;
  amber: string;
  text: string;
}) {
  const phaseColor = run.phase === "done" ? green : run.phase === "error" ? red : amber;
  return (
    <div
      style={{
        display: "flex",
        gap: "0.75rem",
        alignItems: "baseline",
        fontSize: "0.65rem",
        padding: "0.3rem 0",
        borderBottom: "1px solid #0f172a",
        flexWrap: "wrap",
      }}
    >
      <span style={{ color: muted, minWidth: 55 }}>{formatTs(run.started_at)}</span>
      <span style={{ color: phaseColor, minWidth: 50, textTransform: "uppercase" }}>
        {run.phase}
      </span>
      <span style={{ color: text }}>
        {he ? "נודים:" : "nodes:"} {run.nodes_found}
      </span>
      <span style={{ color: "#22d3ee" }}>
        {he ? "משימות:" : "tasks:"} {run.tasks_enqueued}
      </span>
      {run.tasks_failed > 0 && (
        <span style={{ color: red }}>
          {he ? "שגיאות:" : "errors:"} {run.tasks_failed}
        </span>
      )}
      <span style={{ color: muted, marginLeft: "auto", fontSize: "0.58rem" }}>
        {run.run_id.slice(0, 8)}…
      </span>
    </div>
  );
}
