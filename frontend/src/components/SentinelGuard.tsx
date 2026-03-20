"use client";

import { useState } from "react";
import useSWR from "swr";
import {
  swrFetcher,
  getSentinelEvents,
  type SentinelStatusResponse,
  type SentinelEventsResponse,
  type SentinelMetricsResponse,
} from "@/lib/api";
import { useStealth } from "@/lib/stealth";

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const SENTINEL_PURPLE   = "#a855f7";
const SENTINEL_PURPLE_DIM = "#a855f722";
const SENTINEL_PURPLE_MID = "#a855f744";

const ACTION_LABEL_HE: Record<string, string> = {
  restart:                  "אתחול",
  stop:                     "עצירה",
  cooldown:                 "המתנה",
  preemptive_restart:       "אתחול מונע",
  reassign_polymarket_to_linux: "העברת עומס",
  "switch_to_backup":       "מעבר לגיבוי",
  cooldown_prediction_tasks: "צינון משימות",
  restart_worker_module:    "אתחול מודול",
};

const EVENT_TYPE_LABEL_HE: Record<string, string> = {
  ai_diagnosis:        "אבחון AI",
  preemptive_recovery: "התאוששות מונעת",
  failover:            "Failover",
  rpc_failover:        "RPC Failover",
};

function getActionColor(action: string): string {
  if (action.includes("stop"))    return "#ef4444";
  if (action.includes("restart")) return SENTINEL_PURPLE;
  if (action.includes("cooldown")) return "#f59e0b";
  if (action.includes("failover") || action.includes("reassign")) return "#06b6d4";
  return SENTINEL_PURPLE;
}

function formatTs(ts: string): string {
  if (!ts) return "--:--";
  try {
    return new Date(ts).toLocaleTimeString("he-IL", { hour12: false });
  } catch {
    return ts;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Status badge
// ─────────────────────────────────────────────────────────────────────────────

function SentinelStatusBadge({
  status,
  stealth,
}: {
  status: SentinelStatusResponse | undefined;
  stealth: boolean;
}) {
  const isActive  = status?.state === "active";
  const isOffline = !status || status.state === "offline" || status.state === "stopped";
  const color     = stealth ? "#334155" : isOffline ? "#334155" : SENTINEL_PURPLE;
  const label     = isOffline ? "OFFLINE" : "SENTINEL AI ACTIVE";
  const dotAnim   = isActive && !stealth;

  return (
    <div className="flex items-center gap-2">
      {/* Animated shield icon */}
      <span
        style={{
          fontSize: "1.1rem",
          filter: stealth ? "none" : isActive ? `drop-shadow(0 0 6px ${SENTINEL_PURPLE})` : "none",
          transition: "filter 0.3s ease",
        }}
      >
        {stealth ? "🛡" : isActive ? "🛡" : "🛡"}
      </span>

      {/* Pulse dot */}
      <span
        className="rounded-full shrink-0"
        style={{
          width: 7,
          height: 7,
          background: color,
          display: "inline-block",
          boxShadow: stealth ? "none" : isActive ? `0 0 8px ${SENTINEL_PURPLE}` : "none",
          animation: dotAnim ? "sentinel-pulse 1.4s ease-in-out infinite" : "none",
        }}
      />

      <span
        className="font-mono text-[9px] font-bold tracking-widest"
        style={{ color }}
      >
        {label}
      </span>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Metric pill
// ─────────────────────────────────────────────────────────────────────────────

function MetricPill({
  label,
  value,
  unit,
  threshold,
  stealth,
}: {
  label: string;
  value: number | null;
  unit: string;
  threshold: number;
  stealth: boolean;
}) {
  const exceeded = value !== null && value > threshold;
  const color    = stealth
    ? "#334155"
    : value === null
    ? "#334155"
    : exceeded
    ? "#ef4444"
    : "#22c55e";

  return (
    <div
      className="flex flex-col items-center justify-center px-3 py-2 rounded-lg"
      style={{
        background: stealth ? "transparent" : `${color}10`,
        border: `1px solid ${stealth ? "#1e293b" : `${color}33`}`,
        minWidth: "90px",
      }}
    >
      <span className="font-mono text-[8px] tracking-widest" style={{ color: stealth ? "#1e293b" : "#64748b" }}>
        {label}
      </span>
      <span
        className="font-mono text-[14px] font-bold"
        style={{
          color,
          textShadow: stealth || value === null ? "none" : `0 0 8px ${color}66`,
        }}
      >
        {value === null ? "—" : value > 9000 ? "ERR" : Math.round(value)}
        <span className="text-[9px] font-normal ml-0.5">{unit}</span>
      </span>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Crash Analysis Modal
// ─────────────────────────────────────────────────────────────────────────────

function CrashAnalysisModal({
  onClose,
  stealth,
}: {
  onClose: () => void;
  stealth: boolean;
}) {
  const { data, isLoading } = useSWR<SentinelEventsResponse>(
    "/api/sentinel/events?limit=3",
    swrFetcher<SentinelEventsResponse>,
    { refreshInterval: 10_000 }
  );

  const events = data?.events ?? [];

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      style={{ background: "rgba(0,0,0,0.75)", backdropFilter: "blur(4px)" }}
      onClick={onClose}
    >
      <div
        className="relative rounded-xl overflow-hidden"
        style={{
          width: "min(580px, 95vw)",
          maxHeight: "80vh",
          background: "#060b18",
          border: `1px solid ${stealth ? "#1e293b" : SENTINEL_PURPLE_MID}`,
          boxShadow: stealth ? "none" : `0 0 40px ${SENTINEL_PURPLE_DIM}, 0 25px 50px rgba(0,0,0,0.6)`,
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Modal header */}
        <div
          className="flex items-center justify-between px-5 py-4"
          style={{
            background: "#0a0e1a",
            borderBottom: `1px solid ${stealth ? "#1e293b" : SENTINEL_PURPLE_MID}`,
          }}
        >
          <div className="flex items-center gap-3">
            <span
              style={{
                fontSize: "1.2rem",
                filter: stealth ? "none" : `drop-shadow(0 0 6px ${SENTINEL_PURPLE})`,
              }}
            >
              🛡
            </span>
            <div>
              <p
                className="font-mono text-[11px] font-bold tracking-widest"
                style={{ color: stealth ? "#334155" : SENTINEL_PURPLE }}
              >
                סורק ארביטראז' — ניתוח קריסות
              </p>
              <p className="font-mono text-[9px]" style={{ color: "#475569" }}>
                3 אירועים אחרונים שנותחו ע"י Gemini AI — יומן אירועים
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="font-mono text-[12px] px-2 py-1 rounded transition-colors"
            style={{
              color: "#64748b",
              background: "transparent",
              border: "1px solid #1e293b",
              cursor: "pointer",
            }}
          >
            ✕
          </button>
        </div>

        {/* Modal body */}
        <div className="overflow-y-auto p-5 flex flex-col gap-4" style={{ maxHeight: "calc(80vh - 80px)" }}>
          {isLoading ? (
            <div className="flex justify-center py-8">
              <span
                className="font-mono text-[11px]"
                style={{
                  color: stealth ? "#1e293b" : SENTINEL_PURPLE,
                  animation: "sentinel-pulse 1s ease-in-out infinite",
                }}
              >
                ● טוען נתונים...
              </span>
            </div>
          ) : events.length === 0 ? (
            <div
              className="text-center py-8 font-mono text-[11px]"
              style={{ color: "#334155" }}
            >
              אין אירועים רשומים עדיין — המערכת פועלת בצורה תקינה.
            </div>
          ) : (
            events.map((event, i) => {
              const actionColor = stealth ? "#334155" : getActionColor(event.action_taken);
              const evtLabel = stealth ? EVENT_TYPE_LABEL_HE[event.event_type] ?? event.event_type : EVENT_TYPE_LABEL_HE[event.event_type] ?? event.event_type;
              const actLabel = ACTION_LABEL_HE[event.action_taken] ?? event.action_taken;

              return (
                <div
                  key={i}
                  className="rounded-lg p-4"
                  style={{
                    background: stealth ? "transparent" : `${actionColor}08`,
                    border: `1px solid ${stealth ? "#1e293b" : `${actionColor}33`}`,
                    direction: "rtl",
                  }}
                >
                  {/* Event header */}
                  <div className="flex items-center justify-between mb-2 flex-wrap gap-2">
                    <div className="flex items-center gap-2">
                      <span
                        className="font-mono text-[9px] font-bold px-2 py-0.5 rounded-full"
                        style={{
                          color: actionColor,
                          background: `${actionColor}18`,
                          border: `1px solid ${actionColor}44`,
                        }}
                      >
                        {evtLabel}
                      </span>
                      <span
                        className="font-mono text-[9px] font-bold px-2 py-0.5 rounded-full"
                        style={{
                          color: stealth ? "#334155" : "#f59e0b",
                          background: stealth ? "transparent" : "#f59e0b18",
                          border: `1px solid ${stealth ? "#1e293b" : "#f59e0b44"}`,
                        }}
                      >
                        פעולה: {actLabel}
                      </span>
                    </div>
                    <span className="font-mono text-[8px]" style={{ color: "#475569" }}>
                      {formatTs(event.ts)}
                    </span>
                  </div>

                  {/* Hebrew reason */}
                  <p
                    className="font-mono text-[11px] leading-relaxed mb-2"
                    style={{ color: stealth ? "#334155" : "#e2e8f0" }}
                  >
                    {event.reason_he || "—"}
                  </p>

                  {/* Trigger info */}
                  <p className="font-mono text-[9px]" style={{ color: "#475569", direction: "ltr" }}>
                    טריגר: <span style={{ color: stealth ? "#334155" : "#64748b" }}>{event.trigger}</span>
                    {event.metric_value > 0 && (
                      <span style={{ color: stealth ? "#334155" : "#94a3b8" }}>
                        {" "}· ערך: {Math.round(event.metric_value)}
                      </span>
                    )}
                  </p>

                  {/* AI English reason (collapsed hint) */}
                  {event.ai_reason_en && !stealth && (
                    <p
                      className="font-mono text-[9px] mt-1 italic"
                      style={{ color: "#334155", direction: "ltr" }}
                    >
                      AI: {event.ai_reason_en}
                    </p>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main SentinelGuard component
// ─────────────────────────────────────────────────────────────────────────────

export default function SentinelGuard() {
  const { stealth } = useStealth();
  const [modalOpen, setModalOpen] = useState(false);

  const { data: status } = useSWR<SentinelStatusResponse>(
    "/api/sentinel/status",
    swrFetcher<SentinelStatusResponse>,
    { refreshInterval: 5_000 }
  );

  const { data: metricsData } = useSWR<SentinelMetricsResponse>(
    "/api/sentinel/metrics?limit=1",
    swrFetcher<SentinelMetricsResponse>,
    { refreshInterval: 15_000 }
  );

  const { data: eventsData } = useSWR<SentinelEventsResponse>(
    "/api/sentinel/events?limit=20",
    swrFetcher<SentinelEventsResponse>,
    { refreshInterval: 8_000 }
  );

  const isActive       = status?.state === "active";
  const latestMetric   = metricsData?.metrics?.[metricsData.metrics.length - 1] ?? null;
  const recentEvents   = eventsData?.events ?? [];
  const criticalEvents = recentEvents.filter((e) => e.action_taken.includes("stop") || e.action_taken.includes("failover"));

  const windowsStatus  = status?.windows_worker_online;

  // Glow if active or critical
  const hasCritical    = criticalEvents.length > 0;
  const glowColor      = stealth ? "none" : hasCritical ? "#ef444422" : isActive ? `${SENTINEL_PURPLE}18` : "none";
  const borderColor    = stealth ? "#1e293b" : hasCritical ? "#ef444444" : isActive ? SENTINEL_PURPLE_MID : "#1e293b";

  return (
    <>
      <section
        className="rounded-xl p-5 transition-all duration-500"
        style={{
          background: "#070d1a",
          border: `1px solid ${borderColor}`,
          boxShadow: glowColor === "none"
            ? "0 0 20px #00000066"
            : `0 0 20px #00000066, 0 0 40px ${glowColor}`,
          transition: "box-shadow 0.5s ease, border-color 0.5s ease",
        }}
      >
        {/* ── Header ─────────────────────────────────────────────────────── */}
        <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
          <SentinelStatusBadge status={status} stealth={stealth} />

          <div className="flex items-center gap-2">
            {/* Event count badge */}
            {recentEvents.length > 0 && !stealth && (
              <span
                className="font-mono text-[9px] px-2 py-0.5 rounded-full"
                style={{
                  color: SENTINEL_PURPLE,
                  background: SENTINEL_PURPLE_DIM,
                  border: `1px solid ${SENTINEL_PURPLE_MID}`,
                }}
              >
                {recentEvents.length} אירועים
              </span>
            )}

            {/* Crash analysis button */}
            <button
              onClick={() => setModalOpen(true)}
              className="font-mono text-[9px] font-bold px-3 py-1.5 rounded-lg transition-all"
              style={{
                color: stealth ? "#334155" : SENTINEL_PURPLE,
                background: stealth ? "transparent" : SENTINEL_PURPLE_DIM,
                border: `1px solid ${stealth ? "#1e293b" : SENTINEL_PURPLE_MID}`,
                cursor: "pointer",
                boxShadow: stealth ? "none" : `0 0 10px ${SENTINEL_PURPLE_DIM}`,
              }}
            >
              🔍 ניתוח קריסות
            </button>
          </div>
        </div>

        {/* ── Metrics row ────────────────────────────────────────────────── */}
        <div className="flex flex-wrap gap-3 mb-4">
          <MetricPill
            label="BINANCE LATENCY"
            value={latestMetric?.latency_ms ?? status?.latency_ms ?? null}
            unit="ms"
            threshold={metricsData?.latency_threshold_ms ?? 2000}
            stealth={stealth}
          />
          <MetricPill
            label="RAM USAGE"
            value={latestMetric?.ram_pct ?? status?.ram_pct ?? null}
            unit="%"
            threshold={metricsData?.memory_threshold_pct ?? 90}
            stealth={stealth}
          />

          {/* Worker Windows status */}
          <div
            className="flex flex-col items-center justify-center px-3 py-2 rounded-lg"
            style={{
              background: stealth ? "transparent" : windowsStatus === false ? "#ef444410" : windowsStatus === true ? "#22c55e10" : "transparent",
              border: `1px solid ${stealth ? "#1e293b" : windowsStatus === false ? "#ef444433" : windowsStatus === true ? "#22c55e33" : "#1e293b"}`,
              minWidth: "90px",
            }}
          >
            <span className="font-mono text-[8px] tracking-widest" style={{ color: stealth ? "#1e293b" : "#64748b" }}>
              WIN WORKER
            </span>
            <span
              className="font-mono text-[11px] font-bold"
              style={{
                color: stealth ? "#334155" : windowsStatus === false ? "#ef4444" : windowsStatus === true ? "#22c55e" : "#334155",
              }}
            >
              {windowsStatus === null ? "—" : windowsStatus ? "ONLINE" : "OFFLINE"}
            </span>
          </div>

          {/* RPC status */}
          {status?.rpc_switched && !stealth && (
            <div
              className="flex flex-col items-center justify-center px-3 py-2 rounded-lg"
              style={{
                background: "#f59e0b10",
                border: "1px solid #f59e0b33",
                minWidth: "90px",
              }}
            >
              <span className="font-mono text-[8px] tracking-widest" style={{ color: "#64748b" }}>
                RPC
              </span>
              <span className="font-mono text-[10px] font-bold" style={{ color: "#f59e0b" }}>
                BACKUP
              </span>
            </div>
          )}
        </div>

        {/* ── Recent events feed ─────────────────────────────────────────── */}
        <div
          className="rounded-lg overflow-hidden"
          style={{
            border: `1px solid ${stealth ? "#1e293b" : "#0f172a"}`,
            background: "#030810",
          }}
        >
          {/* Feed header */}
          <div
            className="flex items-center justify-between px-3 py-1.5"
            style={{
              background: "#0a0e1a",
              borderBottom: `1px solid ${stealth ? "#1e293b" : "#0f172a"}`,
            }}
          >
            <span className="font-mono text-[9px] tracking-widest" style={{ color: stealth ? "#1e293b" : SENTINEL_PURPLE }}>
              SENTINEL LOG
            </span>
            <span className="font-mono text-[8px]" style={{ color: "#1e293b" }}>
              {recentEvents.length} entries
            </span>
          </div>

          {/* Feed entries */}
          <div className="flex flex-col gap-0.5 p-3" style={{ maxHeight: "160px", overflowY: "auto" }}>
            {recentEvents.length === 0 ? (
              <span className="font-mono text-[10px]" style={{ color: "#1e293b" }}>
                {isActive ? "ממתין לאירועים..." : "Sentinel AI לא פעיל"}
              </span>
            ) : (
              recentEvents.slice(0, 8).map((event, i) => {
                const actionColor = stealth ? "#334155" : getActionColor(event.action_taken);
                const actLabel    = ACTION_LABEL_HE[event.action_taken] ?? event.action_taken;
                return (
                  <div key={i} className="flex items-start gap-2 font-mono text-[10px] leading-relaxed">
                    <span className="shrink-0" style={{ color: "#1e293b" }}>
                      {formatTs(event.ts)}
                    </span>
                    <span
                      className="shrink-0 font-bold"
                      style={{
                        color: actionColor,
                        textShadow: stealth ? "none" : `0 0 6px ${actionColor}44`,
                      }}
                    >
                      ◆
                    </span>
                    <span
                      style={{
                        color: stealth ? "#334155" : SENTINEL_PURPLE,
                        wordBreak: "break-word",
                      }}
                    >
                      [{actLabel}]
                    </span>
                    <span style={{ color: stealth ? "#1e293b" : "#94a3b8", direction: "rtl", flex: 1, textAlign: "right" }}>
                      {event.reason_he?.slice(0, 80)}{event.reason_he?.length > 80 ? "..." : ""}
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>
      </section>

      {/* ── Modal ────────────────────────────────────────────────────────── */}
      {modalOpen && (
        <CrashAnalysisModal
          onClose={() => setModalOpen(false)}
          stealth={stealth}
        />
      )}

      <style>{`
        @keyframes sentinel-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%       { opacity: 0.5; transform: scale(1.15); }
        }
      `}</style>
    </>
  );
}
