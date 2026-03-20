"use client";

/**
 * TopologyVisual — 2D Technical Network Schematic
 *
 * Premium financial-terminal network diagram with computer/server icons,
 * live WebSocket log streams, self-healing status coloring, and RTL support.
 * Log lines are color-coded: [SUCCESS]=green, [REPAIRING]=amber, [CRITICAL]=red+flash.
 */

import { useEffect, useRef, useState } from "react";
import useSWR from "swr";
import { swrFetcher, API_BASE } from "@/lib/api";
import { useNexus } from "@/lib/nexus-context";
import { useStealth } from "@/lib/stealth";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import type { ClusterStatusResponse } from "@/lib/api";

// ── WebSocket log stream ──────────────────────────────────────────────────────

interface LogLine {
  timestamp: string;
  level: string;
  message: string;
  id: number;
}

function useWebSocketLogs(url: string, maxLines = 18): LogLine[] {
  const [logs, setLogs] = useState<LogLine[]>([]);
  const wsRef  = useRef<WebSocket | null>(null);
  const lineId = useRef(0);

  useEffect(() => {
    try {
      wsRef.current = new WebSocket(url.replace("http", "ws").replace("https", "wss"));
      wsRef.current.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);
          const entry: LogLine = {
            timestamp: new Date().toLocaleTimeString("en-GB", { hour12: false }),
            level:     data.level   || "INFO",
            message:   data.message || data.event || "event",
            id:        lineId.current++,
          };
          setLogs(prev => [...prev.slice(-(maxLines - 1)), entry]);
        } catch { /* skip malformed */ }
      };
    } catch { /* WS unavailable */ }
    return () => wsRef.current?.close();
  }, [url, maxLines]);

  return logs;
}

// ── Self-repair state machine ─────────────────────────────────────────────────
// Tracks: none → error → repairing → resolved → none

type RepairPhase = "none" | "error" | "repairing" | "resolved";

function useRepairState(hasError: boolean): RepairPhase {
  const [phase, setPhase] = useState<RepairPhase>("none");
  const timer = useRef<ReturnType<typeof setTimeout>>();

  useEffect(() => {
    if (hasError) {
      clearTimeout(timer.current);
      if (phase === "none" || phase === "resolved") {
        setPhase("error");
        timer.current = setTimeout(() => setPhase("repairing"), 10_000);
      }
    } else if (phase === "error" || phase === "repairing") {
      clearTimeout(timer.current);
      setPhase("resolved");
      timer.current = setTimeout(() => setPhase("none"), 6_000);
    }
    return () => clearTimeout(timer.current);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasError]);

  return phase;
}

// ── SVG Icons ─────────────────────────────────────────────────────────────────

function ServerRackIcon({ size = 36, color }: { size?: number; color: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
      stroke={color} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"
      style={{ display: "block" }}
    >
      <rect x="2" y="2" width="20" height="8" rx="2" />
      <rect x="2" y="14" width="20" height="8" rx="2" />
      <circle cx="18" cy="6"  r="1" fill={color} stroke="none" />
      <circle cx="18" cy="18" r="1" fill={color} stroke="none" />
      <line x1="6" y1="6"  x2="13" y2="6"  strokeWidth="1.2" />
      <line x1="6" y1="18" x2="13" y2="18" strokeWidth="1.2" />
    </svg>
  );
}

function MonitorIcon({ size = 34, color }: { size?: number; color: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none"
      stroke={color} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"
      style={{ display: "block" }}
    >
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <line x1="8"  y1="21" x2="16" y2="21" />
      <line x1="12" y1="17" x2="12" y2="21" />
      <circle cx="12" cy="10" r="2" fill={color} stroke="none" opacity="0.6" />
    </svg>
  );
}

// ── Log line style helper — keyword-based coloring ───────────────────────────

function getLogLineStyle(
  ln: LogLine,
  stealth: boolean,
  isHighContrast: boolean,
): { color: string; animation: string; fontWeight: string; rowBg?: string } {
  if (stealth && !isHighContrast) {
    return { color: "#2a3450", animation: "none", fontWeight: "400" };
  }
  const upper = `${ln.message} ${ln.level}`.toUpperCase();

  if (upper.includes("[CRITICAL]") || upper.includes("CRITICAL")) {
    return {
      color:      isHighContrast ? "#AA0000" : "#ff3355",
      animation:  "critical-flash 0.85s step-end infinite",
      fontWeight: "700",
      rowBg:      isHighContrast ? "rgba(170,0,0,0.06)" : "rgba(255,30,60,0.08)",
    };
  }
  if (upper.includes("[REPAIRING]") || upper.includes("REPAIRING") || upper.includes("ACTION:")) {
    return {
      color:      isHighContrast ? "#7c4e00" : "#ffb800",
      animation:  "none",
      fontWeight: "600",
      rowBg:      isHighContrast ? "rgba(124,78,0,0.05)" : "rgba(255,184,0,0.06)",
    };
  }
  if (upper.includes("[SUCCESS]") || upper.includes("SUCCESS")) {
    return {
      color:      isHighContrast ? "#0a6640" : "#00e096",
      animation:  "none",
      fontWeight: "600",
      rowBg:      isHighContrast ? "rgba(10,102,64,0.05)" : "rgba(0,224,150,0.06)",
    };
  }
  if (upper.includes("[RESOLVED]") || upper.includes("RESOLVED")) {
    return {
      color:      isHighContrast ? "#166534" : "#10b981",
      animation:  "none",
      fontWeight: "700",
      rowBg:      isHighContrast ? "rgba(22,101,52,0.06)" : "rgba(16,185,129,0.07)",
    };
  }

  // Standard level coloring
  const color =
    ln.level === "ERROR"   ? (isHighContrast ? "#B91C1C" : "#ef4444") :
    ln.level === "WARNING" ? (isHighContrast ? "#92400E" : "#f59e0b") :
    (isHighContrast ? "#166534" : "#10b981");
  return { color, animation: "none", fontWeight: "400" };
}

// ── Log panel ─────────────────────────────────────────────────────────────────

function LogPanel({
  title, logs, stealth, accent, isHighContrast, terminalBg, borderSubtle, textMuted,
}: {
  title: string;
  logs: LogLine[];
  stealth: boolean;
  accent: string;
  isHighContrast: boolean;
  terminalBg: string;
  borderSubtle: string;
  textMuted: string;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [logs]);

  const panelBg = isHighContrast
    ? "rgba(240,242,245,0.97)"
    : stealth ? "rgba(20,24,36,0.6)" : "rgba(10,14,22,0.7)";
  const panelBorder = isHighContrast
    ? `2px solid ${borderSubtle}`
    : `1.5px solid ${stealth ? "#21293d" : `${accent}25`}`;
  const headerBorder = isHighContrast
    ? `1px solid ${borderSubtle}`
    : `1px solid ${stealth ? "#21293d" : `${accent}18`}`;

  return (
    <div style={{
      flex: 1,
      background: panelBg,
      border: panelBorder,
      borderRadius: "10px",
      overflow: "hidden",
      display: "flex",
      flexDirection: "column",
      minWidth: 0,
      boxShadow: isHighContrast ? "0 2px 10px rgba(0,0,0,0.12)" : "none",
    }}>
      {/* Panel header */}
      <div style={{
        padding: "7px 14px",
        borderBottom: headerBorder,
        display: "flex",
        alignItems: "center",
        gap: "7px",
        flexShrink: 0,
        background: isHighContrast ? "rgba(0,68,187,0.04)" : "transparent",
      }}>
        <span style={{
          width: 7, height: 7, borderRadius: "50%",
          background: logs.length > 0 && !stealth ? accent : isHighContrast ? "#9CA3AF" : "#2a3450",
          display: "inline-block",
          boxShadow: logs.length > 0 && !stealth && !isHighContrast ? `0 0 7px ${accent}` : "none",
        }} />
        <span style={{
          fontFamily: "var(--font-sans)",
          fontSize: "0.75rem",
          fontWeight: 700,
          letterSpacing: "0.1em",
          color: stealth ? "#2a3450" : accent,
          textTransform: "uppercase",
        }}>
          {title}
        </span>
      </div>

      {/* Log lines */}
      <div
        ref={scrollRef}
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "7px 12px",
          fontFamily: "var(--font-mono)",
          fontSize: "0.75rem",
          lineHeight: "1.65",
          background: isHighContrast ? terminalBg : "transparent",
        }}
      >
        {logs.length === 0 ? (
          <span style={{ color: isHighContrast ? textMuted : stealth ? "#21293d" : "#2a3450" }}>
            {">"} waiting...
          </span>
        ) : (
          logs.map((ln, idx) => {
            const isNew = idx === logs.length - 1;
            const lineStyle = getLogLineStyle(ln, stealth, isHighContrast);
            return (
              <div
                key={ln.id}
                style={{
                  display: "flex",
                  gap: "9px",
                  opacity: 0.4 + (idx / logs.length) * 0.6,
                  animation: lineStyle.animation !== "none"
                    ? lineStyle.animation
                    : (isNew && !stealth && !isHighContrast ? "log-flash 0.4s ease-out" : "none"),
                  borderRadius: "4px",
                  padding: "1px 4px",
                  marginBottom: "1px",
                  background: lineStyle.rowBg ?? "transparent",
                }}
              >
                <span style={{
                  color: isHighContrast ? textMuted : stealth ? "#21293d" : "#1a2030",
                  flexShrink: 0,
                  fontSize: "0.7rem",
                }}>
                  {ln.timestamp}
                </span>
                <span style={{
                  color: lineStyle.color,
                  flexShrink: 0,
                  minWidth: "40px",
                  fontWeight: lineStyle.fontWeight,
                }}>
                  {ln.level.slice(0, 4)}
                </span>
                <span style={{
                  color: lineStyle.color,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  fontWeight: lineStyle.fontWeight,
                  opacity: 0.9,
                }}>
                  {ln.message}
                </span>
              </div>
            );
          })
        )}
        {logs.length > 0 && !stealth && (
          <span style={{
            color: accent,
            animation: isHighContrast ? "none" : "terminal-blink 1s step-end infinite",
          }}>
            {"\u2588"}
          </span>
        )}
      </div>
    </div>
  );
}

// ── Node box ──────────────────────────────────────────────────────────────────

function NodeBox({
  label, id, role, online, cpu, stealth, accent, isRTL, isHighContrast,
  textSecondary, textMuted, danger,
}: {
  label: string;
  id: string;
  role: "master" | "worker";
  online: boolean;
  cpu?: number;
  stealth: boolean;
  accent: string;
  isRTL: boolean;
  isHighContrast: boolean;
  textSecondary: string;
  textMuted: string;
  danger: string;
}) {
  const statusColor = online
    ? (stealth && !isHighContrast) ? "#2a3450" : (role === "master" ? accent : (isHighContrast ? "#166534" : "#10b981"))
    : (stealth && !isHighContrast) ? "#2a3450" : danger;

  const nodeBg = isHighContrast
    ? (online ? "#FFFFFF" : "#FEF2F2")
    : stealth ? "rgba(20,24,36,0.7)" : (online ? `rgba(14,165,233,0.05)` : "rgba(239,68,68,0.05)");
  const nodeBorder = isHighContrast
    ? (online ? `2px solid ${accent}` : `2px solid ${danger}`)
    : `1.5px solid ${stealth ? "#21293d" : online ? `${accent}35` : "rgba(239,68,68,0.3)"}`;
  const nodeBoxShadow = isHighContrast
    ? (online ? `0 2px 10px rgba(0,0,0,0.12), 0 0 0 1px ${accent}20` : "0 2px 10px rgba(185,28,28,0.18)")
    : (!stealth && online ? `0 0 0 1px ${accent}12, 0 4px 20px rgba(0,0,0,0.35)` : "0 4px 16px rgba(0,0,0,0.2)");

  const iconColor = (stealth && !isHighContrast) ? "#21293d" : statusColor;

  return (
    <div style={{
      background: nodeBg,
      border: nodeBorder,
      borderRadius: "10px",
      padding: "12px 16px 10px",
      minWidth: role === "master" ? "200px" : "148px",
      position: "relative",
      transition: "border-color 0.3s, background 0.25s",
      boxShadow: nodeBoxShadow,
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      gap: "6px",
    }}>
      {/* Status indicator — top-right corner */}
      <span style={{
        position: "absolute",
        top: "9px",
        [isRTL ? "left" : "right"]: "9px",
        width: 8, height: 8,
        borderRadius: "50%",
        background: statusColor,
        display: "block",
        boxShadow: (!stealth && !isHighContrast && online) ? `0 0 8px ${statusColor}` : "none",
        animation: (!stealth && !isHighContrast && online) ? "rgb-pulse 2s infinite" : "none",
      }} />

      {/* Computer/Server icon */}
      <div style={{
        marginTop: "2px",
        opacity: (stealth && !isHighContrast) ? 0.3 : 1,
        filter: (!stealth && !isHighContrast && online)
          ? `drop-shadow(0 0 6px ${iconColor}60)`
          : "none",
      }}>
        {role === "master"
          ? <ServerRackIcon size={36} color={iconColor} />
          : <MonitorIcon size={32} color={iconColor} />
        }
      </div>

      {/* Role badge */}
      <div style={{
        fontFamily: "var(--font-sans)",
        fontSize: "0.65rem",
        fontWeight: 700,
        letterSpacing: "0.14em",
        color: (stealth && !isHighContrast) ? "#2a3450" : accent,
        textTransform: "uppercase",
        textAlign: "center",
      }}>
        {label}
      </div>

      {/* Node ID */}
      <div style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.78rem",
        color: isHighContrast ? textSecondary : (stealth ? "#21293d" : "#6b8fab"),
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
        maxWidth: "155px",
        textAlign: "center",
      }}>
        {id}
      </div>

      {/* Status + CPU row */}
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: "8px",
        flexDirection: isRTL ? "row-reverse" : "row",
        justifyContent: "center",
        width: "100%",
      }}>
        <span style={{
          fontFamily: "var(--font-sans)",
          fontSize: "0.7rem",
          fontWeight: 700,
          color: statusColor,
          letterSpacing: "0.04em",
        }}>
          {online ? "\u25CF ONLINE" : "\u25CB OFFLINE"}
        </span>
        {cpu !== undefined && online && (
          <span style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            color: isHighContrast ? textMuted : (stealth ? "#21293d" : "#6b8fab"),
          }}>
            {cpu.toFixed(1)}% CPU
          </span>
        )}
      </div>
    </div>
  );
}

// ── SVG connector lines ───────────────────────────────────────────────────────

function ConnectorSVG({
  connections, stealth, accent, active,
}: {
  connections: { x1: number; y1: number; x2: number; y2: number }[];
  stealth: boolean;
  accent: string;
  active: boolean;
}) {
  const lineColor = stealth ? "#21293d" : `${accent}45`;
  const dashColor = stealth ? "none" : accent;

  return (
    <svg
      style={{ position: "absolute", inset: 0, pointerEvents: "none", overflow: "visible" }}
      width="100%" height="100%"
    >
      <defs>
        <marker id="dot" markerWidth="4" markerHeight="4" refX="2" refY="2">
          <circle cx="2" cy="2" r="1.5" fill={lineColor} />
        </marker>
      </defs>
      {connections.map((c, i) => (
        <g key={i}>
          <line
            x1={c.x1} y1={c.y1} x2={c.x2} y2={c.y2}
            stroke={lineColor}
            strokeWidth="1.5"
            strokeDasharray="4 3"
          />
          {active && !stealth && (
            <line
              x1={c.x1} y1={c.y1} x2={c.x2} y2={c.y2}
              stroke={dashColor}
              strokeWidth="2"
              strokeDasharray="6 20"
              strokeLinecap="round"
              style={{ animation: "flow-dash 1.4s linear infinite" }}
            />
          )}
        </g>
      ))}
    </svg>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function TopologyVisual() {
  const { stealth } = useStealth();
  const { cluster } = useNexus();
  const { t, isRTL, language } = useI18n();
  const { isHighContrast, tokens } = useTheme();

  const masterLogs = useWebSocketLogs(`${API_BASE}/ws/logs/master`, 18);
  const workerLogs = useWebSocketLogs(`${API_BASE}/ws/logs/worker`, 18);

  const { data: clusterData } = useSWR<ClusterStatusResponse>(
    "/api/cluster/status",
    swrFetcher<ClusterStatusResponse>,
    { refreshInterval: 5_000 }
  );

  const master  = clusterData?.nodes.find(n => n.role === "master") ?? null;
  const workers = clusterData?.nodes.filter(n => n.role === "worker") ?? [];

  const masterOnline = master?.online ?? false;
  const workerOnline = workers.some(w => w.online);
  const logActivity  = masterLogs.length + workerLogs.length;

  const hasSystemError = clusterData !== undefined && (!masterOnline || !workerOnline);
  const repairPhase = useRepairState(hasSystemError);

  const ACCENT = (stealth && !isHighContrast) ? "#21293d" : tokens.accent;

  const containerRef = useRef<HTMLDivElement>(null);

  // Hardcoded bilingual titles to guarantee visibility regardless of i18n state
  const topologyTitle    = language === "he" ? "טופולוגיית אשכול" : t("topology_title");
  const topologySubtitle = language === "he" ? "מיפוי פיזי של הרשת" : t("topology_subtitle");
  const masterNodeLabel  = language === "he" ? "צומת מאסטר" : t("widgets.master_node");
  const workerNodeLabel  = language === "he" ? "צומת מעבד (Worker)" : t("widgets.worker_node");
  const masterLogLabel   = language === "he" ? "יומן אירועים — מאסטר" : t("widgets.master_log");
  const workerLogLabel   = language === "he" ? "יומן אירועים — מעבד" : t("widgets.worker_log");
  const clusterHudLabel  = language === "he" ? "מצב אשכול" : t("widgets.cluster_hud");

  return (
    <div
      className="glass"
      style={{
        width: "100%",
        borderRadius: "18px",
        padding: "1.75rem",
        position: "relative",
        overflow: "hidden",
        border: isHighContrast
          ? "2.5px solid #000000"
          : `2px solid ${stealth ? "#21293d" : `${ACCENT}28`}`,
        boxShadow: isHighContrast
          ? "0 4px 20px rgba(0,0,0,0.15), 4px 4px 0 rgba(0,0,0,0.06)"
          : `0 0 0 1px ${ACCENT}08, 0 8px 48px rgba(0,0,0,0.6), 0 0 32px ${ACCENT}08`,
      }}
    >
      {/* Dot-grid background */}
      <div
        className="dot-grid"
        style={{
          position: "absolute",
          inset: 0,
          opacity: stealth ? 0.3 : isHighContrast ? 0.8 : 0.6,
          borderRadius: "inherit",
          pointerEvents: "none",
        }}
      />

      {/* Section header */}
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: "1rem",
        flexDirection: isRTL ? "row-reverse" : "row",
        marginBottom: "2rem",
        position: "relative",
      }}>
        {/* Accent rule */}
        <span style={{
          display: "inline-block",
          width: "5px",
          height: "36px",
          borderRadius: "3px",
          background: (stealth && !isHighContrast)
            ? "#21293d"
            : isHighContrast
              ? "#0044BB"
              : `linear-gradient(180deg, ${ACCENT} 0%, ${ACCENT}22 100%)`,
          flexShrink: 0,
          alignSelf: "center",
          boxShadow: (stealth || isHighContrast) ? "none" : `0 0 16px ${ACCENT}80`,
        }} />
        <div style={{
          display: "flex",
          flexDirection: "column",
          gap: "0.2rem",
          textAlign: isRTL ? "right" : "left",
        }}>
          <h2 style={{
            fontFamily: "var(--font-sans)",
            fontSize: "1.875rem",
            fontWeight: 900,
            letterSpacing: "0.04em",
            color: isHighContrast ? tokens.textPrimary : stealth ? "#2a3450" : "#e8f2ff",
            textTransform: "uppercase",
            margin: 0,
            lineHeight: 1.15,
            direction: isRTL ? "rtl" : "ltr",
          }}>
            {topologyTitle}
          </h2>
          <span style={{
            fontFamily: "var(--font-sans)",
            fontSize: "1.0rem",
            fontWeight: 500,
            color: isHighContrast ? tokens.textMuted : stealth ? "#21293d" : "#7da8cc",
            direction: isRTL ? "rtl" : "ltr",
          }}>
            {topologySubtitle}
          </span>
        </div>
      </div>

      {/* ── System Error / Self-Repair Banner ── */}
      {repairPhase !== "none" && !stealth && (() => {
        const phaseConfig = {
          error:     { bg: isHighContrast ? "#FFE4E4" : "rgba(255,51,85,0.10)", border: isHighContrast ? "#AA0000" : "#ff3355", color: isHighContrast ? "#AA0000" : "#ff3355", badge: isHighContrast ? "#AA0000" : "#ff3355", badgeBg: isHighContrast ? "rgba(170,0,0,0.1)" : "rgba(255,51,85,0.18)", animation: "error-pulse 1.5s ease-in-out infinite" },
          repairing: { bg: isHighContrast ? "#FFF8DC" : "rgba(255,184,0,0.08)",  border: isHighContrast ? "#7c4e00" : "#ffb800", color: isHighContrast ? "#7c4e00" : "#ffb800", badge: isHighContrast ? "#7c4e00" : "#ffb800", badgeBg: isHighContrast ? "rgba(124,78,0,0.1)" : "rgba(255,184,0,0.18)",   animation: "none" },
          resolved:  { bg: isHighContrast ? "#DCFCE7" : "rgba(0,224,150,0.07)",  border: isHighContrast ? "#0a6640" : "#00e096", color: isHighContrast ? "#0a6640" : "#00e096", badge: isHighContrast ? "#0a6640" : "#00e096", badgeBg: isHighContrast ? "rgba(10,102,64,0.1)" : "rgba(0,224,150,0.15)",   animation: "none" },
        };
        const cfg = phaseConfig[repairPhase];
        const phaseBadge  = { error: "[ERROR]", repairing: "[REPAIRING]", resolved: "[RESOLVED]" }[repairPhase];
        const heText      = { error: "שגיאת מערכת — פרוטוקול תיקון עצמי הופעל", repairing: "שגיאת מערכת — פרוטוקול תיקון עצמי הופעל", resolved: "המערכת שוחזרה — פרוטוקול תיקון הושלם" }[repairPhase];
        const enText      = { error: "SYSTEM ERROR — Self-Repair Protocol Activated", repairing: "SYSTEM ERROR — Self-Repair Protocol Activated", resolved: "SYSTEM RECOVERED — Self-Repair Protocol Complete" }[repairPhase];
        const displayText = language === "he" ? heText : enText;

        return (
          <div style={{
            display: "flex",
            alignItems: "center",
            gap: "0.75rem",
            padding: "0.65rem 1rem",
            borderRadius: "10px",
            background: cfg.bg,
            border: `1.5px solid ${cfg.border}`,
            boxShadow: cfg.animation !== "none" ? `0 0 0 0 ${cfg.border}` : "none",
            animation: cfg.animation,
            marginBottom: "1.25rem",
            position: "relative",
            flexDirection: isRTL ? "row-reverse" : "row",
          }}>
            {/* Phase badge */}
            <span style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.68rem",
              fontWeight: 800,
              letterSpacing: "0.08em",
              color: cfg.badge,
              background: cfg.badgeBg,
              padding: "2px 7px",
              borderRadius: "5px",
              flexShrink: 0,
              border: `1px solid ${cfg.border}40`,
            }}>
              {phaseBadge}
            </span>
            {/* Main message */}
            <span style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.82rem",
              fontWeight: 700,
              color: cfg.color,
              letterSpacing: "0.02em",
              direction: isRTL ? "rtl" : "ltr",
            }}>
              {displayText}
            </span>
            {/* State progression arrow */}
            {repairPhase !== "resolved" && (
              <span style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.6rem",
                color: cfg.color,
                opacity: 0.55,
                marginLeft: isRTL ? 0 : "auto",
                marginRight: isRTL ? "auto" : 0,
                flexShrink: 0,
                letterSpacing: "0.04em",
              }}>
                [ERROR] → [REPAIRING] → [RESOLVED]
              </span>
            )}
          </div>
        );
      })()}

      {/* Network diagram */}
      <div
        ref={containerRef}
        style={{
          position: "relative",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: "2.25rem",
          paddingBottom: "0.5rem",
        }}
      >
        {/* Master row */}
        <div style={{ display: "flex", justifyContent: "center" }}>
          <NodeBox
            label={masterNodeLabel}
            id={master?.node_id ?? "nexus-master"}
            role="master"
            online={masterOnline}
            cpu={master?.cpu_percent}
            stealth={stealth}
            accent={ACCENT}
            isRTL={isRTL}
            isHighContrast={isHighContrast}
            textSecondary={tokens.textSecondary}
            textMuted={tokens.textMuted}
            danger={tokens.danger}
          />
        </div>

        {/* Connector visual — vertical + horizontal branches */}
        <div style={{
          position: "relative",
          width: "100%",
          display: "flex",
          justifyContent: "center",
          alignItems: "flex-start",
          gap: "1.25rem",
          flexWrap: "wrap",
        }}>
          {/* Vertical trunk line */}
          {workers.length > 0 && (
            <div style={{
              position: "absolute",
              top: "-2.25rem",
              left: "50%",
              transform: "translateX(-50%)",
              width: isHighContrast ? "3px" : "2px",
              height: "2.25rem",
              background: (stealth && !isHighContrast) ? "#21293d"
                : masterOnline
                  ? (isHighContrast ? ACCENT : `linear-gradient(180deg, ${ACCENT}70, ${ACCENT}22)`)
                  : (isHighContrast ? tokens.danger : "rgba(239,68,68,0.35)"),
            }}>
              {masterOnline && !stealth && !isHighContrast && (
                <div style={{
                  position: "absolute",
                  top: 0,
                  left: "-1px",
                  width: "4px",
                  height: "10px",
                  background: ACCENT,
                  borderRadius: "2px",
                  animation: "flow-down 1.2s linear infinite",
                  boxShadow: `0 0 8px ${ACCENT}`,
                }} />
              )}
            </div>
          )}

          {/* Worker nodes */}
          {workers.length === 0 ? (
            <div style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.85rem",
              color: isHighContrast ? tokens.textMuted : (stealth ? "#21293d" : "#2a3450"),
              padding: "1rem",
            }}>
              No workers registered
            </div>
          ) : (
            workers.map((w, i) => {
              const workerAccent = (stealth && !isHighContrast)
                ? "#21293d"
                : (isHighContrast ? "#166534" : "#10b981");
              return (
                <div key={w.node_id} style={{ position: "relative" }}>
                  {/* Branch connector */}
                  <div style={{
                    position: "absolute",
                    top: "-1.5rem",
                    left: "50%",
                    transform: "translateX(-50%)",
                    width: isHighContrast ? "2.5px" : "1.5px",
                    height: "1.5rem",
                    background: (stealth && !isHighContrast) ? "#21293d"
                      : w.online ? (isHighContrast ? `${ACCENT}90` : `${ACCENT}50`) : (isHighContrast ? tokens.danger : "rgba(239,68,68,0.28)"),
                    animation: masterOnline && w.online && !stealth && !isHighContrast
                      ? "flow-dash 2s linear infinite" : "none",
                  }} />
                  <NodeBox
                    label={`${workerNodeLabel} ${String(i + 1).padStart(2, "0")}`}
                    id={w.node_id}
                    role="worker"
                    online={w.online}
                    cpu={w.cpu_percent}
                    stealth={stealth}
                    accent={workerAccent}
                    isRTL={isRTL}
                    isHighContrast={isHighContrast}
                    textSecondary={tokens.textSecondary}
                    textMuted={tokens.textMuted}
                    danger={tokens.danger}
                  />
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* Log panels */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: "0.875rem",
        marginTop: "1.5rem",
        height: "155px",
      }}>
        <LogPanel
          title={masterLogLabel}
          logs={masterLogs}
          stealth={stealth}
          accent={ACCENT}
          isHighContrast={isHighContrast}
          terminalBg={tokens.terminalBg}
          borderSubtle={tokens.borderSubtle}
          textMuted={tokens.textMuted}
        />
        <LogPanel
          title={workerLogLabel}
          logs={workerLogs}
          stealth={stealth}
          accent={(stealth && !isHighContrast) ? "#21293d" : (isHighContrast ? "#166534" : "#10b981")}
          isHighContrast={isHighContrast}
          terminalBg={tokens.terminalBg}
          borderSubtle={tokens.borderSubtle}
          textMuted={tokens.textMuted}
        />
      </div>

      {/* Live HUD overlay */}
      <div style={{
        position: "absolute",
        top: "1.75rem",
        [isRTL ? "left" : "right"]: "1.75rem",
        background: isHighContrast
          ? "rgba(255,255,255,0.98)"
          : stealth ? "rgba(20,24,36,0.92)" : "rgba(10,14,22,0.85)",
        backdropFilter: isHighContrast ? "none" : "blur(14px)",
        WebkitBackdropFilter: isHighContrast ? "none" : "blur(14px)",
        border: isHighContrast
          ? `2px solid ${tokens.borderDefault}`
          : `1.5px solid ${stealth ? "#21293d" : `${ACCENT}25`}`,
        borderRadius: "12px",
        padding: "0.875rem 1.125rem",
        minWidth: "180px",
        boxShadow: isHighContrast ? "0 2px 10px rgba(0,0,0,0.12)" : `0 4px 20px rgba(0,0,0,0.4)`,
      }}>
        <div style={{
          fontFamily: "var(--font-sans)",
          fontSize: "0.65rem",
          fontWeight: 700,
          letterSpacing: "0.14em",
          color: (stealth && !isHighContrast) ? "#2a3450" : ACCENT,
          textTransform: "uppercase",
          textAlign: isRTL ? "right" : "left",
          marginBottom: "0.7rem",
        }}>
          {clusterHudLabel}
        </div>

        {[
          {
            label: language === "he" ? "סטטוס מאסטר" : t("widgets.master_status"),
            value: masterOnline
              ? (language === "he" ? "מחובר" : t("status.online"))
              : (language === "he" ? "מנותק" : t("status.offline")),
            color: masterOnline
              ? (stealth && !isHighContrast) ? "#2a3450" : tokens.success
              : tokens.danger,
          },
          {
            label: language === "he" ? "מעבדים פעילים" : t("widgets.active_workers"),
            value: `${workers.filter(w => w.online).length}/${workers.length}`,
            color: workerOnline
              ? (stealth && !isHighContrast) ? "#2a3450" : tokens.success
              : tokens.textMuted,
          },
          {
            label: language === "he" ? "זרם נתונים" : t("widgets.data_stream"),
            value: `${logActivity} msg/m`,
            color: logActivity > 5
              ? (stealth && !isHighContrast) ? "#2a3450" : tokens.warning
              : tokens.textMuted,
          },
          {
            label: language === "he" ? "עומס מעבד" : t("widgets.cpu_load"),
            value: `${(master?.cpu_percent ?? 0).toFixed(1)}%`,
            color: (master?.cpu_percent ?? 0) > 70
              ? tokens.danger
              : (stealth && !isHighContrast) ? "#2a3450" : tokens.success,
          },
        ].map(({ label, value, color }, i) => (
          <div
            key={i}
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              flexDirection: isRTL ? "row-reverse" : "row",
              marginBottom: "0.4rem",
            }}
          >
            <span style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.7rem",
              color: isHighContrast
                ? tokens.textMuted
                : (stealth ? "#2a3450" : "#6b8fab"),
            }}>
              {label}
            </span>
            <span style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.7rem",
              fontWeight: 700,
              color: (stealth && !isHighContrast) ? "#21293d" : color,
            }}>
              {value}
            </span>
          </div>
        ))}

        {/* Activity pulse */}
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: "7px",
          marginTop: "0.7rem",
          padding: "5px 8px",
          borderRadius: "6px",
          background: isHighContrast
            ? tokens.accentSubtle
            : (stealth ? "rgba(20,24,36,0.4)" : `${ACCENT}08`),
          border: `1px solid ${isHighContrast ? tokens.accentDim : (stealth ? "#21293d" : `${ACCENT}20`)}`,
          flexDirection: isRTL ? "row-reverse" : "row",
        }}>
          <span style={{
            width: 7, height: 7,
            borderRadius: "50%",
            background: logActivity > 0 && !stealth ? tokens.success : (isHighContrast ? "#9CA3AF" : (stealth ? "#21293d" : "#2a3450")),
            display: "inline-block",
            boxShadow: logActivity > 0 && !stealth && !isHighContrast ? `0 0 7px ${tokens.success}` : "none",
            animation: logActivity > 0 && !stealth && !isHighContrast ? "rgb-pulse 1s infinite" : "none",
          }} />
          <span style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.65rem",
            color: isHighContrast
              ? (logActivity > 0 ? tokens.success : tokens.textMuted)
              : (stealth ? "#2a3450" : (logActivity > 0 ? "#10b981" : "#2a3450")),
          }}>
            {language === "he" ? "רשת פעילה" : t("widgets.network_active")}
          </span>
        </div>
      </div>

      <style>{`
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.35; }
        }
        @keyframes terminal-blink {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0; }
        }
        @keyframes log-flash {
          0%   { background: rgba(14, 165, 233, 0.14); }
          100% { background: transparent; }
        }
        @keyframes flow-dash {
          from { stroke-dashoffset: 26; }
          to   { stroke-dashoffset: 0; }
        }
        @keyframes flow-down {
          0%   { top: 0; opacity: 1; }
          100% { top: 100%; opacity: 0; }
        }
        @keyframes critical-flash {
          0%,  49% { opacity: 1;   background: rgba(255, 30, 60, 0.12); }
          50%, 100% { opacity: 0.55; background: transparent; }
        }
        @keyframes error-pulse {
          0%, 100% { box-shadow: 0 0 0 0   rgba(255, 51, 85, 0.5); }
          50%       { box-shadow: 0 0 0 8px rgba(255, 51, 85, 0); }
        }
      `}</style>
    </div>
  );
}
