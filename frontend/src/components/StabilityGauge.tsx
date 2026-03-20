"use client";

/**
 * StabilityGauge — מדד חוסן מערכת (System Resilience Gauge)
 *
 * Real-time semi-circle speedometer gauge showing the composite stability
 * score (0–100) computed by the Sentinel AI monitor every 5 seconds.
 *
 * Color zones:
 *   80–100  →  Neon Green   — מצב חסין / Resilient
 *   50–79   →  Bold Yellow  — רעשי מערכת / Noise Detected
 *   0–49    →  Bright Red   — סכנת קיפאון / Critical Instability
 *
 * Design: 30% larger than standard widgets, 2026 high-contrast glassmorphism.
 */

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

// ── Types ─────────────────────────────────────────────────────────────────────

interface StabilityData {
  score: number | null;
  reason: string;
  breakdown: {
    latency_score?: number;
    heartbeat_score?: number;
    error_rate_score?: number;
    resource_score?: number;
  };
  metrics: {
    latency_ms?: number;
    live_nodes?: number;
    error_rate_pct?: number;
    cpu_percent?: number;
    ram_percent?: number;
  };
  timestamp: string | null;
}

// ── SVG gauge geometry (30 % larger than SessionHealthGauge's R=80) ──────────

const CX = 160;   // centre x in viewBox
const CY = 148;   // centre y — gives headroom above the arc
const R  = 120;   // radius (80 × 1.30 ≈ 104; we use 120 for extra impact)

// Full semi-circle arc length  =  π × R
const ARC_LEN = Math.PI * R;   // ≈ 376.99

// Convert a score (0–100) to polar (x, y) on the arc
// θ = π  at score 0 (left)  →  θ = 0 at score 100 (right)
function scoreToPoint(score: number, radius: number) {
  const theta = Math.PI * (1 - score / 100);
  return {
    x: CX + radius * Math.cos(theta),
    y: CY - radius * Math.sin(theta),
  };
}

// ── Color helpers ─────────────────────────────────────────────────────────────

function gaugeColor(score: number): string {
  if (score >= 80) return "#00ff88";   // Neon Green
  if (score >= 50) return "#fbbf24";   // Bold Yellow
  return "#ff3333";                    // Bright Red
}

function gaugeLabel(score: number): string {
  if (score >= 80) return "מצב חסין";
  if (score >= 50) return "רעשי מערכת";
  return "סכנת קיפאון";
}

function gaugeLabelEn(score: number): string {
  if (score >= 80) return "Resilient";
  if (score >= 50) return "Noise Detected";
  return "Critical Instability";
}

// ── Zone arc background segments ──────────────────────────────────────────────

interface ZoneSegment {
  from: number;  // score 0-100
  to: number;
  color: string;
}

const ZONES: ZoneSegment[] = [
  { from: 0,  to: 49,  color: "#ff333320" },
  { from: 50, to: 79,  color: "#fbbf2420" },
  { from: 80, to: 100, color: "#00ff8820" },
];

// Build an SVG arc path between two score values
function zonePath(fromScore: number, toScore: number): string {
  const start = scoreToPoint(fromScore, R);
  const end   = scoreToPoint(toScore,   R);
  // large-arc-flag: 1 if the arc spans > 180°, else 0
  const span  = toScore - fromScore;
  const large = span > 50 ? 1 : 0;
  return `M ${start.x.toFixed(2)} ${start.y.toFixed(2)} A ${R} ${R} 0 ${large} 0 ${end.x.toFixed(2)} ${end.y.toFixed(2)}`;
}

// ── Breakdown bar ─────────────────────────────────────────────────────────────

function BreakdownBar({
  label,
  score,
  stealth,
}: {
  label: string;
  score: number | undefined;
  stealth: boolean;
}) {
  if (score === undefined) return null;
  const c   = gaugeColor(score);
  const pct = `${Math.round(score)}%`;
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem", minWidth: 0 }}>
      <div style={{
        display: "flex", justifyContent: "space-between",
        fontFamily: "var(--font-mono)", fontSize: "0.55rem",
        color: stealth ? "#334155" : "#64748b",
        letterSpacing: "0.06em",
      }}>
        <span>{label}</span>
        <span style={{ color: stealth ? "#334155" : c }}>{pct}</span>
      </div>
      <div style={{
        height: "3px", borderRadius: "2px",
        background: stealth ? "#1e293b" : "#0f172a",
        overflow: "hidden",
      }}>
        <div style={{
          height: "100%",
          width: pct,
          background: stealth
            ? "#334155"
            : `linear-gradient(90deg, ${c}cc, ${c})`,
          borderRadius: "2px",
          boxShadow: stealth ? "none" : `0 0 6px ${c}66`,
          transition: "width 0.8s ease",
        }} />
      </div>
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────

export default function StabilityGauge() {
  const { stealth } = useStealth();

  const { data } = useSWR<StabilityData>(
    "/api/system/stability",
    swrFetcher<StabilityData>,
    { refreshInterval: 5_000 },   // keep in sync with Sentinel's 5 s cadence
  );

  const score     = data?.score  ?? null;
  const reason    = data?.reason ?? "מאתחל...";
  const breakdown = data?.breakdown ?? {};
  const metrics   = data?.metrics   ?? {};

  const displayScore = score ?? 0;
  const color        = stealth ? "#475569" : gaugeColor(displayScore);
  const isLoading    = score === null;

  // Filled arc length based on score
  const filledLen = (displayScore / 100) * ARC_LEN;

  // Arc start/end points (always full semi-circle)
  const arcStart = scoreToPoint(0,   R);
  const arcEnd   = scoreToPoint(100, R);
  const arcPath  = `M ${arcStart.x.toFixed(2)} ${arcStart.y.toFixed(2)} A ${R} ${R} 0 1 0 ${arcEnd.x.toFixed(2)} ${arcEnd.y.toFixed(2)}`;

  // Needle tip
  const needleTip  = scoreToPoint(displayScore, R - 12);
  const needleBase = scoreToPoint(displayScore, 14);

  return (
    <div
      dir="rtl"
      style={{
        background: "linear-gradient(160deg, #0a0e1a 0%, #080d18 100%)",
        backdropFilter: "blur(20px)",
        WebkitBackdropFilter: "blur(20px)",
        border: `1px solid ${stealth ? "#1e293b" : `${color}33`}`,
        borderRadius: "16px",
        padding: "1.5rem",
        boxShadow: stealth
          ? "none"
          : `0 0 40px ${color}18, 0 0 80px ${color}0a, inset 0 1px 0 ${color}15`,
        transition: "border-color 0.4s, box-shadow 0.4s",
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Top glass reflection */}
      <div style={{
        position: "absolute", top: 0, left: "2rem", right: "2rem", height: "1px",
        background: stealth
          ? "transparent"
          : `linear-gradient(90deg, transparent, ${color}55, transparent)`,
        borderRadius: "1px",
      }} />

      {/* Corner radial glow */}
      {!stealth && (
        <div style={{
          position: "absolute", top: 0, right: 0,
          width: "160px", height: "160px",
          background: `radial-gradient(circle, ${color}14 0%, transparent 70%)`,
          transform: "translate(30%, -30%)",
          pointerEvents: "none",
        }} />
      )}

      {/* ── Header ── */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        marginBottom: "0.75rem",
        flexDirection: "row-reverse",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <div style={{
            width: 7, height: 7, borderRadius: "50%",
            background: isLoading ? "#334155" : color,
            boxShadow: stealth || isLoading ? "none" : `0 0 10px ${color}`,
            animation: stealth || isLoading ? "none" : "stability-pulse 2s infinite",
          }} />
          <span style={{
            fontFamily: "var(--font-mono)", fontSize: "0.75rem", fontWeight: 700,
            letterSpacing: "0.1em", textTransform: "uppercase",
            color: stealth ? "#2a3450" : "#6b8fab",
          }}>
            מדד חוסן מערכת
          </span>
        </div>

        {/* Status badge */}
        {!isLoading && (
          <span style={{
            fontFamily: "var(--font-mono)", fontSize: "0.62rem", fontWeight: 700,
            letterSpacing: "0.08em", textTransform: "uppercase",
            color: stealth ? "#334155" : color,
            background: stealth ? "transparent" : `${color}14`,
            border: `1px solid ${stealth ? "#1e293b" : `${color}44`}`,
            borderRadius: "999px", padding: "0.2rem 0.65rem",
          }}>
            {gaugeLabel(displayScore)} · {gaugeLabelEn(displayScore)}
          </span>
        )}
      </div>

      {/* ── SVG Gauge ── */}
      <div style={{ position: "relative", marginBottom: "0.75rem" }}>
        <svg
          viewBox={`0 0 320 ${CY + 18}`}
          style={{ width: "100%", height: "170px" }}
          aria-label={`Stability score: ${displayScore}`}
        >
          {/* Zone segments (coloured background rings) */}
          {!stealth && ZONES.map((z) => (
            <path
              key={z.from}
              d={zonePath(z.from, z.to)}
              fill="none"
              stroke={z.color}
              strokeWidth={22}
              strokeLinecap="butt"
            />
          ))}

          {/* Track background */}
          <path
            d={arcPath}
            fill="none"
            stroke={stealth ? "#1e293b" : "#0d1a2e"}
            strokeWidth={10}
            strokeLinecap="round"
          />

          {/* Filled arc (score progress) */}
          <path
            d={arcPath}
            fill="none"
            stroke={stealth ? "#334155" : color}
            strokeWidth={10}
            strokeLinecap="round"
            strokeDasharray={`${filledLen.toFixed(2)} ${ARC_LEN.toFixed(2)}`}
            style={{
              transition: "stroke-dasharray 0.9s cubic-bezier(0.22,1,0.36,1), stroke 0.4s",
              filter: stealth ? "none" : `drop-shadow(0 0 10px ${color}77)`,
            }}
          />

          {/* Tick marks at 0, 25, 50, 75, 100 */}
          {[0, 25, 50, 75, 100].map((v) => {
            const outer = scoreToPoint(v, R + 4);
            const inner = scoreToPoint(v, R - 14);
            const label = scoreToPoint(v, R + 18);
            return (
              <g key={v}>
                <line
                  x1={outer.x} y1={outer.y}
                  x2={inner.x} y2={inner.y}
                  stroke={stealth ? "#1e293b" : "#1e3a5f"}
                  strokeWidth={1.5}
                />
                <text
                  x={label.x} y={label.y}
                  textAnchor="middle"
                  dominantBaseline="middle"
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.55rem",
                    fill: stealth ? "#1e293b" : "#2d4a6b",
                    fontWeight: 700,
                  }}
                >
                  {v}
                </text>
              </g>
            );
          })}

          {/* Needle */}
          {!isLoading && (
            <g style={{ transition: "transform 0.9s cubic-bezier(0.22,1,0.36,1)" }}>
              <line
                x1={needleBase.x} y1={needleBase.y}
                x2={needleTip.x}  y2={needleTip.y}
                stroke={stealth ? "#475569" : color}
                strokeWidth={2.5}
                strokeLinecap="round"
                style={{
                  filter: stealth ? "none" : `drop-shadow(0 0 6px ${color})`,
                  transition: "x1 0.9s ease, y1 0.9s ease, x2 0.9s ease, y2 0.9s ease",
                }}
              />
              {/* Needle pivot */}
              <circle
                cx={CX} cy={CY}
                r={7}
                fill={stealth ? "#1e293b" : "#0a0e1a"}
                stroke={stealth ? "#334155" : color}
                strokeWidth={2}
                style={{
                  filter: stealth ? "none" : `drop-shadow(0 0 8px ${color}88)`,
                }}
              />
            </g>
          )}

          {/* Score text */}
          <text
            x={CX} y={CY - 28}
            textAnchor="middle"
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "2.6rem",
              fontWeight: 800,
              fill: isLoading ? "#334155" : (stealth ? "#475569" : color),
              filter: stealth || isLoading ? "none" : `drop-shadow(0 0 14px ${color}55)`,
              transition: "fill 0.4s",
            }}
          >
            {isLoading ? "—" : `${Math.round(displayScore)}`}
          </text>

          {/* Score unit */}
          <text
            x={CX} y={CY - 8}
            textAnchor="middle"
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              fontWeight: 600,
              fill: stealth ? "#334155" : "#2d4a6b",
              letterSpacing: "0.12em",
            }}
          >
            / 100 · יציבות רשת
          </text>
        </svg>
      </div>

      {/* ── Main Reason ── */}
      <div style={{
        textAlign: "center",
        fontFamily: "var(--font-mono)",
        fontSize: "0.75rem",
        fontWeight: 600,
        color: stealth ? "#334155" : color,
        letterSpacing: "0.04em",
        marginBottom: "1rem",
        direction: "rtl",
        textShadow: stealth ? "none" : `0 0 18px ${color}44`,
        transition: "color 0.4s",
        minHeight: "1.1rem",
      }}>
        {reason}
      </div>

      {/* ── Breakdown bars ── */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: "0.6rem 1.25rem",
      }}>
        <BreakdownBar label="זמן תגובה רשת"   score={breakdown.latency_score}    stealth={stealth} />
        <BreakdownBar label="דופק צמתים"     score={breakdown.heartbeat_score}  stealth={stealth} />
        <BreakdownBar label="יומן אירועים"   score={breakdown.error_rate_score} stealth={stealth} />
        <BreakdownBar label="עומס משאבים"    score={breakdown.resource_score}   stealth={stealth} />
      </div>

      {/* ── Metric pills row ── */}
      {Object.keys(metrics).length > 0 && (
        <div style={{
          display: "flex", flexWrap: "wrap", gap: "0.4rem",
          marginTop: "1rem",
          direction: "ltr",
        }}>
          {metrics.latency_ms !== undefined && (
            <Pill label="LAT" value={`${metrics.latency_ms.toFixed(0)}ms`} stealth={stealth} color={color} />
          )}
          {metrics.live_nodes !== undefined && (
            <Pill label="NODES" value={String(metrics.live_nodes)} stealth={stealth} color={color} />
          )}
          {metrics.cpu_percent !== undefined && (
            <Pill label="CPU" value={`${metrics.cpu_percent.toFixed(0)}%`} stealth={stealth} color={color} />
          )}
          {metrics.ram_percent !== undefined && (
            <Pill label="RAM" value={`${metrics.ram_percent.toFixed(0)}%`} stealth={stealth} color={color} />
          )}
          {metrics.error_rate_pct !== undefined && (
            <Pill label="ERR" value={`${metrics.error_rate_pct.toFixed(0)}%`} stealth={stealth} color={color} />
          )}
        </div>
      )}

      <style>{`
        @keyframes stability-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%       { opacity: 0.6; transform: scale(1.4); }
        }
      `}</style>
    </div>
  );
}

// ── Metric pill ────────────────────────────────────────────────────────────────

function Pill({
  label,
  value,
  stealth,
  color,
}: {
  label: string;
  value: string;
  stealth: boolean;
  color: string;
}) {
  return (
    <span style={{
      fontFamily: "var(--font-mono)",
      fontSize: "0.55rem",
      fontWeight: 700,
      letterSpacing: "0.07em",
      color: stealth ? "#334155" : "#64748b",
      background: stealth ? "transparent" : "#0f172a",
      border: `1px solid ${stealth ? "#1e293b" : "#1e293b"}`,
      borderRadius: "6px",
      padding: "0.15rem 0.45rem",
      display: "inline-flex",
      gap: "0.3rem",
    }}>
      <span style={{ color: stealth ? "#1e293b" : "#2d4a6b" }}>{label}</span>
      <span style={{ color: stealth ? "#334155" : color }}>{value}</span>
    </span>
  );
}
