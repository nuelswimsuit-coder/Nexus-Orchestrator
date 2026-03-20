"use client";

/**
 * WinRateWidget — Live Win-Rate & Performance Tracker
 *
 * Polls /api/prediction/performance every 30 s and displays:
 *   Center  — SVG circular progress bar with win-rate percentage
 *   Stats   — Total Trades · Virtual Profit · Success Streak
 *   Badge   — "האסטרטגיה מוכנה / STRATEGY READY" glows when win rate > 60%
 *
 * Supports Standard (dark) and High Contrast modes via useStealth and
 * data-theme="high-contrast" CSS tokens defined in globals.css.
 *
 * Hebrew labels:
 *   אחוז הצלחה   — Win Rate
 *   סך עסקאות    — Total Trades
 *   רווח וירטואלי — Virtual Profit
 *   האסטרטגיה מוכנה — Strategy Ready
 */

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

// ── Types ─────────────────────────────────────────────────────────────────────

interface PaperPerformanceData {
  total_trades: number;
  wins:         number;
  losses:       number;
  virtual_pnl:  number;
  win_streak:   number;
  win_rate:     number;
  updated_at:   string | null;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const RING_RADIUS       = 58;
const RING_STROKE       = 9;
const RING_CIRCUMFERENCE = 2 * Math.PI * RING_RADIUS; // ≈ 364.4 px

const STRATEGY_THRESHOLD = 60; // win rate % above which badge fires

// ── SVG Circular Progress Ring ────────────────────────────────────────────────

interface RingProps {
  winRate:  number;  // 0–100
  stealth:  boolean;
  isHC:     boolean;
}

function ProgressRing({ winRate, stealth, isHC }: RingProps) {
  const clampedRate = Math.max(0, Math.min(100, winRate));
  const dashOffset  = RING_CIRCUMFERENCE * (1 - clampedRate / 100);

  const strategyReady = clampedRate > STRATEGY_THRESHOLD;

  // Colour ramps
  const trackColor  = isHC ? "#E4E7EB" : "#1e293b";
  const ringColor   = stealth
    ? "#475569"
    : strategyReady
      ? "#22c55e"
      : clampedRate >= 45
        ? "#0ea5e9"
        : "#ef4444";

  const glowColor   = stealth || isHC ? "none" : `drop-shadow(0 0 8px ${ringColor}88)`;
  const textColor   = isHC ? "#000000" : stealth ? "#94a3b8" : "#dde8f5";
  const subColor    = isHC ? "#374151" : stealth ? "#475569" : "#6b8fab";

  return (
    <div style={{ position: "relative", width: 160, height: 160, flexShrink: 0 }}>
      <svg
        viewBox="0 0 160 160"
        width={160}
        height={160}
        style={{ transform: "rotate(-90deg)", filter: glowColor, transition: "filter 0.4s" }}
        aria-label={`Win rate: ${clampedRate.toFixed(1)}%`}
      >
        {/* Track */}
        <circle
          cx={80}
          cy={80}
          r={RING_RADIUS}
          fill="none"
          stroke={trackColor}
          strokeWidth={RING_STROKE}
        />
        {/* Progress arc */}
        <circle
          cx={80}
          cy={80}
          r={RING_RADIUS}
          fill="none"
          stroke={ringColor}
          strokeWidth={RING_STROKE}
          strokeLinecap="round"
          strokeDasharray={RING_CIRCUMFERENCE}
          strokeDashoffset={dashOffset}
          style={{ transition: "stroke-dashoffset 1s cubic-bezier(0.22,1,0.36,1), stroke 0.5s" }}
        />
      </svg>

      {/* Center text — rendered on top via absolute positioning */}
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          gap: "2px",
          pointerEvents: "none",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "1.6rem",
            fontWeight: 800,
            color: ringColor,
            letterSpacing: "-0.02em",
            lineHeight: 1,
            textShadow: stealth || isHC ? "none" : `0 0 14px ${ringColor}66`,
            transition: "color 0.5s, text-shadow 0.5s",
          }}
        >
          {clampedRate.toFixed(0)}%
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.52rem",
            fontWeight: 600,
            color: subColor,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
            direction: "rtl",
          }}
        >
          אחוז הצלחה
        </span>
      </div>
    </div>
  );
}

// ── Stat card ─────────────────────────────────────────────────────────────────

interface StatCardProps {
  heLabel: string;
  enLabel: string;
  value:   string;
  color:   string;
  isHC:    boolean;
}

function StatCard({ heLabel, enLabel, value, color, isHC }: StatCardProps) {
  const bg     = isHC ? "#F0F2F5" : "#0f172a";
  const border = isHC ? "#D1D5DB" : "#1e293b";
  const sub    = isHC ? "#374151" : "#334155";

  return (
    <div
      style={{
        background:   bg,
        border:       `1px solid ${border}`,
        borderRadius: "10px",
        padding:      "0.6rem 0.75rem",
        display:      "flex",
        flexDirection: "column",
        gap:           "2px",
      }}
    >
      <span
        style={{
          fontFamily:    "var(--font-mono)",
          fontSize:      "0.54rem",
          color:         sub,
          letterSpacing: "0.08em",
          direction:     "rtl",
        }}
      >
        {heLabel}
      </span>
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize:   "0.48rem",
          color:      isHC ? "#6B7280" : "#1e3a5a",
          letterSpacing: "0.06em",
          textTransform: "uppercase",
        }}
      >
        {enLabel}
      </span>
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize:   "1.05rem",
          fontWeight: 700,
          color,
          lineHeight: 1.1,
          marginTop:  "2px",
        }}
      >
        {value}
      </span>
    </div>
  );
}

// ── Strategy Ready Badge ───────────────────────────────────────────────────────

interface BadgeProps {
  active:  boolean;
  stealth: boolean;
  isHC:    boolean;
}

function StrategyBadge({ active, stealth, isHC }: BadgeProps) {
  if (!active) return null;

  const glowColor = stealth || isHC ? "none" : "0 0 18px #22c55e66, 0 0 36px #22c55e33";
  const bg        = isHC ? "#DCFCE7" : stealth ? "#0f172a" : "rgba(34,197,94,0.1)";
  const border    = isHC ? "#166534" : stealth ? "#1e293b" : "#22c55e55";
  const textColor = isHC ? "#166534" : stealth ? "#475569" : "#22c55e";

  return (
    <div
      style={{
        display:       "flex",
        alignItems:    "center",
        justifyContent: "center",
        gap:            "0.4rem",
        padding:       "0.45rem 0.85rem",
        background:    bg,
        border:        `1px solid ${border}`,
        borderRadius:  "8px",
        boxShadow:     glowColor,
        animation:     stealth || isHC ? "none" : "wrw-badge-breathe 2.4s ease-in-out infinite",
        transition:    "all 0.4s",
      }}
    >
      {!stealth && !isHC && (
        <span
          style={{
            width:        8,
            height:       8,
            borderRadius: "50%",
            background:   "#22c55e",
            flexShrink:   0,
            boxShadow:    "0 0 8px #22c55e",
            animation:    "wrw-dot-pulse 1.4s ease-in-out infinite",
            display:      "inline-block",
          }}
        />
      )}
      <span
        style={{
          fontFamily:    "var(--font-mono)",
          fontSize:      "0.65rem",
          fontWeight:    700,
          color:         textColor,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          direction:     "rtl",
        }}
      >
        ✓ האסטרטגיה מוכנה
      </span>
      <span
        style={{
          fontFamily:    "var(--font-mono)",
          fontSize:      "0.55rem",
          color:         isHC ? "#166534" : stealth ? "#334155" : "#4ade80",
          letterSpacing: "0.08em",
        }}
      >
        STRATEGY READY
      </span>
    </div>
  );
}

// ── Main widget ───────────────────────────────────────────────────────────────

export default function WinRateWidget() {
  const { stealth } = useStealth();

  // Detect high-contrast mode from <html data-theme="high-contrast">
  const isHC =
    typeof document !== "undefined" &&
    document.documentElement.dataset.theme === "high-contrast";

  const { data, error, isLoading } = useSWR<PaperPerformanceData>(
    "/api/prediction/performance",
    swrFetcher<PaperPerformanceData>,
    { refreshInterval: 30_000 }
  );

  // ── Theme-aware primitives ────────────────────────────────────────────────
  const surfaceBg   = isHC ? "#FFFFFF"  : "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)";
  const borderColor = isHC ? "#D1D5DB"  : stealth ? "#1e293b" : "#0ea5e9";
  const headerColor = isHC ? "#0055CC"  : stealth ? "#475569" : "#0ea5e9";
  const mutedColor  = isHC ? "#374151"  : stealth ? "#334155" : "#64748b";
  const bodyText    = isHC ? "#000000"  : "#dde8f5";

  const accentGreen  = isHC ? "#166534"  : stealth ? "#475569" : "#22c55e";
  const accentRed    = isHC ? "#B91C1C"  : stealth ? "#475569" : "#ef4444";
  const accentBlue   = isHC ? "#0055CC"  : stealth ? "#475569" : "#0ea5e9";
  const accentAmber  = isHC ? "#92400E"  : stealth ? "#475569" : "#f59e0b";

  // ── Loading skeleton ──────────────────────────────────────────────────────
  if (isLoading || (!data && !error)) {
    return (
      <div
        style={{
          background:   isHC ? "#FFFFFF" : "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)",
          border:       `1px solid ${isHC ? "#D1D5DB" : "#1e293b"}`,
          borderRadius: "12px",
          padding:      "1.25rem",
          minHeight:    "260px",
          display:      "flex",
          alignItems:   "center",
          justifyContent: "center",
        }}
      >
        <span
          style={{
            fontFamily:    "var(--font-mono)",
            fontSize:      "0.65rem",
            color:         isHC ? "#6B7280" : "#334155",
            letterSpacing: "0.1em",
            animation:     "wrw-blink 1.4s ease-in-out infinite",
          }}
        >
          LOADING PERFORMANCE DATA…
        </span>
        <style>{`@keyframes wrw-blink { 0%,100%{opacity:1} 50%{opacity:0.3} }`}</style>
      </div>
    );
  }

  if (error && !data) {
    return (
      <div
        style={{
          background:   isHC ? "#FFFFFF" : "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)",
          border:       `1px solid ${isHC ? "#B91C1C" : "#ef444433"}`,
          borderRadius: "12px",
          padding:      "1.25rem",
          minHeight:    "260px",
          display:      "flex",
          alignItems:   "center",
          justifyContent: "center",
        }}
      >
        <span
          style={{
            fontFamily:    "var(--font-mono)",
            fontSize:      "0.65rem",
            color:         isHC ? "#B91C1C" : "#ef4444",
            letterSpacing: "0.08em",
          }}
        >
          ⚠ STATS UNAVAILABLE — RETRYING…
        </span>
      </div>
    );
  }

  const wr          = data!.win_rate;
  const stratReady  = wr > STRATEGY_THRESHOLD;
  const activeBorderColor = stratReady && !stealth && !isHC ? "#22c55e44" : `${borderColor}33`;
  const activeGlow        = stratReady && !stealth && !isHC ? "0 0 28px #22c55e1a" : "none";

  const pnlPositive = (data!.virtual_pnl ?? 0) >= 0;
  const pnlColor    = pnlPositive ? accentGreen : accentRed;

  const streakColor = data!.win_streak >= 3 ? accentAmber : data!.win_streak >= 1 ? accentGreen : mutedColor;

  return (
    <div
      style={{
        background:   surfaceBg,
        border:       `1px solid ${activeBorderColor}`,
        borderRadius: "12px",
        padding:      "1.25rem",
        boxShadow:    stealth || isHC ? "none" : activeGlow,
        transition:   "all 0.4s",
        display:      "flex",
        flexDirection: "column",
        gap:           "1rem",
      }}
    >
      {/* ── Header ── */}
      <div
        style={{
          display:        "flex",
          alignItems:     "center",
          justifyContent: "space-between",
        }}
      >
        <h3
          style={{
            fontFamily:    "var(--font-mono)",
            fontSize:      "0.7rem",
            fontWeight:    700,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            color:         headerColor,
            margin:        0,
            display:       "flex",
            alignItems:    "center",
            gap:           "0.4rem",
          }}
        >
          📊 Win-Rate Tracker
          {stratReady && !stealth && !isHC && (
            <span
              style={{
                width:        7,
                height:       7,
                borderRadius: "50%",
                background:   "#22c55e",
                display:      "inline-block",
                boxShadow:    "0 0 8px #22c55e",
                animation:    "wrw-dot-pulse 1.8s ease-in-out infinite",
              }}
            />
          )}
        </h3>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.58rem", color: mutedColor }}>
          30s refresh
        </span>
      </div>

      {/* ── Ring + stats grid ── */}
      <div
        style={{
          display:     "flex",
          alignItems:  "center",
          gap:         "1rem",
        }}
      >
        {/* Circular ring */}
        <ProgressRing winRate={wr} stealth={stealth} isHC={isHC} />

        {/* Stat cards */}
        <div
          style={{
            flex:              1,
            display:           "grid",
            gridTemplateColumns: "1fr 1fr",
            gap:               "0.6rem",
          }}
        >
          <StatCard
            heLabel="סך עסקאות"
            enLabel="Total Trades"
            value={String(data!.total_trades)}
            color={bodyText}
            isHC={isHC}
          />
          <StatCard
            heLabel="ניצחונות"
            enLabel="Wins"
            value={`${data!.wins} / ${data!.losses}`}
            color={accentGreen}
            isHC={isHC}
          />
          <StatCard
            heLabel="רווח וירטואלי"
            enLabel="Virtual Profit"
            value={`${pnlPositive ? "+" : ""}$${data!.virtual_pnl.toFixed(2)}`}
            color={pnlColor}
            isHC={isHC}
          />
          <StatCard
            heLabel="רצף הצלחות"
            enLabel="Success Streak"
            value={data!.win_streak > 0 ? `🔥 ${data!.win_streak}` : "—"}
            color={streakColor}
            isHC={isHC}
          />
        </div>
      </div>

      {/* ── Strategy Ready badge ── */}
      <StrategyBadge active={stratReady} stealth={stealth} isHC={isHC} />

      {/* ── Win / Loss mini bar ── */}
      {data!.total_trades > 0 && (
        <div>
          <div
            style={{
              display:      "flex",
              height:       "5px",
              borderRadius: "3px",
              overflow:     "hidden",
              background:   isHC ? "#E4E7EB" : "#1e293b",
            }}
          >
            <div
              style={{
                width:      `${wr}%`,
                background: stealth ? "#334155" : isHC ? "#166534" : "#22c55e",
                transition: "width 1s cubic-bezier(0.22,1,0.36,1)",
                boxShadow:  stealth || isHC ? "none" : "0 0 6px #22c55e88",
              }}
            />
            <div
              style={{
                flex:       1,
                background: stealth ? "#1e293b" : isHC ? "#B91C1C" : "#ef4444",
                opacity:    0.55,
              }}
            />
          </div>
          <div
            style={{
              display:        "flex",
              justifyContent: "space-between",
              marginTop:      "3px",
            }}
          >
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.48rem", color: accentGreen }}>
              WIN {wr.toFixed(1)}%
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.48rem", color: accentRed }}>
              LOSS {(100 - wr).toFixed(1)}%
            </span>
          </div>
        </div>
      )}

      {/* ── Footer ── */}
      <div
        style={{
          display:        "flex",
          justifyContent: "space-between",
          alignItems:     "center",
        }}
      >
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.48rem", color: isHC ? "#9CA3AF" : "#1e293b", letterSpacing: "0.06em" }}>
          SETTLED AFTER 5 MIN &nbsp;|&nbsp; PAPER MODE
        </span>
        {data!.updated_at && (
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.48rem", color: isHC ? "#9CA3AF" : "#1e293b" }}>
            {new Date(data!.updated_at).toLocaleTimeString("he-IL", { hour: "2-digit", minute: "2-digit" })}
          </span>
        )}
      </div>

      <style>{`
        @keyframes wrw-dot-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%       { opacity: 0.45; transform: scale(0.8); }
        }
        @keyframes wrw-badge-breathe {
          0%, 100% { box-shadow: 0 0 18px #22c55e55, 0 0 36px #22c55e22; }
          50%       { box-shadow: 0 0 28px #22c55e88, 0 0 52px #22c55e33; }
        }
        @keyframes wrw-blink {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </div>
  );
}
