"use client";

/**
 * NEXUS-ULTIMATE-SCALPER — Simulation vs live toggle, race-to-1000%, alpha source.
 * Data: GET /api/scalper/status, POST /api/scalper/simulation-mode
 */

import { useCallback, useEffect, useState } from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import CompoundingGrowthChart from "@/components/CompoundingGrowthChart";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8001";

interface RaceState {
  simulation: boolean;
  balance_usd: number;
  baseline_usd: number;
  target_usd: number;
  progress_pct: number;
  target_gain_pct: number;
  updated_at: string;
}

interface ScalperStatus {
  project?: string;
  simulation_mode: boolean;
  virtual_balance_usd: number | null;
  live_balance_usd: number | null;
  binance_velocity: { momentum_pct_30s?: number | null; updated_at?: string } | null;
  openclaw_sentiment: {
    score?: number;
    channel_title?: string;
    excerpt?: string;
    updated_at?: string;
  } | null;
  race_to_1000: RaceState | null;
  last_winning_trade_alpha: {
    channel_title?: string;
    excerpt?: string;
    score?: number;
  } | null;
  last_live_entry_alpha: {
    channel_title?: string;
    excerpt?: string;
    score?: number;
    note?: string;
  } | null;
  pending_settlements: number;
  safety_brake_active: boolean;
  poly_5m_event_id?: string;
  strategy_brain?: {
    confidence_pct?: number;
    market_phase?: string;
    master_strike?: boolean;
    sentiment_arbitrage_gap_s?: number | null;
    fleet_alpha?: { premium_ratio?: number; premium_members?: number };
    swarm?: { max_agent_consensus?: number; top_label?: string | null; preempt_active?: boolean };
  } | null;
  fleet_sentiment_heatmap?: Record<
    string,
    { score?: number; momentum_hint?: number; updated_at?: string }
  >;
  alpha_source_feed?: Array<{
    ts?: string;
    kind?: string;
    detail?: string;
    channel?: string;
    score?: number | null;
  }>;
  compound_reserve_usd?: number;
  /** Same as virtual or live balance for the active mode; explicit for compounding UI. */
  current_balance?: number | null;
  yield_metrics?: {
    session_start_time?: string | null;
    uptime_minutes?: number;
    start_balance_usd?: number | null;
    current_balance_usd?: number;
    profit_usd?: number | null;
    profit_per_minute_usd?: number | null;
    estimated_daily_profit_usd?: number | null;
    session_mode_simulation?: boolean;
  } | null;
  thresholds: {
    news_score_min: number;
    momentum_pct_min: number;
    bet_fraction?: number;
    bet_fraction_legacy?: number;
    sizing_model?: string;
    kelly_half_cap?: number;
  };
}

function parseIsoMs(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const t = Date.parse(iso.includes("Z") || iso.includes("+") ? iso : `${iso}Z`);
  return Number.isFinite(t) ? t : null;
}

export default function UltimateScalperPanel() {
  const { stealth } = useStealth();
  const [toggleBusy, setToggleBusy] = useState(false);
  const [yieldTick, setYieldTick] = useState(0);

  useEffect(() => {
    const id = window.setInterval(() => setYieldTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, []);
  const { data, error, isLoading, mutate } = useSWR<ScalperStatus>(
    `${API_BASE}/api/scalper/status`,
    swrFetcher,
    { refreshInterval: 4000, revalidateOnFocus: true },
  );

  const onToggle = useCallback(
    async (simulation: boolean) => {
      setToggleBusy(true);
      try {
        await fetch(`${API_BASE}/api/scalper/simulation-mode`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ simulation }),
        });
        await mutate();
      } finally {
        setToggleBusy(false);
      }
    },
    [mutate],
  );

  const mono = "var(--font-mono)" as const;
  const sub = stealth ? "#1e293b" : "#64748b";
  const fg = stealth ? "#1e293b" : "#e2e8f0";

  if (error) {
    return (
      <div style={{ fontFamily: mono, fontSize: "0.7rem", color: "#f87171" }}>
        Ultimate Scalper: API unreachable ({API_BASE})
      </div>
    );
  }

  if (isLoading || !data) {
    return (
      <div style={{ fontFamily: mono, fontSize: "0.7rem", color: sub }}>
        Loading Ultimate Scalper…
      </div>
    );
  }

  const race = data.race_to_1000;
  const progress = race?.progress_pct ?? 0;
  const mom = data.binance_velocity?.momentum_pct_30s;
  const sent = data.openclaw_sentiment;
  const winAlpha = data.last_winning_trade_alpha;
  const strat = data.strategy_brain;
  const confPct = strat?.confidence_pct ?? 0;
  const heatEntries = Object.entries(data.fleet_sentiment_heatmap ?? {}).slice(0, 24);

  const activeBal =
    data.current_balance ??
    (data.simulation_mode ? data.virtual_balance_usd : data.live_balance_usd) ??
    0;
  const ym = data.yield_metrics;
  const startMs = parseIsoMs(ym?.session_start_time ?? null);
  const startBal = ym?.start_balance_usd ?? null;
  void yieldTick;
  let ppmDisplay: number | null = null;
  let projDisplay: number | null = null;
  let yieldHrs = 0;
  let yieldMins = 0;
  if (startMs != null && startBal != null && Number.isFinite(activeBal)) {
    const uptimeMin = Math.max(0, (Date.now() - startMs) / 60_000);
    const denom = Math.max(uptimeMin, 1);
    const profit = activeBal - startBal;
    ppmDisplay = profit / denom;
    projDisplay = ppmDisplay * 1440;
    yieldHrs = Math.floor(uptimeMin / 60);
    yieldMins = Math.floor(uptimeMin % 60);
  }
  const posProj = projDisplay != null && projDisplay > 0;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "1rem",
        fontFamily: mono,
        color: fg,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "0.75rem" }}>
        <div>
          <div style={{ fontSize: "0.58rem", letterSpacing: "0.14em", color: sub, textTransform: "uppercase" }}>
            NEXUS · Ultimate Scalper
          </div>
          <div style={{ fontSize: "0.72rem", marginTop: "0.25rem" }}>
            {data.project ?? "NEXUS-ULTIMATE-SCALPER"}
            {data.poly_5m_event_id ? (
              <span style={{ color: sub, marginLeft: "0.5rem" }}>· γ {data.poly_5m_event_id}</span>
            ) : null}
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ fontSize: "0.62rem", color: sub }}>Simulation</span>
          <button
            type="button"
            disabled={toggleBusy}
            onClick={() => onToggle(true)}
            style={{
              padding: "0.35rem 0.65rem",
              fontSize: "0.62rem",
              fontWeight: 700,
              letterSpacing: "0.08em",
              borderRadius: "8px",
              border: `1px solid ${data.simulation_mode ? "#22d3ee" : "#334155"}`,
              background: data.simulation_mode ? "rgba(34,211,238,0.12)" : "transparent",
              color: stealth ? "#334155" : "#e2e8f0",
              cursor: toggleBusy ? "wait" : "pointer",
            }}
          >
            SIM
          </button>
          <button
            type="button"
            disabled={toggleBusy}
            onClick={() => onToggle(false)}
            style={{
              padding: "0.35rem 0.65rem",
              fontSize: "0.62rem",
              fontWeight: 700,
              letterSpacing: "0.08em",
              borderRadius: "8px",
              border: `1px solid ${!data.simulation_mode ? "#f472b6" : "#334155"}`,
              background: !data.simulation_mode ? "rgba(244,114,182,0.12)" : "transparent",
              color: stealth ? "#334155" : "#e2e8f0",
              cursor: toggleBusy ? "wait" : "pointer",
            }}
          >
            REAL
          </button>
        </div>
      </div>

      {data.safety_brake_active && (
        <div
          style={{
            fontSize: "0.65rem",
            color: "#fca5a5",
            border: "1px solid rgba(248,113,113,0.35)",
            borderRadius: "10px",
            padding: "0.5rem 0.75rem",
            background: "rgba(127,29,29,0.2)",
          }}
        >
          Safety brake active (30% drawdown) — trading halted. Check Telegram + Redis.
        </div>
      )}

      <div>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.62rem", color: sub, marginBottom: "0.35rem" }}>
          <span>Race to +{race?.target_gain_pct ?? 1000}%</span>
          <span>{progress.toFixed(1)}%</span>
        </div>
        <div
          style={{
            height: "10px",
            borderRadius: "6px",
            background: stealth ? "#0f172a" : "#1e293b",
            overflow: "hidden",
            border: "1px solid rgba(56,189,248,0.2)",
          }}
        >
          <div
            style={{
              height: "100%",
              width: `${Math.min(100, progress)}%`,
              borderRadius: "5px",
              background: "linear-gradient(90deg, #06b6d4, #a78bfa)",
              transition: "width 0.6s ease",
            }}
          />
        </div>
        <div style={{ fontSize: "0.58rem", color: sub, marginTop: "0.35rem" }}>
          Balance{" "}
          {data.simulation_mode
            ? `$${data.virtual_balance_usd?.toFixed(2) ?? "—"} virtual`
            : `$${data.live_balance_usd?.toFixed(2) ?? "—"} live`}{" "}
          · target ${race?.target_usd?.toFixed(0) ?? "—"}
          {data.compound_reserve_usd != null ? (
            <span style={{ marginLeft: "0.5rem" }}>
              · compound +${data.compound_reserve_usd.toFixed(2)}
            </span>
          ) : null}
        </div>
      </div>

      <div
        style={{
          position: "relative",
          borderRadius: "14px",
          padding: "0.85rem 1rem",
          background: stealth
            ? "linear-gradient(145deg, rgba(15,23,42,0.95), rgba(8,47,73,0.5))"
            : "linear-gradient(145deg, rgba(15,23,42,0.92), rgba(6,78,59,0.25))",
          border: "1px solid rgba(52,211,153,0.45)",
          boxShadow: stealth
            ? "0 0 0 1px rgba(16,185,129,0.15), 0 0 28px rgba(52,211,153,0.12), inset 0 1px 0 rgba(255,255,255,0.06)"
            : "0 0 0 1px rgba(52,211,153,0.2), 0 0 36px rgba(16,185,129,0.18), inset 0 1px 0 rgba(255,255,255,0.08)",
          overflow: "hidden",
        }}
      >
        <div
          aria-hidden
          style={{
            position: "absolute",
            inset: 0,
            background:
              "repeating-linear-gradient(90deg, transparent, transparent 48px, rgba(52,211,153,0.03) 48px, rgba(52,211,153,0.03) 49px)",
            pointerEvents: "none",
          }}
        />
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            height: "2px",
            background: "linear-gradient(90deg, transparent, #34d399, #22d3ee, transparent)",
            opacity: 0.85,
            animation: "nexusYieldPulse 2.2s ease-in-out infinite",
          }}
        />
        <style>{`
          @keyframes nexusYieldPulse {
            0%, 100% { opacity: 0.35; filter: blur(0.5px); }
            50% { opacity: 1; filter: blur(0px); }
          }
          @keyframes nexusTickGlow {
            0%, 100% { text-shadow: 0 0 8px rgba(52,211,153,0.35); }
            50% { text-shadow: 0 0 14px rgba(52,211,153,0.65), 0 0 22px rgba(34,211,238,0.35); }
          }
        `}</style>
        <div style={{ position: "relative", zIndex: 1 }}>
          <div
            style={{
              fontSize: "0.52rem",
              letterSpacing: "0.2em",
              color: stealth ? "#0f766e" : "#6ee7b7",
              textTransform: "uppercase",
              marginBottom: "0.5rem",
              fontWeight: 800,
            }}
          >
            Yield module · live
          </div>
          <div
            style={{
              fontSize: "0.95rem",
              fontWeight: 800,
              fontVariantNumeric: "tabular-nums",
              color: stealth ? "#134e4a" : "#ecfdf5",
              animation: "nexusTickGlow 1s ease-in-out infinite",
            }}
          >
            PPM (Profit Per Minute):{" "}
            <span style={{ color: stealth ? "#0d9488" : "#34d399" }}>
              {ppmDisplay != null && Number.isFinite(ppmDisplay)
                ? `${ppmDisplay >= 0 ? "+" : ""}$${ppmDisplay.toFixed(2)}`
                : "—"}
            </span>
          </div>
          <div
            style={{
              marginTop: "0.45rem",
              fontSize: "0.78rem",
              fontWeight: 700,
              fontVariantNumeric: "tabular-nums",
              color: posProj ? (stealth ? "#047857" : "#4ade80") : stealth ? "#475569" : "#94a3b8",
              textShadow: posProj ? "0 0 12px rgba(74,222,128,0.45)" : undefined,
            }}
          >
            Projected 24h Yield:{" "}
            {projDisplay != null && Number.isFinite(projDisplay)
              ? `${projDisplay >= 0 ? "+" : ""}$${projDisplay.toFixed(2)}`
              : "—"}
          </div>
          <div style={{ marginTop: "0.4rem", fontSize: "0.62rem", color: sub, fontVariantNumeric: "tabular-nums" }}>
            Uptime:{" "}
            {startMs != null && startBal != null ? (
              <>
                {yieldHrs} hrs {yieldMins} mins
              </>
            ) : (
              "—"
            )}
            <span style={{ marginLeft: "0.65rem", opacity: 0.85 }}>
              {data.simulation_mode ? "SIM session" : "RACE / LIVE session"}
            </span>
          </div>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
        {strat ? (
          <div
            style={{
              border: "1px solid rgba(167,139,250,0.35)",
              borderRadius: "12px",
              padding: "0.75rem",
              display: "flex",
              flexDirection: "column",
              gap: "0.5rem",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: "0.5rem" }}>
              <span style={{ fontSize: "0.58rem", color: sub, letterSpacing: "0.12em", textTransform: "uppercase" }}>
                Master confidence
              </span>
              {strat.master_strike ? (
                <span style={{ fontSize: "0.58rem", color: "#f472b6", fontWeight: 800 }}>MASTER STRIKE</span>
              ) : null}
            </div>
            <div
              style={{
                height: "12px",
                borderRadius: "6px",
                background: stealth ? "#0f172a" : "#1e293b",
                overflow: "hidden",
                border: "1px solid rgba(167,139,250,0.25)",
              }}
            >
              <div
                style={{
                  height: "100%",
                  width: `${Math.min(100, confPct)}%`,
                  borderRadius: "5px",
                  background: "linear-gradient(90deg, #a78bfa, #22d3ee)",
                  transition: "width 0.5s ease",
                }}
              />
            </div>
            <div style={{ fontSize: "0.68rem", display: "flex", flexWrap: "wrap", gap: "0.65rem" }}>
              <span>{confPct.toFixed(1)}% fused</span>
              <span style={{ color: sub }}>
                phase: <strong style={{ color: fg }}>{strat.market_phase ?? "—"}</strong>
              </span>
              {strat.sentiment_arbitrage_gap_s != null ? (
                <span style={{ color: sub }}>Telegram↔Poly gap {strat.sentiment_arbitrage_gap_s}s</span>
              ) : null}
              {strat.swarm?.max_agent_consensus != null ? (
                <span style={{ color: sub }}>
                  swarm {strat.swarm.max_agent_consensus}
                  {strat.swarm.top_label ? ` · ${strat.swarm.top_label}` : ""}
                </span>
              ) : null}
            </div>
          </div>
        ) : null}

        <CompoundingGrowthChart
          stealth={stealth}
          currentBalance={
            data.current_balance ??
            (data.simulation_mode ? data.virtual_balance_usd : data.live_balance_usd)
          }
          compoundReserveUsd={data.compound_reserve_usd}
          baselineUsd={race?.baseline_usd}
          targetUsd={race?.target_usd}
          targetGainPct={race?.target_gain_pct}
        />
      </div>

      {heatEntries.length > 0 ? (
        <div>
          <div style={{ fontSize: "0.58rem", color: sub, letterSpacing: "0.1em", marginBottom: "0.4rem" }}>
            FLEET SENTIMENT HEATMAP (channels)
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(72px, 1fr))",
              gap: "5px",
            }}
          >
            {heatEntries.map(([ch, cell]) => {
              const v = Math.max(0, Math.min(10, cell.score ?? 5));
              const hue = (v / 10) * 140;
              return (
                <div
                  key={ch}
                  title={`${ch} · ${cell.score?.toFixed(1) ?? ""}`}
                  style={{
                    height: "36px",
                    borderRadius: "6px",
                    background: stealth ? "#0f172a" : `hsla(${hue}, 70%, 42%, 0.55)`,
                    border: "1px solid rgba(148,163,184,0.2)",
                    fontSize: "0.5rem",
                    padding: "4px",
                    overflow: "hidden",
                    color: stealth ? "#1e293b" : "#e2e8f0",
                    lineHeight: 1.2,
                  }}
                >
                  {ch.slice(0, 10)}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}

      {data.alpha_source_feed && data.alpha_source_feed.length > 0 ? (
        <div style={{ border: "1px solid rgba(56,189,248,0.2)", borderRadius: "10px", padding: "0.6rem" }}>
          <div style={{ fontSize: "0.58rem", color: sub, marginBottom: "0.35rem", letterSpacing: "0.1em" }}>
            ALPHA SOURCE FEED
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.35rem", maxHeight: "140px", overflowY: "auto" }}>
            {data.alpha_source_feed.slice(0, 12).map((row, i) => (
              <div key={i} style={{ fontSize: "0.58rem", lineHeight: 1.35, color: sub }}>
                <span style={{ color: "#38bdf8" }}>{row.kind}</span>
                {row.channel ? <span style={{ color: fg }}> · {row.channel.slice(0, 28)}</span> : null}
                <div style={{ color: fg }}>{row.detail?.slice(0, 160)}</div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.75rem", fontSize: "0.62rem" }}>
        <div style={{ border: "1px solid rgba(148,163,184,0.2)", borderRadius: "10px", padding: "0.65rem" }}>
          <div style={{ color: sub, marginBottom: "0.25rem" }}>Binance velocity (30s)</div>
          <div style={{ fontSize: "0.85rem", fontWeight: 700 }}>
            {mom != null && mom !== undefined ? `${mom >= 0 ? "+" : ""}${mom.toFixed(2)}%` : "—"}
          </div>
          <div style={{ color: sub, marginTop: "0.2rem" }}>
            need &gt; {data.thresholds.momentum_pct_min}%
          </div>
        </div>
        <div style={{ border: "1px solid rgba(148,163,184,0.2)", borderRadius: "10px", padding: "0.65rem" }}>
          <div style={{ color: sub, marginBottom: "0.25rem" }}>OpenClaw sentiment</div>
          <div style={{ fontSize: "0.85rem", fontWeight: 700 }}>{sent?.score?.toFixed(1) ?? "—"} / 10</div>
          <div style={{ color: sub, marginTop: "0.2rem", fontSize: "0.58rem" }}>
            {sent?.channel_title?.slice(0, 36) || "No feed"}
          </div>
          <div style={{ color: sub, marginTop: "0.15rem" }}>
            need &gt; {data.thresholds.news_score_min}
          </div>
        </div>
      </div>

      <div style={{ border: "1px solid rgba(56,189,248,0.25)", borderRadius: "10px", padding: "0.65rem 0.75rem" }}>
        <div style={{ fontSize: "0.58rem", color: sub, textTransform: "uppercase", letterSpacing: "0.12em", marginBottom: "0.35rem" }}>
          Alpha source · last winning trade (sim settlement)
        </div>
        <div style={{ fontSize: "0.68rem", lineHeight: 1.45 }}>
          <strong>{winAlpha?.channel_title || "—"}</strong>
          {winAlpha?.excerpt ? (
            <div style={{ color: sub, marginTop: "0.25rem" }}>{winAlpha.excerpt.slice(0, 220)}</div>
          ) : null}
        </div>
        {data.last_live_entry_alpha?.channel_title ? (
          <div style={{ marginTop: "0.65rem", fontSize: "0.58rem", color: sub }}>
            Last live entry signal: {data.last_live_entry_alpha.channel_title}
          </div>
        ) : null}
      </div>

      <div style={{ fontSize: "0.55rem", color: sub }}>
        Pending sim settlements: {data.pending_settlements} · Sizing:{" "}
        {data.thresholds.sizing_model ?? "adaptive"} (Kelly cap {data.thresholds.kelly_half_cap ?? "—"})
      </div>
    </div>
  );
}
