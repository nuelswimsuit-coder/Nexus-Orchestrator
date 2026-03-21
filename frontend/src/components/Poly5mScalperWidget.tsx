"use client";

/**
 * Poly5mScalperWidget — NEXUS-POLY-SCALPER-5M dashboard strip.
 *
 * Polls GET /api/prediction/poly5m-scalper for Binance velocity, Telefix/Openclaw
 * sentiment, decision, and settled win/loss ratio.
 */

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

interface Poly5mPayload {
  updated_at: string | null;
  event_id: string | null;
  decision: string | null;
  blocked: boolean;
  block_reason?: string;
  btc_price: number | null;
  velocity_pct_60s: number | null;
  sentiment: {
    score?: number;
    label?: string;
    flash?: boolean;
    headline?: string;
    source?: string;
  };
  market_found: boolean | null;
  market_question: string | null;
  yes_price: number | null;
  paper_trading: boolean;
  wins: number;
  losses: number;
  win_loss_ratio: number | null;
  loss_streak: number;
  trading_halted: boolean;
  project?: string;
}

export default function Poly5mScalperWidget() {
  const { stealth } = useStealth();
  const { data, error, isLoading } = useSWR<Poly5mPayload>(
    "/api/prediction/poly5m-scalper",
    swrFetcher<Poly5mPayload>,
    { refreshInterval: 5_000 },
  );

  const fg = stealth ? "#94a3b8" : "#cbd5e1";
  const sub = stealth ? "#64748b" : "#6b8fab";
  const accent = stealth ? "#475569" : "#38bdf8";

  if (error) {
    return (
      <div style={{ color: "#f87171", fontSize: "0.85rem", padding: "0.5rem 0" }}>
        Poly 5m scalper feed unavailable
      </div>
    );
  }

  if (isLoading || !data) {
    return (
      <div style={{ color: sub, fontSize: "0.85rem", padding: "0.5rem 0" }}>
        Loading Poly 5m scalper…
      </div>
    );
  }

  const sent = data.sentiment || {};
  const wlr =
    data.win_loss_ratio != null
      ? `${(data.win_loss_ratio * 100).toFixed(1)}%`
      : "—";
  const vel =
    data.velocity_pct_60s != null ? `${data.velocity_pct_60s.toFixed(4)}%` : "—";

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
        gap: "0.75rem 1rem",
        fontSize: "0.82rem",
        color: fg,
        lineHeight: 1.45,
      }}
    >
      <div>
        <div style={{ color: sub, fontSize: "0.7rem", letterSpacing: "0.06em", textTransform: "uppercase" }}>
          Decision
        </div>
        <div style={{ color: accent, fontWeight: 600 }}>{data.decision ?? "—"}</div>
        {data.blocked && (
          <div style={{ color: "#fbbf24", fontSize: "0.72rem", marginTop: 2 }}>
            {data.block_reason || "blocked"}
          </div>
        )}
      </div>
      <div>
        <div style={{ color: sub, fontSize: "0.7rem", letterSpacing: "0.06em", textTransform: "uppercase" }}>
          BTC / velocity (60s)
        </div>
        <div>
          {data.btc_price != null ? `$${data.btc_price.toLocaleString()}` : "—"}{" "}
          <span style={{ color: sub }}>· {vel}</span>
        </div>
      </div>
      <div>
        <div style={{ color: sub, fontSize: "0.7rem", letterSpacing: "0.06em", textTransform: "uppercase" }}>
          News sentiment
        </div>
        <div>
          {sent.score != null ? `${sent.score.toFixed(1)}/10` : "—"}{" "}
          <span style={{ color: sub }}>({sent.label || "neutral"})</span>
        </div>
        {sent.flash && (
          <div style={{ color: "#f472b6", fontSize: "0.72rem", marginTop: 2 }}>Flash override</div>
        )}
      </div>
      <div>
        <div style={{ color: sub, fontSize: "0.7rem", letterSpacing: "0.06em", textTransform: "uppercase" }}>
          Win / loss (settled)
        </div>
        <div>
          {data.wins}W · {data.losses}L · {wlr}
        </div>
        {(data.loss_streak > 0 || data.trading_halted) && (
          <div style={{ color: data.trading_halted ? "#f87171" : sub, fontSize: "0.72rem", marginTop: 2 }}>
            Streak {data.loss_streak}
            {data.trading_halted ? " · halted" : ""}
          </div>
        )}
      </div>
      <div style={{ gridColumn: "1 / -1" }}>
        <div style={{ color: sub, fontSize: "0.68rem" }}>
          {data.market_question || "Market"} · YES {data.yes_price != null ? `$${data.yes_price.toFixed(3)}` : "—"}{" "}
          · {data.paper_trading ? "PAPER" : "LIVE"} · event {data.event_id ?? "—"}
        </div>
      </div>
    </div>
  );
}
