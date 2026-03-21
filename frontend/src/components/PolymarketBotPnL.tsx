"use client";

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";

type PolymarketBotPnLResponse = {
  available: boolean;
  realized_pnl_usd: number;
  unrealized_pnl_usd: number;
  total_pnl_usd: number;
  btc_spot: number | null;
  target_strike: number | null;
  yes_price: number | null;
  market_question: string | null;
  open_position: Record<string, unknown> | null;
  within_target_band: boolean;
  last_action: string;
  detail: string;
  session_active: boolean;
  session_stage: string;
  session_node_id: string;
  updated_at: string;
};

function fmtUsd(n: number) {
  const sign = n >= 0 ? "" : "−";
  return `${sign}$${Math.abs(n).toFixed(2)}`;
}

export default function PolymarketBotPnL() {
  const { data, isLoading } = useSWR<PolymarketBotPnLResponse>(
    "/api/prediction/polymarket-bot",
    swrFetcher<PolymarketBotPnLResponse>,
    { refreshInterval: 3000 }
  );

  const accent =
    data && data.available && data.total_pnl_usd >= 0 ? "#22c55e" : "#f97316";
  const sessionOk = data?.session_active;

  return (
    <div
      style={{
        background: "linear-gradient(165deg, #0b1220, #0a0f1b)",
        border: `1px solid ${accent}44`,
        borderRadius: "14px",
        padding: "1rem 1.1rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.65rem",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <span
          style={{
            color: "#93c5fd",
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            letterSpacing: "0.08em",
            fontWeight: 700,
          }}
        >
          NEXUS POLY TRADER
        </span>
        <span
          style={{
            color: sessionOk ? "#22c55e" : "#64748b",
            fontFamily: "var(--font-mono)",
            fontSize: "0.62rem",
            fontWeight: 700,
          }}
        >
          {sessionOk ? "SESSION LIVE" : "SESSION IDLE"}
        </span>
      </div>

      {isLoading && !data ? (
        <span style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.68rem" }}>
          Loading PnL…
        </span>
      ) : !data?.available ? (
        <span style={{ color: "#64748b", fontFamily: "var(--font-sans)", fontSize: "0.75rem" }}>
          No bot telemetry yet. Enable <code style={{ color: "#94a3b8" }}>POLYMARKET_BOT_ENABLED=1</code> on
          the master and run a Linux worker with <code style={{ color: "#94a3b8" }}>linux-only</code>.
        </span>
      ) : (
        <>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "1rem" }}>
            <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
              <span style={{ color: "#64748b", fontSize: "0.62rem", fontFamily: "var(--font-mono)" }}>
                TOTAL PnL
              </span>
              <span
                style={{
                  color: accent,
                  fontFamily: "var(--font-mono)",
                  fontSize: "1.35rem",
                  fontWeight: 700,
                }}
              >
                {fmtUsd(data.total_pnl_usd)}
              </span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
              <span style={{ color: "#64748b", fontSize: "0.62rem", fontFamily: "var(--font-mono)" }}>
                REALIZED / UNREAL
              </span>
              <span style={{ color: "#e2e8f0", fontFamily: "var(--font-mono)", fontSize: "0.85rem" }}>
                {fmtUsd(data.realized_pnl_usd)} · {fmtUsd(data.unrealized_pnl_usd)}
              </span>
            </div>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
              gap: "0.5rem",
              fontFamily: "var(--font-mono)",
              fontSize: "0.68rem",
              color: "#94a3b8",
            }}
          >
            <span>BTC {data.btc_spot != null ? `$${data.btc_spot.toLocaleString()}` : "—"}</span>
            <span>Strike {data.target_strike != null ? `$${data.target_strike.toLocaleString()}` : "—"}</span>
            <span>YES {data.yes_price != null ? `$${data.yes_price.toFixed(3)}` : "—"}</span>
            <span style={{ color: data.within_target_band ? "#22c55e" : "#94a3b8" }}>
              {data.within_target_band ? "≤0.5% band" : "outside band"}
            </span>
          </div>

          {data.market_question ? (
            <p
              style={{
                margin: 0,
                color: "#cbd5e1",
                fontSize: "0.72rem",
                lineHeight: 1.45,
                fontFamily: "var(--font-sans)",
              }}
            >
              {data.market_question}
            </p>
          ) : null}

          <div style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.62rem" }}>
            {data.session_stage ? `${data.session_stage}` : "idle"}
            {data.session_node_id ? ` @ ${data.session_node_id}` : ""}
            {data.last_action ? ` · ${data.last_action}` : ""}
            {data.detail ? ` — ${data.detail}` : ""}
          </div>
        </>
      )}
    </div>
  );
}
