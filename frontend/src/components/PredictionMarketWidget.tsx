"use client";

/**
 * Prediction Market — dashboard summary: live BTC vs Polymarket YES,
 * AI implied fair-value band over the arbitrage time-series, and manual
 * override to halt automated Polymarket orders / close open paper legs.
 */

import useSWR from "swr";
import {
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { API_BASE, swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { useCallback, useState } from "react";

const NEON_BLUE = "#00b4ff";
const CYBER_LIME = "#adff2f";
const CI_MUTED = "rgba(0, 180, 255, 0.35)";

interface CrossExchangeData {
  binance: { price: number } | null;
  polymarket: {
    market_found: boolean;
    yes_price: number | null;
    market_question?: string | null;
  } | null;
  prediction_ci?: {
    pred_mid: number | null;
    ci_low: number | null;
    ci_high: number | null;
  } | null;
}

interface ChartPointRaw {
  timestamp: string;
  binance_price: number | null;
  poly_price: number | null;
  pred_mid?: number | null;
  ci_low?: number | null;
  ci_high?: number | null;
}

interface ChartPack {
  data: ChartPointRaw[];
  total: number;
}

interface OverrideStatus {
  active: boolean;
  halted_at?: string | null;
}

function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return iso.slice(11, 19);
  }
}

export default function PredictionMarketWidget() {
  const { stealth } = useStealth();
  const [actionMsg, setActionMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const { data: cx } = useSWR<CrossExchangeData>(
    "/api/prediction/cross-exchange",
    swrFetcher<CrossExchangeData>,
    { refreshInterval: 15_000 }
  );

  const { data: chart } = useSWR<ChartPack>(
    "/api/prediction/chart-data",
    swrFetcher<ChartPack>,
    { refreshInterval: 2_500 }
  );

  const { data: haltStatus, mutate: mutateHalt } = useSWR<OverrideStatus>(
    "/api/prediction/manual-override/status",
    swrFetcher<OverrideStatus>,
    { refreshInterval: 8_000 }
  );

  const btc = cx?.binance?.price ?? null;
  const yes = cx?.polymarket?.market_found ? cx?.polymarket?.yes_price ?? null : null;
  const ci = cx?.prediction_ci;

  const series =
    chart?.data?.map((d) => ({
      ...d,
      time: formatTime(d.timestamp),
    })) ?? [];

  const hasCiBand = series.some(
    (d) => d.pred_mid != null && d.ci_low != null && d.ci_high != null
  );

  const postOverride = useCallback(
    async (path: string, okMsg: string) => {
      setBusy(true);
      setActionMsg(null);
      try {
        const res = await fetch(`${API_BASE}${path}`, { method: "POST" });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) {
          let msg = res.statusText;
          const d = body.detail;
          if (typeof d === "string") msg = d;
          else if (Array.isArray(d))
            msg = d.map((x: { msg?: string }) => x.msg ?? JSON.stringify(x)).join("; ");
          throw new Error(msg);
        }
        setActionMsg(okMsg);
        await mutateHalt();
      } catch (e) {
        setActionMsg(e instanceof Error ? e.message : "Request failed");
      } finally {
        setBusy(false);
        setTimeout(() => setActionMsg(null), 6_000);
      }
    },
    [mutateHalt]
  );

  const haltActive = haltStatus?.active === true;

  return (
    <div
      style={{
        background: "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)",
        border: `1px solid ${stealth ? "#1e293b" : `${NEON_BLUE}33`}`,
        borderRadius: "12px",
        padding: "1.25rem",
        display: "flex",
        flexDirection: "column",
        gap: "1rem",
        boxShadow: stealth ? "none" : `0 0 20px ${NEON_BLUE}12`,
      }}
    >
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "1rem", flexWrap: "wrap" }}>
        <div>
          <h3
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.72rem",
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              color: stealth ? "#475569" : NEON_BLUE,
              margin: 0,
            }}
          >
            Prediction Market
          </h3>
          <p
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.55rem",
              color: stealth ? "#1e293b" : "#64748b",
              margin: "0.35rem 0 0",
              letterSpacing: "0.06em",
              maxWidth: "420px",
              lineHeight: 1.5,
            }}
          >
            Spot BTC vs active Polymarket YES · AI fair-value confidence band · manual kill-switch for volatility
          </p>
        </div>
        {haltActive && !stealth && (
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.52rem",
              fontWeight: 700,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              color: "#f97316",
              border: "1px solid #f9731655",
              background: "#f9731612",
              padding: "0.25rem 0.5rem",
              borderRadius: "6px",
              whiteSpace: "nowrap",
            }}
          >
            Override active
          </span>
        )}
      </div>

      {/* Metrics row */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
          gap: "0.75rem",
        }}
      >
        <MetricTile
          label="BTC (Binance)"
          value={btc != null ? `$${btc.toLocaleString("en-US", { maximumFractionDigits: 0 })}` : "—"}
          accent={NEON_BLUE}
          stealth={stealth}
        />
        <MetricTile
          label="Polymarket YES"
          value={yes != null ? `${(yes * 100).toFixed(1)}¢` : "—"}
          accent={CYBER_LIME}
          stealth={stealth}
        />
        <MetricTile
          label="AI mid (5m proxy)"
          value={ci?.pred_mid != null ? `$${ci.pred_mid.toLocaleString("en-US", { maximumFractionDigits: 0 })}` : "—"}
          accent={stealth ? "#475569" : "#94a3b8"}
          stealth={stealth}
          sub={
            ci?.ci_low != null && ci?.ci_high != null
              ? `CI ${ci.ci_low.toFixed(0)} – ${ci.ci_high.toFixed(0)}`
              : undefined
          }
        />
      </div>

      {/* Chart */}
      <div style={{ width: "100%", height: 220 }}>
        {series.length === 0 ? (
          <div
            style={{
              height: "100%",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              color: "#334155",
            }}
          >
            Collecting time-series…
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={series} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 6" stroke="#1e293b" />
              <XAxis dataKey="time" tick={{ fill: "#475569", fontSize: 9 }} tickLine={false} />
              <YAxis
                yAxisId="btc"
                domain={["auto", "auto"]}
                tick={{ fill: NEON_BLUE, fontSize: 9 }}
                tickLine={false}
                width={52}
                tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
              />
              <YAxis
                yAxisId="poly"
                orientation="right"
                domain={[0, 1]}
                tick={{ fill: CYBER_LIME, fontSize: 9 }}
                tickLine={false}
                width={36}
                tickFormatter={(v) => `${(v * 100).toFixed(0)}¢`}
              />
              <Tooltip
                contentStyle={{
                  background: "rgba(4, 10, 20, 0.94)",
                  border: `1px solid ${NEON_BLUE}33`,
                  fontFamily: "var(--font-mono)",
                  fontSize: 10,
                }}
                labelStyle={{ color: "#94a3b8" }}
              />
              <Legend
                wrapperStyle={{ fontFamily: "var(--font-mono)", fontSize: 10, paddingTop: 8 }}
              />
              <Line
                yAxisId="btc"
                type="monotone"
                dataKey="binance_price"
                name="BTC spot"
                stroke={NEON_BLUE}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
              {hasCiBand && (
                <>
                  <Line
                    yAxisId="btc"
                    type="monotone"
                    dataKey="ci_high"
                    name="CI high"
                    stroke={CI_MUTED}
                    strokeWidth={1}
                    strokeDasharray="4 4"
                    dot={false}
                    isAnimationActive={false}
                    connectNulls
                  />
                  <Line
                    yAxisId="btc"
                    type="monotone"
                    dataKey="pred_mid"
                    name="AI fair mid"
                    stroke="#e2e8f0"
                    strokeWidth={1.2}
                    dot={false}
                    isAnimationActive={false}
                    connectNulls
                  />
                  <Line
                    yAxisId="btc"
                    type="monotone"
                    dataKey="ci_low"
                    name="CI low"
                    stroke={CI_MUTED}
                    strokeWidth={1}
                    strokeDasharray="4 4"
                    dot={false}
                    isAnimationActive={false}
                    connectNulls
                  />
                </>
              )}
              <Line
                yAxisId="poly"
                type="monotone"
                dataKey="poly_price"
                name="Poly YES"
                stroke={CYBER_LIME}
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
                connectNulls
              />
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Actions */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.6rem", alignItems: "center" }}>
        <button
          type="button"
          disabled={busy || stealth}
          onClick={() =>
            postOverride("/api/prediction/manual-override", "Manual override engaged — new bets blocked.")
          }
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.62rem",
            fontWeight: 800,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            color: stealth ? "#334155" : "#fecaca",
            background: stealth ? "transparent" : "rgba(239, 68, 68, 0.12)",
            border: `1px solid ${stealth ? "#1e293b" : "rgba(239, 68, 68, 0.45)"}`,
            borderRadius: "8px",
            padding: "0.55rem 0.9rem",
            cursor: busy || stealth ? "not-allowed" : "pointer",
            opacity: stealth ? 0.5 : 1,
          }}
        >
          {busy ? "Working…" : "Manual override — kill active bets"}
        </button>
        {haltActive && (
          <button
            type="button"
            disabled={busy || stealth}
            onClick={() =>
              postOverride(
                "/api/prediction/manual-override/clear",
                "Override cleared — automated flow restored."
              )
            }
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              fontWeight: 700,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              color: stealth ? "#334155" : "#86efac",
              background: "transparent",
              border: `1px solid ${stealth ? "#1e293b" : "rgba(34, 197, 94, 0.35)"}`,
              borderRadius: "8px",
              padding: "0.5rem 0.75rem",
              cursor: busy || stealth ? "not-allowed" : "pointer",
            }}
          >
            Clear override
          </button>
        )}
      </div>
      {actionMsg && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            color: actionMsg.includes("failed") || actionMsg.includes("Error") ? "#f87171" : "#86efac",
          }}
        >
          {actionMsg}
        </div>
      )}
    </div>
  );
}

function MetricTile({
  label,
  value,
  accent,
  stealth,
  sub,
}: {
  label: string;
  value: string;
  accent: string;
  stealth: boolean;
  sub?: string;
}) {
  return (
    <div
      style={{
        background: "#0f172a",
        border: "1px solid #1e293b",
        borderRadius: "10px",
        padding: "0.65rem 0.75rem",
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.5rem",
          fontWeight: 700,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          color: stealth ? "#1e293b" : "#64748b",
          marginBottom: "0.35rem",
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "1.05rem",
          fontWeight: 700,
          color: stealth ? "#475569" : "#f8fafc",
          letterSpacing: "0.02em",
        }}
      >
        <span style={{ color: stealth ? "#475569" : accent }}>{value}</span>
      </div>
      {sub && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: stealth ? "#1e293b" : "#64748b",
            marginTop: "0.25rem",
          }}
        >
          {sub}
        </div>
      )}
    </div>
  );
}
