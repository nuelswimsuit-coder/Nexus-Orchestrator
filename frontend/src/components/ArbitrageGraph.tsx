"use client";

/**
 * ArbitrageGraph — Real-time Binance vs Polymarket price visualizer.
 *
 * Left Y-axis  : BTC/USDT spot price (Binance) — auto scale, Neon Blue
 * Right Y-axis : Polymarket Yes-share price [0–1] — Cyber Lime
 * Tooltip      : Hebrew labels + arbitrage gap percentage
 * Refresh      : polls /api/prediction/chart-data every 2.5 s via SWR
 */

import { motion } from "framer-motion";
import useSWR from "swr";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { swrFetcher } from "@/lib/api";

// ── Brand colours ──────────────────────────────────────────────────────────────
const NEON_BLUE  = "#00b4ff";
const CYBER_LIME = "#adff2f";

// ── Types ──────────────────────────────────────────────────────────────────────

interface ArbitrageDataPoint {
  timestamp:     string;
  binance_price: number | null;
  poly_price:    number | null;
}

interface ArbitrageChartResponse {
  data:  ArbitrageDataPoint[];
  total: number;
}

interface ChartPoint extends ArbitrageDataPoint {
  time: string;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function formatTime(iso: string): string {
  try {
    return new Date(iso).toLocaleTimeString("he-IL", {
      hour:   "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return iso.slice(11, 19);
  }
}

// Arbitrage gap: how far the Poly Yes-price sits below the 0.52 "priced-in"
// ceiling. Positive value = detectable lag (opportunity window open).
function calcGap(polyPrice: number | null): number | null {
  if (polyPrice == null) return null;
  return parseFloat(Math.max(0, (0.52 - polyPrice) * 100).toFixed(2));
}

// ── Custom Tooltip ─────────────────────────────────────────────────────────────

interface TooltipPayloadEntry {
  name:  string;
  value: number;
  color: string;
}

interface CustomTooltipProps {
  active?:  boolean;
  payload?: TooltipPayloadEntry[];
}

function ArbitrageTooltip({ active, payload }: CustomTooltipProps) {
  if (!active || !payload?.length) return null;

  const binance = payload.find((p) => p.name === "binance_price")?.value ?? null;
  const poly    = payload.find((p) => p.name === "poly_price")?.value ?? null;
  const gap     = calcGap(poly);

  return (
    <div
      style={{
        background:     "rgba(4, 10, 20, 0.92)",
        backdropFilter: "blur(14px)",
        border:         `1px solid ${NEON_BLUE}33`,
        borderRadius:   8,
        padding:        "10px 14px",
        fontFamily:     "monospace",
        fontSize:       11,
        minWidth:       190,
        direction:      "rtl",
        lineHeight:     1.7,
      }}
    >
      {binance != null && (
        <div style={{ color: NEON_BLUE }}>
          Binance Price:{" "}
          <strong>
            ${binance.toLocaleString("en-US", { maximumFractionDigits: 0 })}
          </strong>
        </div>
      )}
      {poly != null && (
        <div style={{ color: CYBER_LIME }}>
          Polymarket Price: <strong>${poly.toFixed(4)}</strong>
        </div>
      )}
      {gap != null && (
        <div
          style={{
            color:        gap > 0 ? "#ff6b35" : "#475569",
            borderTop:    "1px solid rgba(255,255,255,0.06)",
            paddingTop:   4,
            marginTop:    4,
            fontWeight:   700,
          }}
        >
          Arbitrage Gap: {gap.toFixed(2)}%
        </div>
      )}
    </div>
  );
}

// ── Main component ─────────────────────────────────────────────────────────────

export default function ArbitrageGraph() {
  const { data, isLoading } = useSWR<ArbitrageChartResponse>(
    "/api/prediction/chart-data",
    swrFetcher,
    { refreshInterval: 2500 },
  );

  const chartData: ChartPoint[] = (data?.data ?? []).map((pt) => ({
    ...pt,
    time: formatTime(pt.timestamp),
  }));

  const hasData = chartData.length > 0;
  const last    = hasData ? chartData[chartData.length - 1] : null;
  const gap     = calcGap(last?.poly_price ?? null);

  return (
    <motion.div
      initial={{ opacity: 0, y: 14 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
      style={{
        background:     "rgba(4, 10, 20, 0.58)",
        backdropFilter: "blur(20px)",
        border:         "1px solid rgba(0, 180, 255, 0.13)",
        borderRadius:   16,
        padding:        "1.25rem 1.5rem",
        boxShadow:
          "0 0 48px rgba(0, 180, 255, 0.04), inset 0 1px 0 rgba(255,255,255,0.025)",
      }}
    >
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div
        style={{
          display:        "flex",
          alignItems:     "center",
          justifyContent: "space-between",
          marginBottom:   "1rem",
          direction:      "rtl",
          flexWrap:       "wrap",
          gap:            "0.75rem",
        }}
      >
        <div>
          <div
            style={{
              fontFamily:    "var(--font-mono)",
              fontSize:      "0.6rem",
              fontWeight:    700,
              letterSpacing: "0.15em",
              color:         "#1e5a7a",
              marginBottom:  "0.2rem",
            }}
          >
            ARBITRAGE VISUALIZER
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize:   "0.8rem",
              fontWeight: 600,
              color:      "#94a3b8",
            }}
          >
            Binance vs Polymarket — Real Time
          </div>
        </div>

        {/* Legend */}
        <div style={{ display: "flex", gap: "1.25rem", alignItems: "center" }}>
          <LegendDot color={NEON_BLUE}  label="Binance Price" />
          <LegendDot color={CYBER_LIME} label="Polymarket Price" />
        </div>
      </div>

      {/* ── Chart body ─────────────────────────────────────────────────────── */}
      {isLoading && !hasData ? (
        <EmptyState message="Loading data..." />
      ) : !hasData ? (
        <EmptyState message="Awaiting data points..." dashed />
      ) : (
        <ResponsiveContainer width="100%" height={220}>
          <LineChart
            data={chartData}
            margin={{ top: 6, right: 16, bottom: 4, left: 4 }}
          >
            <CartesianGrid
              strokeDasharray="3 3"
              stroke="rgba(255,255,255,0.04)"
              vertical={false}
            />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 9, fill: "#334155", fontFamily: "monospace" }}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
            />
            {/* Left axis — BTC price */}
            <YAxis
              yAxisId="left"
              domain={["auto", "auto"]}
              tick={{ fontSize: 9, fill: `${NEON_BLUE}99`, fontFamily: "monospace" }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: number) =>
                `$${(v / 1000).toFixed(0)}k`
              }
              width={42}
            />
            {/* Right axis — Poly price [0,1] */}
            <YAxis
              yAxisId="right"
              orientation="right"
              domain={[0, 1]}
              tick={{ fontSize: 9, fill: `${CYBER_LIME}99`, fontFamily: "monospace" }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: number) => v.toFixed(2)}
              width={36}
            />
            <Tooltip content={<ArbitrageTooltip />} />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="binance_price"
              stroke={NEON_BLUE}
              strokeWidth={1.5}
              dot={false}
              activeDot={{ r: 3, fill: NEON_BLUE, strokeWidth: 0 }}
              style={{ filter: `drop-shadow(0 0 4px ${NEON_BLUE}66)` }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="poly_price"
              stroke={CYBER_LIME}
              strokeWidth={1.5}
              dot={false}
              activeDot={{ r: 3, fill: CYBER_LIME, strokeWidth: 0 }}
              style={{ filter: `drop-shadow(0 0 4px ${CYBER_LIME}66)` }}
            />
          </LineChart>
        </ResponsiveContainer>
      )}

      {/* ── Footer stats ───────────────────────────────────────────────────── */}
      {hasData && last && (
        <div
          style={{
            display:     "flex",
            gap:         "1.75rem",
            marginTop:   "0.875rem",
            paddingTop:  "0.875rem",
            borderTop:   "1px solid rgba(255,255,255,0.04)",
            direction:   "rtl",
            flexWrap:    "wrap",
            alignItems:  "flex-end",
          }}
        >
          {last.binance_price != null && (
            <StatPill
              label="Binance Price"
              value={`$${last.binance_price.toLocaleString("en-US", { maximumFractionDigits: 0 })}`}
              color={NEON_BLUE}
            />
          )}
          {last.poly_price != null && (
            <StatPill
              label="Polymarket Price"
              value={`$${last.poly_price.toFixed(4)}`}
              color={CYBER_LIME}
            />
          )}
          {gap != null && (
            <StatPill
              label="Arbitrage Gap"
              value={`${gap.toFixed(2)}%`}
              color={gap > 0 ? "#ff6b35" : "#334155"}
            />
          )}
          <div style={{ marginRight: "auto" }}>
            <span
              style={{
                fontFamily: "monospace",
                fontSize:   9,
                color:      "#334155",
              }}
            >
              {chartData.length} / 30 points
            </span>
          </div>
        </div>
      )}
    </motion.div>
  );
}

// ── Sub-components ─────────────────────────────────────────────────────────────

function LegendDot({ color, label }: { color: string; label: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <div
        style={{
          width:      8,
          height:     2,
          borderRadius: 1,
          background: color,
          boxShadow:  `0 0 6px ${color}`,
        }}
      />
      <span style={{ fontFamily: "monospace", fontSize: 10, color: "#64748b" }}>
        {label}
      </span>
    </div>
  );
}

function StatPill({
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
          fontFamily:   "monospace",
          fontSize:     9,
          color:        "#475569",
          marginBottom: 2,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontFamily:  "monospace",
          fontSize:    13,
          fontWeight:  700,
          color,
          textShadow:  `0 0 10px ${color}44`,
        }}
      >
        {value}
      </div>
    </div>
  );
}

function EmptyState({
  message,
  dashed = false,
}: {
  message: string;
  dashed?: boolean;
}) {
  return (
    <div
      style={{
        height:          200,
        display:         "flex",
        alignItems:      "center",
        justifyContent:  "center",
        fontFamily:      "monospace",
        fontSize:        11,
        color:           "#334155",
        border:          dashed ? "1px dashed #1e293b" : "none",
        borderRadius:    8,
        direction:       "rtl",
      }}
    >
      {message}
    </div>
  );
}

