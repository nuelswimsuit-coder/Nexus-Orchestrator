"use client";

/**
 * Stacked capital view: actual balance + compound reserve = effective sizing power.
 * Race-to-1000% target shown as a Y-axis reference (USD or % of baseline).
 */

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const COLOR_ACTUAL = "#00d2ff";
const COLOR_RESERVE = "#00ff87";
const RACE_LINE = "rgba(244, 114, 182, 0.95)";

export interface CompoundingGrowthChartProps {
  stealth: boolean;
  /** Active ledger balance for the current mode (from /api/scalper/status). */
  currentBalance: number | null | undefined;
  compoundReserveUsd: number | null | undefined;
  baselineUsd?: number | null;
  targetUsd?: number | null;
  targetGainPct?: number | null;
}

function fmtUsd(n: number) {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
}

export default function CompoundingGrowthChart({
  stealth,
  currentBalance,
  compoundReserveUsd,
  baselineUsd,
  targetUsd,
  targetGainPct,
}: CompoundingGrowthChartProps) {
  const [usdMode, setUsdMode] = useState(true);

  const actual = Math.max(0, Number(currentBalance ?? 0));
  const reserve = Math.max(0, Number(compoundReserveUsd ?? 0));
  const total = actual + reserve;
  const baseline = baselineUsd != null && baselineUsd > 0 ? baselineUsd : null;

  const chartData = useMemo(() => {
    if (usdMode) {
      return [{ name: "Firepower", actual, reserve, total }];
    }
    if (!baseline) {
      return [{ name: "Firepower", actual: 0, reserve: 0, total: 0 }];
    }
    return [
      {
        name: "Firepower",
        actual: (actual / baseline) * 100,
        reserve: (reserve / baseline) * 100,
        total: (total / baseline) * 100,
      },
    ];
  }, [actual, reserve, total, baseline, usdMode]);

  const yMax = useMemo(() => {
    const top = chartData[0]?.total ?? 0;
    let ref = 0;
    if (usdMode && targetUsd != null && targetUsd > 0) {
      ref = targetUsd;
    } else if (!usdMode && baseline && targetUsd != null && targetUsd > 0) {
      ref = (targetUsd / baseline) * 100;
    }
    const pad = Math.max(top, ref) * 0.08;
    return Math.max(top, ref) + pad || 1;
  }, [chartData, usdMode, targetUsd, baseline]);

  const sub = stealth ? "#1e293b" : "#64748b";
  const fg = stealth ? "#1e293b" : "#e2e8f0";
  const gridStroke = stealth ? "rgba(15,23,42,0.5)" : "rgba(148,163,184,0.12)";
  const axisStroke = stealth ? "#334155" : "rgba(148,163,184,0.35)";

  const showRef =
    targetUsd != null &&
    targetUsd > 0 &&
    (usdMode || (baseline != null && baseline > 0));

  const refY = usdMode ? targetUsd! : (targetUsd! / baseline!) * 100;

  return (
    <div
      style={{
        border: "1px solid rgba(56, 189, 248, 0.28)",
        borderRadius: "12px",
        padding: "0.65rem 0.5rem 0.5rem",
        background: stealth ? "rgba(15,23,42,0.35)" : "rgba(15,23,42,0.25)",
      }}
    >
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "0.5rem",
          padding: "0 0.35rem 0.5rem",
        }}
      >
        <div>
          <div
            style={{
              fontSize: "0.58rem",
              color: sub,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
            }}
          >
            Compounding · sizing power
          </div>
          <div style={{ fontSize: "0.62rem", color: sub, marginTop: "0.2rem" }}>
            Stacked: actual balance + compound reserve
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.35rem" }}>
          <span style={{ fontSize: "0.55rem", color: sub }}>Scale</span>
          <button
            type="button"
            onClick={() => setUsdMode(true)}
            style={{
              padding: "0.3rem 0.55rem",
              fontSize: "0.58rem",
              fontWeight: 700,
              letterSpacing: "0.06em",
              borderRadius: "8px",
              border: `1px solid ${usdMode ? COLOR_ACTUAL : "#334155"}`,
              background: usdMode ? "rgba(0, 210, 255, 0.12)" : "transparent",
              color: stealth ? "#334155" : fg,
              cursor: "pointer",
            }}
          >
            USD
          </button>
          <button
            type="button"
            onClick={() => setUsdMode(false)}
            style={{
              padding: "0.3rem 0.55rem",
              fontSize: "0.58rem",
              fontWeight: 700,
              letterSpacing: "0.06em",
              borderRadius: "8px",
              border: `1px solid ${!usdMode ? COLOR_RESERVE : "#334155"}`,
              background: !usdMode ? "rgba(0, 255, 135, 0.1)" : "transparent",
              color: stealth ? "#334155" : fg,
              cursor: "pointer",
            }}
          >
            % growth
          </button>
        </div>
      </div>

      {!usdMode && !baseline ? (
        <div style={{ fontSize: "0.58rem", color: sub, padding: "0 0.4rem 0.35rem" }}>
          Race baseline not loaded — % growth view needs race state. Try USD.
        </div>
      ) : null}

      <div style={{ width: "100%", height: "clamp(160px, 28vw, 220px)" }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            margin={{ top: 8, right: 8, left: 4, bottom: 4 }}
            barCategoryGap="18%"
          >
            <defs>
              <filter id="compoundGlow" x="-40%" y="-40%" width="180%" height="180%">
                <feGaussianBlur stdDeviation="2.2" result="blur" />
                <feMerge>
                  <feMergeNode in="blur" />
                  <feMergeNode in="SourceGraphic" />
                </feMerge>
              </filter>
            </defs>
            <CartesianGrid strokeDasharray="3 6" stroke={gridStroke} vertical={false} />
            <XAxis dataKey="name" tick={{ fill: sub, fontSize: 10 }} axisLine={{ stroke: axisStroke }} tickLine={false} />
            <YAxis
              domain={[0, yMax]}
              tick={{ fill: sub, fontSize: 10 }}
              axisLine={{ stroke: axisStroke }}
              tickLine={false}
              tickFormatter={(v) =>
                usdMode ? (v >= 1000 ? `$${(v / 1000).toFixed(1)}k` : `$${v}`) : `${v.toFixed(0)}%`
              }
              width={44}
            />
            {showRef ? (
              <ReferenceLine
                y={refY}
                stroke={RACE_LINE}
                strokeDasharray="4 4"
                label={{
                  value: `Race +${targetGainPct ?? 1000}%`,
                  fill: stealth ? "#64748b" : "#f472b6",
                  fontSize: 10,
                  position: "insideTopRight",
                }}
              />
            ) : null}
            <Tooltip
              cursor={{ fill: stealth ? "rgba(15,23,42,0.4)" : "rgba(30,41,59,0.35)" }}
              content={({ active }) => {
                if (!active) return null;
                return (
                  <div
                    style={{
                      background: "rgba(15, 23, 42, 0.94)",
                      border: "1px solid rgba(56, 189, 248, 0.35)",
                      borderRadius: "8px",
                      padding: "0.5rem 0.65rem",
                      fontSize: "0.62rem",
                      color: "#e2e8f0",
                      maxWidth: "min(92vw, 320px)",
                      lineHeight: 1.45,
                      boxShadow: "0 8px 24px rgba(0,0,0,0.35)",
                    }}
                  >
                    <div>
                      Real Funds: {fmtUsd(actual)} | Reserve: {fmtUsd(reserve)} | Total Firepower:{" "}
                      {fmtUsd(total)}
                    </div>
                    {!usdMode && baseline ? (
                      <div style={{ color: sub, marginTop: "0.35rem", fontSize: "0.58rem" }}>
                        vs baseline {fmtUsd(baseline)} · stack {(total / baseline).toFixed(2)}×
                      </div>
                    ) : null}
                  </div>
                );
              }}
            />
            <Bar dataKey="actual" stackId="cap" fill={COLOR_ACTUAL} radius={[0, 0, 0, 0]} maxBarSize={72} />
            <Bar
              dataKey="reserve"
              stackId="cap"
              fill={COLOR_RESERVE}
              radius={[6, 6, 0, 0]}
              maxBarSize={72}
              style={{ filter: "url(#compoundGlow)" }}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.75rem",
          fontSize: "0.58rem",
          color: sub,
          padding: "0.35rem 0.4rem 0",
          justifyContent: "center",
        }}
      >
        <span>
          <span style={{ color: COLOR_ACTUAL }}>■</span> Actual balance
        </span>
        <span>
          <span style={{ color: COLOR_RESERVE, textShadow: "0 0 6px rgba(0,255,135,0.55)" }}>■</span> Compound
          reserve
        </span>
      </div>
    </div>
  );
}
