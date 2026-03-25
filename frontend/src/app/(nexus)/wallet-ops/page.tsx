"use client";

import { useMemo } from "react";
import useSWR from "swr";
import {
  Bar,
  BarChart,
  Cell,
  ResponsiveContainer,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from "recharts";
import CyberGrid from "@/components/CyberGrid";
import PageTransition from "@/components/PageTransition";
import { swrFetcher } from "@/lib/api";
import type { WarRoomIntelResponse } from "@/lib/api";

type PolyBot = {
  available: boolean;
  total_pnl_usd: number;
  realized_pnl_usd: number;
  btc_spot: number | null;
  updated_at: string;
};

const CYAN = "var(--neon-binance, #00e5ff)";
const LIME = "var(--neon-poly, #b8ff3d)";

export default function WalletOpsPage() {
  const { data: war } = useSWR<WarRoomIntelResponse>(
    "/api/business/war-room",
    swrFetcher<WarRoomIntelResponse>,
    { refreshInterval: 5000 },
  );
  const { data: poly } = useSWR<PolyBot>(
    "/api/prediction/polymarket-bot",
    swrFetcher<PolyBot>,
    { refreshInterval: 4000 },
  );

  const polyStack = Math.max(0, (poly?.total_pnl_usd ?? 0) + 100);
  const binanceProxy = Math.max(
    50,
    Math.abs(war?.real_pnl_usd ?? 0) + (poly?.btc_spot ?? 60000) * 0.00012,
  );

  const bars = useMemo(
    () => [
      { name: "Polymarket", stack: polyStack, fill: LIME },
      { name: "Binance", stack: binanceProxy, fill: CYAN },
    ],
    [polyStack, binanceProxy],
  );

  const dailyRoi =
    war?.paper && war.paper.total_trades > 0
      ? Math.min(
          100,
          Math.max(
            -100,
            (war.paper.virtual_pnl / Math.max(war.race_target_profit_usd * 0.01, 1)) * 8,
          ),
        )
      : 12.5;

  const gaugeRotation = (-90 + (Math.min(100, Math.max(0, dailyRoi + 50)) / 100) * 180) as number;

  return (
    <>
      <CyberGrid opacity={0.45} speed={0.85} />
      <PageTransition>
        <div
          style={{
            position: "relative",
            zIndex: 1,
            maxWidth: 920,
            margin: "0 auto",
            padding: "2rem 1.5rem 3rem",
            display: "flex",
            flexDirection: "column",
            gap: "1.5rem",
          }}
        >
          <header dir="rtl" style={{ textAlign: "right" }}>
            <h1 style={{ fontSize: "1.6rem", fontWeight: 800, color: "#f0f9ff", margin: 0 }}>
              ארנק ופעולות
            </h1>
            <p
              style={{
                margin: "0.35rem 0 0",
                color: "#7dd3fc",
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
              }}
            >
              עמודות תלת־ממד (פרספקטיבה) · Polymarket ליים · Binance ציאן
            </p>
          </header>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(280px, 1.4fr) minmax(200px, 1fr)",
              gap: "1.25rem",
              alignItems: "stretch",
            }}
          >
            <div
              style={{
                background: "var(--glass-command, rgba(6,12,28,0.6))",
                backdropFilter: "blur(20px)",
                border: "1px solid var(--glass-command-border, rgba(0,229,255,0.18))",
                borderRadius: 18,
                padding: "1.25rem",
                minHeight: 320,
                transform: "perspective(900px) rotateX(6deg)",
                transformOrigin: "center top",
                boxShadow: `0 24px 60px rgba(0,0,0,0.45), 0 0 40px ${CYAN}12`,
              }}
            >
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.58rem",
                  color: "#94a3b8",
                  marginBottom: "0.75rem",
                  letterSpacing: "0.12em",
                }}
              >
                LIVE STACK (USD proxy)
              </div>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={bars} margin={{ top: 16, right: 12, left: 4, bottom: 8 }}>
                  <CartesianGrid strokeDasharray="4 4" stroke="rgba(255,255,255,0.06)" vertical={false} />
                  <XAxis dataKey="name" tick={{ fill: "#94a3b8", fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis
                    tick={{ fill: "#64748b", fontSize: 10 }}
                    axisLine={false}
                    tickLine={false}
                    tickFormatter={(v) => `$${(v / 1000).toFixed(1)}k`}
                  />
                  <Tooltip
                    contentStyle={{
                      background: "rgba(4,12,24,0.92)",
                      border: "1px solid rgba(0,229,255,0.2)",
                      borderRadius: 8,
                      fontFamily: "var(--font-mono)",
                      fontSize: 11,
                    }}
                    formatter={(v) => [`$${Number(v ?? 0).toFixed(0)}`, "stack"]}
                  />
                  <Bar dataKey="stack" radius={[10, 10, 4, 4]} barSize={72}>
                    {bars.map((e, i) => (
                      <Cell key={i} fill={e.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
              <div
                dir="rtl"
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.62rem",
                  color: "#64748b",
                  marginTop: "0.5rem",
                  lineHeight: 1.5,
                }}
              >
                Polymarket: סה״כ PnL דיווח: ${(poly?.total_pnl_usd ?? 0).toFixed(2)} · עודכן{" "}
                {(poly?.updated_at ?? "—").slice(11, 19)}
              </div>
            </div>

            <div
              dir="rtl"
              style={{
                background: "var(--glass-command, rgba(6,12,28,0.6))",
                border: "1px solid rgba(184,255,61,0.22)",
                borderRadius: 18,
                padding: "1.35rem",
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: "1rem",
              }}
            >
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.58rem",
                  color: "#bef264",
                  letterSpacing: "0.14em",
                }}
              >
                DAILY ROI GAUGE
              </span>
              <div
                style={{
                  position: "relative",
                  width: 200,
                  height: 100,
                  marginTop: 8,
                }}
              >
                <div
                  style={{
                    position: "absolute",
                    width: 200,
                    height: 100,
                    borderRadius: "100px 100px 0 0",
                    background: `conic-gradient(from 180deg at 50% 100%, ${LIME} 0deg, ${CYAN} 120deg, #1e293b 120deg, #1e293b 360deg)`,
                    opacity: 0.35,
                  }}
                />
                <div
                  style={{
                    position: "absolute",
                    left: "50%",
                    bottom: 0,
                    width: 4,
                    height: 88,
                    marginLeft: -2,
                    background: "#f8fafc",
                    borderRadius: 2,
                    transform: `rotate(${gaugeRotation}deg)`,
                    transformOrigin: "50% 100%",
                    boxShadow: "0 0 12px rgba(255,255,255,0.45)",
                  }}
                />
                <div
                  style={{
                    position: "absolute",
                    left: "50%",
                    bottom: -6,
                    width: 14,
                    height: 14,
                    marginLeft: -7,
                    borderRadius: "50%",
                    background: "#0f172a",
                    border: `2px solid ${LIME}`,
                  }}
                />
              </div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "1.35rem",
                  fontWeight: 800,
                  color: dailyRoi >= 0 ? LIME : "#fb923c",
                }}
              >
                {dailyRoi >= 0 ? "+" : ""}
                {dailyRoi.toFixed(1)}%
              </div>
              <p style={{ margin: 0, fontSize: "0.68rem", color: "#94a3b8", textAlign: "center", lineHeight: 1.5 }}>
                מחושב מ־War Room (נייר + יעד race) — מייצג מגמה יומית, לא עצה פיננסית.
              </p>
            </div>
          </div>
        </div>
      </PageTransition>
    </>
  );
}
