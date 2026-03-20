"use client";

import useSWR from "swr";
import {
  AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";
import type { BusinessStatsResponse, ProfitReportResponse } from "@/lib/api";

// ── Helpers ───────────────────────────────────────────────────────────────────
function KpiCard({ icon, label, value, accent, sub }: {
  icon: string; label: string; value: string; accent: string; sub?: string;
}) {
  const { stealth } = useStealth();
  const c = stealth ? "#334155" : accent;
  return (
    <div style={{
      background: "linear-gradient(135deg, #0f172a, #0a0e1a)",
      border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
      borderRadius: "12px",
      padding: "1.25rem",
      boxShadow: stealth ? "none" : `0 0 20px ${c}18`,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", marginBottom: "0.5rem" }}>
        <span>{icon}</span>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#475569", textTransform: "uppercase", letterSpacing: "0.1em" }}>{label}</span>
      </div>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: "1.6rem", fontWeight: 700, color: stealth ? "#475569" : c, textShadow: stealth ? "none" : `0 0 14px ${c}66` }}>
        {value}
      </div>
      {sub && <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155", marginTop: "0.25rem" }}>{sub}</div>}
    </div>
  );
}

const TOOLTIP_STYLE = {
  background: "#0f172a",
  border: "1px solid #1e293b",
  borderRadius: 6,
  fontFamily: "monospace",
  fontSize: 11,
  color: "#94a3b8",
};

// ── Page ──────────────────────────────────────────────────────────────────────
export default function TreasuryPage() {
  const { stealth } = useStealth();

  const { data: biz }    = useSWR<BusinessStatsResponse>("/api/business/stats",  swrFetcher<BusinessStatsResponse>,  { refreshInterval: 60_000 });
  const { data: report } = useSWR<ProfitReportResponse>("/api/business/report", swrFetcher<ProfitReportResponse>, { refreshInterval: 120_000 });

  const roi = report?.estimated_roi ?? 0;
  const roiSign = roi >= 0 ? "+" : "";
  const roiColor = stealth ? "#334155" : (roi >= 0 ? "#22c55e" : "#ef4444");

  // Build ROI trend from forecast history
  const forecastDates = biz?.forecast_history ?? [];
  const roiTrend = forecastDates.slice().reverse().map((date, i) => ({
    date,
    roi: Math.max(0, roi - (forecastDates.length - 1 - i) * Math.max(1, Math.floor(roi / 8))),
  }));

  // Revenue vs Cost bar data (synthetic from available metrics)
  const targets = report?.target_groups ?? biz?.target_groups ?? 0;
  const pipeline = report?.total_pipeline ?? biz?.total_users_pipeline ?? 0;
  const costPerK = 0.5;
  const pricePerGroup = 10;
  const totalCost = targets * 1 * costPerK;
  const revenue = targets * pricePerGroup * 0.8;

  const revCostData = [
    { name: "Realistic", revenue: Math.round(revenue), cost: Math.round(totalCost), net: Math.round(revenue - totalCost) },
    { name: "Liquidation", revenue: Math.round(revenue * 0.875), cost: Math.round(totalCost), net: Math.round(revenue * 0.875 - totalCost) },
  ];

  const gridColor = stealth ? "#0f172a" : "#1e293b";
  const axisColor = "#334155";

  return (
    <PageTransition>
      <div style={{ maxWidth: "1400px", margin: "0 auto", padding: "2rem 1.5rem" }}>
        <div style={{ marginBottom: "1.5rem" }}>
          <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.1rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: stealth ? "#334155" : "#f1f5f9", marginBottom: "0.25rem" }}>
            💰 Treasury
          </h1>
          <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569" }}>
            Revenue analytics — Mangement Ahu financial intelligence
          </p>
        </div>

        {/* ── KPI row ── */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: "1rem", marginBottom: "2rem" }}>
          <KpiCard icon="📈" label="Estimated ROI" value={`${roiSign}${roi}%`} accent={roi >= 0 ? "#22c55e" : "#ef4444"} sub="realistic scenario" />
          <KpiCard icon="💵" label="Net Revenue"   value={`$${Math.round(revenue - totalCost)}`} accent="#00ff88" sub="after costs" />
          <KpiCard icon="🎯" label="Target Groups" value={String(targets)} accent="#22d3ee" sub="active targets" />
          <KpiCard icon="👤" label="Pipeline"      value={String(pipeline)} accent="#f59e0b" sub="users ready" />
          <KpiCard icon="🤖" label="Sessions"      value={String(report?.active_sessions ?? biz?.active_sessions ?? 0)} accent="#6366f1" sub="active" />
          <KpiCard icon="❤️" label="Health"        value={`${(report?.health_ratio ?? 0).toFixed(0)}%`} accent="#22c55e" sub="session ratio" />
        </div>

        {/* ── Charts ── */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.5rem", marginBottom: "2rem" }}>

          {/* ROI Trend */}
          <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.25rem" }}>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", marginBottom: "1rem" }}>
              📈 ROI Trend
            </div>
            {roiTrend.length < 2 ? (
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#334155", padding: "2rem", textAlign: "center" }}>
                Insufficient forecast history
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={180}>
                <AreaChart data={roiTrend} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
                  <defs>
                    <linearGradient id="roiGrad" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={stealth ? "#334155" : "#22c55e"} stopOpacity={0.3} />
                      <stop offset="100%" stopColor={stealth ? "#334155" : "#22c55e"} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                  <XAxis dataKey="date" tick={{ fontSize: 9, fill: axisColor, fontFamily: "monospace" }} axisLine={false} tickLine={false} />
                  <YAxis hide />
                  <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [`${v}%`, "ROI"]} />
                  <Area type="monotone" dataKey="roi" stroke={stealth ? "#334155" : "#22c55e"} strokeWidth={2} fill="url(#roiGrad)" dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            )}
          </div>

          {/* Revenue vs Cost */}
          <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.25rem" }}>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", marginBottom: "1rem" }}>
              💵 Revenue vs Cost
            </div>
            <ResponsiveContainer width="100%" height={180}>
              <BarChart data={revCostData} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridColor} vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize: 9, fill: axisColor, fontFamily: "monospace" }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 9, fill: axisColor, fontFamily: "monospace" }} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [`$${v}`, ""]} />
                <Legend wrapperStyle={{ fontFamily: "monospace", fontSize: 9, color: axisColor }} />
                <Bar dataKey="revenue" fill={stealth ? "#1e293b" : "#22c55e"} radius={[3, 3, 0, 0]} />
                <Bar dataKey="cost"    fill={stealth ? "#0f172a" : "#ef4444"} radius={[3, 3, 0, 0]} />
                <Bar dataKey="net"     fill={stealth ? "#1e293b" : "#6366f1"} radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Last report timestamp */}
        {report?.generated_at && (
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#334155", textAlign: "right" }}>
            Last audit: {new Date(report.generated_at).toLocaleString()}
          </div>
        )}
      </div>
    </PageTransition>
  );
}
