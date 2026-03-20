"use client";

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";
import type { BusinessStatsResponse, ScrapeStatusResponse } from "@/lib/api";

// ── Section header ────────────────────────────────────────────────────────────
function PageHeader({ title, sub }: { title: string; sub: string }) {
  const { stealth } = useStealth();
  return (
    <div style={{ marginBottom: "1.5rem" }}>
      <h1 style={{
        fontFamily: "var(--font-mono)",
        fontSize: "1.1rem",
        fontWeight: 700,
        letterSpacing: "0.1em",
        textTransform: "uppercase",
        color: stealth ? "#334155" : "#f1f5f9",
        marginBottom: "0.25rem",
      }}>
        🎯 {title}
      </h1>
      <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569" }}>
        {sub}
      </p>
    </div>
  );
}

// ── Stat card ─────────────────────────────────────────────────────────────────
function StatCard({ icon, label, value, accent = "#6366f1", sub }: {
  icon: string; label: string; value: string | number; accent?: string; sub?: string;
}) {
  const { stealth } = useStealth();
  const c = stealth ? "#334155" : accent;
  return (
    <div style={{
      background: "linear-gradient(135deg, #0f172a, #0a0e1a)",
      border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
      borderRadius: "12px",
      padding: "1.25rem",
      display: "flex",
      flexDirection: "column",
      gap: "0.5rem",
      boxShadow: stealth ? "none" : `0 0 20px ${c}18`,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
        <span style={{ fontSize: "1rem" }}>{icon}</span>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#475569", letterSpacing: "0.1em", textTransform: "uppercase" }}>
          {label}
        </span>
      </div>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "1.8rem", fontWeight: 700, color: stealth ? "#475569" : c, textShadow: stealth ? "none" : `0 0 16px ${c}66` }}>
        {value}
      </span>
      {sub && <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#334155" }}>{sub}</span>}
    </div>
  );
}

// ── Scrape history row ────────────────────────────────────────────────────────
function ScrapeRow({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "0.5rem 0", borderBottom: "1px solid #0f172a" }}>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569" }}>{label}</span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", fontWeight: 600, color }}>{value}</span>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function OperationsPage() {
  const { stealth } = useStealth();

  const { data: biz } = useSWR<BusinessStatsResponse>(
    "/api/business/stats", swrFetcher<BusinessStatsResponse>, { refreshInterval: 30_000 }
  );
  const { data: scrape } = useSWR<ScrapeStatusResponse>(
    "/api/business/scrape-status", swrFetcher<ScrapeStatusResponse>, { refreshInterval: 8_000 }
  );

  const scrapeRunning = scrape?.status === "running" || scrape?.status === "pending";

  return (
    <PageTransition>
      <div style={{ maxWidth: "1400px", margin: "0 auto", padding: "2rem 1.5rem" }}>
        <PageHeader title="Operations" sub="Mangement Ahu project management — scrape & add pipeline" />

        {/* ── KPI row ── */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: "1rem", marginBottom: "2rem" }}>
          <StatCard icon="📡" label="Source Groups"  value={biz?.source_groups ?? "—"} accent="#6366f1" />
          <StatCard icon="🎯" label="Target Groups"  value={biz?.target_groups ?? "—"} accent="#22d3ee" />
          <StatCard icon="👤" label="Scraped Users"  value={biz?.total_scraped_users ?? "—"} accent="#00ff88" sub="distinct" />
          <StatCard icon="📦" label="Pipeline"       value={biz?.total_users_pipeline ?? "—"} accent="#f59e0b" sub="users" />
          <StatCard icon="🤖" label="Active Sessions" value={biz?.active_sessions ?? "—"} accent="#22c55e" />
          <StatCard icon="❄️" label="Frozen Sessions" value={biz?.frozen_sessions ?? "—"} accent="#ef4444" />
        </div>

        {/* ── Two-column layout ── */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.5rem" }}>

          {/* Scraper status */}
          <div style={{ background: "#0a0e1a", border: `1px solid ${stealth ? "#1e293b" : "#1e293b"}`, borderRadius: "12px", padding: "1.25rem" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "1rem" }}>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569" }}>
                🔍 Scraper Status
              </span>
              {scrapeRunning && (
                <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#6366f1", animation: "rgb-pulse 1s infinite" }}>
                  ● RUNNING
                </span>
              )}
            </div>
            <ScrapeRow label="Status"      value={scrape?.status ?? "idle"}   color={scrapeRunning ? "#6366f1" : "#22c55e"} />
            <ScrapeRow label="Last scrape" value={biz?.last_scraper_run ?? "—"} color="#94a3b8" />
            <ScrapeRow label="Last adder"  value={biz?.last_adder_run ?? "—"}  color="#94a3b8" />
            {scrape?.detail && (
              <div style={{ marginTop: "0.75rem", padding: "0.6rem 0.75rem", background: "#020617", borderRadius: "6px", fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#64748b", borderLeft: "3px solid #6366f1" }}>
                {scrape.detail}
              </div>
            )}
          </div>

          {/* Forecast history */}
          <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.25rem" }}>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", display: "block", marginBottom: "1rem" }}>
              📅 Forecast History
            </span>
            {(biz?.forecast_history ?? []).length === 0 ? (
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#334155" }}>No forecast data yet</span>
            ) : (
              <div style={{ display: "flex", flexWrap: "wrap", gap: "0.4rem" }}>
                {(biz?.forecast_history ?? []).map((d, i) => (
                  <span key={i} style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", padding: "2px 8px", borderRadius: "4px", background: "#0f172a", border: "1px solid #1e293b", color: "#64748b" }}>
                    {d}
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </PageTransition>
  );
}
