"use client";

import { useState } from "react";
import useSWR from "swr";
import {
  swrFetcher,
  forceScrape,
  scaleWorker,
  triggerSuperScrape,
} from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import type {
  BusinessStatsResponse,
  ClusterStatusResponse,
  ProfitReportResponse,
  ScrapeStatusResponse,
  ScrapeStatusValue,
  SuperScraperStatusResponse,
  WindowedStatsResponse,
} from "@/lib/api";
import { SectionHeader } from "@/components/ClusterStatus";

// ─────────────────────────────────────────────────────────────────────────────
// Glassmorphism card
// ─────────────────────────────────────────────────────────────────────────────

interface CardProps {
  icon: string;
  label: string;
  value: string | number;
  sub?: string;
  accent: string;       // neon colour for glow + value text
  loading?: boolean;
  detail?: string;      // small secondary line
}

function GlassCard({ icon, label, value, sub, accent, loading, detail }: CardProps) {
  const { stealth } = useStealth();

  return (
    <div
      className="relative flex flex-col gap-2 rounded-2xl p-5 overflow-hidden"
      style={{
        background: "linear-gradient(135deg, rgba(15,23,42,0.85) 0%, rgba(8,13,24,0.92) 100%)",
        backdropFilter: "blur(16px)",
        WebkitBackdropFilter: "blur(16px)",
        border: `1px solid ${stealth ? "#1e293b" : `${accent}33`}`,
        boxShadow: stealth
          ? "none"
          : `0 0 30px ${accent}18, 0 0 60px ${accent}0a, inset 0 1px 0 ${accent}15`,
        transition: "box-shadow 0.3s, border-color 0.3s",
      }}
    >
      {/* Top glass reflection */}
      <div
        className="absolute top-0 left-4 right-4 h-px rounded-full"
        style={{
          background: stealth
            ? "transparent"
            : `linear-gradient(90deg, transparent, ${accent}44, transparent)`,
        }}
      />

      {/* Subtle corner glow */}
      {!stealth && (
        <div
          className="absolute top-0 right-0 w-20 h-20 rounded-full pointer-events-none"
          style={{
            background: `radial-gradient(circle, ${accent}18 0%, transparent 70%)`,
            transform: "translate(30%, -30%)",
          }}
        />
      )}

      {/* Icon + label */}
      <div className="flex items-center gap-2 z-10">
        <span className="text-xl">{icon}</span>
        <span
          className="font-mono text-[10px] font-bold tracking-[0.15em] uppercase"
          style={{ color: stealth ? "#334155" : "#64748b" }}
        >
          {label}
        </span>
      </div>

      {/* Main value */}
      <div className="z-10">
        {loading ? (
          <div
            className="h-8 w-24 rounded animate-pulse"
            style={{ background: "#1e293b" }}
          />
        ) : (
          <span
            className="font-mono text-3xl font-bold tracking-tight"
            style={{
              color: stealth ? "#475569" : accent,
              textShadow: stealth ? "none" : `0 0 20px ${accent}66`,
            }}
          >
            {value}
          </span>
        )}
      </div>

      {/* Sub label */}
      {sub && (
        <div
          className="font-mono text-[10px] tracking-widest z-10"
          style={{ color: stealth ? "#1e293b" : `${accent}88` }}
        >
          {sub}
        </div>
      )}

      {/* Detail line */}
      {detail && (
        <div
          className="font-mono text-[9px] truncate z-10"
          style={{ color: "#334155" }}
        >
          {detail}
        </div>
      )}

      {/* Bottom accent line */}
      <div
        className="absolute bottom-0 left-4 right-4 h-px rounded-full"
        style={{
          background: stealth
            ? "transparent"
            : `linear-gradient(90deg, transparent, ${accent}33, transparent)`,
        }}
      />
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Status badge
// ─────────────────────────────────────────────────────────────────────────────

function StatusBadge({ available }: { available: boolean }) {
  const { stealth } = useStealth();
  const c = available ? "#22c55e" : "#f59e0b";
  return (
    <span
      className="inline-flex items-center gap-1.5 font-mono text-[9px] font-bold tracking-widest px-2 py-0.5 rounded-full"
      style={{
        color: stealth ? "#334155" : c,
        background: stealth ? "transparent" : `${c}15`,
        border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
      }}
    >
      <span
        className="rounded-full"
        style={{
          width: 5,
          height: 5,
          background: stealth ? "#334155" : c,
          boxShadow: stealth ? "none" : `0 0 4px ${c}`,
          display: "inline-block",
        }}
      />
      {available ? "DB LIVE" : "DB OFFLINE"}
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scrape status badge
// ─────────────────────────────────────────────────────────────────────────────

const SCRAPE_COLORS: Record<ScrapeStatusValue, string> = {
  idle:          "#475569",
  pending:       "#f59e0b",
  running:       "#6366f1",
  completed:     "#22c55e",
  failed:        "#ef4444",
  low_resources: "#f59e0b",
};

const SCRAPE_LABELS: Record<ScrapeStatusValue, string> = {
  idle:          "IDLE",
  pending:       "PENDING",
  running:       "SCANNING...",
  completed:     "COMPLETED",
  failed:        "FAILED",
  low_resources: "LOW RESOURCES",
};

function ScrapeBadge({
  scrapeStatus,
  onForce,
}: {
  scrapeStatus: ScrapeStatusResponse | undefined;
  onForce: () => void;
}) {
  const { stealth } = useStealth();
  const status: ScrapeStatusValue = scrapeStatus?.status ?? "idle";
  const c = stealth ? "#334155" : SCRAPE_COLORS[status];
  const isRunning = status === "running" || status === "pending";

  return (
    <div className="flex items-center gap-2 flex-wrap">
      {/* Status pill */}
      <span
        className="inline-flex items-center gap-1.5 font-mono text-[9px] font-bold tracking-widest px-2 py-1 rounded-full"
        style={{
          color: c,
          background: stealth ? "transparent" : `${c}15`,
          border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
        }}
      >
        {/* Animated dot */}
        <span
          className="rounded-full shrink-0"
          style={{
            width: 6,
            height: 6,
            background: c,
            boxShadow: stealth ? "none" : `0 0 5px ${c}`,
            display: "inline-block",
            animation: isRunning && !stealth ? "rgb-pulse 1s infinite" : "none",
          }}
        />
        NIGHTLY SCRAPE: {SCRAPE_LABELS[status]}
      </span>

      {/* Force Scrape button */}
      <button
        onClick={onForce}
        disabled={isRunning}
        className="font-mono text-[9px] font-bold tracking-widest px-2 py-1 rounded-lg transition-all duration-150"
        style={{
          color: isRunning ? "#334155" : stealth ? "#334155" : "#6366f1",
          border: `1px solid ${isRunning ? "#1e293b" : stealth ? "#1e293b" : "#6366f155"}`,
          background: "transparent",
          cursor: isRunning ? "not-allowed" : "pointer",
        }}
        title="Trigger an immediate scrape run"
      >
        ⚡ FORCE SCRAPE
      </button>

      {/* Detail tooltip */}
      {scrapeStatus?.detail && (
        <span className="font-mono text-[8px] truncate max-w-[200px]" style={{ color: "#334155" }}>
          {scrapeStatus.detail}
        </span>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Force Scrape confirmation modal
// ─────────────────────────────────────────────────────────────────────────────

function ForceScrapeModal({
  onConfirm,
  onCancel,
  loading,
  result,
}: {
  onConfirm: () => void;
  onCancel: () => void;
  loading: boolean;
  result: string | null;
}) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: "rgba(2,6,23,0.88)", backdropFilter: "blur(10px)" }}
      onClick={onCancel}
    >
      <div
        className="relative w-full max-w-sm rounded-2xl p-6 flex flex-col gap-4"
        style={{
          background: "linear-gradient(145deg, #0f172a, #0d1117)",
          border: "1px solid #6366f155",
          boxShadow: "0 0 40px #6366f122",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Top accent */}
        <div className="absolute top-0 left-6 right-6 h-px rounded-full"
          style={{ background: "linear-gradient(90deg, transparent, #6366f1, transparent)" }} />

        <div className="flex items-center gap-3">
          <span className="text-xl">⚡</span>
          <span className="font-mono text-sm font-bold tracking-widest" style={{ color: "#6366f1" }}>
            FORCE SCRAPE
          </span>
        </div>

        {result ? (
          <div className="font-mono text-xs" style={{ color: "#22c55e" }}>
            ✓ {result}
          </div>
        ) : (
          <>
            <p className="font-mono text-[11px] leading-relaxed" style={{ color: "#64748b" }}>
              This will immediately enqueue a <span style={{ color: "#f1f5f9" }}>telegram.auto_scrape</span> task,
              bypassing the nightly schedule and the minimum rescrape interval.
            </p>
            <p className="font-mono text-[10px]" style={{ color: "#334155" }}>
              The task will run on the next available worker. CPU usage will be checked first.
            </p>
          </>
        )}

        <div className="flex gap-3">
          {!result && (
            <button
              onClick={onConfirm}
              disabled={loading}
              className="flex-1 py-2 rounded-lg font-mono text-xs font-bold tracking-widest transition-all"
              style={{
                background: loading ? "#1e293b" : "#6366f1",
                color: loading ? "#475569" : "#fff",
                border: "none",
                cursor: loading ? "not-allowed" : "pointer",
              }}
            >
              {loading ? "ENQUEUEING..." : "✓ CONFIRM SCRAPE"}
            </button>
          )}
          <button
            onClick={onCancel}
            className="flex-1 py-2 rounded-lg font-mono text-xs font-bold tracking-widest"
            style={{
              background: "transparent",
              color: "#475569",
              border: "1px solid #1e293b",
              cursor: "pointer",
            }}
          >
            {result ? "CLOSE" : "✗ CANCEL"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Forecast history pill row
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Hunting Status Card (Super-Scraper)
// ─────────────────────────────────────────────────────────────────────────────

const HUNT_STATUS_COLOR: Record<string, string> = {
  idle:               "#334155",
  hunting:            "#a855f7",
  discovering:        "#6366f1",
  awaiting_approval:  "#f59e0b",
  postponed:          "#475569",
  completed:          "#22c55e",
  no_new_groups:      "#22c55e",
};

const HUNT_STATUS_LABEL: Record<string, string> = {
  idle:               "IDLE",
  hunting:            "HUNTING...",
  discovering:        "DISCOVERING...",
  awaiting_approval:  "AWAITING APPROVAL",
  postponed:          "POSTPONED",
  completed:          "COMPLETED",
  no_new_groups:      "NO NEW GROUPS",
};

function HuntingStatusCard({
  superStatus,
  stealth,
}: {
  superStatus: SuperScraperStatusResponse | undefined;
  stealth: boolean;
}) {
  const status = superStatus?.status ?? "idle";
  const c = stealth ? "#334155" : (HUNT_STATUS_COLOR[status] ?? "#334155");
  const isActive = status === "hunting" || status === "discovering";
  const label = HUNT_STATUS_LABEL[status] ?? status.toUpperCase();

  return (
    <div
      className="flex items-center justify-between rounded-xl px-4 py-3 mb-4"
      style={{
        background: stealth ? "#0d1117" : `${c}08`,
        border: `1px solid ${stealth ? "#1e293b" : `${c}33`}`,
        boxShadow: stealth ? "none" : isActive ? `0 0 20px ${c}18` : "none",
        transition: "box-shadow 0.5s, border-color 0.5s",
      }}
    >
      <div className="flex items-center gap-3">
        <span style={{ fontSize: "1rem" }}>🎯</span>
        <div className="flex flex-col gap-0.5">
          <span
            className="font-mono text-[9px] font-bold tracking-widest uppercase"
            style={{ color: stealth ? "#334155" : "#475569" }}
          >
            Strategic Hunter
          </span>
          <span
            className="font-mono text-[11px] truncate max-w-[280px]"
            style={{ color: stealth ? "#334155" : "#64748b" }}
          >
            {superStatus?.detail || "No hunt in progress"}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-3">
        {superStatus?.candidates_pending ? (
          <span
            className="font-mono text-[9px] font-bold tracking-widest px-2 py-0.5 rounded-full"
            style={{
              color: stealth ? "#334155" : "#f59e0b",
              background: stealth ? "transparent" : "#f59e0b12",
              border: `1px solid ${stealth ? "#1e293b" : "#f59e0b33"}`,
            }}
          >
            {superStatus.candidates_pending} PENDING
          </span>
        ) : null}

        <span
          className="inline-flex items-center gap-1.5 font-mono text-[9px] font-bold tracking-widest px-2 py-1 rounded-full"
          style={{
            color: c,
            background: stealth ? "transparent" : `${c}12`,
            border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
          }}
        >
          <span
            className="rounded-full shrink-0"
            style={{
              width: 5,
              height: 5,
              background: c,
              display: "inline-block",
              boxShadow: isActive && !stealth ? `0 0 5px ${c}` : "none",
              animation: isActive && !stealth ? "rgb-pulse 0.8s infinite" : "none",
            }}
          />
          {label}
        </span>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Forecast history pill row
// ─────────────────────────────────────────────────────────────────────────────

function ForecastRow({ dates }: { dates: string[] }) {
  const { stealth } = useStealth();
  if (dates.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1.5 mt-1">
      {dates.slice(0, 8).map((d, i) => (
        <span
          key={i}
          className="font-mono text-[8px] px-2 py-0.5 rounded"
          style={{
            background: stealth ? "#0f172a" : "#0f172a",
            border: "1px solid #1e293b",
            color: "#334155",
          }}
        >
          {d}
        </span>
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────────────────────────────────

export default function IntelDashboard() {
  // ── Time-window toggle: "60m" | "24h" ─────────────────────────────────────
  const [window, setWindow] = useState<"60m" | "24h">("24h");
  const windowMinutes = window === "60m" ? 60 : 1440;

  // Base stats (totals, no window)
  const { data: biz, isLoading: bizLoading } = useSWR<BusinessStatsResponse>(
    "/api/business/stats",
    swrFetcher<BusinessStatsResponse>,
    { refreshInterval: 60_000 }   // refresh every minute
  );

  // Windowed stats (new users in window)
  const { data: windowed } = useSWR<WindowedStatsResponse>(
    `/api/business/stats/windowed?window=${windowMinutes}`,
    swrFetcher<WindowedStatsResponse>,
    { refreshInterval: 60_000 }   // refresh every minute
  );

  // Latest profit report
  const { data: report } = useSWR<ProfitReportResponse>(
    "/api/business/report",
    swrFetcher<ProfitReportResponse>,
    { refreshInterval: 300_000 }  // refresh every 5 min
  );

  const { data: cluster } = useSWR<ClusterStatusResponse>(
    "/api/cluster/status",
    swrFetcher<ClusterStatusResponse>,
    { refreshInterval: 10_000 }
  );

  const { data: scrapeStatus, mutate: mutateScrape } = useSWR<ScrapeStatusResponse>(
    "/api/business/scrape-status",
    swrFetcher<ScrapeStatusResponse>,
    {
      refreshInterval: (latestData: ScrapeStatusResponse | undefined) => {
        const s = latestData?.status;
        return s === "running" || s === "pending" ? 3_000 : 15_000;
      },
    }
  );

  const [showModal, setShowModal] = useState(false);
  const [forceLoading, setForceLoading] = useState(false);
  const [forceResult, setForceResult] = useState<string | null>(null);
  const [scaleMsg, setScaleMsg] = useState<string | null>(null);
  const [scaleLoading, setScaleLoading] = useState(false);
  const [huntLoading, setHuntLoading] = useState(false);
  const [huntMsg, setHuntMsg] = useState<string | null>(null);

  const { stealth, stealthOverride, toggleOverride } = useStealth();

  const { data: superStatus } = useSWR<SuperScraperStatusResponse>(
    "/api/super-scraper/status",
    swrFetcher<SuperScraperStatusResponse>,
    {
      refreshInterval: (d: SuperScraperStatusResponse | undefined) => {
        const s = d?.status;
        return s === "hunting" || s === "discovering" ? 3_000 : 15_000;
      },
    }
  );

  async function handleSuperScrape() {
    setHuntLoading(true);
    setHuntMsg(null);
    try {
      const res = await triggerSuperScrape(stealthOverride);
      setHuntMsg(res.message);
    } catch (err) {
      setHuntMsg(err instanceof Error ? err.message : "Hunt failed");
    } finally {
      setHuntLoading(false);
      setTimeout(() => setHuntMsg(null), 6000);
    }
  }

  async function handleForceScrape() {
    setForceLoading(true);
    setForceResult(null);
    try {
      const res = await forceScrape([], true);
      setForceResult(res.message);
      await mutateScrape();
    } catch (err) {
      setForceResult(err instanceof Error ? err.message : "Failed to enqueue");
    } finally {
      setForceLoading(false);
    }
  }

  function handleModalClose() {
    setShowModal(false);
    setForceResult(null);
  }

  async function handleScaleWorker() {
    setScaleLoading(true);
    setScaleMsg(null);
    try {
      const res = await scaleWorker();
      setScaleMsg(res.message);
    } catch (err) {
      setScaleMsg(err instanceof Error ? err.message : "Scale failed");
    } finally {
      setScaleLoading(false);
      setTimeout(() => setScaleMsg(null), 6000);
    }
  }

  // Derived values — use windowed data when available
  const activeWorkers = cluster?.nodes.filter((n) => n.role === "worker" && n.online).length ?? 0;
  const masterOnline = cluster?.nodes.some((n) => n.role === "master" && n.online) ?? false;
  const clusterPower = activeWorkers + (masterOnline ? 1 : 0);

  // For the window-aware cards, prefer windowed stats over base stats
  const newScraped = windowed?.new_scraped_users_window ?? biz?.total_scraped_users ?? 0;
  const windowLabel = window === "60m" ? "Last 60 min" : "Last 24 h";
  const growthLabel = windowed
    ? `${newScraped} new · ${windowed.total_managed_groups} groups`
    : biz
    ? `${biz.total_scraped_users} scraped · ${biz.total_managed_groups} groups`
    : "—";

  // ROI from report if available, else proxy
  const roiValue = report
    ? `${report.estimated_roi >= 0 ? "+" : ""}${report.estimated_roi}%`
    : biz && biz.total_targets > 0
    ? `${((biz.total_users_pipeline / Math.max(biz.total_targets, 1)) * 100).toFixed(0)}%`
    : biz
    ? `${biz.total_users_pipeline}`
    : "—";

  const roiSub = report
    ? `${report.total_pipeline} users · ${report.target_groups} targets`
    : biz
    ? `${biz.total_users_pipeline} users · ${biz.target_groups} targets`
    : "pipeline ratio";

  return (
    <section>
      {/* Header row */}
      <div className="flex items-start justify-between mb-5 gap-4 flex-wrap">
        <SectionHeader
          title="Operational Intelligence"
          subtitle={`Telefix · Mangement Ahu — ${windowLabel} · updates every minute`}
        />
        <div className="flex flex-col items-end gap-2">
          {/* ── Time-window toggle ── */}
          <div
            className="flex rounded-lg overflow-hidden"
            style={{ border: "1px solid #1e293b" }}
          >
            {(["60m", "24h"] as const).map((w) => (
              <button
                key={w}
                onClick={() => setWindow(w)}
                className="font-mono text-[9px] font-bold tracking-widest px-3 py-1 transition-all"
                style={{
                  background: window === w
                    ? (stealth ? "#1e293b" : "#6366f1")
                    : "transparent",
                  color: window === w
                    ? (stealth ? "#475569" : "#fff")
                    : (stealth ? "#334155" : "#475569"),
                  border: "none",
                  cursor: "pointer",
                }}
              >
                {w.toUpperCase()}
              </button>
            ))}
          </div>

          {biz && <StatusBadge available={biz.db_available} />}
          <ScrapeBadge
            scrapeStatus={scrapeStatus}
            onForce={() => setShowModal(true)}
          />
        </div>
      </div>

      {/* Force Scrape modal */}
      {showModal && (
        <ForceScrapeModal
          onConfirm={handleForceScrape}
          onCancel={handleModalClose}
          loading={forceLoading}
          result={forceResult}
        />
      )}

      {/* ── 4 primary KPI cards ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4">

        {/* 💰 Financial ROI */}
        <GlassCard
          icon="💰"
          label="Financial ROI"
          value={roiValue}
          sub={roiSub}
          accent="#00ff88"
          loading={bizLoading}
          detail={biz?.last_forecast_run ? `Forecast: ${biz.last_forecast_run}` : undefined}
        />

        {/* 🤖 Active Sessions */}
        <GlassCard
          icon="🤖"
          label="Active Sessions"
          value={biz?.active_sessions ?? "—"}
          sub={`${biz?.frozen_sessions ?? 0} frozen · ${biz?.manager_sessions ?? 0} managers`}
          accent="#6366f1"
          loading={bizLoading}
          detail={biz?.last_adder_run ? `Last adder: ${biz.last_adder_run}` : undefined}
        />

        {/* 📈 Growth */}
        <GlassCard
          icon="📈"
          label={`Growth (${windowLabel})`}
          value={newScraped}
          sub={growthLabel}
          accent="#f59e0b"
          loading={bizLoading}
          detail={biz?.last_scraper_run ? `Last scrape: ${biz.last_scraper_run}` : undefined}
        />

        {/* ⚡ Cluster Power */}
        <GlassCard
          icon="⚡"
          label="Cluster Power"
          value={clusterPower}
          sub={`${activeWorkers} worker${activeWorkers !== 1 ? "s" : ""} · ${masterOnline ? "master ✓" : "master ✗"}`}
          accent="#22d3ee"
          loading={false}
          detail={cluster ? `${cluster.queues[0]?.pending_jobs ?? 0} jobs queued` : undefined}
        />
      </div>

      {/* ── Hunting Status (Super-Scraper) ── */}
      <HuntingStatusCard superStatus={superStatus} stealth={stealth} />

      {/* ── Stealth Override + Hunt buttons ── */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        {/* Stealth Override toggle — only shown in stealth mode */}
        {stealth && (
          <button
            onClick={toggleOverride}
            className="flex items-center gap-2 font-mono text-[10px] font-bold tracking-widest px-3 py-2 rounded-xl transition-all duration-150"
            style={{
              color: stealthOverride ? "#f59e0b" : "#334155",
              border: `1px solid ${stealthOverride ? "#f59e0b44" : "#1e293b"}`,
              background: stealthOverride ? "#f59e0b08" : "transparent",
              cursor: "pointer",
              boxShadow: stealthOverride ? "0 0 12px #f59e0b18" : "none",
            }}
            title="Allow Super-Scraper to run at full CPU priority even in Stealth Mode"
          >
            <span style={{ fontSize: "0.9rem" }}>⚡</span>
            {stealthOverride ? "STEALTH OVERRIDE: ON" : "STEALTH OVERRIDE"}
          </button>
        )}

        {/* Hunt Now button */}
        <button
          onClick={handleSuperScrape}
          disabled={huntLoading}
          className="flex items-center gap-2 font-mono text-[10px] font-bold tracking-widest px-3 py-2 rounded-xl transition-all duration-150"
          style={{
            color: huntLoading ? "#334155" : stealth ? "#334155" : "#a855f7",
            border: `1px solid ${huntLoading ? "#1e293b" : stealth ? "#1e293b" : "#a855f744"}`,
            background: stealth ? "transparent" : huntLoading ? "transparent" : "#a855f708",
            cursor: huntLoading ? "not-allowed" : "pointer",
            boxShadow: stealth || huntLoading ? "none" : "0 0 12px #a855f718",
          }}
          title="Launch Strategic Super-Scraper to hunt for new high-value groups"
        >
          <span style={{ fontSize: "0.9rem" }}>🎯</span>
          {huntLoading ? "HUNTING..." : "HUNT NEW NICHES"}
        </button>
        {huntMsg && (
          <span className="font-mono text-[9px]" style={{ color: "#22c55e" }}>
            ✓ {huntMsg}
          </span>
        )}
      </div>

      {/* ── One-Click Scale Worker ── */}
      <div className="flex items-center gap-3 mb-4 flex-wrap">
        <button
          onClick={handleScaleWorker}
          disabled={scaleLoading}
          className="flex items-center gap-2 font-mono text-[10px] font-bold tracking-widest px-3 py-2 rounded-xl transition-all duration-150"
          style={{
            color: scaleLoading ? "#334155" : stealth ? "#334155" : "#22d3ee",
            border: `1px solid ${scaleLoading ? "#1e293b" : stealth ? "#1e293b" : "#22d3ee44"}`,
            background: stealth ? "transparent" : scaleLoading ? "transparent" : "#22d3ee08",
            cursor: scaleLoading ? "not-allowed" : "pointer",
            boxShadow: stealth || scaleLoading ? "none" : "0 0 12px #22d3ee18",
          }}
          title="Deploy a new worker container via Docker"
        >
          <span style={{ fontSize: "0.9rem" }}>🐳</span>
          {scaleLoading ? "DEPLOYING..." : "ONE-CLICK SCALE WORKER"}
        </button>
        {scaleMsg && (
          <span className="font-mono text-[9px]" style={{ color: "#22c55e" }}>
            ✓ {scaleMsg}
          </span>
        )}
      </div>

      {/* ── Secondary detail row ── */}
      {biz && (
        <div
          className="rounded-2xl p-4 flex flex-col gap-3"
          style={{
            background: "linear-gradient(135deg, rgba(15,23,42,0.7) 0%, rgba(8,13,24,0.8) 100%)",
            backdropFilter: "blur(12px)",
            border: `1px solid ${stealth ? "#1e293b" : "#1e293b"}`,
          }}
        >
          {/* Target breakdown */}
          <div className="flex flex-wrap gap-6">
            {[
              ["Source Groups",  biz.source_groups,         "#6366f1"],
              ["Target Groups",  biz.target_groups,         "#00ff88"],
              ["Total Targets",  biz.total_targets,         "#f59e0b"],
              ["Pipeline Users", biz.total_users_pipeline,  "#22d3ee"],
            ].map(([label, val, color]) => (
              <div key={String(label)} className="flex flex-col">
                <span className="font-mono text-[9px] tracking-widest uppercase"
                  style={{ color: "#334155" }}>
                  {label}
                </span>
                <span className="font-mono text-lg font-bold"
                  style={{ color: stealth ? "#475569" : String(color) }}>
                  {val}
                </span>
              </div>
            ))}
          </div>

          {/* Forecast history */}
          {biz.forecast_history.length > 0 && (
            <div>
              <span className="font-mono text-[9px] tracking-widest uppercase"
                style={{ color: "#334155" }}>
                Forecast History
              </span>
              <ForecastRow dates={biz.forecast_history} />
            </div>
          )}
        </div>
      )}
    </section>
  );
}
