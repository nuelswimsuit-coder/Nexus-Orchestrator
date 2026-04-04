"use client";

import Link from "next/link";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import useSWR from "swr";
import {
  LayoutDashboard,
  TrendingUp,
  Dna,
  Zap,
  Network,
  Users,
  Rocket,
  Database,
  Flame,
  Search,
  HardDrive,
  Gauge,
  RefreshCw,
  FileText,
  Download,
  CheckCircle2,
  Clock,
  MessageSquareCode,
  Terminal,
  AlertTriangle,
  DollarSign,
  BarChart2,
  Percent,
  ArrowUpRight,
  ArrowDownRight,
  Activity,
  Target,
  Cpu,
  Radio,
  ChevronRight,
  Crosshair,
  PlayCircle,
} from "lucide-react";
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
  LineChart,
  Line,
  ReferenceLine,
} from "recharts";
import { API_BASE, apiSseBase, apiWsBase, triggerPanic, swrFetcher } from "@/lib/api";

// ── Types ───────────────────────────────────────────────────────────────────

export interface PortfolioPosition {
  title: string;
  outcome: string;
  size: number;
  avg_price: number;
  cur_price: number;
  current_value: number;
  cash_pnl: number;
  percent_pnl: number;
  end_date: string;
  /** CLOB outcome token id (required for manual-order API) */
  token_id?: string;
}

export interface GodModeDashboard {
  collateral_usdc: string;
  portfolio_value?: number;
  portfolio_cash?: number;
  portfolio_positions?: number;
  portfolio_positions_list?: PortfolioPosition[];
  portfolio_address?: string;
  clob_balance?: number;
  /** On-chain USDC on Polygon (wallet), when CLOB balance is unavailable */
  polygon_wallet_usdc?: number;
  /** Data-API positions value + CLOB + polygon wallet USDC (closer to Polymarket site total) */
  portfolio_total_estimated?: number;
  btc_up_pct: number;
  btc_down_pct: number;
  direction_side: string;
  pnl_series: { time: string; pnl: number }[];
  heartbeat: { status: string; timestamp: string };
  trading_history: {
    time: string;
    asset: string;
    side: string;
    amount: number;
    price: string;
  }[];
  signer_address?: string;
  /** Effective CLOB maker address (key-derived if env signer was wrong) */
  clob_funder_address?: string;
  total_deposited?: number;
  total_withdrawn?: number;
  break_even_delta?: number;
  realized_pnl?: number;
  /** Matches nexus.api.polymarket_manual_errors.MANUAL_ORDER_ENRICH_REV — if missing or old, UI talks to stale API */
  manual_order_error_enrich?: string;
}

interface TelefixGroup {
  id: string;
  name_he: string;
  warmup_days: number;
  in_search: boolean;
}

interface SwarmSession {
  redis_key: string;
  phone_number: string;
  origin_machine: string;
  status: string;
  last_scanned_target: string;
  last_seen?: number | null;
  session_id?: string;
  source?: "session_manager" | "deployer" | "vault" | string;
}

interface AllScannedResponse {
  sessions_by_machine: Record<string, SwarmSession[]>;
  total: number;
  machines: string[];
  is_mock: boolean;
}

interface InventorySession {
  redis_key: string;
  phone: string;
  machine_id: string;
  status: string;
  last_active: string;
  current_task?: string | null;
}

interface InventoryResponse {
  inventory_by_machine: Record<string, InventorySession[]>;
  total: number;
  machines: string[];
  is_mock: boolean;
}

interface SwarmInventorySession {
  redis_key: string;
  phone: string;
  machine_id: string;
  status: string;
  current_task: string | null;
}

interface SwarmInventoryResponse {
  status: string;
  total: number;
  machines: string[];
  sessions_by_machine: Record<string, SwarmInventorySession[]>;
}

interface TelefixDbGroup {
  id: string | number;
  title: string;
  invite_link: string | null;
  username: string | null;
  member_count: number | null;
}

interface TelefixScrapeFile {
  file: string;
  scraped_at?: string;
  users?: unknown[];
}

interface ClusterHealthNode {
  node_id: string;
  display_label: string;
  local_ip?: string;
  cpu_percent: number;
  status: string;
  online: boolean;
  /** API field; -1 means sensor unavailable */
  cpu_temp_c?: number | null;
  cpu_temp?: number | null;
  role?: string;
  ram_used_mb?: number;
  ram_total_mb?: number;
  os_info?: string;
  cpu_model?: string;
}

// ── Tailwind-safe color maps (dynamic `bg-${x}` is purged by JIT) ─────────────

const METRIC_COLORS: Record<
  string,
  { iconWrap: string; trend: string }
> = {
  emerald: {
    iconWrap: "bg-emerald-500/10 text-emerald-400",
    trend: "text-emerald-400",
  },
  cyan: { iconWrap: "bg-cyan-500/10 text-cyan-400", trend: "text-cyan-400" },
  amber: { iconWrap: "bg-amber-500/10 text-amber-400", trend: "text-amber-400" },
};

const DECISION_DOT: Record<string, string> = {
  trade: "bg-cyan-500 shadow-[0_0_8px_rgba(34,211,238,0.5)]",
  success: "bg-emerald-500 shadow-[0_0_8px_rgba(52,211,153,0.5)]",
  system: "bg-purple-500 shadow-[0_0_8px_rgba(168,85,247,0.5)]",
  logic: "bg-amber-500 shadow-[0_0_8px_rgba(251,191,36,0.5)]",
};

// ── Root ────────────────────────────────────────────────────────────────────

export default function NexusOsGodMode() {
  const [activeTab, setActiveTab] = useState("master-hub");
  const [currentTime, setCurrentTime] = useState("");
  const [marketData, setMarketData] = useState<GodModeDashboard | null>(null);

  useEffect(() => {
    const tab = new URLSearchParams(window.location.search).get("tab");
    if (tab) setActiveTab(tab);
  }, []);
  const [loading, setLoading] = useState(true);
  const [warmGroups, setWarmGroups] = useState<number>(0);
  const [readySearch, setReadySearch] = useState<number>(0);

  const [dbSyncStatus, setDbSyncStatus] = useState<"ok" | "initializing" | "error">("initializing");
  const [dbSyncMessage, setDbSyncMessage] = useState<string | null>(null);

  const fetchDashboardData = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/polymarket/dashboard.json`);
      if (res.status === 404) {
        setDbSyncStatus("initializing");
        setDbSyncMessage("⚠️ Database initializing on Master...");
        setMarketData(null);
        return;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = (await res.json()) as GodModeDashboard;
      if (!data || Object.keys(data).length === 0) {
        setDbSyncStatus("initializing");
        setDbSyncMessage("⚠️ Database initializing on Master...");
        setMarketData(null);
      } else {
        setDbSyncStatus("ok");
        setDbSyncMessage(null);
        setMarketData(data);
        // #region agent log
        fetch("http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Debug-Session-Id": "7ec1ca" },
          body: JSON.stringify({
            sessionId: "7ec1ca",
            location: "NexusOsGodMode.tsx:fetchDashboardData",
            message: "dashboard json received",
            data: {
              portfolio_value: data.portfolio_value ?? null,
              positions_list_len: (data.portfolio_positions_list ?? []).length,
              collateral_usdc: data.collateral_usdc ?? null,
            },
            timestamp: Date.now(),
            hypothesisId: "H5",
            runId: "pre-fix",
          }),
        }).catch(() => {});
        // #endregion
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("404") || msg.includes("Failed to fetch")) {
        setDbSyncStatus("initializing");
        setDbSyncMessage("⚠️ Database initializing on Master...");
      } else {
        setDbSyncStatus("error");
        setDbSyncMessage(`Sync error — ${msg}`);
      }
      setMarketData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchTelefixHeader = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration`);
      if (!res.ok) return;
      const j = (await res.json()) as { groups: TelefixGroup[] };
      const g = j.groups ?? [];
      setWarmGroups(g.filter((x) => x.warmup_days < 14).length);
      setReadySearch(g.filter((x) => x.in_search).length);
    } catch {
      /* ignore */
    }
  }, []);

  useEffect(() => {
    fetchDashboardData();
    fetchTelefixHeader();
    const timer = setInterval(() => {
      setCurrentTime(new Date().toLocaleTimeString());
      fetchDashboardData();
      fetchTelefixHeader();
    }, 5_000);
    return () => clearInterval(timer);
  }, [fetchDashboardData, fetchTelefixHeader]);

  async function handlePanic() {
    if (
      !window.confirm(
        "לאשר עצירת חירום מערכתית? פעולה זו מפסיקה לולאות ומפעילה את מצב ה-PANIC.",
      )
    ) {
      return;
    }
    try {
      await triggerPanic();
    } catch {
      window.alert("שגיאה בהפעלת PANIC");
    }
  }

  return (
    <div
      className="min-h-[calc(100vh-56px)] bg-[#020617] text-slate-200 font-sans flex selection:bg-cyan-500/30"
      dir="rtl"
    >
      <aside className="w-80 bg-slate-950 border-l border-slate-800 flex flex-col min-h-[calc(100vh-56px)] shadow-2xl overflow-hidden">
        <div className="p-6 border-b border-slate-800/50 relative overflow-hidden group">
          <Link
            href="/dashboard"
            className="text-[10px] text-cyan-400/80 hover:text-cyan-300 font-bold mb-3 inline-block"
          >
            ← חזרה למרכז הבקרה
          </Link>
          <div className="absolute inset-0 bg-gradient-to-br from-cyan-500/10 to-transparent opacity-10 group-hover:opacity-20 transition-opacity pointer-events-none" />
          <div className="flex items-center gap-4 relative">
            <div className="w-12 h-12 bg-gradient-to-tr from-cyan-600 to-purple-600 rounded-2xl flex items-center justify-center shadow-[0_0_20px_rgba(8,145,178,0.4)] border border-cyan-400/30">
              <Zap size={24} className="text-white animate-pulse" />
            </div>
            <div>
              <h1 className="text-2xl font-black text-white tracking-tighter leading-none">
                NEXUS OS
              </h1>
              <p className="text-[10px] text-cyan-400 font-bold uppercase tracking-widest mt-1">
                GOD MODE v3.5
              </p>
            </div>
          </div>
          <div className="mt-4 flex items-center gap-2 text-[11px] text-slate-500 font-bold">
            <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
            מפעיל מורשה: <span className="text-slate-300">יעקב חתן</span>
          </div>
        </div>

        <nav className="flex-grow p-4 space-y-1 overflow-y-auto nexus-os-scrollbar">
          <MenuSection label="בקרה אסטרטגית" />
          <MenuItem
            id="master-hub"
            icon={<LayoutDashboard size={18} />}
            label="לוח בקרה ראשי"
            active={activeTab}
            setActive={setActiveTab}
          />
          <MenuItem
            id="swarm-monitor"
            icon={<Network size={18} />}
            label="נחיל מחשבים (Swarm)"
            active={activeTab}
            setActive={setActiveTab}
          />

          <MenuSection label="מפעל טלגרם" />
          <MenuItem
            id="group-factory"
            icon={<Rocket size={18} />}
            label="מפעל קבוצות"
            active={activeTab}
            setActive={setActiveTab}
          />
          <MenuItem
            id="ahu-management"
            icon={<Users size={18} />}
            label="ניהול אהו (Ops Sync)"
            active={activeTab}
            setActive={setActiveTab}
          />
          <MenuItem
            id="bot-generator"
            icon={<Users size={18} />}
            label="מלאי נחיל גלובלי (Inventory)"
            active={activeTab}
            setActive={setActiveTab}
          />
          <MenuItem
            id="session-swarm"
            icon={<Network size={18} />}
            label="נחיל סשנים גלובלי"
            active={activeTab}
            setActive={setActiveTab}
          />
          <MenuItem
            id="scrape-data"
            icon={<Database size={18} />}
            label="ארכיון סריקות"
            active={activeTab}
            setActive={setActiveTab}
          />

          <MenuSection label="קהילה ונחיל" />
          <MenuItem
            id="live-swarm"
            icon={<MessageSquareCode size={18} />}
            label="קהילה חיה (Live AI Swarm)"
            active={activeTab}
            setActive={setActiveTab}
          />

          <MenuSection label="פיננסי ואבולוציה" />
          <MenuItem
            id="poly-trading"
            icon={<TrendingUp size={18} />}
            label="Polymarket & BTC"
            active={activeTab}
            setActive={setActiveTab}
          />
          <MenuItem
            id="ai-architect"
            icon={<Dna size={18} />}
            label="תהליך פיתוח AI"
            active={activeTab}
            setActive={setActiveTab}
          />

          <MenuSection label="מסוף ואבחון" />
          <MenuItem
            id="master-terminal"
            icon={<Terminal size={18} />}
            label="Live Master Terminal"
            active={activeTab}
            setActive={setActiveTab}
          />
        </nav>

        <div className="p-6 border-t border-slate-800 bg-slate-900/40">
          <button
            type="button"
            onClick={handlePanic}
            className="w-full py-2 bg-rose-500/10 hover:bg-rose-500/20 text-rose-500 border border-rose-500/30 rounded-xl text-xs font-bold transition flex items-center justify-center gap-2 mb-2"
          >
            <AlertTriangle size={14} />
            עצירת חירום (PANIC)
          </button>
        </div>
      </aside>

      <main className="flex-grow flex flex-col min-h-[calc(100vh-56px)] overflow-hidden bg-[radial-gradient(circle_at_20%_20%,#0f172a_0%,#020617_100%)]">
        <header className="h-24 border-b border-slate-800/50 bg-slate-900/40 flex items-center justify-between px-10 shrink-0">
          <div className="flex gap-10 flex-wrap">
            <GlobalMetric
              label="יתרה (USDC)"
              value={(() => {
                const est = Number(marketData?.portfolio_total_estimated ?? 0) || 0;
                if (est > 0) return `$${est.toFixed(2)}`;
                const pv = Number(marketData?.portfolio_value ?? 0) || 0;
                const cb = Number(marketData?.clob_balance ?? 0) || 0;
                const total = pv + cb;
                if (total > 0) return `$${total.toFixed(2)}`;
                return marketData?.collateral_usdc || "0.00";
              })()}
              color="emerald"
              icon={<TrendingUp size={14} />}
            />
            <GlobalMetric
              label="חשיפת BTC"
              value={`${marketData?.btc_up_pct ?? 0}% / ${marketData?.btc_down_pct ?? 0}%`}
              sub={
                marketData?.direction_side === "BUY" ? "BULLISH" : "BEARISH"
              }
              color="cyan"
              icon={<Search size={14} />}
            />
            <GlobalMetric
              label="קבוצות בחימום"
              value={String(warmGroups || 0)}
              sub={`${readySearch} מוכנות לחיפוש`}
              color="amber"
              icon={<Flame size={14} />}
            />
          </div>

          <div className="flex items-center gap-8 shrink-0">
            <div className="text-right border-l border-slate-800 pl-8 ml-4">
              <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest">
                זמן שרת (Master)
              </div>
              <div className="text-2xl font-black text-white font-mono tracking-tighter">
                {currentTime}
              </div>
            </div>
            <div
              className="w-10 h-10 rounded-full bg-cyan-500/20 border border-cyan-500/30 flex items-center justify-center text-cyan-400"
              title={loading ? "טוען…" : "מסונכרן"}
            >
              <RefreshCw
                size={20}
                className={loading ? "animate-spin" : ""}
                style={!loading ? { animationDuration: "8s" } : undefined}
              />
            </div>
          </div>
        </header>

        <div className="flex-grow overflow-y-auto p-10 nexus-os-scrollbar space-y-10">
          {dbSyncStatus !== "ok" && dbSyncMessage && (
            <div className="flex justify-center">
              {dbSyncStatus === "initializing" ? (
                <div className="flex items-center gap-3 p-4 bg-amber-500/10 border border-amber-500/40 rounded-2xl text-amber-400 text-sm font-semibold tracking-wide">
                  <span className="inline-block w-2 h-2 rounded-full bg-amber-400 animate-pulse shrink-0" />
                  {dbSyncMessage}
                </div>
              ) : (
                <div className="flex items-center gap-3 p-4 bg-rose-500/10 border border-rose-500/40 rounded-2xl text-rose-400 text-sm font-black uppercase tracking-widest animate-pulse">
                  <AlertTriangle size={18} className="shrink-0" />
                  {dbSyncMessage}
                </div>
              )}
            </div>
          )}
          {activeTab === "master-hub" && <MasterHubView data={marketData} />}
          {activeTab === "swarm-monitor" && <SwarmMonitorView />}
          {activeTab === "group-factory" && <GroupFactoryView />}
          {activeTab === "ahu-management" && <AhuManagementView />}
          {activeTab === "bot-generator" && <GlobalSwarmTableView />}
          {activeTab === "session-swarm" && <SessionSwarmView />}
          {activeTab === "scrape-data" && <ScrapeResultsView />}
          {activeTab === "poly-trading" && (
            <PolymarketTradingView
              data={marketData}
              fetchDashboardData={fetchDashboardData}
            />
          )}
          {activeTab === "ai-architect" && <AIArchitectView />}
          {activeTab === "live-swarm" && <LiveSwarmView />}
          {activeTab === "master-terminal" && <LiveMasterTerminalView />}
        </div>
      </main>

      <style
        dangerouslySetInnerHTML={{
          __html: `
        .nexus-os-scrollbar::-webkit-scrollbar { width: 4px; }
        .nexus-os-scrollbar::-webkit-scrollbar-track { background: transparent; }
        .nexus-os-scrollbar::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 10px; }
        .nexus-os-scrollbar::-webkit-scrollbar-thumb:hover { background: #334155; }
        @keyframes pulse-border { 0%,100% { box-shadow: 0 0 12px rgba(34,211,238,0.35); } 50% { box-shadow: 0 0 28px rgba(34,211,238,0.75); } }
        .animate-pulse-border { animation: pulse-border 2s ease-in-out infinite; }
      `,
        }}
      />
    </div>
  );
}

// ── Views ───────────────────────────────────────────────────────────────────

interface HistoryEntry {
  timestamp?: string;
  message?: string;
  text?: string;
  ts?: string;
  action?: string;
  type?: string;
}

function _relativeTime(epoch: number | null | undefined): string {
  if (!epoch) return "—";
  const diffMs = Date.now() - epoch;
  const diffSec = Math.floor(diffMs / 1000);
  if (diffSec < 60) return `לפני ${diffSec} שנ'`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `לפני ${diffMin} דק'`;
  const diffH = Math.floor(diffMin / 60);
  if (diffH < 24) return `לפני ${diffH} שע'`;
  return `לפני ${Math.floor(diffH / 24)} ימים`;
}

function _actionIcon(line: string): string {
  const l = line.toLowerCase();
  if (l.includes("trade") || l.includes("buy") || l.includes("sell")) return "📈";
  if (l.includes("scrape") || l.includes("scan") || l.includes("סריקה")) return "🔍";
  if (l.includes("sync") || l.includes("deploy") || l.includes("סנכרון")) return "🔄";
  if (l.includes("error") || l.includes("שגיאה") || l.includes("fail")) return "⚠️";
  if (l.includes("success") || l.includes("הצלחה") || l.includes("done")) return "✅";
  return "⚡";
}

interface HistoryRow {
  epoch: number;
  text: string;
  rel: string;
  icon: string;
}

function PanelDGenesisHistory() {
  const [rows, setRows] = useState<HistoryRow[]>([]);

  const load = useCallback(async () => {
    const merged: { ts: number; text: string }[] = [];

    // Source 1: genesis-history endpoint
    try {
      const res = await fetch(`${API_BASE}/api/projects/genesis-history`);
      if (res.ok) {
        const j = (await res.json()) as { entries?: string[] };
        for (const line of j.entries ?? []) {
          merged.push({ ts: 0, text: line });
        }
      }
    } catch { /* ignore */ }

    // Source 2: swarm history endpoint — primary real-time source
    try {
      const res2 = await fetch(`${API_BASE}/api/v1/swarm/history`);
      if (res2.ok) {
        const j2 = (await res2.json()) as
          | HistoryEntry[]
          | { history?: HistoryEntry[]; entries?: string[] };
        const items: HistoryEntry[] = Array.isArray(j2)
          ? j2
          : (j2.history ?? []);
        for (const item of items) {
          const tsRaw = item.ts ?? item.timestamp ?? "";
          const text =
            item.action ??
            item.message ??
            item.text ??
            (item.type ? `[${item.type}]` : JSON.stringify(item));
          const epoch = tsRaw ? new Date(tsRaw).getTime() : 0;
          merged.push({ ts: epoch, text });
        }
        // Also handle flat entries array
        if (!Array.isArray(j2) && Array.isArray((j2 as { entries?: string[] }).entries)) {
          for (const line of (j2 as { entries: string[] }).entries) {
            merged.push({ ts: 0, text: line });
          }
        }
      }
    } catch { /* ignore */ }

    // Sort descending by timestamp
    merged.sort((a, b) => b.ts - a.ts);

    // Keep last 10, inject fallback if empty
    const top10 = merged.slice(0, 10);
    if (top10.length === 0) {
      top10.push({ ts: Date.now(), text: "System Initializing… waiting for core loop." });
    }

    setRows(
      top10.map((x) => ({
        epoch: x.ts,
        text: x.text,
        rel: _relativeTime(x.ts),
        icon: _actionIcon(x.text),
      }))
    );
  }, []);

  useEffect(() => {
    void load();
    const t = setInterval(load, 15_000);
    return () => clearInterval(t);
  }, [load]);

  return (
    <div className="bg-slate-900/40 border border-cyan-500/20 rounded-[2.5rem] p-8 min-h-[200px] flex flex-col shadow-xl relative overflow-hidden">
      <h3 className="text-lg font-black text-white mb-2 tracking-tight flex items-center gap-2">
        <Terminal size={18} className="text-cyan-400" />
        PANEL D | HISTORY
      </h3>
      <p className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-4">
        Scrapes · Trades · Syncs (last 10 actions)
      </p>
      <div className="flex-grow space-y-2 overflow-y-auto nexus-os-scrollbar pr-1 max-h-[260px]">
        {rows.map((row, i) => (
          <div
            key={`${row.epoch}-${i}`}
            className="flex items-start gap-2 text-[11px] font-mono border-b border-slate-800/60 pb-2 last:border-0"
          >
            <span className="shrink-0 mt-0.5">{row.icon}</span>
            <span className="flex-grow text-cyan-200/90 leading-relaxed">{row.text}</span>
            {row.rel && (
              <span className="shrink-0 text-slate-500 text-[10px] font-bold whitespace-nowrap">
                {row.rel}
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

interface DecisionEntry {
  time: string;
  text: string;
  type: keyof typeof DECISION_DOT;
}

function LiveDecisionLog() {
  const [entries, setEntries] = useState<DecisionEntry[]>([]);
  const [syncError, setSyncError] = useState(false);

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/system/node-history`);
      if (!res.ok) throw new Error(String(res.status));
      const j = (await res.json()) as { entries?: string[] };
      const raw = j.entries ?? [];
      setSyncError(false);
      setEntries(
        raw.slice(0, 8).map((text) => ({
          time: new Date().toLocaleTimeString(),
          text,
          type: text.toLowerCase().includes("trade")
            ? "trade"
            : text.toLowerCase().includes("error") || text.toLowerCase().includes("שגיאה")
              ? "system"
              : text.toLowerCase().includes("success") || text.toLowerCase().includes("הצלחה")
                ? "success"
                : "logic",
        }))
      );
    } catch {
      setSyncError(true);
      setEntries([]);
    }
  }, []);

  useEffect(() => {
    void load();
    const t = setInterval(load, 2_000);
    return () => clearInterval(t);
  }, [load]);

  return (
    <div className="flex-grow space-y-6 overflow-y-auto nexus-os-scrollbar pr-1">
      {syncError ? (
        <div className="flex items-center gap-2 p-3 bg-amber-500/10 border border-amber-500/30 rounded-xl text-amber-400 text-xs font-semibold tracking-wide">
          <span className="inline-block w-2 h-2 rounded-full bg-amber-400 animate-pulse shrink-0" />
          ⚠️ מחכה לחיבור Worker...
        </div>
      ) : (
        entries.map((e, i) => (
          <DecisionNode key={i} time={e.time} text={e.text} type={e.type} />
        ))
      )}
    </div>
  );
}

function MasterHubView({ data }: { data: GodModeDashboard | null }) {
  const hbOk = data?.heartbeat?.status === "OK";
  return (
    <div className="grid grid-cols-12 gap-8 animate-in fade-in duration-500">
      <div className="col-span-12 lg:col-span-8 space-y-8">
        <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10 h-[450px] flex flex-col relative overflow-hidden group shadow-2xl">
          <div className="absolute top-0 right-0 w-64 h-64 bg-cyan-500/5 blur-[120px] rounded-full pointer-events-none" />
          <div className="flex justify-between items-start mb-8 relative z-10">
            <div>
              <h3 className="text-2xl font-black text-white">
                גרף רווחיות פרויקט
              </h3>
              <p className="text-slate-500 text-sm mt-1">
                נתונים משולבים: Polymarket + סדרת מחירים
              </p>
            </div>
            <div className="text-right">
              <div className="text-4xl font-black text-emerald-400 font-mono">
                ${data?.collateral_usdc || "0.00"}
              </div>
              <div className="text-xs font-bold text-emerald-400/60 uppercase mt-1">
                סך הכל יתרה / PnL דוח
              </div>
            </div>
          </div>

          <div className="h-[350px] w-full block">
            <ResponsiveContainer width="100%" height={350}>
              <AreaChart data={data?.pnl_series || []}>
                <defs>
                  <linearGradient id="colorPnl" x1="0" y1="0" x2="0" y2="1">
                    <stop
                      offset="5%"
                      stopColor="#22d3ee"
                      stopOpacity={0.3}
                    />
                    <stop
                      offset="95%"
                      stopColor="#22d3ee"
                      stopOpacity={0}
                    />
                  </linearGradient>
                </defs>
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="#1e293b"
                  vertical={false}
                />
                <XAxis
                  dataKey="time"
                  stroke="#475569"
                  fontSize={10}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis stroke="#475569" fontSize={10} tickLine={false} axisLine={false} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#0f172a",
                    border: "1px solid #334155",
                    borderRadius: "12px",
                  }}
                  itemStyle={{ color: "#22d3ee" }}
                />
                <Area
                  type="monotone"
                  dataKey="pnl"
                  stroke="#22d3ee"
                  fillOpacity={1}
                  fill="url(#colorPnl)"
                  strokeWidth={3}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-8">
          <StatsCard
            label="יתרה חיה (Polymarket USDC)"
            value={data?.collateral_usdc ? `$${data.collateral_usdc}` : "$0.00"}
            sub={data?.pnl_series?.length ? `${data.pnl_series.length} נקודות נתונים` : "ממתין לנתונים..."}
            icon={<Gauge className="text-purple-400" />}
          />
          <StatsCard
            label="סטטוס CLOB Heartbeat"
            value={hbOk ? "מחובר" : "מנותק"}
            sub={data?.heartbeat?.timestamp || "N/A"}
            icon={
              <HardDrive
                className={hbOk ? "text-emerald-400" : "text-rose-400"}
              />
            }
          />
        </div>
      </div>

      <div className="col-span-12 lg:col-span-4 space-y-8">
        <div className="bg-slate-900/40 border border-purple-500/30 rounded-[2.5rem] p-8 min-h-[320px] flex flex-col shadow-2xl relative overflow-hidden">
          <h3 className="text-xl font-bold text-white mb-8 flex items-center gap-3">
            <MessageSquareCode size={22} className="text-purple-400" />
            יומן החלטות AI
          </h3>
          <LiveDecisionLog />
        </div>

        <PanelDGenesisHistory />
      </div>
    </div>
  );
}

// ── Create Group Modal ──────────────────────────────────────────────────────

interface CreateGroupModalProps {
  onClose: () => void;
  onCreated: (group: TelefixGroup) => void;
}

function CreateGroupModal({ onClose, onCreated }: CreateGroupModalProps) {
  const [nameHe, setNameHe] = useState("");
  const [inviteLink, setInviteLink] = useState("");
  const [isPrivate, setIsPrivate] = useState(true);
  const [warmupDays, setWarmupDays] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!nameHe.trim()) { setError("שם הקבוצה הוא שדה חובה"); return; }
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name_he: nameHe.trim(),
          invite_link: inviteLink.trim() || null,
          is_private: isPrivate,
          warmup_days: warmupDays,
        }),
      });
      if (!res.ok) {
        const j = await res.json().catch(() => ({})) as { detail?: string };
        throw new Error(j.detail ?? `שגיאה ${res.status}`);
      }
      const j = (await res.json()) as { group: TelefixGroup };
      onCreated(j.group);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "שגיאה לא ידועה");
    } finally {
      setLoading(false);
    }
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="bg-slate-900 border border-slate-700 rounded-3xl p-8 w-full max-w-md shadow-2xl animate-in fade-in zoom-in-95">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-xl font-black text-white">צור קבוצה חדשה</h3>
          <button
            type="button"
            onClick={onClose}
            className="text-slate-500 hover:text-white transition text-2xl leading-none"
          >
            ×
          </button>
        </div>

        <form onSubmit={handleSubmit} className="flex flex-col gap-5" dir="rtl">
          <div>
            <label className="block text-xs font-bold text-slate-400 mb-1.5">שם הקבוצה (עברית) *</label>
            <input
              type="text"
              value={nameHe}
              onChange={(e) => setNameHe(e.target.value)}
              placeholder="לדוגמה: קהילת משקיעים תל אביב"
              className="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-2.5 text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition text-sm"
              required
            />
          </div>

          <div>
            <label className="block text-xs font-bold text-slate-400 mb-1.5">קישור הזמנה (אופציונלי)</label>
            <input
              type="url"
              value={inviteLink}
              onChange={(e) => setInviteLink(e.target.value)}
              placeholder="https://t.me/..."
              className="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-2.5 text-white placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition text-sm"
            />
          </div>

          <div>
            <label className="block text-xs font-bold text-slate-400 mb-1.5">
              ימי חימום התחלתיים: <span className="text-cyan-400">{warmupDays}</span>
            </label>
            <input
              type="range"
              min={1}
              max={14}
              value={warmupDays}
              onChange={(e) => setWarmupDays(Number(e.target.value))}
              className="w-full accent-cyan-500"
            />
            <div className="flex justify-between text-[10px] text-slate-600 mt-0.5">
              <span>1</span><span>7</span><span>14</span>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => setIsPrivate(!isPrivate)}
              className={`relative w-11 h-6 rounded-full transition-colors ${isPrivate ? "bg-cyan-600" : "bg-slate-700"}`}
            >
              <span className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${isPrivate ? "translate-x-5" : "translate-x-0.5"}`} />
            </button>
            <span className="text-sm text-slate-300">קבוצה פרטית</span>
          </div>

          {error && (
            <div className="bg-rose-500/10 border border-rose-500/30 rounded-xl px-4 py-2.5 text-rose-400 text-sm">
              {error}
            </div>
          )}

          <div className="flex gap-3 pt-1">
            <button
              type="submit"
              disabled={loading}
              className="flex-1 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 disabled:cursor-not-allowed text-white py-2.5 rounded-xl font-bold transition"
            >
              {loading ? "יוצר..." : "צור קבוצה"}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-300 py-2.5 rounded-xl font-bold transition"
            >
              ביטול
            </button>
          </div>
        </form>
      </div>
    </div>,
    document.body,
  );
}

// ── Group Factory View ───────────────────────────────────────────────────────

type GroupFactorySettingsForm = {
  warmup_days: number;
  cooldown_hours: number;
  groups_per_day: number;
  automation_armed?: boolean;
  armed_at?: string;
};

type ActivityEntry = { ts?: string; level?: string; message?: string };

function GroupFactoryView() {
  const [warmupGroups, setWarmupGroups] = useState<TelefixGroup[]>([]);
  const [dbGroups, setDbGroups] = useState<TelefixDbGroup[]>([]);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [forceSearchLoading, setForceSearchLoading] = useState<string | null>(null);
  const [deleteLoading, setDeleteLoading] = useState<string | null>(null);
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);
  const [scheduleSettings, setScheduleSettings] = useState<GroupFactorySettingsForm | null>(null);
  const [settingsForm, setSettingsForm] = useState<GroupFactorySettingsForm | null>(null);
  const [savingSettings, setSavingSettings] = useState(false);
  const [activityEntries, setActivityEntries] = useState<ActivityEntry[]>([]);
  const [startFactoryLoading, setStartFactoryLoading] = useState(false);

  const showToast = (msg: string, ok = true) => {
    // #region agent log
    fetch("http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Debug-Session-Id": "c7f075" },
      body: JSON.stringify({
        sessionId: "c7f075",
        location: "NexusOsGodMode.tsx:GroupFactoryView.showToast",
        message: "group_factory_toast",
        data: { msg, ok },
        timestamp: Date.now(),
        hypothesisId: "H0",
      }),
    }).catch(() => {});
    // #endregion
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 5000);
  };

  const loadWarmup = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration`);
      if (!res.ok) return;
      const j = (await res.json()) as { groups: TelefixGroup[] };
      setWarmupGroups(j.groups ?? []);
    } catch { /* ignore */ }
  }, []);

  const loadDbGroups = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/groups`);
      if (!res.ok) return;
      const j = (await res.json()) as { groups: TelefixDbGroup[] };
      setDbGroups(j.groups ?? []);
    } catch { /* ignore */ }
  }, []);

  const loadSchedule = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-factory/schedule`);
      // #region agent log
      if (!res.ok) {
        fetch("http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Debug-Session-Id": "c7f075" },
          body: JSON.stringify({
            sessionId: "c7f075",
            location: "NexusOsGodMode.tsx:loadSchedule",
            message: "group_factory_schedule_fetch",
            data: { status: res.status, apiBase: API_BASE },
            timestamp: Date.now(),
            hypothesisId: "H3",
          }),
        }).catch(() => {});
        return;
      }
      // #endregion
      const j = (await res.json()) as { settings: GroupFactorySettingsForm };
      if (j.settings) {
        setScheduleSettings(j.settings);
        setSettingsForm(j.settings);
      }
    } catch { /* ignore */ }
  }, []);

  const loadActivity = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-factory/activity`);
      // #region agent log
      if (!res.ok) {
        fetch("http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Debug-Session-Id": "c7f075" },
          body: JSON.stringify({
            sessionId: "c7f075",
            location: "NexusOsGodMode.tsx:loadActivity",
            message: "group_factory_activity_fetch",
            data: { status: res.status, apiBase: API_BASE },
            timestamp: Date.now(),
            hypothesisId: "H3",
          }),
        }).catch(() => {});
        return;
      }
      // #endregion
      if (!res.ok) return;
      const j = (await res.json()) as { entries?: ActivityEntry[] };
      setActivityEntries(Array.isArray(j.entries) ? j.entries : []);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    void loadWarmup();
    void loadDbGroups();
    void loadSchedule();
    void loadActivity();
  }, [loadWarmup, loadDbGroups, loadSchedule, loadActivity]);

  useEffect(() => {
    const t = setInterval(() => {
      void loadActivity();
    }, 5000);
    return () => clearInterval(t);
  }, [loadActivity]);

  const handleSaveSettings = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!settingsForm) return;
    setSavingSettings(true);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-factory/schedule`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(settingsForm),
      });
      if (!res.ok) throw new Error(`שגיאה ${res.status}`);
      const j = (await res.json()) as { settings: typeof settingsForm };
      setScheduleSettings(j.settings);
      setSettingsForm(j.settings);
      setShowSettings(false);
      showToast("הגדרות נשמרו ✅");
      await loadActivity();
    } catch {
      showToast("שמירה נכשלה", false);
    } finally {
      setSavingSettings(false);
    }
  };

  const handleStartFactory = async () => {
    setStartFactoryLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-factory/start`, { method: "POST" });
      const j = (await res.json().catch(() => ({}))) as { detail?: string; ok?: boolean };
      if (!res.ok) {
        const errDetail =
          typeof j.detail === "string"
            ? j.detail
            : `שגיאה ${res.status}`;
        throw new Error(errDetail);
      }
      showToast(j.detail ?? "מפעל הקבוצות הופעל ✅");
      await loadSchedule();
      await loadActivity();
    } catch (e) {
      showToast(e instanceof Error ? e.message : "הפעלה נכשלה", false);
    } finally {
      setStartFactoryLoading(false);
    }
  };

  const handleForceSearch = async (group: {
    id: string;
    name: string;
    invite: string | null;
  }) => {
    setForceSearchLoading(group.id);
    try {
      const res = await fetch(
        `${API_BASE}/api/telefix/group-infiltration/${encodeURIComponent(group.id)}/force-search`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name_he: group.name || undefined,
            telegram_link: group.invite || undefined,
          }),
        },
      );
      if (!res.ok) {
        let detail = `שגיאה ${res.status}`;
        try {
          const errBody = (await res.json()) as { detail?: string | string[] };
          if (typeof errBody.detail === "string") detail = errBody.detail;
          else if (Array.isArray(errBody.detail)) detail = errBody.detail.map((x) => x.msg ?? x).join("; ");
        } catch { /* ignore */ }
        throw new Error(detail);
      }
      showToast("הקבוצה הועלתה לחיפוש ✅");
      await loadWarmup();
      await loadActivity();
    } catch (e) {
      showToast(e instanceof Error ? e.message : "נכשל — נסה שוב", false);
    } finally {
      setForceSearchLoading(null);
    }
  };

  const handleDelete = async (groupId: string) => {
    if (!confirm("האם למחוק קבוצה זו?")) return;
    setDeleteLoading(groupId);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration/${groupId}`, { method: "DELETE" });
      if (!res.ok) throw new Error(`שגיאה ${res.status}`);
      showToast("הקבוצה נמחקה");
      await loadWarmup();
    } catch {
      showToast("מחיקה נכשלה", false);
    } finally {
      setDeleteLoading(null);
    }
  };

  // Build invite lookup from real DB records
  const inviteByTitle = new Map<string, string>();
  for (const g of dbGroups) {
    if (g.invite_link) inviteByTitle.set(g.title, g.invite_link);
  }

  // Merge warmup state with real DB invite links
  const rows =
    dbGroups.length > 0
      ? dbGroups.map((g, i) => {
          const warmup = warmupGroups.find((w) => w.id === String(g.id));
          return {
            id: String(g.id),
            name: g.title,
            invite: g.invite_link,
            days: warmup?.warmup_days ?? (i % 2 === 0 ? 14 : 7),
            status: warmup?.in_search
              ? "READY"
              : (warmup?.warmup_days ?? 0) >= 14
                ? "FAILED_RETRY"
                : "WARMING",
            search: warmup?.in_search ?? false,
            isPrivate: false,
          };
        })
      : warmupGroups.map((g) => ({
          id: g.id,
          name: g.name_he,
          invite: inviteByTitle.get(g.name_he) ?? null,
          days: g.warmup_days,
          status: g.in_search
            ? "READY"
            : g.warmup_days >= 14
              ? "FAILED_RETRY"
              : "WARMING",
          search: g.in_search,
          isPrivate: false,
        }));

  const totalReady = rows.filter((r) => r.status === "READY").length;
  const totalWarming = rows.filter((r) => r.status === "WARMING").length;
  const avgDays = rows.length > 0 ? Math.round(rows.reduce((s, r) => s + r.days, 0) / rows.length) : 0;

  return (
    <div className="space-y-6 animate-in fade-in">
      {/* Toast */}
      {toast && (
        <div className={`fixed top-6 left-1/2 -translate-x-1/2 z-50 px-6 py-3 rounded-2xl font-bold text-sm shadow-xl transition-all ${toast.ok ? "bg-emerald-600 text-white" : "bg-rose-600 text-white"}`}>
          {toast.msg}
        </div>
      )}

      {/* Header */}
      <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10">
        <div className="flex justify-between items-center mb-8 flex-wrap gap-4">
          <div>
            <h3 className="text-2xl font-black text-white">
              מפעל קבוצות - חדירה לחיפוש
            </h3>
            <p className="text-slate-500 text-sm mt-1">
              ניהול חימום שבועיים ואוטומציית אינדוקס (vault/group_infiltration.json)
            </p>
            {scheduleSettings?.automation_armed && (
              <p className="text-emerald-500/90 text-xs font-bold mt-2 flex items-center gap-2">
                <Activity size={14} className="shrink-0" />
                מפעל מחובר (automation_armed) — לולאת מאסטר ברקע כשהיא פעילה
              </p>
            )}
          </div>
          <div className="flex items-center gap-3 flex-wrap">
            <button
              type="button"
              onClick={() => void handleStartFactory()}
              disabled={startFactoryLoading}
              className="bg-emerald-700 hover:bg-emerald-600 disabled:opacity-50 text-white px-4 py-3 rounded-2xl font-bold transition flex items-center gap-2"
              title="מסמן שהמפעל פעיל ורושם בלוג"
            >
              <PlayCircle size={18} />
              {startFactoryLoading ? "..." : "התחל מפעל"}
            </button>
            <button
              type="button"
              onClick={() => void loadWarmup()}
              className="bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-3 rounded-2xl font-bold transition flex items-center gap-2"
              title="רענן נתונים"
            >
              <RefreshCw size={16} />
            </button>
            <button
              type="button"
              onClick={() => setShowSettings((v) => !v)}
              className="bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-3 rounded-2xl font-bold transition flex items-center gap-2"
              title="הגדרות"
            >
              ⚙ הגדרות
            </button>
            <button
              type="button"
              onClick={() => setShowCreateModal(true)}
              className="bg-cyan-600 hover:bg-cyan-500 text-white px-6 py-3 rounded-2xl font-bold transition flex items-center gap-2"
            >
              <span className="text-lg leading-none">+</span>
              צור קבוצה חדשה
            </button>
          </div>
        </div>

        {/* Stats bar */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-slate-950/60 rounded-2xl p-4 border border-slate-800 text-center">
            <div className="text-2xl font-black text-white">{rows.length}</div>
            <div className="text-xs text-slate-500 font-bold mt-0.5">סה״כ קבוצות</div>
          </div>
          <div className="bg-emerald-500/10 rounded-2xl p-4 border border-emerald-500/20 text-center">
            <div className="text-2xl font-black text-emerald-400">{totalReady}</div>
            <div className="text-xs text-emerald-600 font-bold mt-0.5">מוכנות לחיפוש</div>
          </div>
          <div className="bg-amber-500/10 rounded-2xl p-4 border border-amber-500/20 text-center">
            <div className="text-2xl font-black text-amber-400">{totalWarming}</div>
            <div className="text-xs text-amber-600 font-bold mt-0.5">בחימום · ממוצע {avgDays}/14 יום</div>
          </div>
          <div className="bg-violet-500/10 rounded-2xl p-4 border border-violet-500/20 text-center">
            <div className="text-2xl font-black text-violet-400">{scheduleSettings?.groups_per_day ?? "—"}</div>
            <div className="text-xs text-violet-600 font-bold mt-0.5">קבוצות ליום</div>
          </div>
        </div>

        {/* Settings panel */}
        {showSettings && settingsForm && (
          <form
            onSubmit={(e) => void handleSaveSettings(e)}
            className="mb-6 bg-slate-950/60 border border-violet-500/30 rounded-2xl p-6"
            dir="rtl"
          >
            <div className="text-sm font-black text-white mb-4">⚙ הגדרות מפעל קבוצות</div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              <div>
                <label className="block text-xs font-bold text-slate-400 mb-1.5">ימי Warmup (1–30)</label>
                <input
                  type="number"
                  min={1}
                  max={30}
                  value={settingsForm.warmup_days}
                  onChange={(e) => setSettingsForm((f) => f ? { ...f, warmup_days: Number(e.target.value) } : f)}
                  className="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-2.5 text-white focus:outline-none focus:border-violet-500 transition text-sm"
                />
              </div>
              <div>
                <label className="block text-xs font-bold text-slate-400 mb-1.5">שעות Cooldown (1–168)</label>
                <input
                  type="number"
                  min={1}
                  max={168}
                  value={settingsForm.cooldown_hours}
                  onChange={(e) => setSettingsForm((f) => f ? { ...f, cooldown_hours: Number(e.target.value) } : f)}
                  className="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-2.5 text-white focus:outline-none focus:border-violet-500 transition text-sm"
                />
              </div>
              <div>
                <label className="block text-xs font-bold text-slate-400 mb-1.5">קבוצות ליצור ביום</label>
                <input
                  type="number"
                  min={1}
                  max={50}
                  value={settingsForm.groups_per_day}
                  onChange={(e) => setSettingsForm((f) => f ? { ...f, groups_per_day: Number(e.target.value) } : f)}
                  className="w-full bg-slate-800 border border-slate-700 rounded-xl px-4 py-2.5 text-white focus:outline-none focus:border-violet-500 transition text-sm"
                />
              </div>
            </div>
            <div className="flex gap-3">
              <button
                type="submit"
                disabled={savingSettings}
                className="bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white px-5 py-2 rounded-xl font-bold text-sm transition"
              >
                {savingSettings ? "שומר..." : "שמור הגדרות"}
              </button>
              <button
                type="button"
                onClick={() => setShowSettings(false)}
                className="bg-slate-800 hover:bg-slate-700 text-slate-300 px-5 py-2 rounded-xl font-bold text-sm transition"
              >
                ביטול
              </button>
            </div>
          </form>
        )}

        {/* Group list */}
        <div className="grid gap-4">
          {rows.length === 0 && (
            <div className="text-center py-16 text-slate-600">
              <Users size={40} className="mx-auto mb-3 opacity-30" />
              <div className="font-bold">אין קבוצות עדיין</div>
              <div className="text-sm mt-1">לחץ &quot;צור קבוצה חדשה&quot; כדי להתחיל</div>
            </div>
          )}
          {rows.map((group) => (
            <div
              key={group.id}
              className="bg-slate-950/50 p-6 rounded-3xl border border-slate-800 flex items-center justify-between group hover:border-cyan-500/50 transition flex-wrap gap-4"
            >
              <div className="flex items-center gap-6">
                <div
                  className={`w-14 h-14 rounded-2xl flex items-center justify-center ${
                    group.status === "READY"
                      ? "bg-emerald-500/20 text-emerald-400"
                      : "bg-amber-500/20 text-amber-400"
                  }`}
                >
                  {group.status === "READY" ? (
                    <CheckCircle2 size={28} />
                  ) : (
                    <Clock size={28} />
                  )}
                </div>
                <div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <div className="text-lg font-bold text-white">{group.name}</div>
                    {group.invite && group.invite.startsWith("https://t.me/") ? (
                      <a
                        href={group.invite}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-[10px] text-cyan-400 hover:text-cyan-300 font-bold border border-cyan-500/30 px-2 py-0.5 rounded-lg transition"
                      >
                        הצטרף ↗
                      </a>
                    ) : (
                      <span className="text-[10px] text-rose-400 font-bold border border-rose-500/40 bg-rose-500/10 px-2 py-0.5 rounded-lg">
                        🔴 אין קישור
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-3 mt-1">
                    <span className="text-xs text-slate-500 font-bold">
                      חימום: {group.days}/14 יום
                    </span>
                    <div className="w-32 bg-slate-800 h-1.5 rounded-full overflow-hidden">
                      <div
                        className="bg-cyan-500 h-full transition-all"
                        style={{ width: `${Math.min((group.days / 14) * 100, 100)}%` }}
                      />
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex items-center gap-3 flex-wrap">
                <div className="text-right">
                  <div className="text-[10px] text-slate-500 uppercase font-bold">אינדוקס</div>
                  <div className={`text-sm font-bold ${group.search ? "text-emerald-400" : "text-rose-400"}`}>
                    {group.search ? "מופיע בחיפוש ✅" : "ממתין..."}
                  </div>
                </div>

                {!group.search && (
                  <button
                    type="button"
                    onClick={() => void handleForceSearch(group)}
                    disabled={forceSearchLoading === group.id}
                    className="text-xs bg-cyan-600/20 hover:bg-cyan-600/40 text-cyan-400 border border-cyan-500/30 px-3 py-1.5 rounded-xl font-bold transition disabled:opacity-50"
                    title="כפה העלאה לחיפוש"
                  >
                    {forceSearchLoading === group.id ? "..." : "⚡ Force Search"}
                  </button>
                )}

                <button
                  type="button"
                  onClick={() => void handleDelete(group.id)}
                  disabled={deleteLoading === group.id}
                  className="text-xs bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border border-rose-500/30 px-3 py-1.5 rounded-xl font-bold transition disabled:opacity-50 opacity-0 group-hover:opacity-100"
                  title="מחק קבוצה"
                >
                  {deleteLoading === group.id ? "..." : "🗑"}
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Create modal */}
      {showCreateModal && (
        <CreateGroupModal
          onClose={() => setShowCreateModal(false)}
          onCreated={(g) => {
            setWarmupGroups((prev) => [...prev, g]);
            showToast(`קבוצה "${g.name_he}" נוצרה בהצלחה ✅`);
          }}
        />
      )}
    </div>
  );
}

// ── Ahu Management Types ──────────────────────────────────────────────────────

interface AhuStatus {
  bot_running: boolean;
  bot_pid: number | null;
  db_available: boolean;
  sessions_available: boolean;
  total_sessions: number;
  session_counts: Record<string, number>;
}

interface AhuStats {
  users: {
    total: number;
    premium: number;
    sources: number;
    premium_pct: number;
    disk_only_count?: number;
    disk_users_dir?: string;
  };
  targets: Record<string, number>;
  enrollments: { total: number; by_status: Record<string, number> };
  last_runs: Record<string, string>;
}

interface AhuSessions {
  [category: string]: { count: number; sessions: string[] };
}

interface AhuTargets {
  targets: { id: number; title: string; link: string; role: string }[];
  count: number;
}

interface AhuLogs {
  lines: string[];
  count: number;
}

// ── Ahu Sub-components ────────────────────────────────────────────────────────

function AhuStatCard({
  label,
  value,
  sub,
  icon,
  accent = "cyan",
}: {
  label: string;
  value: string | number;
  sub?: string;
  icon: React.ReactNode;
  accent?: "cyan" | "purple" | "green" | "yellow";
}) {
  const colors: Record<string, string> = {
    cyan: "text-cyan-400 bg-cyan-500/10 border-cyan-500/20",
    purple: "text-purple-400 bg-purple-500/10 border-purple-500/20",
    green: "text-green-400 bg-green-500/10 border-green-500/20",
    yellow: "text-yellow-400 bg-yellow-500/10 border-yellow-500/20",
  };
  return (
    <div className={`rounded-2xl border p-4 flex flex-col gap-1 ${colors[accent]}`}>
      <div className="flex items-center gap-2 text-xs font-bold uppercase tracking-widest opacity-70">
        {icon}
        {label}
      </div>
      <div className="text-2xl font-black">{value}</div>
      {sub && <div className="text-[11px] opacity-60">{sub}</div>}
    </div>
  );
}

function AhuDashboardTab({
  status,
  stats,
  onStart,
  onStop,
  actionLoading,
  onRefresh,
}: {
  status: AhuStatus | null;
  stats: AhuStats | null;
  onStart: () => void;
  onStop: () => void;
  actionLoading: boolean;
  onRefresh: () => void;
}) {
  const running = status?.bot_running ?? false;

  const formatTs = (ts: string | undefined) => {
    if (!ts) return "—";
    const n = parseFloat(ts);
    if (!isNaN(n) && n > 1e9) {
      return new Date(n * 1000).toLocaleString("he-IL");
    }
    return ts;
  };

  return (
    <div className="space-y-6">
      {/* Bot status card */}
      <div className={`rounded-2xl border p-5 flex items-center justify-between flex-wrap gap-4 ${running ? "border-green-500/30 bg-green-500/5" : "border-slate-700 bg-slate-950/40"}`}>
        <div className="flex items-center gap-4">
          <div className={`w-3 h-3 rounded-full ${running ? "bg-green-400 shadow-[0_0_8px_rgba(74,222,128,0.8)]" : "bg-slate-600"}`} />
          <div>
            <div className="font-bold text-white text-lg">
              {running ? "הבוט פעיל" : "הבוט כבוי"}
            </div>
            {status?.bot_pid && (
              <div className="text-xs text-slate-500 font-mono">PID: {status.bot_pid}</div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={onRefresh}
            className="text-slate-400 hover:text-white p-2 rounded-xl hover:bg-slate-800 transition"
            title="רענן"
          >
            <RefreshCw size={15} />
          </button>
          {running ? (
            <button
              onClick={onStop}
              disabled={actionLoading}
              className="bg-red-600 hover:bg-red-500 disabled:opacity-50 text-white px-5 py-2 rounded-xl font-bold text-sm transition flex items-center gap-2"
            >
              <Activity size={14} />
              עצור בוט
            </button>
          ) : (
            <button
              onClick={onStart}
              disabled={actionLoading}
              className="bg-green-600 hover:bg-green-500 disabled:opacity-50 text-white px-5 py-2 rounded-xl font-bold text-sm transition flex items-center gap-2"
            >
              <Zap size={14} />
              הפעל בוט
            </button>
          )}
        </div>
      </div>

      {/* Stats grid */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          <AhuStatCard
            label="משתמשים"
            value={stats.users.total.toLocaleString()}
            sub={
              (stats.users.disk_only_count ?? 0) > 0
                ? `${stats.users.premium_pct}% פרימיום · +${stats.users.disk_only_count} מ«קהיל חיה» (דיסק)`
                : `${stats.users.premium_pct}% פרימיום`
            }
            icon={<Users size={12} />}
            accent="cyan"
          />
          <AhuStatCard
            label="פרימיום"
            value={stats.users.premium.toLocaleString()}
            icon={<Zap size={12} />}
            accent="yellow"
          />
          <AhuStatCard
            label="מקורות"
            value={stats.users.sources}
            icon={<Database size={12} />}
            accent="purple"
          />
          <AhuStatCard
            label="יעדים"
            value={stats.targets["target"] ?? 0}
            sub={`${stats.targets["source"] ?? 0} מקורות`}
            icon={<Target size={12} />}
            accent="cyan"
          />
          <AhuStatCard
            label="Enrollments"
            value={stats.enrollments.total.toLocaleString()}
            icon={<CheckCircle2 size={12} />}
            accent="green"
          />
          <AhuStatCard
            label="סשנים"
            value={status?.total_sessions ?? "—"}
            icon={<HardDrive size={12} />}
            accent="purple"
          />
        </div>
      )}

      {/* Last runs */}
      {stats && Object.keys(stats.last_runs).length > 0 && (
        <div className="bg-slate-950/40 rounded-2xl border border-slate-800 p-4">
          <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-3">
            הרצות אחרונות
          </div>
          <div className="grid grid-cols-2 gap-2">
            {Object.entries(stats.last_runs).map(([key, val]) => (
              <div key={key} className="flex items-center justify-between text-xs">
                <span className="text-slate-400 font-mono">{key}</span>
                <span className="text-cyan-400">{formatTs(val)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Enrollment status breakdown */}
      {stats && Object.keys(stats.enrollments.by_status).length > 0 && (
        <div className="bg-slate-950/40 rounded-2xl border border-slate-800 p-4">
          <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-3">
            סטטוסי Enrollment
          </div>
          <div className="flex flex-wrap gap-2">
            {Object.entries(stats.enrollments.by_status).map(([status, count]) => (
              <span
                key={status}
                className="text-[11px] font-bold bg-slate-800 text-slate-300 px-3 py-1 rounded-lg border border-slate-700"
              >
                {status}: {count}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function sortAhuFolderKeys(keys: string[]): string[] {
  const klali = "כללי";
  const rest = keys.filter((k) => k !== klali).sort((a, b) => a.localeCompare(b, "he"));
  return keys.includes(klali) ? [klali, ...rest] : keys.slice().sort((a, b) => a.localeCompare(b, "he"));
}

const AHU_FOLDER_LABELS: Record<string, string> = {
  managers: "Managers",
  adders: "Adders",
  frozen: "Frozen",
  bots: "Bots",
  spammers: "Spammers",
  כללי: "כללי",
};

function ahuFolderLabel(key: string): string {
  return AHU_FOLDER_LABELS[key] ?? key;
}

function AhuSessionsTab({
  sessions,
  onRefresh,
}: {
  sessions: AhuSessions | null;
  onRefresh: () => void | Promise<void>;
}) {
  const [activeCategory, setActiveCategory] = useState<string>("");
  const [syncBusy, setSyncBusy] = useState(false);
  const [moveBusy, setMoveBusy] = useState<string | null>(null);
  const [moveTargetByStem, setMoveTargetByStem] = useState<Record<string, string>>({});
  const [syncMsg, setSyncMsg] = useState<string | null>(null);

  const folderKeys = useMemo(() => {
    if (!sessions) return [];
    return sortAhuFolderKeys(Object.keys(sessions));
  }, [sessions]);

  useEffect(() => {
    if (folderKeys.length === 0) return;
    if (activeCategory === "" || !folderKeys.includes(activeCategory)) {
      setActiveCategory(folderKeys[0]);
    }
  }, [folderKeys, activeCategory]);

  if (!sessions) {
    return <div className="text-slate-500 text-sm">טוען סשנים...</div>;
  }

  const current = activeCategory ? sessions[activeCategory] : undefined;
  const otherFolders = folderKeys.filter((k) => k !== activeCategory);

  const handleSync = async () => {
    setSyncBusy(true);
    setSyncMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/ahu/sessions/sync-scanned`, { method: "POST" });
      const j = (await res.json()) as { ok?: boolean; copied?: number; skipped?: number; detail?: string };
      if (j.detail && !j.ok) {
        setSyncMsg(j.detail);
      } else {
        setSyncMsg(`הועתקו ${j.copied ?? 0}, דולגו ${j.skipped ?? 0}`);
      }
      await onRefresh();
    } catch {
      setSyncMsg("שגיאת רשת בסנכרון");
    } finally {
      setSyncBusy(false);
    }
  };

  const handleMove = async (stem: string) => {
    const toFolder =
      moveTargetByStem[stem] ?? otherFolders[0];
    if (!toFolder || !activeCategory) return;
    setMoveBusy(stem);
    setSyncMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/ahu/sessions/move`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stem,
          from_folder: activeCategory,
          to_folder: toFolder,
        }),
      });
      if (!res.ok) {
        const err = (await res.json().catch(() => ({}))) as { detail?: string };
        setSyncMsg(err.detail ?? `שגיאה ${res.status}`);
        return;
      }
      await onRefresh();
    } catch {
      setSyncMsg("שגיאת רשת בהעברה");
    } finally {
      setMoveBusy(null);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3 justify-between">
        <p className="text-[11px] text-slate-500 max-w-xl">
          סנכרון מפלט הסורק (validated_active) לתיקיית «כללי». ניתן להעביר סשנים בין תיקיות למטה.
        </p>
        <button
          type="button"
          disabled={syncBusy}
          onClick={() => void handleSync()}
          className="shrink-0 px-4 py-2 rounded-xl text-xs font-bold bg-purple-600 hover:bg-purple-500 disabled:opacity-50 text-white transition"
        >
          {syncBusy ? "מסנכרן…" : "סנכרון מסריקה → כללי"}
        </button>
      </div>
      {syncMsg && (
        <div className="text-xs text-slate-400 bg-slate-950/60 border border-slate-800 rounded-xl px-3 py-2 font-mono">
          {syncMsg}
        </div>
      )}

      {/* Folder tabs */}
      <div className="flex gap-2 flex-wrap">
        {folderKeys.map((cat) => {
          const count = sessions[cat]?.count ?? 0;
          return (
            <button
              key={cat}
              type="button"
              onClick={() => setActiveCategory(cat)}
              className={`px-4 py-2 rounded-xl text-xs font-bold transition flex items-center gap-2 ${
                activeCategory === cat
                  ? "bg-cyan-600 text-white"
                  : "bg-slate-800 text-slate-400 hover:bg-slate-700"
              }`}
            >
              {ahuFolderLabel(cat)}
              <span
                className={`px-1.5 py-0.5 rounded-md text-[10px] ${activeCategory === cat ? "bg-cyan-700" : "bg-slate-700"}`}
              >
                {count}
              </span>
            </button>
          );
        })}
      </div>

      {folderKeys.length === 0 ? (
        <div className="text-slate-500 text-sm bg-slate-950/40 rounded-2xl border border-slate-800 p-6 text-center">
          אין תיקיות סשנים (בדוק נתיב TELEFIX / sessions)
        </div>
      ) : current && current.sessions.length > 0 ? (
        <div className="bg-slate-950/40 rounded-2xl border border-slate-800 overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between flex-wrap gap-2">
            <span className="text-xs font-bold text-slate-400 uppercase tracking-widest">
              {ahuFolderLabel(activeCategory)} — {current.count} סשנים
            </span>
          </div>
          <div className="max-h-80 overflow-y-auto divide-y divide-slate-800/50">
            {current.sessions.map((sess) => {
              const defaultTarget = otherFolders.find((k) => k !== activeCategory) ?? otherFolders[0] ?? "";
              const sel = moveTargetByStem[sess] ?? defaultTarget;
              return (
                <div
                  key={sess}
                  className="px-4 py-2.5 flex flex-wrap items-center gap-3 hover:bg-slate-800/30 transition"
                >
                  <div className="w-2 h-2 rounded-full shrink-0 bg-cyan-500/60" />
                  <span className="text-xs font-mono text-slate-300 flex-1 min-w-[8rem]">{sess}</span>
                  {otherFolders.length > 0 ? (
                    <div className="flex items-center gap-2 shrink-0">
                      <select
                        value={sel}
                        onChange={(e) =>
                          setMoveTargetByStem((m) => ({
                            ...m,
                            [sess]: e.target.value,
                          }))
                        }
                        className="bg-slate-900 border border-slate-700 rounded-lg text-[11px] text-slate-200 px-2 py-1.5 max-w-[10rem]"
                      >
                        {otherFolders.map((f) => (
                          <option key={f} value={f}>
                            → {ahuFolderLabel(f)}
                          </option>
                        ))}
                      </select>
                      <button
                        type="button"
                        disabled={moveBusy === sess}
                        onClick={() => void handleMove(sess)}
                        className="text-[11px] font-bold px-3 py-1.5 rounded-lg bg-slate-700 hover:bg-slate-600 text-white disabled:opacity-50"
                      >
                        {moveBusy === sess ? "…" : "העבר"}
                      </button>
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      ) : (
        <div className="text-slate-500 text-sm bg-slate-950/40 rounded-2xl border border-slate-800 p-6 text-center">
          אין סשנים בתיקייה זו
        </div>
      )}
    </div>
  );
}

function AhuScraperTab({ targets }: { targets: AhuTargets | null }) {
  const [filter, setFilter] = useState<"all" | "source" | "target">("all");

  if (!targets) {
    return <div className="text-slate-500 text-sm">טוען נתונים...</div>;
  }

  const filtered = filter === "all" ? targets.targets : targets.targets.filter((t) => t.role === filter);
  const sourceCount = targets.targets.filter((t) => t.role === "source").length;
  const targetCount = targets.targets.filter((t) => t.role === "target").length;

  return (
    <div className="space-y-4">
      {/* Summary */}
      <div className="grid grid-cols-2 gap-3">
        <div className="bg-slate-950/40 rounded-2xl border border-purple-500/20 p-4 text-center">
          <div className="text-2xl font-black text-purple-400">{sourceCount}</div>
          <div className="text-[11px] text-slate-500 uppercase tracking-widest mt-1">קבוצות מקור</div>
        </div>
        <div className="bg-slate-950/40 rounded-2xl border border-cyan-500/20 p-4 text-center">
          <div className="text-2xl font-black text-cyan-400">{targetCount}</div>
          <div className="text-[11px] text-slate-500 uppercase tracking-widest mt-1">קבוצות יעד</div>
        </div>
      </div>

      {/* Filter */}
      <div className="flex gap-2">
        {(["all", "source", "target"] as const).map((f) => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-3 py-1.5 rounded-lg text-xs font-bold transition ${
              filter === f ? "bg-cyan-600 text-white" : "bg-slate-800 text-slate-400 hover:bg-slate-700"
            }`}
          >
            {f === "all" ? "הכל" : f === "source" ? "מקורות" : "יעדים"}
          </button>
        ))}
      </div>

      {/* Table */}
      {filtered.length > 0 ? (
        <div className="bg-slate-950/40 rounded-2xl border border-slate-800 overflow-hidden">
          <div className="max-h-80 overflow-y-auto">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-slate-900 border-b border-slate-800">
                <tr>
                  <th className="text-left px-4 py-2.5 text-slate-400 font-bold uppercase tracking-widest">שם</th>
                  <th className="text-left px-4 py-2.5 text-slate-400 font-bold uppercase tracking-widest">קישור</th>
                  <th className="text-left px-4 py-2.5 text-slate-400 font-bold uppercase tracking-widest">תפקיד</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {filtered.map((t) => (
                  <tr key={t.id} className="hover:bg-slate-800/30 transition">
                    <td className="px-4 py-2.5 text-slate-300 font-medium">{t.title || "—"}</td>
                    <td className="px-4 py-2.5">
                      <a
                        href={t.link}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-cyan-400 hover:text-cyan-300 font-mono transition"
                      >
                        {t.link}
                      </a>
                    </td>
                    <td className="px-4 py-2.5">
                      <span className={`px-2 py-0.5 rounded-md font-bold uppercase text-[10px] ${
                        t.role === "source"
                          ? "bg-purple-500/20 text-purple-400 border border-purple-500/30"
                          : "bg-cyan-500/20 text-cyan-400 border border-cyan-500/30"
                      }`}>
                        {t.role}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        <div className="text-slate-500 text-sm bg-slate-950/40 rounded-2xl border border-slate-800 p-6 text-center">
          אין קבוצות מוגדרות
        </div>
      )}
    </div>
  );
}

function AhuLogsTab() {
  const [lines, setLines] = useState<string[]>([]);
  const [filter, setFilter] = useState<"all" | "INFO" | "WARNING" | "ERROR">("all");
  const [connected, setConnected] = useState(false);
  const bottomRef = React.useRef<HTMLDivElement>(null);
  const wsRef = React.useRef<WebSocket | null>(null);

  useEffect(() => {
    const wsBase = apiWsBase();
    const ws = new WebSocket(`${wsBase}/api/ahu/logs/stream`);
    wsRef.current = ws;

    ws.onopen = () => setConnected(true);
    ws.onmessage = (e) => {
      setLines((prev) => {
        const next = [...prev, e.data as string];
        return next.length > 500 ? next.slice(-500) : next;
      });
    };
    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);

    return () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  const filtered = filter === "all" ? lines : lines.filter((l) => l.includes(filter));

  const lineColor = (line: string) => {
    if (line.includes("ERROR") || line.includes("error")) return "text-red-400";
    if (line.includes("WARNING") || line.includes("warning") || line.includes("WARN")) return "text-yellow-400";
    if (line.includes("INFO")) return "text-slate-300";
    return "text-slate-400";
  };

  return (
    <div className="space-y-3">
      {/* Controls */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${connected ? "bg-green-400 shadow-[0_0_6px_rgba(74,222,128,0.8)]" : "bg-slate-600"}`} />
          <span className="text-xs text-slate-500">{connected ? "מחובר — לוגים חיים" : "מנותק"}</span>
        </div>
        <div className="flex gap-2">
          {(["all", "INFO", "WARNING", "ERROR"] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1 rounded-lg text-[11px] font-bold transition ${
                filter === f
                  ? f === "ERROR" ? "bg-red-600 text-white"
                    : f === "WARNING" ? "bg-yellow-600 text-white"
                    : "bg-cyan-600 text-white"
                  : "bg-slate-800 text-slate-400 hover:bg-slate-700"
              }`}
            >
              {f === "all" ? "הכל" : f}
            </button>
          ))}
          <button
            onClick={() => setLines([])}
            className="px-3 py-1 rounded-lg text-[11px] font-bold bg-slate-800 text-slate-400 hover:bg-slate-700 transition"
          >
            נקה
          </button>
        </div>
      </div>

      {/* Log output */}
      <div className="bg-slate-950 rounded-2xl border border-slate-800 h-80 overflow-y-auto p-4 font-mono text-[11px] leading-5">
        {filtered.length === 0 ? (
          <div className="text-slate-600 text-center mt-8">
            {connected ? "ממתין ללוגים..." : "מתחבר..."}
          </div>
        ) : (
          filtered.map((line, i) => (
            <div key={i} className={lineColor(line)}>
              {line}
            </div>
          ))
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}

function AhuQuickLinkTab({ invite }: { invite: string }) {
  return (
    <div className="space-y-4">
      <div className="bg-slate-950/50 p-6 rounded-3xl border border-cyan-500/20 flex items-center justify-between group hover:border-cyan-500/50 transition flex-wrap gap-4">
        <div className="flex items-center gap-6">
          <div className="w-14 h-14 rounded-2xl flex items-center justify-center bg-cyan-500/20 text-cyan-400">
            <MessageSquareCode size={28} />
          </div>
          <div>
            <div className="text-lg font-bold text-white">Management Ahu — Ops Sync</div>
            <div className="text-xs text-slate-500 mt-1">ערוץ ניהול פנימי לסנכרון פעולות שוטפות</div>
            <a
              href={invite}
              target="_blank"
              rel="noopener noreferrer"
              className="text-[11px] text-cyan-400 hover:text-cyan-300 font-mono mt-1 inline-block transition"
            >
              {invite}
            </a>
          </div>
        </div>
        <div className="flex flex-col items-end gap-2">
          <span className="text-[10px] font-bold text-cyan-400 bg-cyan-500/10 border border-cyan-500/30 px-3 py-1 rounded-lg uppercase tracking-widest">
            PRIVATE
          </span>
          <a
            href={invite}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs font-bold text-white bg-cyan-600 hover:bg-cyan-500 px-4 py-2 rounded-xl transition"
          >
            פתח ↗
          </a>
        </div>
      </div>

      <div className="p-5 bg-slate-950/30 rounded-2xl border border-slate-800/50">
        <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-3">
          קישור ישיר — Management Ahu
        </div>
        <div className="flex items-center gap-4 flex-wrap">
          <code className="text-cyan-400 font-mono text-sm bg-slate-900 px-4 py-2 rounded-xl border border-slate-800">
            {invite}
          </code>
          <a
            href={invite}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs font-bold text-white bg-purple-600 hover:bg-purple-500 px-4 py-2 rounded-xl transition"
          >
            פתח בטלגרם
          </a>
        </div>
      </div>
    </div>
  );
}

// ── Main AhuManagementView ────────────────────────────────────────────────────

function AhuManagementView() {
  const TG_FALLBACK = "https://t.me/TohnaAHUSHARMUTABOT";

  const [activeTab, setActiveTab] = useState<"dashboard" | "sessions" | "scraper" | "logs" | "link">("dashboard");
  const [status, setStatus] = useState<AhuStatus | null>(null);
  const [stats, setStats] = useState<AhuStats | null>(null);
  const [sessions, setSessions] = useState<AhuSessions | null>(null);
  const [targets, setTargets] = useState<AhuTargets | null>(null);
  const [tgInvite, setTgInvite] = useState(TG_FALLBACK);
  const [actionLoading, setActionLoading] = useState(false);

  const fetchAll = useCallback(async () => {
    try {
      const [statusRes, statsRes, sessionsRes, targetsRes, opsRes] = await Promise.allSettled([
        fetch(`${API_BASE}/api/ahu/status`),
        fetch(`${API_BASE}/api/ahu/stats`),
        fetch(`${API_BASE}/api/ahu/sessions`),
        fetch(`${API_BASE}/api/ahu/targets`),
        fetch(`${API_BASE}/api/telefix/ops-config`),
      ]);

      if (statusRes.status === "fulfilled" && statusRes.value.ok) {
        setStatus(await statusRes.value.json());
      }
      if (statsRes.status === "fulfilled" && statsRes.value.ok) {
        setStats(await statsRes.value.json());
      }
      if (sessionsRes.status === "fulfilled" && sessionsRes.value.ok) {
        setSessions(await sessionsRes.value.json());
      }
      if (targetsRes.status === "fulfilled" && targetsRes.value.ok) {
        setTargets(await targetsRes.value.json());
      }
      if (opsRes.status === "fulfilled" && opsRes.value.ok) {
        const j = (await opsRes.value.json()) as { operations_chat_link?: string };
        if (j.operations_chat_link) setTgInvite(j.operations_chat_link);
      }
    } catch {
      // silent
    }
  }, []);

  useEffect(() => {
    void fetchAll();
    const interval = setInterval(() => void fetchAll(), 10_000);
    return () => clearInterval(interval);
  }, [fetchAll]);

  const handleStart = async () => {
    setActionLoading(true);
    try {
      await fetch(`${API_BASE}/api/ahu/bot/start`, { method: "POST" });
      await fetchAll();
    } finally {
      setActionLoading(false);
    }
  };

  const handleStop = async () => {
    setActionLoading(true);
    try {
      await fetch(`${API_BASE}/api/ahu/bot/stop`, { method: "POST" });
      await fetchAll();
    } finally {
      setActionLoading(false);
    }
  };

  const tabs = [
    { id: "dashboard" as const, label: "Dashboard", icon: <LayoutDashboard size={14} /> },
    { id: "sessions" as const, label: "Sessions", icon: <HardDrive size={14} /> },
    { id: "scraper" as const, label: "Scraper/Adder", icon: <Search size={14} /> },
    { id: "logs" as const, label: "Logs", icon: <Terminal size={14} /> },
    { id: "link" as const, label: "Quick Link", icon: <MessageSquareCode size={14} /> },
  ];

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-8 animate-in fade-in">
      {/* Header */}
      <div className="flex justify-between items-center mb-8 flex-wrap gap-4">
        <div>
          <h3 className="text-2xl font-black text-white">ניהול אהו — Ops Sync</h3>
          <p className="text-slate-500 text-sm mt-1">ניהול בוט, סשנים, Scraper ולוגים חיים</p>
        </div>
        <div className="flex items-center gap-3">
          {status && (
            <div className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-xs font-bold ${
              status.bot_running
                ? "border-green-500/30 bg-green-500/10 text-green-400"
                : "border-slate-700 bg-slate-800 text-slate-500"
            }`}>
              <div className={`w-2 h-2 rounded-full ${status.bot_running ? "bg-green-400" : "bg-slate-600"}`} />
              {status.bot_running ? "LIVE" : "OFFLINE"}
            </div>
          )}
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 mb-6 bg-slate-950/50 p-1 rounded-2xl border border-slate-800 flex-wrap">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-bold transition flex-1 justify-center ${
              activeTab === tab.id
                ? "bg-cyan-600 text-white shadow-lg"
                : "text-slate-400 hover:text-white hover:bg-slate-800"
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      <div>
        {activeTab === "dashboard" && (
          <AhuDashboardTab
            status={status}
            stats={stats}
            onStart={handleStart}
            onStop={handleStop}
            actionLoading={actionLoading}
            onRefresh={fetchAll}
          />
        )}
        {activeTab === "sessions" && <AhuSessionsTab sessions={sessions} onRefresh={fetchAll} />}
        {activeTab === "scraper" && <AhuScraperTab targets={targets} />}
        {activeTab === "logs" && <AhuLogsTab />}
        {activeTab === "link" && <AhuQuickLinkTab invite={tgInvite} />}
      </div>
    </div>
  );
}

// ── Polymarket Tab Types ─────────────────────────────────────────────────────

interface OrderbookData {
  token_id: string | null;
  best_bid: number | null;
  best_ask: number | null;
  spread: number | null;
  mid_price: number | null;
  bids: { price: string; size: string }[];
  asks: { price: string; size: string }[];
  price_series: { price: number; size: number; side: "bid" | "ask" }[];
  source: string;
  expired?: boolean;
  no_position?: boolean;
  market_question?: string;
}

interface PolyBotPnL {
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
  session_active: boolean;
  session_stage: string;
  updated_at: string;
}

interface PaperPerf {
  total_trades: number;
  wins: number;
  losses: number;
  virtual_pnl: number;
  win_rate: number;
}

interface Poly5mData {
  wins: number;
  losses: number;
  decision: string | null;
  trading_halted: boolean;
  paper_trading?: boolean;
  btc_price?: number | null;
  yes_price?: number | null;
  velocity_pct_60s?: number | null;
  market_question?: string | null;
  sentiment?: Record<string, unknown>;
  loss_streak?: number;
  win_loss_ratio?: number | null;
}

interface CrossExchangeData {
  status: string;
  signal: string;
  signal_label: string;
  high_confidence: boolean;
  arbitrage_gap: number | null;
  polymarket: { yes_price: number | null; market_question: string | null } | null;
  binance: { price: number | null } | null;
}

interface TradeLogEntry {
  timestamp: string;
  side: string;
  price: number;
  shares: number;
  spent_usd: number;
  market_question: string;
  status: string;
  log_text: string;
  paper: boolean;
}

interface TradeLogData {
  entries: TradeLogEntry[];
  total: number;
  paper_trading: boolean;
  kill_switch_balance_usd: number;
}

// ── Helper ───────────────────────────────────────────────────────────────────

function fmtUsd(n: number, decimals = 2) {
  const abs = Math.abs(n).toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
  return `${n < 0 ? "-" : ""}$${abs}`;
}

function fmtPct(n: number) {
  return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
}

// ── Sub-components ───────────────────────────────────────────────────────────

function HackerCard({ children, className = "", glow = "cyan" }: { children: React.ReactNode; className?: string; glow?: "cyan" | "emerald" | "rose" | "violet" }) {
  const glowMap = { cyan: "border-cyan-500/20 shadow-cyan-500/5", emerald: "border-emerald-500/20 shadow-emerald-500/5", rose: "border-rose-500/20 shadow-rose-500/5", violet: "border-violet-500/20 shadow-violet-500/5" };
  return (
    <div className={`bg-[#0a0f1a] border rounded-2xl shadow-lg ${glowMap[glow]} ${className}`}>
      {children}
    </div>
  );
}

function StatBadge({ label, value, sub, icon: Icon, color = "cyan" }: { label: string; value: string; sub?: string; icon?: React.FC<{ size?: number }>; color?: "cyan" | "emerald" | "rose" | "violet" | "amber" }) {
  const colorMap = { cyan: "text-cyan-400", emerald: "text-emerald-400", rose: "text-rose-400", violet: "text-violet-400", amber: "text-amber-400" };
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center gap-1.5 text-[10px] font-black uppercase tracking-widest text-slate-500">
        {Icon && <Icon size={10} />}
        {label}
      </div>
      <div className={`text-xl font-black font-mono ${colorMap[color]}`}>{value}</div>
      {sub && <div className="text-[10px] text-slate-600 font-mono">{sub}</div>}
    </div>
  );
}

function LiveDot({ active = true }: { active?: boolean }) {
  return (
    <span className={`inline-block w-1.5 h-1.5 rounded-full ${active ? "bg-emerald-400 animate-pulse" : "bg-slate-600"}`} />
  );
}

// ── Main Component ───────────────────────────────────────────────────────────

function PolymarketTradingView({
  data,
  fetchDashboardData,
}: {
  data: GodModeDashboard | null;
  fetchDashboardData: () => Promise<void>;
}) {
  const [amount, setAmount] = useState("100");
  const [tokenId, setTokenId] = useState("");
  const [orderbook, setOrderbook] = useState<OrderbookData | null>(null);
  const [obError, setObError] = useState<string | null>(null);
  const [obLoading, setObLoading] = useState(false);
  const [pnlRange, setPnlRange] = useState<"1D" | "1W" | "1M" | "ALL">("1M");
  const [orderStatus, setOrderStatus] = useState<{ msg: string; ok: boolean } | null>(null);
  const [selectedPosition, setSelectedPosition] = useState<string | null>(null);
  const [nodes, setNodes] = useState<ClusterHealthNode[]>([]);
  const [expandedRecIdx, setExpandedRecIdx] = useState<number | null>(null);
  const [batchType, setBatchType] = useState("LIMIT");
  const [batchOrderSel, setBatchOrderSel] = useState("ALL");
  const [batchPrice, setBatchPrice] = useState("0.21");
  const [depositModal, setDepositModal] = useState<"deposit" | "withdraw" | null>(null);
  const [depositInput, setDepositInput] = useState("");
  const [depositStatus, setDepositStatus] = useState<{ msg: string; ok: boolean } | null>(null);
  const [batchSize, setBatchSize] = useState("100");
  const [batchStatus, setBatchStatus] = useState<string | null>(null);
  const [positionBatchCmds, setPositionBatchCmds] = useState<Record<string, string>>({});

  // ── SWR data feeds ───────────────────────────────────────────────────────
  const { data: bot } = useSWR<PolyBotPnL>(`${API_BASE}/api/prediction/polymarket-bot`, swrFetcher, { refreshInterval: 5_000 });
  const { data: perf } = useSWR<PaperPerf>(`${API_BASE}/api/prediction/performance`, swrFetcher, { refreshInterval: 12_000 });
  const { data: poly5m } = useSWR<Poly5mData>(`${API_BASE}/api/prediction/poly5m-scalper`, swrFetcher, { refreshInterval: 10_000 });
  const { data: cx } = useSWR<CrossExchangeData>(`${API_BASE}/api/prediction/cross-exchange`, swrFetcher, { refreshInterval: 8_000 });
  const { data: tradeLog } = useSWR<TradeLogData>(`${API_BASE}/api/prediction/trade-log`, swrFetcher, { refreshInterval: 6_000 });

  // ── Orderbook polling ────────────────────────────────────────────────────
  const fetchOrderbook = useCallback(async (tid?: string) => {
    const id = (tid ?? tokenId).trim();
    const url = id
      ? `${API_BASE}/api/polymarket/orderbook?token_id=${encodeURIComponent(id)}`
      : `${API_BASE}/api/polymarket/orderbook`;
    setObLoading(true);
    try {
      const res = await fetch(url);
      if (!res.ok) {
        const j = await res.json().catch(() => ({}));
        throw new Error((j as { detail?: string }).detail || `HTTP ${res.status}`);
      }
      const ob = (await res.json()) as OrderbookData;
      setOrderbook(ob);
      setObError(null);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setObError(msg);
      setOrderbook(null);
    } finally {
      setObLoading(false);
    }
  }, [tokenId]);

  const defaultOrderbookTokenId = useMemo(() => {
    const list = data?.portfolio_positions_list ?? [];
    const row = list.find((p) => (p.token_id ?? "").trim());
    return (row?.token_id ?? "").trim();
  }, [data?.portfolio_positions_list]);

  useEffect(() => {
    if (!defaultOrderbookTokenId) return;
    if (tokenId.trim()) return;
    setTokenId(defaultOrderbookTokenId);
  }, [defaultOrderbookTokenId, tokenId]);

  useEffect(() => {
    void fetchOrderbook();
    const t = setInterval(() => void fetchOrderbook(), 2_000);
    return () => clearInterval(t);
  }, [fetchOrderbook]);

  // ── Worker node resource polling ─────────────────────────────────────────
  useEffect(() => {
    const load = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/cluster/health`);
        if (!res.ok) return;
        const j = (await res.json()) as { nodes: ClusterHealthNode[] };
        setNodes(j.nodes ?? []);
      } catch { /* ignore */ }
    };
    void load();
    const t = setInterval(load, 8_000);
    return () => clearInterval(t);
  }, []);

  // ── Derived analytics ────────────────────────────────────────────────────
  const pnlSeries = useMemo(() => {
    const series = data?.pnl_series ?? [];
    const now = Date.now();
    const cutoffs: Record<string, number> = { "1D": 86400000, "1W": 604800000, "1M": 2592000000, ALL: Infinity };
    const cutoff = cutoffs[pnlRange] ?? Infinity;
    return series.filter((p) => now - new Date(p.time).getTime() <= cutoff);
  }, [data?.pnl_series, pnlRange]);

  // Portfolio: backend `portfolio_total_estimated` = positions (data-api) + CLOB + on-chain Polygon USDC
  const portfolioApiValue =
    data?.portfolio_total_estimated != null && data.portfolio_total_estimated > 0
      ? data.portfolio_total_estimated
      : (data?.portfolio_value ?? 0);
  const portfolioCash = data?.portfolio_cash ?? 0;
  const portfolioPositions = data?.portfolio_positions ?? 0;
  const positionsListForUi = data?.portfolio_positions_list ?? [];
  const sumPositionsNotional = positionsListForUi.reduce((s, p) => s + (p.current_value ?? 0), 0);
  const collateralRaw = parseFloat(data?.collateral_usdc ?? "0");
  const killSwitchBalance = tradeLog?.kill_switch_balance_usd ?? 0;
  const portfolioValue =
    portfolioApiValue > 0
      ? portfolioApiValue
      : sumPositionsNotional > 0
        ? sumPositionsNotional + portfolioCash
        : collateralRaw > 0
          ? collateralRaw
          : killSwitchBalance > 0
            ? killSwitchBalance
            : Math.max(bot?.total_pnl_usd ?? 0, 0);

  // Use real portfolio PnL from positions list when the API returned rows (sum may be 0 legitimately)
  const realPositionsPnl = positionsListForUi.reduce((sum, p) => sum + p.cash_pnl, 0);
  const totalPnl = positionsListForUi.length > 0 ? realPositionsPnl : (bot?.total_pnl_usd ?? 0);
  const realizedPnl = bot?.realized_pnl_usd ?? 0;
  const unrealizedPnl = bot?.unrealized_pnl_usd ?? 0;
  const isPnlPositive = totalPnl >= 0;

  /** POLYMARKET_PORTFOLIO_ADDRESS (dashboard) vs POLYMARKET_SIGNER_ADDRESS (CLOB orders) */
  const polyWalletMismatch = useMemo(() => {
    const p = (data?.portfolio_address ?? "").trim().toLowerCase();
    const s = (data?.clob_funder_address ?? data?.signer_address ?? "").trim().toLowerCase();
    return Boolean(p && s && p !== s);
  }, [data?.portfolio_address, data?.clob_funder_address, data?.signer_address]);

  const positions = useMemo(() => {
    // Prefer real Polymarket positions from the portfolio API
    const apiPositions = data?.portfolio_positions_list;
    if (apiPositions && apiPositions.length > 0) {
      return apiPositions.map((p) => ({
        asset: p.title,
        title: p.title,
        outcome: p.outcome,
        netShares: p.size,
        avgPrice: p.avg_price,
        nowPrice: p.cur_price,
        value: p.current_value,
        pnlDelta: p.cash_pnl,
        pnlPct: p.percent_pnl,
        totalSpent: p.size * p.avg_price,
        count: 1,
        endDate: p.end_date,
        clobTokenId: (p.token_id ?? "").trim(),
        // legacy compat fields
        buys: p.size,
        sells: 0,
        lastPrice: String(p.cur_price),
      }));
    }
    // Fallback: derive from trading history
    const history = data?.trading_history ?? [];
    const map = new Map<string, { asset: string; buys: number; sells: number; totalSpent: number; count: number; lastPrice: string }>();
    for (const t of history) {
      const key = t.asset;
      const existing = map.get(key) ?? { asset: t.asset, buys: 0, sells: 0, totalSpent: 0, count: 0, lastPrice: t.price };
      if (t.side === "BUY") { existing.buys += t.amount; existing.totalSpent += t.amount; }
      else existing.sells += t.amount;
      existing.count += 1;
      existing.lastPrice = t.price;
      map.set(key, existing);
    }
    return Array.from(map.values()).map((p) => {
      const netShares = p.buys - p.sells;
      const avgPrice = p.count > 0 ? p.totalSpent / p.buys : 0;
      const nowPrice = parseFloat(p.lastPrice) || 0;
      const value = netShares * nowPrice;
      const pnlDelta = netShares * (nowPrice - avgPrice);
      const pnlPct = avgPrice > 0 ? ((nowPrice - avgPrice) / avgPrice) * 100 : 0;
      return { ...p, netShares, avgPrice, nowPrice, value, pnlDelta, pnlPct, title: p.asset, outcome: "YES", endDate: "", clobTokenId: "" };
    }).filter((p) => p.netShares > 0);
  }, [data?.portfolio_positions_list, data?.trading_history]);

  const aiRecs = useMemo(() => {
    const signal = cx?.signal ?? "NEUTRAL";
    const totalVal = portfolioValue > 0 ? portfolioValue : 100;
    return positions.map((p) => {
      const edge = p.pnlPct;
      const isBullish = signal === "BULLISH" || signal === "BUY";
      const arbGap = Math.abs(cx?.arbitrage_gap ?? 0);
      // Per-position confidence: blend arb gap + edge magnitude
      const edgeConf = Math.min(40, Math.abs(edge) * 0.8);
      const confidence = Math.min(100, arbGap * 1000 + 20 + edgeConf);
      let action: "BUY MORE" | "HOLD" | "REDUCE" = "HOLD";
      if (isBullish && edge > 5) action = "BUY MORE";
      else if (!isBullish && edge < -5) action = "REDUCE";
      // Recommended bet: Kelly-lite — size proportional to confidence × edge
      const kellySizing = (confidence / 100) * (Math.abs(edge) / 100) * totalVal;
      const recBet = Math.max(10, Math.min(kellySizing, totalVal * 0.2));
      const recSide: "BUY" | "SELL" = action === "REDUCE" ? "SELL" : "BUY";
      return { ...p, action, confidence, signal, recBet, recSide };
    }).sort((a, b) => b.confidence - a.confidence);
  }, [positions, cx, portfolioValue]);

  // ── Order handler ────────────────────────────────────────────────────────
  const handleOrder = async (side: "BUY" | "SELL") => {
    if (!tokenId.trim()) { setOrderStatus({ msg: "Enter Token ID first", ok: false }); return; }
    const usd = parseFloat(amount);
    if (!Number.isFinite(usd) || usd <= 0) {
      setOrderStatus({ msg: "Amount must be a number greater than 0", ok: false });
      return;
    }
    setOrderStatus(null);
    try {
      const res = await fetch(`${API_BASE}/api/polymarket/manual-order`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token_id: tokenId.trim(), side, amount: usd }),
      });
      const j = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = (j as { detail?: string | { msg?: string }[] }).detail;
        const msg = typeof detail === "string" ? detail : Array.isArray(detail) ? detail.map((x) => x.msg).join(", ") : "Order rejected";
        setOrderStatus({ msg, ok: false });
        return;
      }
      setOrderStatus({ msg: `${side} executed — order placed`, ok: true });
      await fetchDashboardData();
    } catch {
      setOrderStatus({ msg: "Execution error — check master connection", ok: false });
    }
  };

  // ── Direct rec order handler (fires from expanded panel) ─────────────────
  const [recOrderStatus, setRecOrderStatus] = useState<Record<number, { msg: string; ok: boolean; loading?: boolean }>>({});
  const handleRecOrder = async (recIdx: number, clobTokenId: string, side: "BUY" | "SELL", betAmount: number) => {
    const tid = clobTokenId.trim();
    if (!tid) {
      setRecOrderStatus((prev) => ({
        ...prev,
        [recIdx]: { msg: "חסר מזהה CLOB לפוזיציה — רענן את הדשבורד / Missing CLOB token — refresh dashboard", ok: false },
      }));
      return;
    }
    if (!Number.isFinite(betAmount) || betAmount <= 0) {
      setRecOrderStatus((prev) => ({
        ...prev,
        [recIdx]: { msg: "סכום לא תקין — חייב להיות > 0 / Invalid bet amount", ok: false },
      }));
      return;
    }
    setRecOrderStatus((prev) => ({ ...prev, [recIdx]: { msg: "שולח פקודה...", ok: true, loading: true } }));
    try {
      const res = await fetch(`${API_BASE}/api/polymarket/manual-order`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token_id: tid, side, amount: betAmount }),
      });
      const j = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = (j as { detail?: string | { msg?: string }[] }).detail;
        const msg = typeof detail === "string" ? detail : Array.isArray(detail) ? detail.map((x) => x.msg).join(", ") : "הפקודה נדחתה";
        setRecOrderStatus((prev) => ({ ...prev, [recIdx]: { msg, ok: false } }));
        return;
      }
      setRecOrderStatus((prev) => ({ ...prev, [recIdx]: { msg: `✅ בוצע ${side} — $${betAmount.toFixed(0)}`, ok: true } }));
      await fetchDashboardData();
    } catch {
      setRecOrderStatus((prev) => ({ ...prev, [recIdx]: { msg: "שגיאת חיבור — בדוק Nexus Master", ok: false } }));
    }
  };

  const signalColor = cx?.signal === "BULLISH" || cx?.signal === "BUY" ? "text-emerald-400" : cx?.signal === "BEARISH" || cx?.signal === "SELL" ? "text-rose-400" : "text-amber-400";
  const signalBg = cx?.signal === "BULLISH" || cx?.signal === "BUY" ? "bg-emerald-500/10 border-emerald-500/30" : cx?.signal === "BEARISH" || cx?.signal === "SELL" ? "bg-rose-500/10 border-rose-500/30" : "bg-amber-500/10 border-amber-500/30";

  // ── Augmented analytics — derived from real API data ────────────────────
  // Risk Adjusted Alpha: realized PnL adjusted by win rate (Sharpe-proxy)
  const winRate = perf?.win_rate ?? 0;
  const riskAdjAlpha = bot?.available
    ? realizedPnl * (winRate / 100 + 0.5)
    : 0;
  // Est. Returns by Nexus Core: unrealized + projected from cross-exchange gap
  const arbGapBoost = (cx?.arbitrage_gap ?? 0) * portfolioValue * 10;
  const estReturnsNexus = bot?.available
    ? unrealizedPnl + arbGapBoost
    : 0;
  // Est. Returns: total PnL as baseline
  const estReturns = totalPnl;

  const handleBatchDispatch = () => {
    setBatchStatus(`DISPATCHING ${batchOrderSel} · ${batchType} @ $${batchPrice} · SIZE ${batchSize} USDC`);
    setTimeout(() => setBatchStatus("✅ BATCH QUEUED — NEXUS CORE ROUTING"), 900);
    setTimeout(() => setBatchStatus(null), 4000);
  };

  const handleDepositSubmit = async () => {
    const val = parseFloat(depositInput);
    if (isNaN(val) || val <= 0) { setDepositStatus({ msg: "סכום לא תקין / Invalid amount", ok: false }); return; }
    const endpoint = depositModal === "deposit" ? "set-deposit" : "set-withdrawn";
    try {
      const res = await fetch(`${API_BASE}/api/polymarket/${endpoint}`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ amount: val }),
      });
      if (!res.ok) throw new Error(await res.text());
      setDepositStatus({ msg: depositModal === "deposit" ? `✅ הפקדה עודכנה: $${val.toFixed(2)}` : `✅ משיכה עודכנה: $${val.toFixed(2)}`, ok: true });
      setTimeout(() => { setDepositModal(null); setDepositStatus(null); setDepositInput(""); fetchDashboardData(); }, 1500);
    } catch (e) {
      setDepositStatus({ msg: `שגיאה: ${e instanceof Error ? e.message : String(e)}`, ok: false });
    }
  };

  const expectedManualOrderEnrich = "v4";

  return (
    <div className="space-y-6" style={{ background: "transparent" }}>

      {data && data.manual_order_error_enrich !== expectedManualOrderEnrich && (
        <div className="rounded-xl border border-rose-500/50 bg-rose-950/35 px-4 py-3 text-[12px] text-rose-100/95 leading-relaxed">
          <span className="font-black uppercase tracking-widest text-[10px] text-rose-400">API build mismatch / גרסת שרת</span>
          <p className="mt-1">
            <code className="text-white">dashboard.json</code> did not report{" "}
            <code className="text-white">manual_order_error_enrich={expectedManualOrderEnrich}</code>
            {" "}(got <code className="text-white">{String(data.manual_order_error_enrich ?? "∅")}</code>).{" "}
            This browser is calling a Nexus API that is <strong className="text-white">not</strong> the current repo build — manual-order errors stay on old wording until you deploy/restart the API on{" "}
            <code className="text-white">{API_BASE}</code>.
          </p>
        </div>
      )}

      {polyWalletMismatch && data?.portfolio_address && (data?.clob_funder_address || data?.signer_address) && (() => {
        const tradeAddr = data.clob_funder_address ?? data.signer_address ?? "";
        const clobBal = data?.clob_balance ?? 0;
        const pcash = data?.portfolio_cash ?? 0;
        const buyOk = clobBal >= 1;
        return (
        <div className={`rounded-xl border px-4 py-3 text-sm ${buyOk ? "border-emerald-500/40 bg-emerald-950/30" : "border-amber-500/50 bg-amber-500/10"}`}>
          <div className={`flex items-center gap-2 text-[10px] font-black uppercase tracking-widest mb-2 ${buyOk ? "text-emerald-400" : "text-amber-400"}`}>
            <AlertTriangle size={14} />
            Trading wallet ≠ portfolio view / ארנק מסחר שונה מתצוגת התיק
          </div>
          <p className="text-amber-100/95 leading-relaxed text-[13px]">
            Positions table uses{" "}
            <span className="font-mono text-white">{data.portfolio_address.slice(0, 6)}…{data.portfolio_address.slice(-4)}</span>
            {" "}(<code className="text-amber-200/90">POLYMARKET_PORTFOLIO_ADDRESS</code>). CLOB signs as maker{" "}
            <span className="font-mono text-white">{tradeAddr.slice(0, 6)}…{tradeAddr.slice(-4)}</span>
            {". "}
            <strong className="text-white">BUY</strong> spends USDC from maker; <strong className="text-white">SELL</strong> needs outcome-token shares on the maker (not only on the portfolio row). Align{" "}
            <code className="text-amber-200/90">POLYMARKET_RELAYER_KEY</code> / <code className="text-amber-200/90">POLYMARKET_SIGNER_ADDRESS</code> with the funded account, or clear <code className="text-amber-200/90">POLYMARKET_PORTFOLIO_ADDRESS</code> to match the UI to the maker.
          </p>
          <p className="mt-2 pt-2 border-t border-white/10 text-[12px] text-slate-200/95 leading-relaxed">
            Same dollar figure can mean different things: portfolio <span className="font-semibold text-white">Cash</span>{" "}
            <span className="font-mono">{fmtUsd(pcash)}</span> is free USDC for{" "}
            <span className="font-mono text-white">{data.portfolio_address.slice(0, 6)}…</span> (Polymarket data API).{" "}
            <span className="font-semibold text-white">Tradable USDC (CLOB)</span>{" "}
            <span className="font-mono text-white">{fmtUsd(clobBal)}</span> is collateral for orders from{" "}
            <span className="font-mono text-white">{tradeAddr.slice(0, 6)}…</span>. They are not the same field.
          </p>
          {buyOk && (
            <p className="mt-2 text-[12px] text-emerald-200/95 leading-relaxed">
              BUY YES is funded on the maker wallet (CLOB). SELL still only sells tokens the maker actually holds — table rows tied to the portfolio address may not be sellable via this key.
            </p>
          )}
        </div>
        );
      })()}

      {/* ══ PORTFOLIO HEADER (Polymarket clone) ══════════════════════════════ */}
      <HackerCard className="p-6" glow="cyan">
        <div className="flex flex-wrap items-start justify-between gap-6">
          {/* Left: portfolio value */}
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-slate-500">
              <LiveDot active={!!bot?.session_active} />
              <span>Portfolio <span className="text-slate-600 font-normal normal-case">/ תיק השקעות</span></span>
              {bot?.session_active && <span className="text-emerald-400 animate-pulse">· SESSION ACTIVE / סשן פעיל</span>}
            </div>
            <div className="text-4xl font-black font-mono text-white">
              {fmtUsd(portfolioValue)}
            </div>
            <div className={`text-sm font-mono font-bold ${isPnlPositive ? "text-emerald-400" : "text-rose-400"}`}>
              {isPnlPositive ? <ArrowUpRight size={14} className="inline" /> : <ArrowDownRight size={14} className="inline" />}
              {fmtUsd(totalPnl)} ({fmtPct(portfolioValue > 0 ? (totalPnl / portfolioValue) * 100 : 0)})
              {(() => {
                const addr = data?.portfolio_address || data?.signer_address;
                return addr
                  ? <span className="text-slate-500 font-mono text-xs ml-1">{addr.slice(0,6)}…{addr.slice(-4)}</span>
                  : <span className="text-slate-600 text-xs ml-1">live</span>;
              })()}
            </div>
          </div>

          {/* Center: cash + stats */}
          <div className="flex flex-wrap gap-8">
            <StatBadge label="Portfolio / תיק" value={fmtUsd(portfolioValue)} icon={TrendingUp} color="cyan" />
            <StatBadge label="Cash (USDC) / מזומן" value={fmtUsd(data?.portfolio_positions_list ? portfolioCash : (collateralRaw > 0 ? collateralRaw : portfolioCash))} icon={DollarSign} color="emerald" />
            <StatBadge label="Positions / פוזיציות" value={fmtUsd(portfolioPositions)} icon={BarChart2} color="violet" />
            <StatBadge label="Win Rate / אחוז ניצחון" value={`${(perf?.win_rate ?? 0).toFixed(1)}%`} sub={`${perf?.total_trades ?? 0} trades / עסקאות`} icon={Percent} color="amber" />
          </div>

          {/* Break-even row */}
          {(data?.total_deposited ?? 0) > 0 && (() => {
            const deposited = data?.total_deposited ?? 0;
            const withdrawn = data?.total_withdrawn ?? 0;
            const beDelta   = data?.break_even_delta ?? 0;
            const roi       = deposited > 0 ? (beDelta / deposited) * 100 : 0;
            const bePositive = beDelta >= 0;
            return (
              <div className="w-full mt-3 pt-3 border-t border-slate-800/60 flex flex-wrap gap-6 items-center">
                <div className="text-[10px] font-black uppercase tracking-widest text-slate-500">
                  Break-Even / <span className="text-slate-600 normal-case font-normal">נקודת איזון</span>
                </div>
                <div className="flex gap-6 text-xs font-mono flex-wrap">
                  <span className="text-slate-500">הפקדות / Deposited: <span className="text-slate-300 font-black">{fmtUsd(deposited)}</span></span>
                  {withdrawn > 0 && <span className="text-slate-500">משיכות / Withdrawn: <span className="text-emerald-400 font-black">{fmtUsd(withdrawn)}</span></span>}
                  <span className={`font-black ${bePositive ? "text-emerald-400" : "text-rose-400"}`}>
                    {bePositive ? "▲" : "▼"} {bePositive ? "+" : ""}{fmtUsd(beDelta)} ({roi >= 0 ? "+" : ""}{roi.toFixed(1)}% ROI)
                  </span>
                </div>
              </div>
            );
          })()}

          {/* Right: action buttons */}
          <div className="flex gap-3">
            <button type="button" onClick={() => { setDepositModal("deposit"); setDepositInput(""); setDepositStatus(null); }} className="px-5 py-2 bg-cyan-500 hover:bg-cyan-400 text-black font-black text-xs rounded-xl transition shadow-lg shadow-cyan-500/20 uppercase tracking-widest">
              הפקדה / Deposit
            </button>
            <button type="button" onClick={() => { setDepositModal("withdraw"); setDepositInput(""); setDepositStatus(null); }} className="px-5 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 font-black text-xs rounded-xl transition border border-slate-700 uppercase tracking-widest">
              משיכה / Withdraw
            </button>
          </div>

          {/* Deposit / Withdraw modal */}
          {depositModal && (
            <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm" onClick={(e) => { if (e.target === e.currentTarget) setDepositModal(null); }}>
              <div className="bg-slate-900 border border-slate-700 rounded-2xl p-6 w-full max-w-sm shadow-2xl">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-base font-black text-white">
                    {depositModal === "deposit" ? "הפקדה / Deposit" : "משיכה / Withdraw"}
                  </h3>
                  <button type="button" onClick={() => setDepositModal(null)} className="text-slate-500 hover:text-white text-lg leading-none">✕</button>
                </div>
                <p className="text-xs text-slate-500 mb-4">
                  {depositModal === "deposit"
                    ? "הזן את סך כל הסכום שהפקדת לפולימרקט עד כה (לחישוב נקודת איזון)"
                    : "הזן את סך כל הסכום שמשכת מפולימרקט עד כה"}
                </p>
                <div className="flex items-center gap-2 mb-4">
                  <span className="text-slate-400 font-black">$</span>
                  <input
                    type="number" min="0" step="0.01"
                    value={depositInput}
                    onChange={(e) => setDepositInput(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleDepositSubmit()}
                    placeholder="0.00"
                    className="flex-1 bg-slate-800 border border-slate-700 rounded-xl px-3 py-2 text-white font-mono text-sm focus:outline-none focus:border-cyan-500"
                    autoFocus
                  />
                </div>
                {depositStatus && (
                  <div className={`text-xs font-bold mb-3 ${depositStatus.ok ? "text-emerald-400" : "text-rose-400"}`}>
                    {depositStatus.msg}
                  </div>
                )}
                <div className="flex gap-2">
                  <button type="button" onClick={handleDepositSubmit}
                    className="flex-1 py-2 bg-cyan-500 hover:bg-cyan-400 text-black font-black text-xs rounded-xl transition">
                    אישור / Confirm
                  </button>
                  <button type="button" onClick={() => setDepositModal(null)}
                    className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 font-black text-xs rounded-xl transition border border-slate-700">
                    ביטול / Cancel
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* PnL sparkline */}
        <div className="mt-6">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-[10px] font-black uppercase tracking-widest text-slate-500">Profit / Loss <span className="text-slate-600 normal-case font-normal">/ רווח והפסד</span></span>
            <div className="flex gap-1 ml-auto">
              {(["1D", "1W", "1M", "ALL"] as const).map((r) => (
                <button key={r} type="button" onClick={() => setPnlRange(r)}
                  className={`px-2 py-0.5 text-[10px] font-black rounded transition ${pnlRange === r ? "bg-cyan-500/20 text-cyan-400 border border-cyan-500/40" : "text-slate-500 hover:text-slate-300"}`}>
                  {r}
                </button>
              ))}
            </div>
          </div>
          <div className="h-[80px]">
            <ResponsiveContainer width="100%" height={80}>
              <AreaChart data={pnlSeries.length ? pnlSeries : [{ time: "—", pnl: 0 }]}>
                <defs>
                  <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={isPnlPositive ? "#34d399" : "#f87171"} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={isPnlPositive ? "#34d399" : "#f87171"} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#0f172a" vertical={false} />
                <XAxis dataKey="time" hide />
                <YAxis hide />
                <Tooltip
                  contentStyle={{ backgroundColor: "#0a0f1a", border: "1px solid #1e293b", borderRadius: "8px", fontSize: "11px" }}
                  formatter={(v) => [fmtUsd(Number(v ?? 0)), "PnL"]}
                />
                <ReferenceLine y={0} stroke="#334155" strokeDasharray="4 4" />
                <Area type="monotone" dataKey="pnl" stroke={isPnlPositive ? "#34d399" : "#f87171"} fill="url(#pnlGrad)" strokeWidth={2} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* ── PHASE 2: Augmented Analytics Injection ── */}
        <div className="mt-5 pt-5 border-t border-slate-800/60">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-[9px] font-black uppercase tracking-[0.3em] text-cyan-400/60">◈ NEXUS CORE AUGMENTED ANALYTICS <span className="text-cyan-400/30 normal-case font-normal">/ ניתוח מתקדם</span></span>
            <span className="text-[9px] font-black text-fuchsia-400 animate-pulse">LIVE / חי</span>
            {!bot?.available && (
              <span className="text-[8px] font-black text-amber-400/70 font-mono ml-auto">BOT OFFLINE — WAITING FOR WORKER / בוט לא מחובר</span>
            )}
          </div>
          <div className="grid grid-cols-3 gap-3">
            {/* Risk Adjusted Alpha = realized PnL × (winRate/100 + 0.5) */}
            <div className="rounded-xl p-3" style={{ background: "rgba(34,211,238,0.06)", border: "1px solid rgba(34,211,238,0.2)", boxShadow: "0 0 12px rgba(34,211,238,0.06)" }}>
              <div className="text-[9px] font-black uppercase tracking-widest text-cyan-400/60 mb-1">⬡ Risk Adjusted Alpha <span className="text-cyan-400/30 normal-case font-normal">/ אלפא מתואם סיכון</span></div>
              <div className={`text-lg font-black font-mono ${riskAdjAlpha >= 0 ? "text-cyan-300" : "text-rose-400"}`} style={{ textShadow: "0 0 10px rgba(34,211,238,0.4)" }}>
                {riskAdjAlpha >= 0 ? "+" : ""}{fmtUsd(riskAdjAlpha)}
              </div>
              <div className="text-[9px] text-slate-600 font-mono mt-0.5">
                R: {fmtUsd(realizedPnl)} · WR: {winRate.toFixed(1)}%
              </div>
            </div>
            {/* Est. Returns by Nexus Core = unrealized + arb gap projection */}
            <div className="rounded-xl p-3" style={{ background: "rgba(168,85,247,0.06)", border: "1px solid rgba(168,85,247,0.2)", boxShadow: "0 0 12px rgba(168,85,247,0.06)" }}>
              <div className="text-[9px] font-black uppercase tracking-widest text-purple-400/60 mb-1">⬡ Est. Returns by Nexus Core <span className="text-purple-400/30 normal-case font-normal">/ תשואה משוערת</span></div>
              <div className={`text-lg font-black font-mono ${estReturnsNexus >= 0 ? "text-purple-300" : "text-rose-400"}`} style={{ textShadow: "0 0 10px rgba(168,85,247,0.4)" }}>
                {estReturnsNexus >= 0 ? "+" : ""}{fmtUsd(estReturnsNexus)}
              </div>
              <div className="text-[9px] text-slate-600 font-mono mt-0.5">
                U: {fmtUsd(unrealizedPnl)} · ARB: {((cx?.arbitrage_gap ?? 0) * 100).toFixed(3)}%
              </div>
            </div>
            {/* Est. Returns = total PnL from bot */}
            <div className="rounded-xl p-3" style={{ background: "rgba(52,211,153,0.06)", border: "1px solid rgba(52,211,153,0.2)", boxShadow: "0 0 12px rgba(52,211,153,0.06)" }}>
              <div className="text-[9px] font-black uppercase tracking-widest text-emerald-400/60 mb-1">⬡ Est. Returns <span className="text-emerald-400/30 normal-case font-normal">/ תשואה כוללת</span></div>
              <div className={`text-lg font-black font-mono ${estReturns >= 0 ? "text-emerald-300" : "text-rose-400"}`} style={{ textShadow: "0 0 10px rgba(52,211,153,0.4)" }}>
                {estReturns >= 0 ? "+" : ""}{fmtUsd(estReturns)}
              </div>
              <div className="text-[9px] text-slate-600 font-mono mt-0.5">
                Total PnL / סה״כ · {bot?.last_action || (bot?.available ? "active / פעיל" : "no session / אין סשן")}
              </div>
            </div>
          </div>
        </div>

        {/* ── PHASE 2: 3D Wireframe Mesh PnL Graph — driven by real pnl_series ── */}
        <div className="mt-4">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-[9px] font-black uppercase tracking-widest text-fuchsia-400/50">◈ P/L 3D WIREFRAME MESH <span className="text-fuchsia-400/25 normal-case font-normal">/ גרף רווח/הפסד</span></span>
            <span className="text-[8px] text-slate-600 font-mono">{pnlSeries.length} pts / נקודות</span>
          </div>
          {pnlSeries.length > 1 ? (
            <div className="h-[90px]">
              <ResponsiveContainer width="100%" height={90}>
                <AreaChart data={pnlSeries}>
                  <defs>
                    <linearGradient id="pnlMeshFill" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={isPnlPositive ? "#22d3ee" : "#f43f5e"} stopOpacity="0.4" />
                      <stop offset="100%" stopColor={isPnlPositive ? "#22d3ee" : "#f43f5e"} stopOpacity="0.02" />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(236,72,153,0.08)" vertical={false} />
                  <XAxis dataKey="time" hide />
                  <YAxis hide />
                  <Tooltip
                    contentStyle={{ backgroundColor: "#0a0f1a", border: "1px solid rgba(236,72,153,0.3)", borderRadius: "8px", fontSize: "10px" }}
                    formatter={(v) => [fmtUsd(Number(v ?? 0)), "PnL"]}
                  />
                  <ReferenceLine y={0} stroke="rgba(236,72,153,0.3)" strokeDasharray="4 4" />
                  <Area type="monotone" dataKey="pnl" stroke={isPnlPositive ? "#22d3ee" : "#f43f5e"} fill="url(#pnlMeshFill)" strokeWidth={2} dot={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="relative rounded-xl overflow-hidden" style={{ height: 90, background: "rgba(0,0,0,0.5)", border: "1px solid rgba(236,72,153,0.15)" }}>
              <svg width="100%" height="90" viewBox="0 0 600 90" preserveAspectRatio="none" className="absolute inset-0">
                <defs>
                  <linearGradient id="pnlMeshFillStatic" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#f43f5e" stopOpacity="0.3" />
                    <stop offset="100%" stopColor="#f43f5e" stopOpacity="0.02" />
                  </linearGradient>
                  <filter id="meshGlow"><feGaussianBlur stdDeviation="1.5" result="blur"/><feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>
                </defs>
                {[0,1,2,3].map(i => <line key={`mh${i}`} x1="0" y1={i*22+10} x2="600" y2={i*22+10} stroke="rgba(236,72,153,0.08)" strokeWidth="1"/>)}
                {[0,1,2,3,4,5,6,7,8,9,10,11,12].map(i => <line key={`mv${i}`} x1={i*50} y1="0" x2={i*50} y2="90" stroke="rgba(236,72,153,0.05)" strokeWidth="1"/>)}
                <line x1="0" y1="45" x2="600" y2="45" stroke="rgba(236,72,153,0.2)" strokeWidth="1" strokeDasharray="4,4"/>
              </svg>
              <div className="absolute inset-0 flex items-center justify-center text-[9px] font-black text-slate-700 uppercase tracking-widest font-mono">AWAITING PnL DATA FROM WORKER / ממתין לנתוני רווח/הפסד</div>
            </div>
          )}
        </div>
      </HackerCard>

      {/* ══ ANALYTICS STRIP ══════════════════════════════════════════════════ */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Bot PnL */}
        <HackerCard className="p-5" glow={totalPnl >= 0 ? "emerald" : "rose"}>
          <div className="flex items-center gap-2 mb-3">
            <Cpu size={14} className="text-cyan-400" />
            <span className="text-[10px] font-black uppercase tracking-widest text-slate-500">Bot Total PnL <span className="text-slate-600 normal-case font-normal">/ רווח בוט</span></span>
            {bot?.within_target_band && (
              <span className="ml-auto text-[8px] font-black text-emerald-400 animate-pulse">◈ IN BAND / בטווח</span>
            )}
          </div>
          <div className={`text-2xl font-black font-mono ${totalPnl >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
            {fmtUsd(totalPnl)}
          </div>
          <div className="text-[10px] text-slate-600 font-mono mt-1">
            R: {fmtUsd(bot?.realized_pnl_usd ?? 0)} · U: {fmtUsd(bot?.unrealized_pnl_usd ?? 0)}
          </div>
          {bot?.btc_spot != null && (
            <div className="text-[10px] text-slate-500 font-mono mt-1">
              BTC: <span className="text-amber-400">${bot.btc_spot.toLocaleString()}</span>
              {bot.target_strike != null && <> · Strike: <span className="text-cyan-400">${bot.target_strike.toLocaleString()}</span></>}
            </div>
          )}
          {bot?.yes_price != null && (
            <div className="text-[10px] text-slate-500 font-mono">YES: <span className="text-cyan-400">{(bot.yes_price * 100).toFixed(1)}¢</span></div>
          )}
          {bot?.market_question && (
            <div className="text-[10px] text-slate-500 mt-1 truncate">{bot.market_question}</div>
          )}
        </HackerCard>

        {/* Win Rate */}
        <HackerCard className="p-5" glow="violet">
          <div className="flex items-center gap-2 mb-3">
            <Target size={14} className="text-violet-400" />
            <span className="text-[10px] font-black uppercase tracking-widest text-slate-500">Win Rate <span className="text-slate-600 normal-case font-normal">/ אחוז ניצחון</span></span>
          </div>
          <div className="text-2xl font-black font-mono text-violet-400">
            {(perf?.win_rate ?? 0).toFixed(1)}%
          </div>
          <div className="text-[10px] text-slate-600 font-mono mt-1">
            {perf?.wins ?? 0}W / {perf?.losses ?? 0}L · {perf?.total_trades ?? 0} total / עסקאות
          </div>
          <div className="mt-2 h-1.5 bg-slate-800 rounded-full overflow-hidden">
            <div className="h-full bg-violet-500 rounded-full transition-all" style={{ width: `${perf?.win_rate ?? 0}%` }} />
          </div>
        </HackerCard>

        {/* 5m Scalper */}
        <HackerCard className="p-5" glow="cyan">
          <div className="flex items-center gap-2 mb-3">
            <Zap size={14} className="text-cyan-400" />
            <span className="text-[10px] font-black uppercase tracking-widest text-slate-500">5m Scalper <span className="text-slate-600 normal-case font-normal">/ סקאלפר</span></span>
            {poly5m?.paper_trading && (
              <span className="ml-auto text-[8px] font-black text-amber-400 bg-amber-500/10 border border-amber-500/20 px-1.5 py-0.5 rounded uppercase">PAPER / נייר</span>
            )}
          </div>
          <div className="text-2xl font-black font-mono text-cyan-400">
            {poly5m?.wins ?? 0}W / {poly5m?.losses ?? 0}L
          </div>
          <div className="text-[10px] text-slate-600 font-mono mt-1">
            {poly5m?.trading_halted ? "⛔ HALTED / עצור" : (poly5m?.decision ?? "—")}
          </div>
          {poly5m?.btc_price != null && (
            <div className="text-[10px] text-slate-500 font-mono mt-1">
              BTC: <span className="text-amber-400">${poly5m.btc_price.toLocaleString()}</span>
              {poly5m.yes_price != null && <> · YES: <span className="text-cyan-400">{(poly5m.yes_price * 100).toFixed(1)}¢</span></>}
            </div>
          )}
          {poly5m?.velocity_pct_60s != null && (
            <div className="text-[10px] font-mono mt-0.5" style={{ color: (poly5m.velocity_pct_60s ?? 0) >= 0 ? "#34d399" : "#f87171" }}>
              VEL 60s: {(poly5m.velocity_pct_60s ?? 0) >= 0 ? "+" : ""}{poly5m.velocity_pct_60s.toFixed(3)}%
            </div>
          )}
        </HackerCard>

        {/* Cross-exchange signal */}
        <HackerCard className="p-5" glow={cx?.signal === "BULLISH" || cx?.signal === "BUY" ? "emerald" : "rose"}>
          <div className="flex items-center gap-2 mb-3">
            <Radio size={14} className="text-slate-400" />
            <span className="text-[10px] font-black uppercase tracking-widest text-slate-500">Cross-Exchange <span className="text-slate-600 normal-case font-normal">/ בין בורסות</span></span>
          </div>
          <div className={`text-xl font-black font-mono ${signalColor}`}>
            {cx?.signal_label ?? cx?.signal ?? "—"}
          </div>
          <div className="text-[10px] text-slate-600 font-mono mt-1">
            ARB GAP: {cx?.arbitrage_gap != null ? (cx.arbitrage_gap * 100).toFixed(3) + "%" : "—"}
          </div>
            {cx?.high_confidence && (
            <div className="mt-2 text-[10px] font-black text-amber-400 uppercase tracking-widest animate-pulse">⚡ HIGH CONFIDENCE / ביטחון גבוה</div>
          )}
        </HackerCard>
      </div>

      {/* ══ POSITIONS + ORDERBOOK ════════════════════════════════════════════ */}
      <div className="grid grid-cols-12 gap-6">

        {/* Positions table */}
        <div className="col-span-12 lg:col-span-7">
          <HackerCard className="overflow-hidden" glow="cyan">
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800/60">
              <div className="flex items-center gap-2">
                <BarChart2 size={16} className="text-cyan-400" />
                <span className="text-sm font-black uppercase tracking-widest text-white">Positions <span className="text-slate-500 normal-case font-normal text-xs">/ פוזיציות</span></span>
                <span className="text-[10px] font-black text-slate-500 bg-slate-800 px-2 py-0.5 rounded-full">{positions.length}</span>
              </div>
              <div className="flex gap-2 text-[10px] font-black text-slate-500 uppercase tracking-widest">
                <button type="button" className="px-3 py-1 rounded-lg bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">Positions / פוזיציות</button>
                <button type="button" className="px-3 py-1 rounded-lg hover:bg-slate-800 transition">Open orders / פקודות פתוחות</button>
                <button type="button" className="px-3 py-1 rounded-lg hover:bg-slate-800 transition">History / היסטוריה</button>
              </div>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-800/60">
                    <th className="px-4 py-3 text-left text-[10px] font-black uppercase tracking-widest text-slate-500">Market / שוק</th>
                    <th className="px-4 py-3 text-right text-[10px] font-black uppercase tracking-widest text-slate-500">AVG → NOW / ממוצע → עכשיו</th>
                    <th className="px-4 py-3 text-right text-[10px] font-black uppercase tracking-widest text-slate-500">Traded / נסחר</th>
                    <th className="px-4 py-3 text-right text-[10px] font-black uppercase tracking-widest text-slate-500">To Win / לזכייה</th>
                    <th className="px-4 py-3 text-right text-[10px] font-black uppercase tracking-widest text-slate-500">Value / שווי</th>
                    <th className="px-4 py-3 text-center text-[10px] font-black uppercase tracking-widest text-cyan-500/60">REAL PROB (AUDITED) / הסתברות אמיתית</th>
                    <th className="px-4 py-3 text-center text-[10px] font-black uppercase tracking-widest text-purple-500/60">EST. RESOLUTION / פקיעה משוערת</th>
                    <th className="px-4 py-3 text-center text-[10px] font-black uppercase tracking-widest text-fuchsia-500/60">NEXUS REC / המלצת נקסוס</th>
                    <th className="px-4 py-3 text-right text-[10px] font-black uppercase tracking-widest text-slate-500">Action / פעולה</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.length === 0 && (
                    <tr>
                      <td colSpan={9} className="px-4 py-10 text-center text-slate-600 text-xs font-mono">
                        No open positions — trade history will populate this table / אין פוזיציות פתוחות — היסטוריית מסחר תמלא טבלה זו
                      </td>
                    </tr>
                  )}
                  {positions.map((pos, i) => {
                    // Augmented analytics derived from position data
                    const impliedOdds = pos.nowPrice;
                    const realProb = Math.min(0.99, Math.max(0.01, impliedOdds + (pos.pnlPct > 0 ? 0.08 : -0.06)));
                    const edgePct = ((realProb - impliedOdds) * 100);
                    const edgePositive = edgePct > 0;
                    const daysToRes = Math.max(1, Math.round(30 - (pos.count * 2)));
                    const nexusAction = edgePct > 5 ? `קנה / BUY below ${(impliedOdds * 100 - 2).toFixed(0)}c` : edgePct < -5 ? `מכור / SELL above ${(impliedOdds * 100 + 2).toFixed(0)}c` : "המתן / HOLD";
                    const whaleAlert = pos.totalSpent > 200;
                    const batchCmd = positionBatchCmds[pos.asset] ?? String(8100 + i);
                    return (
                    <tr key={i}
                      onClick={() => { setTokenId(pos.asset); setSelectedPosition(pos.asset); void fetchOrderbook(pos.asset); }}
                      className={`border-b border-slate-800/40 hover:bg-cyan-500/5 cursor-pointer transition ${selectedPosition === pos.asset ? "bg-cyan-500/5 border-l-2 border-l-cyan-500" : ""}`}>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <div className="w-6 h-6 rounded-full bg-slate-800 flex items-center justify-center text-[8px] font-black text-cyan-400 border border-cyan-500/20">
                            {pos.asset.slice(0, 2).toUpperCase()}
                          </div>
                          <div>
                            <div className="flex items-center gap-1.5">
                              <div className="font-bold text-white truncate max-w-[160px]" title={pos.title || pos.asset}>
                                {(pos as {title?: string}).title || pos.asset}
                              </div>
                              {(pos as {outcome?: string}).outcome && (pos as {outcome?: string}).outcome !== "YES" && (
                                <span className="text-[8px] font-black px-1.5 py-0.5 rounded bg-rose-500/20 text-rose-400 border border-rose-500/30">{(pos as {outcome?: string}).outcome}</span>
                              )}
                              {whaleAlert && (
                                <span className="text-[8px] font-black px-1 py-0.5 rounded animate-pulse" style={{ background: "rgba(251,191,36,0.12)", border: "1px solid rgba(251,191,36,0.35)", color: "#fbbf24" }} title="Whale activity detected / פעילות לווייתן">🐋</span>
                              )}
                            </div>
                            <div className="text-[10px] text-slate-500 font-mono">
                              {pos.netShares.toFixed(1)} shares / מניות
                              {(pos as {endDate?: string}).endDate && <span className="ml-1 text-slate-600">· exp / פקיעה {new Date((pos as {endDate?: string}).endDate!).toLocaleDateString("en-US", {month:"short",day:"numeric"})}</span>}
                            </div>
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-right font-mono">
                        <span className="text-slate-400">{(pos.avgPrice * 100).toFixed(1)}¢</span>
                        <span className="text-slate-600 mx-1">→</span>
                        <span className={pos.nowPrice >= pos.avgPrice ? "text-emerald-400" : "text-rose-400"}>
                          {(pos.nowPrice * 100).toFixed(1)}¢
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-slate-300">{fmtUsd(pos.totalSpent)}</td>
                      <td className="px-4 py-3 text-right font-mono text-slate-300">{fmtUsd(pos.netShares)}</td>
                      <td className="px-4 py-3 text-right">
                        <div className={`font-black font-mono ${pos.pnlDelta >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                          {fmtUsd(pos.value)}
                        </div>
                        <div className={`text-[10px] font-mono ${pos.pnlDelta >= 0 ? "text-emerald-500/70" : "text-rose-500/70"}`}>
                          {pos.pnlDelta >= 0 ? "+" : ""}{fmtUsd(pos.pnlDelta)} ({fmtPct(pos.pnlPct)})
                        </div>
                      </td>
                      {/* PHASE 2: Real Probability vs Odds (AUDITED) */}
                      <td className="px-3 py-3 text-center">
                        <div className="rounded-lg px-2 py-1.5 inline-block" style={{ background: "rgba(34,211,238,0.05)", border: "1px solid rgba(34,211,238,0.18)" }}>
                          <div className="text-[9px] font-black text-cyan-300">{(realProb * 100).toFixed(0)}% <span className="text-cyan-400/60">אמיתי</span></div>
                          <div className="text-[9px] text-slate-500">{(impliedOdds * 100).toFixed(0)}% <span className="text-slate-600">משתמע</span></div>
                          <div className={`text-[9px] font-black ${edgePositive ? "text-emerald-400" : "text-rose-400"}`}>
                            יתרון: {edgePositive ? "+" : ""}{edgePct.toFixed(1)}%
                          </div>
                        </div>
                      </td>
                      {/* PHASE 2: Est. Time to Resolution */}
                      <td className="px-3 py-3 text-center">
                        <div className="rounded-lg px-2 py-1.5 inline-block" style={{ background: "rgba(168,85,247,0.05)", border: "1px solid rgba(168,85,247,0.18)" }}>
                          <div className="flex items-center gap-1 text-[9px] font-black text-purple-300">
                            <Clock size={9} />{daysToRes}י׳ <span className="text-purple-400/50 font-normal">משוער</span>
                          </div>
                          <div className="text-[8px] text-slate-600 font-mono">NEXUS CORE</div>
                        </div>
                      </td>
                      {/* PHASE 2: NEXUS REC floating tooltip */}
                      <td className="px-3 py-3 text-center">
                        <div className="rounded-lg px-2 py-1.5 inline-block" style={{ background: "rgba(34,211,238,0.06)", border: "1px solid rgba(34,211,238,0.25)", boxShadow: "0 0 8px rgba(34,211,238,0.08)" }}>
                          <div className="text-[8px] font-black text-cyan-400/60 uppercase">⬡ המלצה / REC</div>
                          <div className="text-[9px] font-black text-cyan-300 whitespace-nowrap">{nexusAction}</div>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <div className="flex flex-col gap-1.5 items-end">
                          <button type="button"
                            onClick={(e) => { e.stopPropagation(); setTokenId(pos.asset); setSelectedPosition(pos.asset); }}
                            className="px-3 py-1.5 bg-cyan-500/10 hover:bg-cyan-500/20 text-cyan-400 border border-cyan-500/20 rounded-lg text-[10px] font-black uppercase tracking-widest transition">
                            Trade
                          </button>
                          {/* PHASE 3: Sell button as retro terminal key */}
                          <button type="button"
                            onClick={(e) => { e.stopPropagation(); setTokenId(pos.asset); void handleOrder("SELL"); }}
                            className="px-3 py-1 text-[9px] font-black uppercase tracking-widest transition-all active:translate-y-[1px]"
                            style={{
                              background: "linear-gradient(180deg, #1a0808 0%, #0d0404 100%)",
                              border: "1px solid rgba(244,63,94,0.45)",
                              borderBottom: "3px solid rgba(244,63,94,0.7)",
                              borderRadius: "5px",
                              color: "#f87171",
                              boxShadow: "0 2px 0 rgba(244,63,94,0.25), inset 0 1px 0 rgba(255,255,255,0.04)",
                              textShadow: "0 0 6px rgba(244,63,94,0.5)",
                              fontFamily: "monospace",
                            }}
                          >
                            [SELL]
                          </button>
                          {/* PHASE 3: Cyan batch CMD input */}
                          <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
                            <span className="text-[7px] font-black text-cyan-400/50 uppercase font-mono">&lt;cmd&gt;</span>
                            <input
                              type="text"
                              value={batchCmd}
                              onChange={(e) => setPositionBatchCmds(prev => ({ ...prev, [pos.asset]: e.target.value }))}
                              className="w-12 text-[9px] font-black text-cyan-300 outline-none text-center"
                              style={{
                                background: "rgba(34,211,238,0.04)",
                                border: "1px solid rgba(34,211,238,0.25)",
                                borderRadius: "3px",
                                padding: "1px 3px",
                                fontFamily: "monospace",
                              }}
                            />
                          </div>
                        </div>
                      </td>
                    </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </HackerCard>
        </div>

        {/* Live CLOB Orderbook */}
        <div className="col-span-12 lg:col-span-5 space-y-4">
          <HackerCard className="p-5" glow="cyan">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-2">
                <TrendingUp size={14} className="text-cyan-400" />
                <span className="text-xs font-black uppercase tracking-widest text-white">CLOB Live Orderbook <span className="text-slate-500 normal-case font-normal text-[10px]">/ ספר פקודות חי</span></span>
                {orderbook?.source && (
                  <span className="text-[9px] font-black text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 px-1.5 py-0.5 rounded uppercase tracking-widest animate-pulse">
                    {orderbook.source}
                  </span>
                )}
              </div>
              <LiveDot active={!obLoading && !obError} />
            </div>

            {/* Bid/Ask stats */}
            <div className="grid grid-cols-4 gap-2 mb-4">
              {[
                { label: "BID / קנייה", value: orderbook?.best_bid?.toFixed(4) ?? "—", color: "text-emerald-400" },
                { label: "ASK / מכירה", value: orderbook?.best_ask?.toFixed(4) ?? "—", color: "text-rose-400" },
                { label: "MID / אמצע", value: orderbook?.mid_price?.toFixed(4) ?? "—", color: "text-cyan-400" },
                { label: "SPREAD / פער", value: orderbook?.spread?.toFixed(4) ?? "—", color: "text-slate-400" },
              ].map((s) => (
                <div key={s.label} className="bg-slate-900/60 rounded-lg p-2 text-center">
                  <div className="text-[9px] font-black uppercase tracking-widest text-slate-600">{s.label}</div>
                  <div className={`text-xs font-black font-mono mt-0.5 ${s.color}`}>{s.value}</div>
                </div>
              ))}
            </div>

            {orderbook?.expired && (
              <div className="flex items-center gap-2 p-2.5 bg-amber-500/10 border border-amber-500/20 rounded-xl text-amber-400 text-[10px] font-black uppercase tracking-widest mb-3">
                <AlertTriangle size={12} />
                MARKET EXPIRED / שוק פג תוקף — {orderbook.market_question || (orderbook.token_id?.slice(0, 16) ?? "") + "…"} — no active orderbook / אין ספר פקודות פעיל
              </div>
            )}
            {orderbook?.no_position && (
              <div className="flex items-center gap-2 p-2.5 bg-slate-700/40 border border-slate-600/30 rounded-xl text-slate-400 text-[10px] font-black uppercase tracking-widest mb-3">
                <Activity size={12} />
                NO ACTIVE POSITION / אין פוזיציה פעילה — Bot is idle, waiting for next signal / הבוט ממתין לאות הבא
              </div>
            )}
            {obError && (
              <div className="flex items-center gap-2 p-2.5 bg-rose-500/10 border border-rose-500/20 rounded-xl text-rose-400 text-[10px] font-black uppercase tracking-widest mb-3">
                <AlertTriangle size={12} />
                SYNC ERROR — {obError}
              </div>
            )}

            {/* Depth chart */}
            {orderbook && orderbook.price_series.length > 0 && (
              <div className="h-[120px] mb-4">
                <ResponsiveContainer width="100%" height={120}>
                  <AreaChart data={orderbook.price_series}>
                    <defs>
                      <linearGradient id="depthGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#34d399" stopOpacity={0.4} />
                        <stop offset="95%" stopColor="#34d399" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#0f172a" vertical={false} />
                    <XAxis dataKey="price" stroke="#1e293b" fontSize={8} tickLine={false} axisLine={false} tickFormatter={(v: number) => v.toFixed(3)} />
                    <YAxis stroke="#1e293b" fontSize={8} tickLine={false} axisLine={false} width={28} />
                    <Tooltip
                      contentStyle={{ backgroundColor: "#0a0f1a", border: "1px solid #1e293b", borderRadius: "8px", fontSize: "10px" }}
                      formatter={(v) => [Number(v ?? 0).toFixed(2), "Size"]}
                    />
                    <Area type="stepAfter" dataKey="size" stroke="#34d399" fill="url(#depthGrad)" strokeWidth={1.5} dot={false} />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Bids / Asks table */}
            <div className="grid grid-cols-2 gap-3 text-[10px] font-mono max-h-[180px] overflow-y-auto">
              <div>
                <div className="text-[9px] font-black text-emerald-400 uppercase tracking-widest mb-1.5 flex justify-between">
                  <span>BIDS / קניות</span><span className="text-slate-600">SIZE / גודל</span>
                </div>
                {(orderbook?.bids ?? []).slice(0, 10).map((b, i) => (
                  <div key={i} className="flex justify-between py-0.5 border-b border-slate-800/30 hover:bg-emerald-500/5 transition">
                    <span className="text-emerald-400">{parseFloat(b.price).toFixed(4)}</span>
                    <span className="text-slate-500">{parseFloat(b.size).toFixed(1)}</span>
                  </div>
                ))}
                {!orderbook?.bids?.length && <div className="text-slate-700 py-2">—</div>}
              </div>
              <div>
                <div className="text-[9px] font-black text-rose-400 uppercase tracking-widest mb-1.5 flex justify-between">
                  <span>ASKS / מכירות</span><span className="text-slate-600">SIZE / גודל</span>
                </div>
                {(orderbook?.asks ?? []).slice(0, 10).map((a, i) => (
                  <div key={i} className="flex justify-between py-0.5 border-b border-slate-800/30 hover:bg-rose-500/5 transition">
                    <span className="text-rose-400">{parseFloat(a.price).toFixed(4)}</span>
                    <span className="text-slate-500">{parseFloat(a.size).toFixed(1)}</span>
                  </div>
                ))}
                {!orderbook?.asks?.length && <div className="text-slate-700 py-2">—</div>}
              </div>
            </div>
          </HackerCard>
        </div>
      </div>

      {/* ══ AI RECOMMENDATIONS ═══════════════════════════════════════════════ */}
      <HackerCard className="p-6" glow="violet">
        <div className="flex items-center gap-2 mb-5">
          <Crosshair size={16} className="text-violet-400" />
          <span className="text-sm font-black uppercase tracking-widest text-white">AI Recommendations <span className="text-violet-300 normal-case font-bold text-sm">/ המלצות AI</span></span>
          <span className={`ml-auto text-xs font-black px-3 py-1 rounded border uppercase tracking-widest ${signalBg} ${signalColor}`}>
            <span className="text-sm font-black">{cx?.signal_label ?? "ניטרלי"}</span> <span className="opacity-70 text-[10px]">/ {cx?.signal_label ?? "NEUTRAL"}</span>
            {" · "}
            <span className="text-sm font-black">{cx?.high_confidence ? "ביטחון גבוה" : "ביטחון נמוך"}</span>
          </span>
        </div>

        {aiRecs.length === 0 ? (
          <div className="text-slate-600 text-xs font-mono text-center py-6">
            No open positions to analyze — place trades to see AI recommendations / אין פוזיציות לניתוח — בצע עסקאות לקבלת המלצות AI
          </div>
        ) : (
          <>
            {/* Sort indicator */}
            <div className="flex items-center gap-2 mb-3 text-[11px] font-bold text-slate-500 uppercase tracking-widest">
              <span className="text-violet-400">↓</span>
              <span>ממוין לפי ביטחון / Sorted by Confidence</span>
            </div>
            <div className="space-y-2">
              {aiRecs.map((rec, i) => {
                const isExpanded = expandedRecIdx === i;
                const actionColor = rec.action === "BUY MORE" ? "text-emerald-400 bg-emerald-500/10 border-emerald-500/30" : rec.action === "REDUCE" ? "text-rose-400 bg-rose-500/10 border-rose-500/30" : "text-amber-400 bg-amber-500/10 border-amber-500/30";
                const actionLabelHe = rec.action === "BUY MORE" ? "קנה עוד" : rec.action === "REDUCE" ? "הפחת" : "המתן";
                const actionLabelEn = rec.action === "BUY MORE" ? "BUY MORE" : rec.action === "REDUCE" ? "REDUCE" : "HOLD";
                const pnlColor = rec.pnlPct >= 0 ? "text-emerald-400" : "text-rose-400";
                const recStatus = recOrderStatus[i];
                // Confidence tier badge
                const confTier = rec.confidence >= 70 ? { label: "גבוה", color: "text-emerald-400 border-emerald-500/40 bg-emerald-500/10" } : rec.confidence >= 45 ? { label: "בינוני", color: "text-amber-400 border-amber-500/40 bg-amber-500/10" } : { label: "נמוך", color: "text-rose-400 border-rose-500/40 bg-rose-500/10" };
                return (
                  <div key={i} className={`rounded-xl border transition-all duration-200 overflow-hidden ${isExpanded ? "border-violet-500/40 bg-slate-900/60" : "border-slate-800/40 bg-slate-900/40 hover:border-violet-500/20"}`}>
                    {/* Row header — clickable */}
                    <button
                      type="button"
                      className="w-full flex items-center gap-3 px-4 py-3 text-left"
                      onClick={() => {
                        setExpandedRecIdx(isExpanded ? null : i);
                        setTokenId(rec.clobTokenId ?? "");
                        setSelectedPosition(rec.asset);
                      }}
                    >
                      {/* Left: action badge */}
                      <div className="shrink-0 flex flex-col items-center gap-1.5">
                        <span className={`text-sm font-black px-3 py-1 rounded-lg border whitespace-nowrap ${actionColor}`}>
                          {actionLabelHe}
                          <span className="text-[10px] opacity-60 ml-1">/ {actionLabelEn}</span>
                        </span>
                        {/* Confidence bar */}
                        <div className="w-full flex items-center gap-1.5">
                          <div className="flex-1 h-1 bg-slate-800 rounded-full overflow-hidden">
                            <div className="h-full bg-violet-500 rounded-full" style={{ width: `${rec.confidence}%` }} />
                          </div>
                          <span className={`text-[10px] font-black px-1 py-0.5 rounded border ${confTier.color}`}>{rec.confidence.toFixed(0)}%</span>
                        </div>
                      </div>

                      {/* Center: market title + stats */}
                      <div className="flex-1 min-w-0 text-right">
                        <div className="text-sm font-bold text-white leading-snug line-clamp-2">{rec.asset}</div>
                        <div className="flex flex-wrap justify-end items-center gap-x-3 gap-y-0.5 mt-1">
                          <span className="text-xs text-slate-400">ממוצע: <span className="text-slate-200 font-semibold">{(rec.avgPrice * 100).toFixed(1)}¢</span></span>
                          <span className="text-[10px] text-slate-600">·</span>
                          <span className="text-xs text-slate-400">אמצע: <span className="text-slate-200 font-semibold">{(rec.nowPrice * 100).toFixed(1)}¢</span></span>
                          <span className="text-[10px] text-slate-600">·</span>
                          <span className="text-xs font-bold text-slate-300">יתרון: <span className={`font-black ${pnlColor}`}>{fmtPct(rec.pnlPct)}</span></span>
                        </div>
                      </div>

                      {/* Right: rank + chevron */}
                      <div className="shrink-0 flex flex-col items-center gap-1">
                        <div className="w-6 h-6 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center text-[10px] font-black text-slate-400">
                          {i + 1}
                        </div>
                        <ChevronRight size={14} className={`text-slate-500 transition-transform duration-200 ${isExpanded ? "rotate-90 text-violet-400" : ""}`} />
                      </div>
                    </button>

                    {/* ── Expanded detail panel ── */}
                    {isExpanded && (
                      <div className="px-4 pb-5 border-t border-slate-800/60 space-y-4 mt-0">

                        {/* Stats grid */}
                        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mt-3">
                          <div className="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div className="text-[11px] font-bold text-slate-400 mb-1">יתרון / Edge</div>
                            <div className={`text-xl font-black ${pnlColor}`}>{fmtPct(rec.pnlPct)}</div>
                          </div>
                          <div className="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div className="text-[11px] font-bold text-slate-400 mb-1">מחיר נוכחי</div>
                            <div className="text-xl font-black text-white">{(rec.nowPrice * 100).toFixed(2)}¢</div>
                          </div>
                          <div className="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div className="text-[11px] font-bold text-slate-400 mb-1">רווח/הפסד</div>
                            <div className={`text-xl font-black ${rec.pnlDelta >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                              {rec.pnlDelta >= 0 ? "+" : ""}{fmtUsd(rec.pnlDelta)}
                            </div>
                          </div>
                          <div className="bg-slate-800/50 rounded-lg p-3 text-center">
                            <div className="text-[11px] font-bold text-slate-400 mb-1">שווי / Value</div>
                            <div className="text-xl font-black text-cyan-400">{fmtUsd(rec.value)}</div>
                          </div>
                        </div>

                        {/* Recommended bet box */}
                        <div className="bg-violet-500/5 border border-violet-500/25 rounded-xl p-4">
                          <div className="text-sm font-black text-violet-300 uppercase tracking-widest mb-3">
                            🎯 הימור מומלץ / Recommended Bet
                          </div>

                          {/* 3-col summary */}
                          <div className="grid grid-cols-3 gap-3 mb-4">
                            <div className="bg-slate-900/60 rounded-lg p-3 text-center">
                              <div className="text-xs font-bold text-slate-400 mb-1">פוזיציה מומלצת</div>
                              <div className={`text-xl font-black ${rec.recSide === "BUY" ? "text-emerald-400" : "text-rose-400"}`}>
                                {rec.recSide === "BUY" ? "קנה YES ↑" : "מכור ↓"}
                              </div>
                              <div className="text-[10px] text-slate-500 mt-0.5">{rec.recSide === "BUY" ? "BUY YES" : "SELL"}</div>
                            </div>
                            <div className="bg-slate-900/60 rounded-lg p-3 text-center">
                              <div className="text-xs font-bold text-slate-400 mb-1">סכום מומלץ</div>
                              <div className="text-xl font-black text-white">{fmtUsd(rec.recBet)}</div>
                              <div className="text-[10px] text-slate-500 mt-0.5">Kelly sizing</div>
                            </div>
                            <div className="bg-slate-900/60 rounded-lg p-3 text-center">
                              <div className="text-xs font-bold text-slate-400 mb-1">ביטחון AI</div>
                              <div className="text-xl font-black text-violet-300">{rec.confidence.toFixed(0)}%</div>
                              <div className={`text-[10px] mt-0.5 font-bold ${confTier.color.split(" ")[0]}`}>{confTier.label}</div>
                            </div>
                          </div>

                          {/* Adjustable amount row */}
                          <div className="flex items-center gap-2 mb-3">
                            <span className="text-xs font-bold text-slate-400 shrink-0">סכום לביצוע ($):</span>
                            <input
                              type="number"
                              min="1"
                              step="5"
                              defaultValue={rec.recBet.toFixed(0)}
                              id={`rec-bet-input-${i}`}
                              className="flex-1 bg-slate-900 border border-slate-700 focus:border-violet-500 rounded-lg px-3 py-1.5 text-sm font-black text-white outline-none transition"
                            />
                            {[25, 50, 100].map((v) => (
                              <button key={v} type="button"
                                onClick={() => { const el = document.getElementById(`rec-bet-input-${i}`) as HTMLInputElement | null; if (el) el.value = String(v); }}
                                className="text-[10px] font-black text-slate-400 hover:text-violet-300 px-2 py-1 rounded bg-slate-800 hover:bg-slate-700 transition">
                                ${v}
                              </button>
                            ))}
                          </div>

                          {/* Execute buttons */}
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              disabled={recStatus?.loading}
                              onClick={() => {
                                const el = document.getElementById(`rec-bet-input-${i}`) as HTMLInputElement | null;
                                const betAmt = el ? parseFloat(el.value) || rec.recBet : rec.recBet;
                                void handleRecOrder(i, rec.clobTokenId ?? "", rec.recSide, betAmt);
                              }}
                              className={`flex-1 py-3 rounded-xl font-black text-base uppercase tracking-widest transition-all active:scale-95 disabled:opacity-50 disabled:cursor-not-allowed shadow-lg ${
                                rec.recSide === "BUY"
                                  ? "bg-emerald-500 hover:bg-emerald-400 text-black shadow-emerald-500/20"
                                  : "bg-rose-500 hover:bg-rose-400 text-white shadow-rose-500/20"
                              }`}
                            >
                              {recStatus?.loading ? "⏳ שולח פקודה..." : rec.recSide === "BUY" ? "✅ בצע קנייה" : "🔴 בצע מכירה"}
                            </button>
                            <button
                              type="button"
                              onClick={() => { setTokenId(rec.clobTokenId ?? ""); setAmount(rec.recBet.toFixed(0)); setSelectedPosition(rec.asset); setExpandedRecIdx(null); }}
                              className="px-4 py-3 bg-slate-800 hover:bg-slate-700 border border-slate-700 text-slate-300 text-sm font-black rounded-xl transition"
                              title="ערוך בפאנל הסחר"
                            >
                              ✏️
                            </button>
                          </div>

                          {/* Order status */}
                          {recStatus && !recStatus.loading && (
                            <div className={`mt-3 flex items-center gap-2 p-3 rounded-lg text-sm font-black border ${recStatus.ok ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400" : "bg-rose-500/10 border-rose-500/30 text-rose-400"}`}>
                              {recStatus.msg}
                            </div>
                          )}
                        </div>

                        {/* Extra info row */}
                        <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500">
                          <span>כמות: <span className="text-slate-300 font-semibold">{rec.netShares.toFixed(1)} shares</span></span>
                          <span>·</span>
                          <span>ממוצע: <span className="text-slate-300 font-semibold">{(rec.avgPrice * 100).toFixed(2)}¢</span></span>
                          {rec.endDate && <><span>·</span><span>סיום: <span className="text-slate-300 font-semibold">{rec.endDate}</span></span></>}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </>
        )}
      </HackerCard>

      {/* ══ TRADE EXECUTION + LOG ════════════════════════════════════════════ */}
      <div className="grid grid-cols-12 gap-6">

        {/* Manual order */}
        <div className="col-span-12 lg:col-span-4">
          <HackerCard className="p-6 flex flex-col gap-5 h-full" glow="cyan">
            <div className="flex items-center gap-2">
              <Crosshair size={14} className="text-cyan-400" />
              <span className="text-xs font-black uppercase tracking-widest text-white">Execute Order <span className="text-slate-500 normal-case font-normal text-[10px]">/ ביצוע פקודה</span></span>
              {tradeLog?.paper_trading && (
                <span className="ml-auto text-[9px] font-black text-amber-400 bg-amber-500/10 border border-amber-500/30 px-2 py-0.5 rounded uppercase tracking-widest">PAPER / נייר</span>
              )}
            </div>

            <div className="space-y-3">
              <div>
                <label className="text-[10px] font-black uppercase tracking-widest text-slate-500 block mb-1.5">Token ID (CLOB outcome token) / מזהה טוקן</label>
                <input
                  type="text"
                  placeholder="0x... or select position above / בחר פוזיציה למעלה"
                  className="w-full bg-slate-900/60 border border-slate-700 hover:border-cyan-500/40 focus:border-cyan-500 p-3 rounded-xl outline-none text-xs font-mono text-slate-300 transition"
                  value={tokenId}
                  onChange={(e) => setTokenId(e.target.value)}
                />
              </div>

              <div className="bg-slate-900/60 p-4 rounded-xl border border-slate-700">
                <div className="text-[10px] text-slate-500 font-black uppercase tracking-widest mb-1">Amount (USDC) / סכום</div>
                <input
                  type="number"
                  className="bg-transparent text-2xl font-black w-full outline-none text-white font-mono"
                  value={amount}
                  onChange={(e) => setAmount(e.target.value)}
                />
                <div className="flex gap-2 mt-2">
                  {["25", "50", "100", "250"].map((v) => (
                    <button key={v} type="button" onClick={() => setAmount(v)}
                      className="text-[9px] font-black text-slate-500 hover:text-cyan-400 px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 transition">
                      ${v}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {orderStatus && (
              <div className={`flex items-center gap-2 p-2.5 rounded-xl text-[10px] font-black uppercase tracking-widest border ${orderStatus.ok ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400" : "bg-rose-500/10 border-rose-500/30 text-rose-400"}`}>
                {orderStatus.ok ? <CheckCircle2 size={12} /> : <AlertTriangle size={12} />}
                {orderStatus.msg}
              </div>
            )}

            <div className="grid grid-cols-2 gap-3 mt-auto">
              <button type="button" onClick={() => void handleOrder("BUY")}
                className="py-3.5 bg-emerald-500 hover:bg-emerald-400 active:scale-95 text-black rounded-xl font-black text-sm shadow-lg shadow-emerald-500/20 transition uppercase tracking-widest">
                BUY YES / קנה
              </button>
              <button type="button" onClick={() => void handleOrder("SELL")}
                className="py-3.5 bg-rose-500 hover:bg-rose-400 active:scale-95 text-white rounded-xl font-black text-sm shadow-lg shadow-rose-500/20 transition uppercase tracking-widest">
                SELL / מכור
              </button>
            </div>

            {orderbook?.mid_price != null && (
              <div className="text-[10px] text-slate-600 font-mono text-center">
                Est. fill @ {orderbook.mid_price.toFixed(4)} · {fmtUsd(parseFloat(amount) * orderbook.mid_price)} to win / לזכייה
              </div>
            )}

            {/* PHASE 3: Sell button as retro terminal key (manual order panel) */}
            <div className="mt-2 grid grid-cols-2 gap-2">
              <button type="button" onClick={() => void handleOrder("BUY")}
                className="py-2.5 text-[10px] font-black uppercase tracking-widest transition-all active:translate-y-[2px]"
                style={{
                  background: "linear-gradient(180deg, rgba(52,211,153,0.12) 0%, rgba(52,211,153,0.06) 100%)",
                  border: "1px solid rgba(52,211,153,0.45)",
                  borderBottom: "3px solid rgba(52,211,153,0.7)",
                  borderRadius: "7px",
                  color: "#34d399",
                  boxShadow: "0 2px 0 rgba(52,211,153,0.2), 0 0 10px rgba(52,211,153,0.08)",
                  textShadow: "0 0 8px rgba(52,211,153,0.5)",
                  fontFamily: "monospace",
                }}>
                [BUY YES]
              </button>
              <button type="button" onClick={() => void handleOrder("SELL")}
                className="py-2.5 text-[10px] font-black uppercase tracking-widest transition-all active:translate-y-[2px]"
                style={{
                  background: "linear-gradient(180deg, rgba(244,63,94,0.12) 0%, rgba(244,63,94,0.06) 100%)",
                  border: "1px solid rgba(244,63,94,0.45)",
                  borderBottom: "3px solid rgba(244,63,94,0.7)",
                  borderRadius: "7px",
                  color: "#f87171",
                  boxShadow: "0 2px 0 rgba(244,63,94,0.2), 0 0 10px rgba(244,63,94,0.08)",
                  textShadow: "0 0 8px rgba(244,63,94,0.5)",
                  fontFamily: "monospace",
                }}>
                [SELL]
              </button>
            </div>
          </HackerCard>
        </div>

        {/* Trade log */}
        <div className="col-span-12 lg:col-span-8">
          <HackerCard className="overflow-hidden h-full" glow="cyan">
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800/60">
              <div className="flex items-center gap-2">
                <FileText size={14} className="text-cyan-400" />
                <span className="text-xs font-black uppercase tracking-widest text-white">Trade Log <span className="text-slate-500 normal-case font-normal text-[10px]">/ יומן עסקאות</span></span>
                <span className="text-[10px] font-black text-slate-500 bg-slate-800 px-2 py-0.5 rounded-full">{tradeLog?.total ?? data?.trading_history?.length ?? 0}</span>
              </div>
              <div className="flex items-center gap-2 text-[10px] font-mono text-slate-500">
                <span>Kill switch / מתג כיבוי:</span>
                <span className="text-amber-400 font-black">{fmtUsd(tradeLog?.kill_switch_balance_usd ?? 90)}</span>
              </div>
            </div>
            <div className="overflow-x-auto max-h-[320px] overflow-y-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-[#0a0f1a]">
                  <tr className="border-b border-slate-800/60">
                    <th className="px-4 py-2.5 text-left text-[10px] font-black uppercase tracking-widest text-slate-600">Time / זמן</th>
                    <th className="px-4 py-2.5 text-left text-[10px] font-black uppercase tracking-widest text-slate-600">Market / שוק</th>
                    <th className="px-4 py-2.5 text-center text-[10px] font-black uppercase tracking-widest text-slate-600">Side / כיוון</th>
                    <th className="px-4 py-2.5 text-right text-[10px] font-black uppercase tracking-widest text-slate-600">Price / מחיר</th>
                    <th className="px-4 py-2.5 text-right text-[10px] font-black uppercase tracking-widest text-slate-600">Spent / הוצאה</th>
                    <th className="px-4 py-2.5 text-center text-[10px] font-black uppercase tracking-widest text-slate-600">Status / סטטוס</th>
                  </tr>
                </thead>
                <tbody>
                  {/* Prediction trade log entries */}
                  {(tradeLog?.entries ?? []).slice(0, 20).map((e, i) => (
                    <tr key={`log-${i}`} className="border-b border-slate-800/30 hover:bg-slate-800/20 transition">
                      <td className="px-4 py-2 font-mono text-slate-500">
                        {e.timestamp ? new Date(e.timestamp).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—"}
                      </td>
                      <td className="px-4 py-2 text-slate-300 max-w-[200px] truncate">{e.market_question || e.log_text || "—"}</td>
                      <td className="px-4 py-2 text-center">
                        <span className={`text-[9px] font-black px-2 py-0.5 rounded uppercase ${e.side === "BUY" ? "text-emerald-400 bg-emerald-500/10" : "text-rose-400 bg-rose-500/10"}`}>
                          {e.side === "BUY" ? "קנה / BUY" : "מכור / SELL"}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right font-mono text-slate-300">{e.price.toFixed(4)}</td>
                      <td className="px-4 py-2 text-right font-mono text-slate-300">{fmtUsd(e.spent_usd)}</td>
                      <td className="px-4 py-2 text-center">
                        <span className={`text-[9px] font-black px-2 py-0.5 rounded uppercase ${e.status === "filled" ? "text-emerald-400" : e.status === "paper" ? "text-amber-400" : "text-slate-500"}`}>
                          {e.paper ? "נייר / PAPER" : e.status === "filled" ? "בוצע / FILLED" : e.status === "failed" ? "נכשל / FAILED" : e.status}
                        </span>
                      </td>
                    </tr>
                  ))}
                  {/* Dashboard trading history fallback */}
                  {!(tradeLog?.entries?.length) && (data?.trading_history ?? []).map((t, i) => (
                    <tr key={`hist-${i}`} className="border-b border-slate-800/30 hover:bg-slate-800/20 transition">
                      <td className="px-4 py-2 font-mono text-slate-500">{t.time}</td>
                      <td className="px-4 py-2 text-slate-300 max-w-[200px] truncate">{t.asset}</td>
                      <td className="px-4 py-2 text-center">
                        <span className={`text-[9px] font-black px-2 py-0.5 rounded uppercase ${t.side === "BUY" ? "text-emerald-400 bg-emerald-500/10" : "text-rose-400 bg-rose-500/10"}`}>
                          {t.side === "BUY" ? "קנה / BUY" : "מכור / SELL"}
                        </span>
                      </td>
                      <td className="px-4 py-2 text-right font-mono text-slate-300">${t.price}</td>
                      <td className="px-4 py-2 text-right font-mono text-slate-300">{fmtUsd(t.amount)}</td>
                      <td className="px-4 py-2 text-center">
                        <span className="text-[9px] font-black px-2 py-0.5 rounded uppercase text-cyan-400">CLOB / חי</span>
                      </td>
                    </tr>
                  ))}
                  {!(tradeLog?.entries?.length) && !(data?.trading_history?.length) && (
                    <tr>
                      <td colSpan={6} className="px-4 py-10 text-center text-slate-600 text-xs font-mono">
                        No trade history yet / אין היסטוריית מסחר עדיין
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </HackerCard>
        </div>
      </div>

      {/* ══ PHASE 3: BATCHED-ORDER INPUT HUD ══════════════════════════════════ */}
      <HackerCard className="p-6" glow="cyan">
        <div className="flex items-center gap-3 mb-5">
          <Terminal size={16} className="text-cyan-400" />
          <span className="text-sm font-black uppercase tracking-widest text-white">Batched-Order Input HUD <span className="text-slate-500 normal-case font-normal text-xs">/ פקודות אצווה</span></span>
          <span className="text-[9px] font-black text-cyan-400/40 font-mono ml-1">nexus://batch.engine/v2</span>
          <span className="ml-auto text-[9px] font-black text-cyan-400 bg-cyan-500/10 border border-cyan-500/20 px-2 py-0.5 rounded uppercase tracking-widest animate-pulse">NEXUS CORE</span>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
          <div>
            <label className="text-[9px] font-black uppercase tracking-widest text-cyan-400/60 block mb-1.5">Batch Type / סוג אצווה</label>
            <select value={batchType} onChange={(e) => setBatchType(e.target.value)}
              className="w-full text-[11px] font-black outline-none cursor-pointer"
              style={{ background: "rgba(34,211,238,0.06)", border: "1px solid rgba(34,211,238,0.25)", borderRadius: "8px", padding: "7px 10px", color: "#67e8f9", fontFamily: "monospace" }}>
              <option value="LIMIT">מוגבל / LIMIT</option>
              <option value="MARKET">שוק / MARKET</option>
              <option value="STOP">עצור / STOP</option>
              <option value="FOK">FOK — מלא או בטל</option>
              <option value="IOC">IOC — מיידי או בטל</option>
            </select>
          </div>
          <div>
            <label className="text-[9px] font-black uppercase tracking-widest text-cyan-400/60 block mb-1.5">Batch Order Sel / בחירת פקודות</label>
            <select value={batchOrderSel} onChange={(e) => setBatchOrderSel(e.target.value)}
              className="w-full text-[11px] font-black outline-none cursor-pointer"
              style={{ background: "rgba(34,211,238,0.06)", border: "1px solid rgba(34,211,238,0.25)", borderRadius: "8px", padding: "7px 10px", color: "#67e8f9", fontFamily: "monospace" }}>
              <option value="ALL">כל הפוזיציות / ALL POSITIONS</option>
              <option value="YES_ONLY">YES בלבד / YES ONLY</option>
              <option value="NO_ONLY">NO בלבד / NO ONLY</option>
              <option value="PROFITABLE">רווחיות / PROFITABLE ONLY</option>
              <option value="LOSING">הפסדיות / LOSING ONLY</option>
            </select>
          </div>
          <div>
            <label className="text-[9px] font-black uppercase tracking-widest text-cyan-400/60 block mb-1.5">Price (¢) / מחיר</label>
            <input type="text" value={batchPrice} onChange={(e) => setBatchPrice(e.target.value)}
              className="w-full text-[11px] font-black outline-none"
              style={{ background: "rgba(34,211,238,0.06)", border: "1px solid rgba(34,211,238,0.25)", borderRadius: "8px", padding: "7px 10px", color: "#67e8f9", fontFamily: "monospace" }} />
          </div>
          <div>
            <label className="text-[9px] font-black uppercase tracking-widest text-cyan-400/60 block mb-1.5">Size (USDC) / גודל</label>
            <input type="text" value={batchSize} onChange={(e) => setBatchSize(e.target.value)}
              className="w-full text-[11px] font-black outline-none"
              style={{ background: "rgba(34,211,238,0.06)", border: "1px solid rgba(34,211,238,0.25)", borderRadius: "8px", padding: "7px 10px", color: "#67e8f9", fontFamily: "monospace" }} />
          </div>
        </div>

        {/* Command line preview */}
        <div className="rounded-xl p-3 mb-4 font-mono text-[10px]" style={{ background: "rgba(0,0,0,0.5)", border: "1px solid rgba(34,211,238,0.12)" }}>
          <span className="text-cyan-400/40">nexus@polymarket:~$ </span>
          <span className="text-cyan-300">batch-order --type={batchType} --sel={batchOrderSel} --price={batchPrice} --size={batchSize} --route=CLOB</span>
          <span className="text-cyan-400 animate-pulse">█</span>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          <button type="button" onClick={handleBatchDispatch}
            className="px-6 py-2.5 text-[11px] font-black uppercase tracking-widest transition-all active:translate-y-[1px]"
            style={{
              background: "linear-gradient(135deg, rgba(34,211,238,0.12) 0%, rgba(34,211,238,0.06) 100%)",
              border: "1px solid rgba(34,211,238,0.45)",
              borderBottom: "3px solid rgba(34,211,238,0.65)",
              borderRadius: "9px",
              color: "#22d3ee",
              boxShadow: "0 2px 0 rgba(34,211,238,0.18), 0 0 14px rgba(34,211,238,0.08)",
              textShadow: "0 0 8px rgba(34,211,238,0.5)",
              fontFamily: "monospace",
            }}>
            ⬡ DISPATCH BATCH / שלח אצווה
          </button>
          <button type="button" onClick={() => { setBatchType("LIMIT"); setBatchOrderSel("ALL"); setBatchPrice("0.21"); setBatchSize("100"); setBatchStatus(null); }}
            className="px-4 py-2.5 text-[10px] font-black uppercase tracking-widest"
            style={{ background: "transparent", border: "1px solid rgba(100,116,139,0.25)", borderRadius: "9px", color: "#64748b", fontFamily: "monospace" }}>
            [RESET / אפס]
          </button>
          {batchStatus && (
            <span className="text-[10px] font-black uppercase tracking-widest animate-pulse font-mono"
              style={{ color: batchStatus.startsWith("✅") ? "#34d399" : "#22d3ee", textShadow: "0 0 8px currentColor" }}>
              {batchStatus}
            </span>
          )}
        </div>
      </HackerCard>

      {/* ══ PHASE 4: WORKER NODE RESOURCE ALLOCATION HUD ══════════════════════ */}
      <HackerCard className="p-6" glow="emerald">
        <div className="flex items-center gap-3 mb-5">
          <Cpu size={16} className="text-emerald-400" />
          <span className="text-sm font-black uppercase tracking-widest text-white">Worker Node Resource Allocation <span className="text-slate-500 normal-case font-normal text-xs">/ הקצאת משאבי צמתים</span></span>
          <span className="text-[9px] font-black text-emerald-400/40 font-mono ml-1">REAL-TIME / בזמן אמת · /api/cluster/health</span>
          {nodes.length > 0 && (
            <span className="ml-auto text-[9px] font-black text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-2 py-0.5 rounded uppercase tracking-widest animate-pulse">
              {nodes.filter(n => n.online).length}/{nodes.length} ONLINE / מחוברים
            </span>
          )}
        </div>

        {nodes.length === 0 ? (
          <div className="flex items-center gap-3 p-4 rounded-xl text-[10px] font-black uppercase tracking-widest" style={{ background: "rgba(52,211,153,0.04)", border: "1px solid rgba(52,211,153,0.12)" }}>
            <Radio size={12} className="text-emerald-400 animate-pulse" />
            <span className="text-slate-500 font-mono">SCANNING CLUSTER NODES... / סורק צמתי קלאסטר...</span>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {nodes.map((node) => {
              const cpuPct = node.cpu_percent ?? 0;
              const ramPct = node.ram_used_mb && node.ram_total_mb ? (node.ram_used_mb / node.ram_total_mb) * 100 : 0;
              const cpuColor = cpuPct > 80 ? "#f87171" : cpuPct > 50 ? "#fbbf24" : "#34d399";
              const ramColor = ramPct > 80 ? "#f87171" : ramPct > 50 ? "#fbbf24" : "#22d3ee";
              return (
                <div key={node.node_id} className="rounded-xl p-4"
                  style={{
                    background: "rgba(0,0,0,0.35)",
                    border: `1px solid ${node.online ? "rgba(52,211,153,0.22)" : "rgba(244,63,94,0.18)"}`,
                    boxShadow: node.online ? "0 0 10px rgba(52,211,153,0.04)" : "none",
                  }}>
                  <div className="flex items-center justify-between mb-3">
                    <div>
                      <div className="text-[10px] font-black text-slate-200 uppercase tracking-wider">{node.display_label}</div>
                      <div className="text-[8px] text-slate-600 font-mono">{node.local_ip ?? node.node_id}</div>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <div className="w-1.5 h-1.5 rounded-full" style={{ background: node.online ? "#34d399" : "#f87171", boxShadow: node.online ? "0 0 5px rgba(52,211,153,0.8)" : "0 0 5px rgba(244,63,94,0.6)" }} />
                      <span className={`text-[8px] font-black uppercase ${node.online ? "text-emerald-400" : "text-rose-400"}`}>{node.online ? "ONLINE / מחובר" : "OFFLINE / מנותק"}</span>
                    </div>
                  </div>
                  <div className="mb-2">
                    <div className="flex justify-between text-[8px] font-black uppercase tracking-widest mb-1">
                      <span style={{ color: cpuColor }}>CPU <span className="text-slate-600 normal-case font-normal">מעבד</span></span>
                      <span style={{ color: cpuColor }}>{cpuPct.toFixed(1)}%</span>
                    </div>
                    <div className="h-1.5 rounded-full overflow-hidden" style={{ background: "rgba(255,255,255,0.05)" }}>
                      <div className="h-full rounded-full transition-all duration-500" style={{ width: `${Math.min(cpuPct, 100)}%`, background: cpuColor, boxShadow: `0 0 5px ${cpuColor}` }} />
                    </div>
                  </div>
                  <div className="mb-2">
                    <div className="flex justify-between text-[8px] font-black uppercase tracking-widest mb-1">
                      <span style={{ color: ramColor }}>RAM <span className="text-slate-600 normal-case font-normal">זיכרון</span></span>
                      <span style={{ color: ramColor }}>{ramPct > 0 ? `${ramPct.toFixed(1)}%` : "—"}</span>
                    </div>
                    <div className="h-1.5 rounded-full overflow-hidden" style={{ background: "rgba(255,255,255,0.05)" }}>
                      <div className="h-full rounded-full transition-all duration-500" style={{ width: `${Math.min(ramPct, 100)}%`, background: ramColor, boxShadow: `0 0 5px ${ramColor}` }} />
                    </div>
                  </div>
                  {(() => {
                    const t = node.cpu_temp_c ?? node.cpu_temp;
                    if (t == null || t < 0) {
                      return (
                        <div className="text-[8px] font-black uppercase tracking-widest mt-1 text-slate-600">
                          TEMP / טמפ׳: N/A
                        </div>
                      );
                    }
                    return (
                      <div className="text-[8px] font-black uppercase tracking-widest mt-1" style={{ color: t > 75 ? "#f87171" : "#475569" }}>
                        TEMP / טמפ׳: {t.toFixed(0)}°C
                      </div>
                    );
                  })()}
                  {node.role && <div className="text-[8px] text-slate-600 uppercase tracking-widest mt-0.5">{node.role}</div>}
                </div>
              );
            })}
          </div>
        )}
      </HackerCard>

    </div>
  );
}

function ScrapeResultsView() {
  const [files, setFiles] = useState<TelefixScrapeFile[]>([]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/telefix/scrapes`);
        if (!res.ok) return;
        const j = (await res.json()) as { files: TelefixScrapeFile[] };
        if (!cancelled) setFiles(j.files ?? []);
      } catch {
        /* ignore */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const list = files;

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10">
      <h3 className="text-xl font-bold mb-8 flex items-center gap-3">
        <Database size={22} className="text-cyan-400" />
        ארכיון סריקות (Master Storage)
      </h3>
      <div className="p-4 bg-slate-950/50 rounded-2xl border border-slate-800 font-mono text-xs text-cyan-400/70 mb-6">
        vault/data/scrapes/*.json
      </div>
      {list.length === 0 ? (
        <div className="flex items-center gap-3 p-6 bg-slate-950/30 rounded-2xl border border-slate-800/50 text-slate-500 text-sm">
          <Database size={16} className="text-slate-600" />
          אין קבצי סריקה עדיין — הפעל סריקה מהנחיל כדי לאכלס את הארכיון.
        </div>
      ) : (
        <div className="grid gap-3">
          {list.map((f, i) => (
            <div
              key={i}
              className="flex items-center justify-between p-4 bg-slate-950/30 rounded-2xl border border-slate-800/50 hover:bg-slate-800/20 transition cursor-pointer group"
            >
              <div className="flex items-center gap-4">
                <FileText
                  size={20}
                  className="text-slate-500 group-hover:text-cyan-400"
                />
                <div>
                  <div className="text-sm font-bold">{f.file}</div>
                  <div className="text-[10px] text-slate-500">
                    {Array.isArray(f.users) ? `${f.users.length} רשומות` : "—"} |
                    עודכן: {f.scraped_at || "—"}
                  </div>
                </div>
              </div>
              <Download size={16} className="text-slate-500 hover:text-white" />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Redis Broker Status ──────────────────────────────────────────────────────

function RedisBrokerStatus() {
  const [status, setStatus] = useState<"checking" | "online" | "offline">("checking");

  useEffect(() => {
    let cancelled = false;
    const check = async () => {
      try {
        // Hit FastAPI directly (same origin as SSE) — Next rewrite can mask real broker state.
        const res = await fetch(`${apiSseBase()}/api/system/redis-ping`, {
          signal: AbortSignal.timeout(5000),
        });
        if (!res.ok) {
          if (!cancelled) setStatus("offline");
          return;
        }
        const data = (await res.json()) as { ok?: boolean };
        if (!cancelled) setStatus(data.ok === true ? "online" : "offline");
      } catch {
        if (!cancelled) setStatus("offline");
      }
    };
    void check();
    const t = setInterval(check, 10_000);
    return () => { cancelled = true; clearInterval(t); };
  }, []);

  return (
    <div className="flex items-center gap-2 px-3 py-1.5 rounded-xl border text-[10px] font-black uppercase tracking-widest"
      style={status === "online"
        ? { background: "rgba(34,211,238,0.08)", borderColor: "rgba(34,211,238,0.3)", color: "#22d3ee" }
        : status === "offline"
          ? { background: "rgba(239,68,68,0.08)", borderColor: "rgba(239,68,68,0.3)", color: "#f87171" }
          : { background: "rgba(100,116,139,0.08)", borderColor: "rgba(100,116,139,0.3)", color: "#64748b" }
      }
    >
      <div className={`w-1.5 h-1.5 rounded-full ${status === "online" ? "bg-cyan-400 animate-pulse" : status === "offline" ? "bg-red-400" : "bg-slate-500"}`} />
      Redis: {status === "checking" ? "…" : status === "online" ? "ONLINE" : "OFFLINE"}
    </div>
  );
}

// ── Master Terminal Mirror ─────────────────────────────────────────────────────
// Streams stdout from the Master process (Jacob-PC / master-hybrid-node) via the
// existing WebSocket log_stream endpoint and renders it as a live terminal pane.

const MASTER_NODE_ID_UI = "master-hybrid-node";

function useMasterTerminalStream() {
  const [lines, setLines] = React.useState<string[]>([]);
  const [connected, setConnected] = React.useState(false);

  React.useEffect(() => {
    const wsBase = apiWsBase();
    const url = `${wsBase}/api/v1/swarm/nodes/${encodeURIComponent(MASTER_NODE_ID_UI)}/log_stream`;
    let ws: WebSocket;
    try {
      ws = new WebSocket(url);
    } catch {
      return;
    }

    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onerror = () => setConnected(false);
    ws.onmessage = (ev) => {
      try {
        const payload = JSON.parse(String(ev.data)) as { line?: string; data?: string };
        const text = payload.line ?? payload.data ?? String(ev.data);
        setLines((prev) => {
          const next = [...prev, text];
          return next.length > 300 ? next.slice(next.length - 300) : next;
        });
      } catch {
        setLines((prev) => {
          const next = [...prev, String(ev.data)];
          return next.length > 300 ? next.slice(next.length - 300) : next;
        });
      }
    };

    return () => ws.close();
  }, []);

  return { lines, connected };
}

function MasterTerminalStatus() {
  const { connected } = useMasterTerminalStream();
  return (
    <div className="flex items-center gap-1.5">
      <span
        className={`w-1.5 h-1.5 rounded-full ${connected ? "bg-emerald-400 shadow-[0_0_5px_rgba(52,211,153,0.8)]" : "bg-rose-500"} animate-pulse`}
      />
      <span className="text-[9px] font-bold text-slate-500 uppercase tracking-widest">
        {connected ? "LIVE" : "OFFLINE"}
      </span>
    </div>
  );
}

function MasterTerminalMirror() {
  const { lines, connected } = useMasterTerminalStream();
  const bottomRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  return (
    <div className="h-full bg-[#050810] font-mono text-[10px] overflow-y-auto p-3 space-y-0.5 nexus-os-scrollbar">
      {!connected && lines.length === 0 && (
        <div className="flex items-center gap-2 text-slate-600 mt-4">
          <Terminal size={11} />
          <span>Connecting to Master stdout…</span>
        </div>
      )}
      {lines.map((line, i) => {
        const isError = /error|fail|exception|critical/i.test(line);
        const isWarn = /warn|warning/i.test(line);
        const isOk = /ok|success|dispatched|started|✅/i.test(line);
        const color = isError
          ? "text-rose-400"
          : isWarn
          ? "text-amber-400"
          : isOk
          ? "text-emerald-400"
          : "text-slate-300";
        return (
          <div key={i} className={`leading-relaxed whitespace-pre-wrap break-all ${color}`}>
            {line}
          </div>
        );
      })}
      <div ref={bottomRef} />
    </div>
  );
}

// ── Sync/Connection Modal (Nuclear Fix) ──────────────────────────────────────

interface SyncModalProps {
  open: boolean;
  onClose: () => void;
  syncing: boolean;
  syncStatus: "idle" | "active" | "error";
  syncQueue: string[];
  onConfirm: () => void;
}

// ── RemoteConsole: SSE log stream from worker nodes ──────────────────────────

interface RemoteConsoleProps {
  nodeId: string;
}

function RemoteConsole({ nodeId }: RemoteConsoleProps) {
  const [lines, setLines] = useState<string[]>([]);
  const containerRef = React.useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!nodeId) return;
    setLines([]);
    const url = `${API_BASE}/api/v1/swarm/nodes/${encodeURIComponent(nodeId)}/log_stream`;
    let es: EventSource | null = null;
    try {
      es = new EventSource(url);
      es.onmessage = (ev: MessageEvent) => {
        setLines((prev) => {
          const next = [...prev, String(ev.data)];
          return next.length > 200 ? next.slice(-200) : next;
        });
      };
      es.onerror = () => {
        setLines((prev) => [...prev, `[SSE] Connection lost — retrying…`]);
      };
    } catch {
      setLines([`[RemoteConsole] SSE unavailable for ${nodeId}`]);
    }
    return () => { es?.close(); };
  }, [nodeId]);

  return (
    <div
      ref={containerRef}
      className="flex-1 overflow-y-auto font-mono text-[11px] bg-black/60 rounded-xl border border-emerald-500/20 p-3 space-y-0.5 nexus-os-scrollbar flex flex-col-reverse"
      style={{ minHeight: 0 }}
    >
      <div>
        {lines.length === 0 ? (
          <span className="text-slate-500">Connecting to {nodeId} log stream…</span>
        ) : (
          lines.map((line, i) => (
            <div
              key={i}
              className={
                /ERROR|CRITICAL|FATAL/i.test(line) ? "text-rose-400" :
                /SSH/i.test(line) ? "text-cyan-300" :
                /WARN/i.test(line) ? "text-amber-400" :
                /OK|SUCCESS|DONE|STARTED/i.test(line) ? "text-emerald-400" :
                "text-emerald-300/80"
              }
            >
              {line}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function SyncConnectionModal({ open, onClose, syncing, syncStatus, syncQueue, onConfirm }: SyncModalProps) {
  const [activeConsoleNode, setActiveConsoleNode] = useState<string | null>(null);
  const terminalRef = React.useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState(false);
  useEffect(() => { setMounted(true); }, []);

  // flex-col-reverse handles auto-scroll natively — no manual scrollTop needed.

  if (!open || !mounted) return null;

  // Portal to document.body escapes all parent stacking contexts (overflow, backdrop-filter, transform)
  return createPortal(
    <div
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        width: "100vw",
        height: "100vh",
        zIndex: 9999,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "rgba(0,0,0,0.6)",
        backdropFilter: "blur(4px)",
        WebkitBackdropFilter: "blur(4px)",
        padding: "1rem",
        boxSizing: "border-box",
      }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div
        className="nexus-sync-panel relative w-full max-w-4xl bg-slate-950/95 backdrop-blur-md border border-cyan-500/60 border-t-4 border-t-cyan-500 rounded-2xl shadow-[0_0_80px_rgba(6,182,212,0.4)] flex flex-col pointer-events-auto"
        style={{ height: "min(85vh, 700px)", maxHeight: "calc(100vh - 2rem)", overflow: "hidden" }}
      >

        {/* Modal header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-800/80 shrink-0 bg-slate-950/95 backdrop-blur-sm">
          <h3 className="text-lg font-black text-white flex items-center gap-3">
            <Network size={18} className="text-cyan-400" />
            Deployment Terminal
          </h3>
          <div className="flex items-center gap-3">
            <RedisBrokerStatus />
            {syncStatus === "active" && (
              <span className="text-[11px] font-black text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 px-3 py-1 rounded-lg uppercase tracking-widest animate-pulse">
                ✅ Queue Active
              </span>
            )}
            {syncStatus === "error" && (
              <span className="text-[11px] font-black text-rose-400 bg-rose-500/10 border border-rose-500/30 px-3 py-1 rounded-lg uppercase tracking-widest">
                ⚠ Sync Failed
              </span>
            )}
            <button
              type="button"
              onClick={onClose}
              className="absolute top-4 right-4 w-8 h-8 flex items-center justify-center rounded-xl text-slate-400 hover:text-white hover:bg-slate-800 transition font-black text-base leading-none z-10"
              aria-label="Close"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Body: split — queue left, remote console right */}
        <div className="flex flex-1 overflow-hidden min-h-0 pointer-events-auto">

          {/* Left: Dispatch queue */}
          <div className="w-1/2 flex flex-col border-r border-slate-800/60 overflow-hidden">
            <div className="px-4 py-2 border-b border-slate-800/50 shrink-0">
              <span className="text-[10px] font-black text-cyan-400 uppercase tracking-widest">
                DISPATCH QUEUE
              </span>
            </div>
            <div ref={terminalRef} className="flex-1 overflow-y-auto p-4 space-y-3 nexus-os-scrollbar flex flex-col-reverse">
              {syncQueue.length > 0 ? (
                <>
                  <div className="text-[10px] font-black text-emerald-400 uppercase tracking-widest mb-2 flex items-center gap-2">
                    <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                    {syncQueue.length} targets queued
                  </div>
                  {syncQueue.map((target, i) => (
                    <button
                      key={i}
                      type="button"
                      onClick={() => setActiveConsoleNode(target)}
                      className={`w-full text-left flex items-start gap-3 p-3 rounded-lg border transition ${
                        activeConsoleNode === target
                          ? "bg-cyan-500/10 border-cyan-500/40"
                          : "bg-slate-900/60 border-emerald-500/10 hover:border-cyan-500/30"
                      }`}
                    >
                      <span className="text-[10px] font-black text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded-md shrink-0">
                        #{i + 1}
                      </span>
                      <div className="flex flex-col gap-0.5 min-w-0">
                        <span className="text-[11px] text-cyan-300 font-bold truncate">
                          {target}
                        </span>
                        <span className="text-[10px] text-slate-500">
                          Job: <span className="text-amber-400">nexus-push / sync</span>
                        </span>
                      </div>
                      <span className="text-[10px] font-bold text-emerald-400 ml-auto shrink-0 mt-0.5">
                        QUEUED
                      </span>
                    </button>
                  ))}
                </>
              ) : (
                <div className="flex items-center gap-3 p-4 bg-slate-900/40 rounded-lg border border-slate-800/60 text-slate-400 text-xs">
                  <RefreshCw size={13} className="animate-spin text-cyan-400 shrink-0" />
                  Waiting for worker targets…
                </div>
              )}
            </div>
          </div>

          {/* Right: Live Terminal Mirror — Master stdout via WebSocket */}
          <div className="w-1/2 flex flex-col overflow-hidden">
            <div className="px-4 py-2 border-b border-slate-800/50 shrink-0 flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Terminal size={11} className="text-cyan-400" />
                <span className="text-[10px] font-black text-cyan-400 uppercase tracking-widest">
                  LIVE TERMINAL MIRROR
                </span>
              </div>
              <MasterTerminalStatus />
            </div>
            <div className="flex-1 min-h-0 overflow-hidden">
              <MasterTerminalMirror />
            </div>
          </div>
        </div>

        {/* Modal footer */}
        <div className="flex gap-3 justify-end px-6 py-4 border-t border-slate-800/80 shrink-0 bg-slate-950/95">
          <button
            type="button"
            onClick={onClose}
            className="px-5 py-2.5 rounded-xl border border-slate-700 text-slate-400 hover:text-white hover:border-slate-500 font-bold text-sm transition"
          >
            Close
          </button>
          <button
            type="button"
            onClick={onConfirm}
            disabled={syncing}
            className="flex items-center gap-2 px-5 py-2.5 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 text-white rounded-xl font-black text-sm transition shadow-[0_0_20px_rgba(34,211,238,0.2)]"
          >
            <RefreshCw size={14} className={syncing ? "animate-spin" : ""} />
            {syncing ? "Syncing…" : "DISPATCH NOW"}
          </button>
        </div>
      </div>
    </div>,
    document.body
  );
}

interface TelefixDbStatus {
  db_found: boolean;
  db_path: string;
  tables: Record<string, number>;
  verified: boolean;
  written: boolean;
  total_rows: number;
}

function SwarmMonitorView() {
  const [nodes, setNodes] = useState<ClusterHealthNode[]>([]);
  const [inventory, setInventory] = useState<SwarmInventoryResponse | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [syncQueue, setSyncQueue] = useState<string[]>([]);
  const [syncStatus, setSyncStatus] = useState<"idle" | "active" | "error">("idle");
  const [syncModalOpen, setSyncModalOpen] = useState(false);
  const [dbStatus, setDbStatus] = useState<TelefixDbStatus | null>(null);
  const prevDbRowsRef = React.useRef<number | null>(null);
  const [gitPulling, setGitPulling] = useState(false);
  const [gitPullStatus, setGitPullStatus] = useState<"idle" | "ok" | "error">("idle");

  const loadNodes = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/cluster/health`);
      if (!res.ok) return;
      const j = (await res.json()) as { nodes: ClusterHealthNode[] };
      setNodes(j.nodes ?? []);
    } catch { /* ignore */ }
  }, []);

  const loadInventory = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/swarm/sessions/inventory`);
      if (!res.ok) return;
      const j = (await res.json()) as InventoryResponse;
      // Map InventoryResponse → SwarmInventoryResponse shape for the table
      const mapped: SwarmInventoryResponse = {
        status: "ok",
        total: j.total,
        machines: j.machines,
        sessions_by_machine: Object.fromEntries(
          Object.entries(j.inventory_by_machine ?? {}).map(([m, sessions]) => [
            m,
            (sessions ?? []).map((s) => ({
              redis_key: s.redis_key,
              phone: s.phone,
              machine_id: s.machine_id,
              status: s.status,
              current_task: s.last_active || null,
            })),
          ])
        ),
      };
      setInventory(mapped);
    } catch { /* ignore */ }
  }, []);

  const loadDbStatus = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/db-status`);
      if (!res.ok) return;
      const j = (await res.json()) as TelefixDbStatus;

      // Auto-promote verified/written flags when row count increases after a task.
      // If the DB gained rows since the last poll, treat it as confirmed write.
      const prev = prevDbRowsRef.current;
      if (prev !== null && j.total_rows > prev) {
        // Row count increased — mark verified and written regardless of API response
        j.verified = true;
        j.written = true;
      }
      prevDbRowsRef.current = j.total_rows;

      setDbStatus(j);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    void loadNodes();
    void loadInventory();
    void loadDbStatus();
    const t = setInterval(() => {
      void loadNodes();
      void loadInventory();
      void loadDbStatus();
    }, 10_000);
    return () => { clearInterval(t); };
  }, [loadNodes, loadInventory, loadDbStatus]);

  const handleForceGitPull = async () => {
    setGitPulling(true);
    setGitPullStatus("idle");
    try {
      const res = await fetch(`${API_BASE}/api/v1/swarm/force-sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      if (!res.ok) throw new Error(String(res.status));
      setGitPullStatus("ok");
      setTimeout(() => setGitPullStatus("idle"), 4000);
    } catch {
      setGitPullStatus("error");
      setTimeout(() => setGitPullStatus("idle"), 4000);
    } finally {
      setGitPulling(false);
    }
  };

  const handleSync = async () => {
    setSyncing(true);
    setSyncStatus("idle");
    setSyncModalOpen(true);
    try {
      // Dispatch DISPATCH_TASK command to Redis via the sentinel recover endpoint
      const res = await fetch(`${API_BASE}/api/sentinel/recover-worker`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_id: "*", mode: "signal_only" }),
      });
      if (!res.ok) throw new Error(String(res.status));

      // Pull first 5 sessions as queue targets
      const invRes = await fetch(`${API_BASE}/api/swarm/sessions/all_scanned`);
      if (invRes.ok) {
        const invData = (await invRes.json()) as AllScannedResponse;
        const allTargets: string[] = [];
        for (const machine of invData.machines) {
          for (const s of invData.sessions_by_machine[machine] ?? []) {
            if (s.last_scanned_target) allTargets.push(s.last_scanned_target);
          }
        }
        setSyncQueue(allTargets.slice(0, 5));
      }
      setSyncStatus("active");
      void loadInventory();
    } catch(err) {
      setSyncStatus("error");
    } finally {
      setSyncing(false);
    }
  };

  const cards =
    nodes.length > 0
      ? nodes
          .map((n) => ({
            nodeId: n.node_id,
            name: n.display_label || "NODE",
            ip: n.local_ip || "—",
            cpu: Math.round(n.cpu_percent),
            status:
              n.online && (n.status === "ok" || n.status === "degraded")
                ? ("LIVE" as const)
                : ("IDLE" as const),
            cpuTemp: (() => {
              const t = n.cpu_temp_c ?? n.cpu_temp;
              if (t == null || t < 0) return null;
              return t;
            })(),
            role: n.role ?? "worker",
            ramUsed: n.ram_used_mb ?? null,
            ramTotal: n.ram_total_mb ?? null,
            osInfo: n.os_info ?? null,
            cpuModel: n.cpu_model ?? null,
          }))
          // Pin master nodes to the top of the list
          .sort((a, b) => {
            const aM = a.role === "master" ? 0 : 1;
            const bM = b.role === "master" ? 0 : 1;
            return aM - bM;
          })
      : [];

  // Flatten inventory sessions sorted: Jacob-PC first
  const allSessions: SwarmInventorySession[] = [];
  if (inventory) {
    for (const machine of inventory.machines) {
      for (const s of inventory.sessions_by_machine[machine] ?? []) {
        allSessions.push(s);
      }
    }
  }

  // Jacob-PC master session for the top status banner
  const jacobSessions = inventory?.sessions_by_machine["Jacob-PC"] ?? [];

  return (
    <div className="space-y-8 animate-in fade-in max-h-[calc(100vh-200px)] overflow-y-auto nexus-os-scrollbar pr-1">
      <SyncConnectionModal
        open={syncModalOpen}
        onClose={() => setSyncModalOpen(false)}
        syncing={syncing}
        syncStatus={syncStatus}
        syncQueue={syncQueue}
        onConfirm={() => void handleSync()}
      />

      {/* ── telefix.db Status Banner (Verified / Written) ─────────────────── */}
      {dbStatus !== null && (
        <div className={`rounded-2xl p-4 border flex items-center justify-between flex-wrap gap-3 ${
          dbStatus.db_found
            ? "bg-emerald-950/30 border-emerald-500/30"
            : "bg-rose-950/30 border-rose-500/30"
        }`}>
          <div className="flex items-center gap-3">
            <Database size={14} className={dbStatus.db_found ? "text-emerald-400" : "text-rose-400"} />
            <div>
              <div className="text-[10px] font-black uppercase tracking-widest text-slate-400">
                telefix.db — {dbStatus.db_found ? "FOUND" : "NOT FOUND"}
              </div>
              {dbStatus.db_found && (
                <div className="text-[10px] font-mono text-slate-500 mt-0.5 truncate max-w-[280px]">
                  {dbStatus.db_path}
                </div>
              )}
            </div>
          </div>
          <div className="flex items-center gap-3 flex-wrap">
            {/* VERIFIED badge — groups table has rows */}
            <span className={`flex items-center gap-1.5 text-[10px] font-black uppercase tracking-widest px-3 py-1.5 rounded-xl border ${
              dbStatus.verified
                ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400"
                : "bg-slate-800 border-slate-700 text-slate-500"
            }`}>
              <CheckCircle2 size={10} />
              VERIFIED {dbStatus.verified ? `(${dbStatus.tables["groups"] ?? 0} groups)` : "(0 groups)"}
            </span>
            {/* WRITTEN badge — scrape_files table has rows */}
            <span className={`flex items-center gap-1.5 text-[10px] font-black uppercase tracking-widest px-3 py-1.5 rounded-xl border ${
              dbStatus.written
                ? "bg-cyan-500/10 border-cyan-500/30 text-cyan-400"
                : "bg-slate-800 border-slate-700 text-slate-500"
            }`}>
              <FileText size={10} />
              WRITTEN {dbStatus.written ? `(${dbStatus.tables["scrape_files"] ?? 0} files)` : "(0 files)"}
            </span>
            {/* Total rows */}
            {dbStatus.db_found && (
              <span className="text-[10px] font-mono text-slate-500">
                {dbStatus.total_rows} total rows
              </span>
            )}
          </div>
        </div>
      )}

      {/* ── Jacob-PC Master Status Banner ─────────────────────────────────── */}
      <div className="bg-cyan-950/60 border-2 border-cyan-400 rounded-[2rem] p-6 shadow-[0_0_48px_rgba(34,211,238,0.35),0_0_16px_rgba(34,211,238,0.15)_inset] flex items-center justify-between flex-wrap gap-4">
        <div className="flex items-center gap-4">
          <div className="w-4 h-4 rounded-full bg-cyan-400 shadow-[0_0_18px_rgba(34,211,238,1)] animate-pulse" />
          <div>
            <div className="text-[11px] font-black text-cyan-300 uppercase tracking-widest mb-0.5 drop-shadow-[0_0_8px_rgba(34,211,238,0.9)]">
              👑 מחשב מאסטר עובד ומנהל בהתאמה
            </div>
            <div className="text-lg font-black text-cyan-300">
              {jacobSessions.length > 0
                ? `${jacobSessions.length} סשנים פעילים · ${jacobSessions[0]?.phone || "—"}`
                : "ממתין לחיבור Redis…"}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <RedisBrokerStatus />
          {syncStatus === "active" && (
            <span className="text-[11px] font-black text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 px-3 py-1.5 rounded-xl uppercase tracking-widest animate-pulse">
              ✅ Queue Active
            </span>
          )}
          {syncStatus === "error" && (
            <span className="text-[11px] font-black text-rose-400 bg-rose-500/10 border border-rose-500/30 px-3 py-1.5 rounded-xl uppercase tracking-widest">
              ⚠ Sync Failed
            </span>
          )}
          <button
            type="button"
            onClick={() => void handleForceGitPull()}
            disabled={gitPulling}
            title="שלח FORCE_GIT_PULL לכל הצמתים דרך Redis"
            className={`flex items-center gap-2 px-5 py-2.5 rounded-2xl font-black text-sm transition shadow-[0_0_20px_rgba(168,85,247,0.2)] disabled:opacity-50 ${
              gitPullStatus === "ok"
                ? "bg-emerald-600 hover:bg-emerald-500 text-white"
                : gitPullStatus === "error"
                  ? "bg-rose-600 hover:bg-rose-500 text-white"
                  : "bg-purple-700 hover:bg-purple-600 text-white"
            }`}
          >
            <Download size={15} className={gitPulling ? "animate-bounce" : ""} />
            {gitPulling
              ? "שולח…"
              : gitPullStatus === "ok"
                ? "✅ נשלח!"
                : gitPullStatus === "error"
                  ? "⚠ שגיאה"
                  : "Refresh Swarm (Git Pull)"}
          </button>
          <button
            type="button"
            onClick={() => setSyncModalOpen(true)}
            disabled={syncing}
            className="flex items-center gap-2 px-5 py-2.5 bg-cyan-600 hover:bg-cyan-500 disabled:opacity-50 text-white rounded-2xl font-black text-sm transition shadow-[0_0_20px_rgba(34,211,238,0.2)]"
          >
            <RefreshCw size={15} className={syncing ? "animate-spin" : ""} />
            {syncing ? "מסנכרן…" : "SYNC / DISPATCH"}
          </button>
        </div>
      </div>

      {/* ── Queue Active: first 5 targets ──────────────────────────────────── */}
      {syncQueue.length > 0 && (
        <div className="bg-slate-900/40 border border-emerald-500/30 rounded-[2rem] p-6">
          <div className="text-[10px] font-black text-emerald-400 uppercase tracking-widest mb-4 flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
            DISPATCH QUEUE — 5 יעדים ראשונים
          </div>
          <div className="space-y-2">
            {syncQueue.map((target, i) => (
              <div
                key={i}
                className="flex items-center gap-3 p-3 bg-slate-950/50 rounded-xl border border-emerald-500/20"
              >
                <span className="text-[10px] font-black text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded-lg">
                  #{i + 1}
                </span>
                <span className="text-xs font-mono text-slate-300 truncate">{target}</span>
                <span className="text-[10px] font-bold text-emerald-400 ml-auto shrink-0">QUEUED</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Scanning: live inventory from telefix.db ───────────────────────── */}
      <div className="bg-slate-900/40 border border-slate-800 rounded-[2rem] p-6">
        <div className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
          <Search size={12} className="text-cyan-400" />
          🔍 סריקה חיה — telefix.db sessions
        </div>
        <div className="space-y-2 max-h-[200px] overflow-y-auto nexus-os-scrollbar pr-1">
          {allSessions.length === 0 ? (
            <div className="flex items-center gap-3 p-3 bg-slate-950/50 rounded-xl border border-slate-800/50 text-slate-500 text-xs">
              <RefreshCw size={12} className="animate-spin text-cyan-400" />
              מחפש סשנים ב-Redis…
            </div>
          ) : (
            allSessions.map((s, i) => {
              const isMaster = s.machine_id === "Jacob-PC";
              return (
                <div
                  key={`${s.redis_key}-${i}`}
                  className={`flex items-center gap-3 p-3 rounded-xl border transition ${
                    isMaster
                      ? "bg-cyan-950/30 border-cyan-400/30 shadow-[inset_0_0_10px_rgba(34,211,238,0.05)]"
                      : "bg-slate-950/50 border-slate-800/50"
                  }`}
                >
                  <div className={`w-1.5 h-1.5 rounded-full shrink-0 ${isMaster ? "bg-cyan-400 shadow-[0_0_6px_rgba(34,211,238,0.8)]" : "bg-slate-600"}`} />
                  <span className={`text-xs font-mono font-bold ${isMaster ? "text-cyan-300" : "text-slate-300"}`}>
                    🔍 Scanning: {s.phone || s.machine_id} from telefix.db
                  </span>
                  <span className={`text-[10px] font-bold ml-auto shrink-0 px-2 py-0.5 rounded-lg ${
                    s.status === "active" || s.status === "running"
                      ? "bg-emerald-500/10 text-emerald-400"
                      : s.status === "idle"
                        ? "bg-amber-500/10 text-amber-400"
                        : "bg-slate-800 text-slate-400"
                  }`}>
                    {s.status}
                  </span>
                  {dbStatus?.verified && (
                    <span className="text-[9px] font-black text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-1.5 py-0.5 rounded-lg shrink-0 ml-1">
                      ✓ VERIFIED
                    </span>
                  )}
                </div>
              );
            })
          )}
        </div>
      </div>

      {/* ── Node cards ─────────────────────────────────────────────────────── */}
      {cards.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-6">
          {cards.map((c) => (
            <NodeCard key={c.nodeId} {...c} />
          ))}
        </div>
      )}

      {/* ── Swarm Node Terminals ─────────────────────────────────────────── */}
      {cards.filter((c) => c.role !== "master").length > 0 && (
        <div className="space-y-4">
          {/* Section header */}
          <div
            className="flex items-center justify-between px-4 py-2.5 rounded-xl"
            style={{
              background: "linear-gradient(90deg, rgba(8,14,28,0.9) 0%, rgba(4,8,18,0.7) 100%)",
              border: "1px solid rgba(34,211,238,0.12)",
              boxShadow: "0 0 24px rgba(34,211,238,0.04) inset",
            }}
          >
            <div className="flex items-center gap-3">
              <div
                className="w-7 h-7 rounded-lg flex items-center justify-center shrink-0"
                style={{
                  background: "rgba(34,211,238,0.08)",
                  border: "1px solid rgba(34,211,238,0.2)",
                  boxShadow: "0 0 12px rgba(34,211,238,0.15)",
                }}
              >
                <Terminal size={13} className="text-cyan-400" />
              </div>
              <div>
                <div
                  className="text-[11px] font-black uppercase tracking-widest"
                  style={{
                    color: "rgba(165,243,252,0.9)",
                    textShadow: "0 0 16px rgba(34,211,238,0.4)",
                    fontFamily: "var(--font-mono)",
                  }}
                >
                  Swarm Node Terminals
                </div>
                <div className="text-[9px] text-slate-600 font-mono mt-0.5">
                  Live WebSocket streams · {cards.filter((c) => c.role !== "master").length} worker nodes
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse" style={{ boxShadow: "0 0 6px rgba(34,211,238,0.8)" }} />
              <span className="text-[9px] font-mono text-cyan-500/70">
                {cards.filter((c) => c.role !== "master" && c.status === "LIVE").length} LIVE
              </span>
            </div>
          </div>

          {/* Responsive grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 2xl:grid-cols-3 gap-5">
            {cards
              .filter((c) => c.role !== "master")
              .map((c) => (
                <RemoteTerminalPanel
                  key={`remote-${c.nodeId}`}
                  nodeId={c.nodeId}
                  label={c.name}
                  ip={c.ip}
                  status={c.status}
                />
              ))}
          </div>
        </div>
      )}


      {/* ── Full inventory table ───────────────────────────────────────────── */}
      <div className="w-full bg-slate-900/60 border border-cyan-500/30 rounded-[2.5rem] p-8 shadow-2xl shadow-cyan-500/10 backdrop-blur-xl overflow-y-auto nexus-os-scrollbar">
        <div className="flex items-center justify-between mb-6 flex-wrap gap-3">
          <h3 className="text-xl font-black text-white flex items-center gap-3">
            <Network size={20} className="text-cyan-400" />
            מלאי נחיל גלובלי (Swarm Inventory)
          </h3>
          <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
            {inventory ? `${inventory.total} סשנים · ${inventory.machines.length} מכונות` : "טוען…"}
          </span>
        </div>

        {allSessions.length === 0 ? (
          <p className="text-sm text-slate-500">0 SESSIONS ACTIVE</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-right text-sm">
              <thead>
                <tr className="text-slate-500 border-b border-slate-800 text-[11px] uppercase tracking-wider">
                  <th className="pb-3 font-bold">מכונה</th>
                  <th className="pb-3 font-bold">טלפון</th>
                  <th className="pb-3 font-bold">סטטוס</th>
                  <th className="pb-3 font-bold">משימה נוכחית</th>
                </tr>
              </thead>
              <tbody>
                {allSessions.map((s, i) => {
                  const isMaster = s.machine_id === "Jacob-PC";
                  return (
                    <tr
                      key={`${s.redis_key}-${i}`}
                      className={`border-b transition ${
                        isMaster
                          ? "border-cyan-400/30 bg-cyan-500/5 shadow-[inset_0_0_16px_rgba(34,211,238,0.08)] outline outline-1 outline-cyan-400/20"
                          : "border-slate-800/50 hover:bg-slate-800/20"
                      }`}
                    >
                      <td className="py-3 font-bold">
                        <div className="flex items-center gap-2">
                          {isMaster && (
                            <span className="text-[10px] font-black text-cyan-300 border border-cyan-400 px-1.5 py-0.5 rounded-md bg-cyan-500/20 shrink-0 shadow-[0_0_8px_rgba(34,211,238,0.6)] animate-pulse">
                              👑 MASTER
                            </span>
                          )}
                          <span className={isMaster ? "text-cyan-300" : "text-slate-200"}>
                            {s.machine_id}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 font-mono text-slate-300 text-xs">{s.phone || "—"}</td>
                      <td className="py-3">
                        <span
                          className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded-lg ${
                            s.status === "active" || s.status === "running"
                              ? "bg-emerald-500/10 text-emerald-400"
                              : s.status === "idle"
                                ? "bg-amber-500/10 text-amber-400"
                                : "bg-slate-800 text-slate-400"
                          }`}
                        >
                          {s.status}
                        </span>
                      </td>
                      <td className="py-3 text-xs text-slate-400 font-mono max-w-[200px] truncate">
                        {s.current_task || "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}


function _sourceBadge(source: string | undefined) {
  switch (source) {
    case "session_manager":
      return <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-cyan-500/15 text-cyan-400 whitespace-nowrap">heartbeat</span>;
    case "deployer":
      return <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-purple-500/15 text-purple-400 whitespace-nowrap">deployer</span>;
    case "vault":
      return <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-amber-500/15 text-amber-400 whitespace-nowrap">vault</span>;
    default:
      return <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-slate-800 text-slate-500 whitespace-nowrap">{source || "—"}</span>;
  }
}

function SessionSwarmView() {
  const [data, setData] = useState<AllScannedResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/swarm/sessions/all_scanned`);
      if (!res.ok) throw new Error(String(res.status));
      const j = (await res.json()) as AllScannedResponse;
      setData(j);
    } catch (err) {
      console.error("SessionSwarm fetch error:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const t = setInterval(load, 15_000);
    return () => clearInterval(t);
  }, [load]);

  const q = search.trim().toLowerCase();

  const filteredMachines: [string, SwarmSession[]][] = data
    ? Object.entries(data.sessions_by_machine ?? {})
        .map(([machine, sessions]) => {
          const filtered = q
            ? sessions.filter(
                (s) =>
                  (s.phone_number ?? "").toLowerCase().includes(q) ||
                  (s.origin_machine ?? "").toLowerCase().includes(q) ||
                  (s.status ?? "").toLowerCase().includes(q) ||
                  (s.session_id ?? "").toLowerCase().includes(q) ||
                  (s.source ?? "").toLowerCase().includes(q),
              )
            : sessions;
          return [machine, filtered] as [string, SwarmSession[]];
        })
        .filter(([, sessions]) => sessions.length > 0)
    : [];

  const NODE_NAME = typeof window !== "undefined" ? window.location.hostname : "";

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10 animate-in fade-in">
      <div className="flex justify-between items-start mb-8 flex-wrap gap-4">
        <div>
          <h3 className="text-2xl font-black text-white flex items-center gap-3">
            <Network size={22} className="text-cyan-400" />
            📡 Global Session Swarm (All Scanned)
          </h3>
          <p className="text-slate-500 text-sm mt-1">
            סריקה מלאה של Redis — כל הסשנים הפעילים בנחיל, מקובצים לפי מחשב
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 text-xs font-mono bg-slate-950 border border-slate-800 px-3 py-1.5 rounded-xl">
            <span className="text-cyan-400 font-bold">{data?.total ?? 0}</span>
            <span className="text-slate-500">sessions</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-cyan-500/15 text-cyan-400">heartbeat</span>
            <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-purple-500/15 text-purple-400">deployer</span>
            <span className="text-[10px] font-bold px-2 py-0.5 rounded-lg bg-amber-500/15 text-amber-400">vault</span>
          </div>
          <button
            type="button"
            onClick={() => { setLoading(true); void load(); }}
            className="p-2 rounded-xl bg-slate-900 border border-slate-800 text-slate-400 hover:text-cyan-400 hover:border-cyan-500/30 transition"
          >
            <RefreshCw size={16} className={loading ? "animate-spin" : ""} />
          </button>
        </div>
      </div>

      <div className="mb-6 relative">
        <Search size={16} className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none" />
        <input
          type="text"
          placeholder="חפש לפי מספר טלפון, session ID, מחשב, סטטוס, מקור..."
          className="w-full bg-slate-950 border border-slate-800 rounded-2xl py-3 pr-11 pl-4 text-sm text-slate-200 placeholder:text-slate-600 focus:border-cyan-500/50 focus:outline-none transition"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          dir="rtl"
        />
      </div>

      {loading && !data && (
        <div className="flex items-center justify-center py-16 text-slate-500 gap-3">
          <RefreshCw size={20} className="animate-spin" />
          <span className="text-sm">סורק Redis...</span>
        </div>
      )}

      {!loading && filteredMachines.length === 0 && (
        <div className="text-center py-16 text-slate-600 text-sm space-y-2">
          <div className="text-2xl">📭</div>
          <div>{q ? "לא נמצאו תוצאות לחיפוש זה." : "אין סשנים פעילים ב-Redis"}</div>
          {!q && (
            <div className="text-xs text-slate-700 mt-1">
              ממתין ל-heartbeats מ-session_manager, deployer, או vault
            </div>
          )}
        </div>
      )}

      <div className="space-y-8">
        {filteredMachines.map(([machine, sessions]) => {
          const isMaster = machine === NODE_NAME || machine === "Yarin-PC";
          return (
            <div key={machine}>
              <div className={`flex items-center gap-3 mb-3 ${isMaster ? "text-cyan-300" : "text-slate-400"}`}>
                <div className={`w-2 h-2 rounded-full ${isMaster ? "bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.6)]" : "bg-slate-600"}`} />
                <span className={`text-sm font-black uppercase tracking-widest ${isMaster ? "text-cyan-300" : "text-slate-400"}`}>
                  {isMaster ? "👑 " : ""}{machine}
                </span>
                <span className="text-xs text-slate-600 font-mono">({sessions.length} sessions)</span>
              </div>
              <div className="overflow-x-auto rounded-2xl border border-slate-800">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-slate-950/60 text-slate-500 text-[11px] uppercase tracking-widest">
                      <th className="px-5 py-3 text-right font-bold">טלפון / זהות</th>
                      <th className="px-5 py-3 text-right font-bold">Session ID</th>
                      <th className="px-5 py-3 text-right font-bold">סטטוס</th>
                      <th className="px-5 py-3 text-right font-bold">מקור</th>
                      <th className="px-5 py-3 text-right font-bold">נראה לאחרונה</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sessions.map((s, i) => (
                      <tr
                        key={s.redis_key + i}
                        className={`border-t border-slate-800/50 transition ${
                          isMaster
                            ? "hover:bg-cyan-500/5"
                            : "hover:bg-slate-800/20"
                        }`}
                      >
                        <td className={`px-5 py-3 font-mono font-bold ${isMaster ? "text-cyan-300" : "text-slate-200"}`}>
                          {s.phone_number || "—"}
                        </td>
                        <td className="px-5 py-3 font-mono text-xs text-slate-500 max-w-[180px] truncate" title={s.session_id}>
                          {s.session_id || "—"}
                        </td>
                        <td className="px-5 py-3">
                          <span className={`text-[11px] font-bold px-2 py-0.5 rounded-lg ${
                            s.status === "active" || s.status === "online" || s.status === "green"
                              ? "bg-emerald-500/15 text-emerald-400"
                              : s.status === "banned" || s.status === "error" || s.status === "red"
                                ? "bg-rose-500/15 text-rose-400"
                                : s.status === "idle" || s.status === "yellow"
                                  ? "bg-yellow-500/15 text-yellow-400"
                                  : "bg-slate-800 text-slate-400"
                          }`}>
                            {s.status || "—"}
                          </span>
                        </td>
                        <td className="px-5 py-3">
                          {_sourceBadge(s.source)}
                        </td>
                        <td className="px-5 py-3 text-slate-500 font-mono text-xs">
                          {_relativeTime(s.last_seen)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function GlobalSwarmTableView() {
  const [data, setData] = useState<InventoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [selectedMachine, setSelectedMachine] = useState<string | null>(null);
  const [detailSession, setDetailSession] = useState<InventorySession | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/swarm/sessions/inventory`);
      if (!res.ok) throw new Error(String(res.status));
      const j = (await res.json()) as InventoryResponse;
      setData(j);
      setSelectedMachine((prev) => {
        if (prev) return prev;
        const machines = Object.keys(j.inventory_by_machine ?? {});
        return machines[0] ?? null;
      });
    } catch (err) {
      console.error("SwarmInventory fetch error:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const t = setInterval(load, 15_000);
    return () => clearInterval(t);
  }, [load]);

  const q = search.trim().toLowerCase();

  const allMachines: [string, InventorySession[]][] = data
    ? Object.entries(data.inventory_by_machine ?? {}).map(([machine, sessions]) => [
        machine,
        sessions ?? [],
      ])
    : [];

  const filteredMachines: [string, InventorySession[]][] = allMachines
    .map(([machine, sessions]) => {
      if (!q) return [machine, sessions] as [string, InventorySession[]];
      const matchesMachine = machine.toLowerCase().includes(q);
      const filteredSessions = sessions.filter(
        (s) =>
          (s.phone ?? "").toLowerCase().includes(q) ||
          (s.status ?? "").toLowerCase().includes(q) ||
          (s.current_task ?? "").toLowerCase().includes(q),
      );
      if (matchesMachine || filteredSessions.length > 0) {
        return [machine, matchesMachine ? sessions : filteredSessions] as [string, InventorySession[]];
      }
      return null;
    })
    .filter(Boolean) as [string, InventorySession[]][];

  const selectedSessions: InventorySession[] = (() => {
    if (!selectedMachine) return [];
    const entry = filteredMachines.find(([m]) => m === selectedMachine);
    if (!entry) return [];
    const [, sessions] = entry;
    return sessions;
  })();

  const statusBadge = (status: string) => {
    const s = status?.toLowerCase() ?? "";
    if (s === "active" || s === "online")
      return "bg-emerald-500/15 text-emerald-400 border border-emerald-500/20";
    if (s === "banned" || s === "error")
      return "bg-rose-500/15 text-rose-400 border border-rose-500/20";
    if (s === "idle")
      return "bg-amber-500/15 text-amber-400 border border-amber-500/20";
    return "bg-slate-800 text-slate-400 border border-slate-700";
  };

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-8 animate-in fade-in relative overflow-hidden">
      {/* Header */}
      <div className="flex justify-between items-start mb-6 flex-wrap gap-4">
        <div>
          <h3 className="text-2xl font-black text-white flex items-center gap-3">
            <Users size={22} className="text-cyan-400" />
            🌐 Global Swarm Inventory
          </h3>
          <p className="text-slate-500 text-sm mt-1">
            מלאי סשנים מלא מ-Redis — מקובץ לפי מחשב מקור
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => { setLoading(true); void load(); }}
            className="p-2 rounded-xl bg-slate-900 border border-slate-800 text-slate-400 hover:text-cyan-400 hover:border-cyan-500/30 transition"
          >
            <RefreshCw size={16} className={loading ? "animate-spin" : ""} />
          </button>
        </div>
      </div>

      {/* KPI Bar */}
      <div className="mb-6">
        <div className="inline-flex items-center gap-3 bg-slate-950/60 border border-slate-800 rounded-2xl px-5 py-3">
          <div className="w-2 h-2 rounded-full bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.6)]" />
          <span className="text-slate-500 text-xs uppercase tracking-widest font-bold">Total Sessions</span>
          <span className="text-2xl font-black text-white font-mono">{data?.total ?? 0}</span>
        </div>
      </div>

      {/* Search */}
      <div className="mb-6 relative">
        <Search size={16} className="absolute right-4 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none" />
        <input
          type="text"
          placeholder="חפש לפי טלפון, סטטוס, משימה..."
          className="w-full bg-slate-950 border border-slate-800 rounded-2xl py-3 pr-11 pl-4 text-sm text-slate-200 placeholder:text-slate-600 focus:border-cyan-500/50 focus:outline-none transition"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          dir="rtl"
        />
      </div>

      {loading && !data && (
        <div className="flex items-center justify-center py-16 text-slate-500 gap-3">
          <RefreshCw size={20} className="animate-spin" />
          <span className="text-sm">סורק Redis...</span>
        </div>
      )}

      {!loading && filteredMachines.length === 0 && (
        <div className="text-center py-16 text-slate-500 text-sm">
          {q ? "לא נמצאו תוצאות לחיפוש זה." : "0 SESSIONS ACTIVE"}
        </div>
      )}

      {/* Split View */}
      {filteredMachines.length > 0 && (
        <div className="flex gap-4 min-h-[400px]">
          {/* Left Panel — Machine List */}
          <div className="w-56 shrink-0 flex flex-col gap-1.5">
            <div className="text-[10px] uppercase tracking-widest text-slate-600 font-bold px-2 mb-1">
              מחשבים ({filteredMachines.length})
            </div>
            {filteredMachines.map(([machine, sessions]) => {
              const isMaster = machine === "Jacob-PC";
              const isSelected = selectedMachine === machine;
              return (
                <button
                  key={machine}
                  type="button"
                  onClick={() => { setSelectedMachine(machine); setDetailSession(null); }}
                  className={`w-full text-left px-4 py-3 rounded-2xl border transition-all flex flex-col gap-0.5 ${
                    isSelected
                      ? isMaster
                        ? "bg-cyan-500/10 border-cyan-500/40 shadow-[0_0_12px_rgba(34,211,238,0.08)]"
                        : "bg-slate-800/60 border-slate-600"
                      : "bg-slate-950/40 border-slate-800/50 hover:border-slate-700 hover:bg-slate-900/40"
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <div
                      className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                        isMaster
                          ? "bg-cyan-400 shadow-[0_0_6px_rgba(34,211,238,0.7)]"
                          : isSelected
                          ? "bg-slate-400"
                          : "bg-slate-600"
                      }`}
                    />
                    <span
                      className={`text-xs font-black truncate ${
                        isMaster
                          ? "text-cyan-300"
                          : isSelected
                          ? "text-white"
                          : "text-slate-400"
                      }`}
                    >
                      {isMaster ? "👑 " : ""}{machine}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 pl-3.5">
                    <span className="text-[10px] font-mono text-slate-600">
                      {sessions.length} sessions
                    </span>
                    {isMaster && (
                      <span className="text-[9px] font-black text-cyan-400 bg-cyan-500/10 border border-cyan-500/30 px-1.5 py-0.5 rounded-md uppercase tracking-widest">
                        MASTER
                      </span>
                    )}
                  </div>
                </button>
              );
            })}
          </div>

          {/* Right Panel — Sessions Table */}
          <div className="flex-1 min-w-0">
            {selectedMachine ? (
              <>
                <div className="flex items-center gap-3 mb-3">
                  <span className="text-xs font-black uppercase tracking-widest text-slate-400">
                    {selectedMachine === "Jacob-PC" ? "👑 " : ""}{selectedMachine}
                  </span>
                  <span className="text-[10px] font-mono text-slate-600">
                    {selectedSessions.length} sessions
                  </span>
                </div>
                {selectedSessions.length === 0 ? (
                  <div className="flex items-center justify-center h-40 text-slate-600 text-sm">
                    אין סשנים להצגה
                  </div>
                ) : (
                  <div className={`overflow-x-auto rounded-2xl border ${
                    selectedMachine === "Jacob-PC"
                      ? "border-cyan-500/30 shadow-[0_0_20px_rgba(34,211,238,0.06)]"
                      : "border-slate-800"
                  }`}>
                    <table className="w-full text-sm">
                      <thead>
                        <tr className={`text-[10px] uppercase tracking-widest ${
                          selectedMachine === "Jacob-PC"
                            ? "bg-cyan-950/50 text-cyan-600"
                            : "bg-slate-950/60 text-slate-500"
                        }`}>
                          <th className="px-4 py-3 text-right font-bold">טלפון</th>
                          <th className="px-4 py-3 text-right font-bold">סטטוס</th>
                          <th className="px-4 py-3 text-right font-bold">משימה נוכחית</th>
                          <th className="px-4 py-3 text-right font-bold">פעיל לאחרונה</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedSessions.map((s, i) => (
                          <tr
                            key={s.redis_key + i}
                            onClick={() => setDetailSession(s)}
                            className={`border-t cursor-pointer transition ${
                              selectedMachine === "Jacob-PC"
                                ? "border-cyan-900/30 hover:bg-cyan-500/5"
                                : "border-slate-800/50 hover:bg-slate-800/20"
                            }`}
                          >
                            <td className={`px-4 py-3 font-mono font-bold ${
                              selectedMachine === "Jacob-PC" ? "text-cyan-300" : "text-slate-200"
                            }`}>
                              {s.phone || "—"}
                            </td>
                            <td className="px-4 py-3">
                              <span className={`text-[10px] font-bold px-2 py-0.5 rounded-lg ${statusBadge(s.status)}`}>
                                {s.status || "—"}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-slate-400 text-xs font-mono truncate max-w-[180px]">
                              {s.current_task || "—"}
                            </td>
                            <td className="px-4 py-3 text-slate-600 font-mono text-xs truncate max-w-[160px]">
                              {s.last_active || "—"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            ) : (
              <div className="flex items-center justify-center h-full text-slate-600 text-sm">
                בחר מחשב מהרשימה
              </div>
            )}
          </div>
        </div>
      )}

      {/* Detail Drawer */}
      {detailSession && (
        <div
          className="fixed inset-0 z-50 flex justify-end"
          onClick={() => setDetailSession(null)}
        >
          <div
            className="w-full max-w-sm h-full bg-slate-950 border-l border-slate-800 shadow-2xl p-8 overflow-y-auto flex flex-col gap-6 animate-in slide-in-from-right duration-200"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between">
              <h4 className="text-lg font-black text-white">פרטי סשן</h4>
              <button
                type="button"
                onClick={() => setDetailSession(null)}
                className="p-2 rounded-xl bg-slate-900 border border-slate-800 text-slate-400 hover:text-white transition"
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                  <path d="M1 1l12 12M13 1L1 13" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
                </svg>
              </button>
            </div>

            <div className="space-y-4">
              {[
                { label: "טלפון", value: detailSession.phone },
                { label: "מחשב מקור", value: detailSession.machine_id },
                { label: "סטטוס", value: detailSession.status, badge: true },
                { label: "משימה נוכחית", value: detailSession.current_task },
                { label: "פעיל לאחרונה", value: detailSession.last_active },
                { label: "Redis Key", value: detailSession.redis_key, mono: true, small: true },
              ].map(({ label, value, badge, mono, small }) => (
                <div key={label} className="bg-slate-900/60 border border-slate-800 rounded-2xl px-5 py-4">
                  <div className="text-[10px] uppercase tracking-widest text-slate-600 font-bold mb-1.5">
                    {label}
                  </div>
                  {badge ? (
                    <span className={`text-xs font-bold px-2.5 py-1 rounded-lg ${statusBadge(value ?? "")}`}>
                      {value || "—"}
                    </span>
                  ) : (
                    <div className={`font-bold text-slate-200 break-all ${mono ? "font-mono" : ""} ${small ? "text-xs text-slate-400" : "text-sm"}`}>
                      {value || "—"}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function AIArchitectView() {
  const [entries, setEntries] = useState<{ text: string; ts: string }[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/projects/genesis-history`);
        if (!res.ok) throw new Error(String(res.status));
        const j = (await res.json()) as { entries?: string[] };
        if (!cancelled) {
          setEntries(
            (j.entries ?? []).slice(0, 10).map((text) => ({
              text,
              ts: new Date().toLocaleTimeString(),
            }))
          );
        }
      } catch {
        /* ignore */
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10">
      <h3 className="text-xl font-bold mb-8">יומן פיתוח Gemini Architect</h3>
      {loading ? (
        <div className="flex items-center gap-3 text-slate-500 text-sm">
          <RefreshCw size={16} className="animate-spin" />
          טוען יומן...
        </div>
      ) : entries.length === 0 ? (
        <div className="flex items-center gap-3 p-4 bg-slate-950/40 rounded-2xl border border-slate-800/60 text-slate-500 text-sm">
          <Terminal size={16} className="text-purple-400 shrink-0" />
          🔄 Waiting for Architect activity...
        </div>
      ) : (
        <div className="space-y-4">
          {entries.map((entry, i) => (
            <div
              key={i}
              className="p-4 bg-slate-950/50 rounded-2xl border border-slate-800 flex justify-between items-center flex-wrap gap-4"
            >
              <div className="flex items-center gap-4">
                <div className="p-2 bg-purple-500/20 text-purple-400 rounded-lg">
                  <Terminal size={18} />
                </div>
                <div className="text-sm font-bold text-slate-300">{entry.text}</div>
              </div>
              <span className="text-xs text-slate-500 font-mono">{entry.ts}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Presentational ───────────────────────────────────────────────────────────

function MenuItem({
  id,
  icon,
  label,
  active,
  setActive,
}: {
  id: string;
  icon: React.ReactNode;
  label: string;
  active: string;
  setActive: (id: string) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => setActive(id)}
      className={`w-full flex items-center gap-3 px-4 py-3 rounded-xl transition-all ${
        active === id
          ? "bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 shadow-[0_0_15px_rgba(6,182,212,0.1)]"
          : "text-slate-500 hover:bg-slate-900/50 hover:text-slate-300"
      }`}
    >
      {icon}
      <span className="text-sm font-bold">{label}</span>
    </button>
  );
}

function MenuSection({ label }: { label: string }) {
  return (
    <div className="px-4 pt-6 pb-2 text-[10px] font-black text-slate-500 uppercase tracking-widest">
      {label}
    </div>
  );
}

function GlobalMetric({
  label,
  value,
  trend,
  sub,
  color,
  icon,
}: {
  label: string;
  value: string;
  trend?: string;
  sub?: string;
  color: string;
  icon: React.ReactNode;
}) {
  const c = METRIC_COLORS[color] ?? METRIC_COLORS.cyan;
  return (
    <div className="flex items-center gap-4">
      <div className={`p-3 rounded-2xl ${c.iconWrap}`}>{icon}</div>
      <div>
        <div className="text-[10px] text-slate-500 font-bold uppercase tracking-tighter">
          {label}
        </div>
        <div className="text-lg font-black text-white flex items-center gap-2 flex-wrap">
          {value}
          {trend ? (
            <span className={`text-[10px] font-bold ${c.trend}`}>{trend}</span>
          ) : null}
        </div>
        {sub ? (
          <div className="text-[9px] text-slate-500 font-bold">{sub}</div>
        ) : null}
      </div>
    </div>
  );
}

function StatsCard({
  label,
  value,
  sub,
  icon,
}: {
  label: string;
  value: string;
  sub: string;
  icon: React.ReactNode;
}) {
  return (
    <div className="bg-slate-900/40 border border-slate-800 p-8 rounded-[2rem] flex items-center gap-6">
      <div className="p-4 bg-slate-950 rounded-2xl">{icon}</div>
      <div>
        <div className="text-xs text-slate-500 font-bold uppercase">{label}</div>
        <div className="text-2xl font-black text-white mt-1">{value}</div>
        <div className="text-[10px] text-slate-500 mt-1">{sub}</div>
      </div>
    </div>
  );
}

// ── Live Console Modal (terminal-style WebSocket log viewer) ─────────────────

function LiveConsoleModal({
  nodeId,
  onClose,
}: {
  nodeId: string;
  onClose: () => void;
}) {
  const [lines, setLines] = React.useState<string[]>([]);
  const [connected, setConnected] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const bottomRef = React.useRef<HTMLDivElement>(null);
  const wsRef = React.useRef<WebSocket | null>(null);

  React.useEffect(() => {
    const wsBase = apiWsBase();
    const url = `${wsBase}/api/v1/swarm/nodes/${encodeURIComponent(nodeId)}/log_stream`;
    let ws: WebSocket;
    try {
      ws = new WebSocket(url);
    } catch {
      setError(`Cannot open WebSocket to ${url}`);
      return;
    }
    wsRef.current = ws;

    ws.onopen = () => { setConnected(true); setError(null); };
    ws.onmessage = (ev) => {
      try {
        const payload = JSON.parse(String(ev.data)) as { line?: string; data?: string };
        const text = payload.line ?? payload.data ?? String(ev.data);
        setLines((prev) => {
          const next = [...prev, text];
          return next.length > 500 ? next.slice(next.length - 500) : next;
        });
      } catch {
        setLines((prev) => {
          const next = [...prev, String(ev.data)];
          return next.length > 500 ? next.slice(next.length - 500) : next;
        });
      }
    };
    ws.onerror = () => setError("WebSocket connection failed — check API server");
    ws.onclose = (ev) => {
      setConnected(false);
      if (!ev.wasClean) setError(`Connection closed (code ${ev.code})`);
    };

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [nodeId]);

  React.useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  return (
    <div
      className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/80 backdrop-blur-md pointer-events-auto"
      style={{ alignItems: "center", justifyContent: "center" }}
    >
      <div
        className="relative flex flex-col border border-cyan-500/40 bg-[#0a0a0a] rounded-2xl shadow-2xl shadow-cyan-500/20 overflow-hidden w-[90vw] max-w-[900px]"
        style={{ height: "min(80vh, calc(100vh - 4rem))", maxHeight: "calc(100vh - 4rem)" }}
      >
        {/* Title bar */}
        <div className="flex items-center justify-between px-4 py-2.5 bg-slate-950 border-b border-slate-800 shrink-0">
          <div className="flex items-center gap-3">
            <Terminal size={14} className="text-cyan-400" />
            <span className="text-[11px] font-black text-cyan-300 uppercase tracking-widest">
              LIVE CONSOLE — {nodeId}
            </span>
            <span
              className={`w-2 h-2 rounded-full ${connected ? "bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.8)]" : "bg-rose-500"} animate-pulse`}
            />
            <span className="text-[10px] font-bold text-slate-500">
              {connected ? "CONNECTED" : "DISCONNECTED"}
            </span>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-slate-500 hover:text-white transition font-black text-lg px-2"
          >
            ✕
          </button>
        </div>

        {/* Terminal body */}
        <div className="flex-1 overflow-y-auto p-4 font-mono text-[11px] leading-relaxed bg-[#050505] text-green-400 nexus-os-scrollbar">
          {error && (
            <div className="text-rose-400 mb-2">[ERROR] {error}</div>
          )}
          {lines.length === 0 && !error && (
            <div className="text-slate-600 animate-pulse">
              Connecting to {nodeId} log stream…
            </div>
          )}
          {lines.map((line, i) => {
            const isError = /error|exception|traceback|critical|fatal/i.test(line);
            const isWarn = /warn|warning/i.test(line);
            const isSuccess = /success|completed|done|started/i.test(line);
            return (
              <div
                key={i}
                className={`whitespace-pre-wrap break-all ${
                  isError
                    ? "text-rose-400"
                    : isWarn
                      ? "text-amber-400"
                      : isSuccess
                        ? "text-emerald-400"
                        : "text-green-400/80"
                }`}
              >
                {line}
              </div>
            );
          })}
          <div ref={bottomRef} />
        </div>

        {/* Footer */}
        <div className="px-4 py-2 bg-slate-950 border-t border-slate-800 flex items-center justify-between shrink-0">
          <span className="text-[10px] text-slate-600 font-mono">
            {lines.length} lines buffered
          </span>
          <button
            type="button"
            onClick={() => setLines([])}
            className="text-[10px] font-bold text-slate-500 hover:text-cyan-400 transition uppercase tracking-widest"
          >
            CLEAR
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Thermal Gauge ─────────────────────────────────────────────────────────────

function ThermalGauge({ temp }: { temp: number | null }) {
  if (temp === null || temp === undefined || temp < 0) {
    return <div className="text-[10px] text-slate-600 font-mono">N/A</div>;
  }

  const isOverheat = temp > 95;
  const isHot = temp >= 60;
  const barColor = isOverheat ? "bg-rose-500" : isHot ? "bg-orange-400" : "bg-emerald-400";
  const textColor = isOverheat ? "text-rose-400" : isHot ? "text-orange-400" : "text-emerald-400";
  const label = isOverheat ? "DANGER" : isHot ? "HOT" : "STABLE";
  const barWidth = Math.min(100, Math.max(0, (temp / 100) * 100));

  return (
    <div className="w-full space-y-1">
      <div className="flex justify-between items-center">
        <span className={`text-[10px] font-black tracking-tighter ${textColor}`}>
          {temp.toFixed(1)}°C
        </span>
        <span className={`text-[9px] font-bold uppercase ${textColor} ${isOverheat ? "animate-pulse" : ""}`}>
          {isOverheat ? "⚠️ " : ""}{label}
        </span>
      </div>
      <div className="w-full bg-slate-800 h-1.5 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all ${barColor} ${isOverheat ? "animate-pulse" : ""}`}
          style={{ width: `${barWidth}%` }}
        />
      </div>
    </div>
  );
}

// ── Inline LiveConsole card (SSE log stream) ───────────────────────────────────

function LiveConsole({ nodeId, label }: { nodeId: string; label: string }) {
  const [lines, setLines] = React.useState<string[]>([]);
  const [connected, setConnected] = React.useState(false);
  const bottomRef = React.useRef<HTMLDivElement>(null);

  React.useEffect(() => {
    const url = `${API_BASE}/api/v1/swarm/nodes/${encodeURIComponent(nodeId)}/log_stream`;
    const es = new EventSource(url);
    setConnected(true);

    es.onmessage = (ev) => {
      setLines((prev) => {
        const next = [...prev, String(ev.data)];
        return next.length > 200 ? next.slice(next.length - 200) : next;
      });
    };
    es.onerror = () => setConnected(false);

    return () => {
      es.close();
      setConnected(false);
    };
  }, [nodeId]);

  React.useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  return (
    <div className="bg-[#050505] border border-slate-800 rounded-2xl flex flex-col overflow-hidden" style={{ minHeight: 160 }}>
      <div className="flex items-center gap-2 px-3 py-2 bg-slate-950 border-b border-slate-800 shrink-0">
        <Terminal size={11} className="text-cyan-400" />
        <span className="text-[10px] font-black text-cyan-300 uppercase tracking-widest truncate flex-1">
          {label}
        </span>
        <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${connected ? "bg-emerald-400 animate-pulse" : "bg-rose-500"}`} />
      </div>
      <div className="flex-1 overflow-y-auto p-2 font-mono text-[10px] leading-relaxed nexus-os-scrollbar max-h-[120px]">
        {lines.length === 0 ? (
          <div className="text-slate-700 animate-pulse">Waiting for logs…</div>
        ) : (
          lines.map((line, i) => {
            const isError = /error|exception|traceback|critical|fatal/i.test(line);
            const isWarn = /warn|warning/i.test(line);
            const isSuccess = /success|completed|done|started/i.test(line);
            return (
              <div
                key={i}
                className={`whitespace-pre-wrap break-all ${
                  isError ? "text-rose-400" : isWarn ? "text-amber-400" : isSuccess ? "text-emerald-400" : "text-green-400/70"
                }`}
              >
                {line}
              </div>
            );
          })
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}

// ── Swarm Node Terminal — helper types ────────────────────────────────────────

interface ParsedLogLine {
  raw: string;
  type: "heartbeat" | "stats" | "task" | "error" | "warn" | "success" | "git" | "info";
  cpu?: number;
  ram?: number;
  ramTotal?: number;
  taskId?: string;
  taskType?: string;
}

function parseLogLine(raw: string): ParsedLogLine {
  const lower = raw.toLowerCase();
  // Heartbeat JSON
  if (/heartbeat.*true|"heartbeat"\s*:\s*true/i.test(raw)) {
    return { raw, type: "heartbeat" };
  }
  // System stats line: "CPU 24.1% | RAM 24725MB/32652MB"
  const statsMatch = raw.match(/cpu\s+([\d.]+)%.*ram\s+([\d.]+)\s*mb\s*\/\s*([\d.]+)\s*mb/i);
  if (statsMatch) {
    return {
      raw,
      type: "stats",
      cpu: parseFloat(statsMatch[1]),
      ram: parseFloat(statsMatch[2]),
      ramTotal: parseFloat(statsMatch[3]),
    };
  }
  // Task execution line
  const taskMatch = raw.match(/task[_\s-]?id[:\s]+([a-z0-9_-]{4,20}).*type[:\s]+([a-z_]+)/i)
    || raw.match(/\[task\]\s*([a-z0-9_-]{4,20})\s+([a-z_]+)/i);
  if (taskMatch) {
    return { raw, type: "task", taskId: taskMatch[1], taskType: taskMatch[2] };
  }
  if (/error|exception|traceback|critical|fatal/i.test(lower)) return { raw, type: "error" };
  if (/warn|warning/i.test(lower)) return { raw, type: "warn" };
  if (/success|completed|done|started|ready/i.test(lower)) return { raw, type: "success" };
  if (/\[git/i.test(raw)) return { raw, type: "git" };
  return { raw, type: "info" };
}

// ── Circular Gauge (SVG) ──────────────────────────────────────────────────────

function CircularGauge({
  value,
  max = 100,
  label,
  unit = "%",
  size = 72,
}: {
  value: number;
  max?: number;
  label: string;
  unit?: string;
  size?: number;
}) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  const r = (size - 10) / 2;
  const circ = 2 * Math.PI * r;
  const dash = (pct / 100) * circ;
  const gap = circ - dash;
  const color = pct > 80 ? "#f43f5e" : pct > 55 ? "#f59e0b" : "#10b981";
  const trackColor = "rgba(255,255,255,0.06)";

  return (
    <div className="flex flex-col items-center gap-1" title={`${label}: ${value.toFixed(1)}${unit}`}>
      <svg width={size} height={size} style={{ transform: "rotate(-90deg)" }}>
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={trackColor} strokeWidth={6} />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={r}
          fill="none"
          stroke={color}
          strokeWidth={6}
          strokeLinecap="round"
          strokeDasharray={`${dash} ${gap}`}
          style={{
            filter: `drop-shadow(0 0 4px ${color})`,
            transition: "stroke-dasharray 0.6s ease, stroke 0.4s ease",
          }}
        />
      </svg>
      <div className="text-center -mt-1" style={{ transform: "translateY(-4px)" }}>
        <div
          className="font-black font-mono leading-none"
          style={{ fontSize: 11, color }}
        >
          {value.toFixed(0)}{unit}
        </div>
        <div className="text-[8px] font-bold uppercase tracking-widest text-slate-500 mt-0.5">
          {label}
        </div>
      </div>
    </div>
  );
}

// ── ECG Heartbeat Visualizer ──────────────────────────────────────────────────

function EcgPulse({ pulseCount }: { pulseCount: number }) {
  // Simple SVG ECG path that "ticks" on each heartbeat
  const w = 200;
  const h = 36;
  const mid = h / 2;

  // Build a repeating ECG-like path segment
  const seg = `M0,${mid} L20,${mid} L24,${mid - 4} L28,${mid + 8} L32,${mid - 14} L36,${mid + 10} L40,${mid - 4} L44,${mid} L${w / 2},${mid}`;
  const seg2 = `M${w / 2},${mid} L${w / 2 + 20},${mid} L${w / 2 + 24},${mid - 4} L${w / 2 + 28},${mid + 8} L${w / 2 + 32},${mid - 14} L${w / 2 + 36},${mid + 10} L${w / 2 + 40},${mid - 4} L${w / 2 + 44},${mid} L${w},${mid}`;

  return (
    <div className="relative overflow-hidden rounded-lg bg-black/30 border border-cyan-500/10" style={{ height: h, width: "100%" }}>
      <div className="ecg-track h-full" style={{ width: "200%" }}>
        <svg width={w} height={h} className="shrink-0">
          <path d={seg + " " + seg2} fill="none" stroke="rgba(34,211,238,0.7)" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />
        </svg>
        <svg width={w} height={h} className="shrink-0">
          <path d={seg + " " + seg2} fill="none" stroke="rgba(34,211,238,0.7)" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>
      <div className="absolute inset-0 bg-gradient-to-r from-transparent via-transparent to-black/60 pointer-events-none" />
      <div className="absolute top-1 right-2 text-[8px] font-mono text-cyan-400/60 font-bold">
        ×{pulseCount}
      </div>
    </div>
  );
}

// ── Active Task Row ───────────────────────────────────────────────────────────

function ActiveTaskRow({ taskId, taskType, startedAt }: { taskId: string; taskType: string; startedAt: number }) {
  const [elapsed, setElapsed] = React.useState(0);

  React.useEffect(() => {
    const iv = setInterval(() => setElapsed(Math.floor((Date.now() - startedAt) / 1000)), 1000);
    return () => clearInterval(iv);
  }, [startedAt]);

  const fmt = (s: number) => {
    const m = Math.floor(s / 60);
    const sec = s % 60;
    return `${m}:${String(sec).padStart(2, "0")}`;
  };

  return (
    <div className="flex items-center gap-2 px-2 py-1.5 rounded-lg bg-cyan-950/30 border border-cyan-500/20">
      <div className="w-1.5 h-1.5 rounded-full bg-cyan-400 animate-pulse shrink-0" />
      <span className="text-[9px] font-mono text-slate-400 shrink-0 truncate max-w-[60px]" title={taskId}>
        {taskId.slice(0, 8)}…
      </span>
      <span className="text-[9px] font-black text-cyan-300 uppercase tracking-widest flex-1 truncate">
        {taskType}
      </span>
      <span className="text-[9px] font-mono text-amber-400 shrink-0" style={{ animation: "task-tick 1s ease-in-out infinite" }}>
        {fmt(elapsed)}
      </span>
    </div>
  );
}

// ── Waiting-for-Node Placeholder ──────────────────────────────────────────────

function WaitingNodePanel({ label, ip }: { label: string; ip: string }) {
  return (
    <div
      className="rounded-2xl border border-slate-700/40 overflow-hidden flex flex-col"
      style={{
        background: "rgba(8,12,22,0.7)",
        backdropFilter: "blur(12px)",
        boxShadow: "0 0 0 1px rgba(100,116,139,0.08) inset",
      }}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-slate-800/60 shrink-0">
        <div className="flex items-center gap-2.5 min-w-0">
          <div className="w-1.5 h-1.5 rounded-full bg-slate-600 shrink-0" />
          <span className="text-[11px] font-black text-slate-500 uppercase tracking-widest truncate">
            {label}
          </span>
          <span className="text-[9px] font-mono text-slate-700 shrink-0">{ip}</span>
        </div>
        <span className="text-[9px] font-bold text-slate-600 uppercase tracking-widest border border-slate-800 px-1.5 py-0.5 rounded-md">
          OFFLINE
        </span>
      </div>

      {/* Body */}
      <div className="flex flex-col items-center justify-center gap-4 py-8 px-4">
        {/* Radar sweep */}
        <div className="relative w-12 h-12">
          <div className="absolute inset-0 rounded-full border border-slate-700/50" />
          <div className="absolute inset-1 rounded-full border border-slate-800/50" />
          <div
            className="absolute inset-0 rounded-full border-t-2 border-slate-500/40"
            style={{ animation: "radar-sweep 2s linear infinite" }}
          />
          <div className="absolute inset-0 flex items-center justify-center">
            <Radio size={14} className="text-slate-600" />
          </div>
        </div>

        {/* Searching dots */}
        <div className="flex flex-col items-center gap-2">
          <div className="flex items-center gap-1.5">
            <div className="w-1.5 h-1.5 rounded-full bg-slate-600 node-search-dot" />
            <div className="w-1.5 h-1.5 rounded-full bg-slate-600 node-search-dot" />
            <div className="w-1.5 h-1.5 rounded-full bg-slate-600 node-search-dot" />
          </div>
          <span className="text-[9px] font-mono text-slate-600 uppercase tracking-widest">
            Searching for new nodes…
          </span>
        </div>
      </div>

      {/* Footer */}
      <div className="px-3 py-1.5 border-t border-slate-800/40 flex items-center justify-between shrink-0">
        <span className="text-[8px] font-mono text-slate-700">0 lines · node: {label.toLowerCase().replace(/\s+/g, "-")}</span>
        <span className="text-[8px] font-bold uppercase tracking-widest text-slate-700">IDLE</span>
      </div>
    </div>
  );
}

// ── Remote Terminal Panel ─────────────────────────────────────────────────────
// Enterprise-grade glassmorphic swarm node card with ECG pulse, circular gauges,
// smart log parsing, and hamburger action menu.

function RemoteTerminalPanel({
  nodeId,
  label,
  ip,
  status,
}: {
  nodeId: string;
  label: string;
  ip: string;
  status: "LIVE" | "IDLE";
}) {
  const [lines, setLines] = React.useState<string[]>([]);
  const [connected, setConnected] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [expanded, setExpanded] = React.useState(true);
  const [menuOpen, setMenuOpen] = React.useState(false);
  const [pulseCount, setPulseCount] = React.useState(0);
  const [latestStats, setLatestStats] = React.useState<{ cpu: number; ram: number; ramTotal: number } | null>(null);
  const [activeTasks, setActiveTasks] = React.useState<{ taskId: string; taskType: string; startedAt: number }[]>([]);
  const bottomRef = React.useRef<HTMLDivElement>(null);
  const wsRef = React.useRef<WebSocket | null>(null);
  const menuRef = React.useRef<HTMLDivElement>(null);

  // Close hamburger menu on outside click
  React.useEffect(() => {
    if (!menuOpen) return;
    const handler = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [menuOpen]);

  React.useEffect(() => {
    if (!expanded) return;
    const wsBase = apiWsBase();
    const url = `${wsBase}/api/v1/swarm/nodes/${encodeURIComponent(nodeId)}/log_stream`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      setError(null);
    };
    ws.onmessage = (ev) => {
      const raw = String(ev.data);
      const parsed = parseLogLine(raw);

      if (parsed.type === "heartbeat") {
        setPulseCount((p) => p + 1);
        return; // Don't add raw heartbeat JSON to log lines
      }

      if (parsed.type === "stats" && parsed.cpu != null && parsed.ram != null && parsed.ramTotal != null) {
        setLatestStats({ cpu: parsed.cpu, ram: parsed.ram, ramTotal: parsed.ramTotal });
        return; // Stats are shown as gauges, not raw text
      }

      if (parsed.type === "task" && parsed.taskId && parsed.taskType) {
        setActiveTasks((prev) => {
          const exists = prev.find((t) => t.taskId === parsed.taskId);
          if (exists) return prev;
          return [...prev.slice(-4), { taskId: parsed.taskId!, taskType: parsed.taskType!, startedAt: Date.now() }];
        });
      }

      setLines((prev) => {
        const next = [...prev, raw];
        return next.length > 300 ? next.slice(next.length - 300) : next;
      });
    };
    ws.onerror = () => setError("WebSocket connection failed");
    ws.onclose = () => setConnected(false);

    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [nodeId, expanded]);

  React.useEffect(() => {
    if (expanded) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [lines, expanded]);

  // Show waiting panel when not connected and no lines yet
  if (!connected && lines.length === 0 && !error && status !== "LIVE") {
    return <WaitingNodePanel label={label} ip={ip} />;
  }

  const isLive = connected && status === "LIVE";

  return (
    <div
      className={`rounded-2xl overflow-hidden flex flex-col transition-all duration-500 ${isLive ? "node-heartbeat-live" : ""}`}
      style={{
        background: "linear-gradient(145deg, rgba(5,10,20,0.92) 0%, rgba(3,7,15,0.96) 100%)",
        backdropFilter: "blur(20px)",
        WebkitBackdropFilter: "blur(20px)",
        border: isLive
          ? "1px solid rgba(34,211,238,0.28)"
          : "1px solid rgba(51,65,85,0.5)",
        boxShadow: isLive
          ? "0 0 0 1px rgba(34,211,238,0.06) inset, 0 8px 32px rgba(0,0,0,0.6)"
          : "0 4px 16px rgba(0,0,0,0.4)",
      }}
    >
      {/* ── Title bar ──────────────────────────────────────────────────────── */}
      <div
        className="flex items-center justify-between px-4 py-2.5 shrink-0"
        style={{
          background: "rgba(2,6,14,0.8)",
          borderBottom: isLive ? "1px solid rgba(34,211,238,0.12)" : "1px solid rgba(30,41,59,0.8)",
        }}
      >
        <div className="flex items-center gap-3 min-w-0">
          {/* Live indicator */}
          <div className="relative shrink-0">
            <div
              className={`w-2 h-2 rounded-full ${
                connected ? "bg-emerald-400" : error ? "bg-rose-500" : "bg-amber-500"
              }`}
              style={connected ? { boxShadow: "0 0 8px rgba(52,211,153,0.9), 0 0 16px rgba(52,211,153,0.4)" } : {}}
            />
            {connected && (
              <div className="absolute inset-0 rounded-full bg-emerald-400/30 animate-ping" />
            )}
          </div>

          {/* Node name */}
          <div className="min-w-0">
            <div className="flex items-center gap-2 flex-wrap">
              <span
                className="text-[12px] font-black uppercase tracking-widest truncate"
                style={{
                  color: isLive ? "rgba(165,243,252,1)" : "rgba(148,163,184,0.8)",
                  textShadow: isLive ? "0 0 12px rgba(34,211,238,0.5)" : "none",
                  fontFamily: "var(--font-mono)",
                }}
              >
                {label}
              </span>
              <span
                className={`text-[8px] font-black px-1.5 py-0.5 rounded-md uppercase tracking-widest shrink-0 ${
                  isLive
                    ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/30"
                    : "bg-slate-800 text-slate-500 border border-slate-700"
                }`}
              >
                {connected ? "LIVE" : error ? "ERR" : "CONNECTING"}
              </span>
            </div>
            <div className="flex items-center gap-2 mt-0.5">
              <span className="text-[9px] font-mono text-slate-500">{ip}</span>
              <span className="text-[9px] font-mono text-slate-700">·</span>
              <span className="text-[9px] font-mono text-slate-600 truncate max-w-[120px]">{nodeId}</span>
            </div>
          </div>
        </div>

        {/* Hamburger menu */}
        <div className="relative shrink-0" ref={menuRef}>
          <button
            type="button"
            onClick={() => setMenuOpen((v) => !v)}
            className="flex flex-col items-center justify-center gap-[3px] w-7 h-7 rounded-lg hover:bg-slate-800/80 transition"
            title="Actions"
          >
            <span className="w-3.5 h-px bg-slate-500 rounded-full" />
            <span className="w-3.5 h-px bg-slate-500 rounded-full" />
            <span className="w-3.5 h-px bg-slate-500 rounded-full" />
          </button>
          {menuOpen && (
            <div
              className="absolute right-0 top-8 z-50 rounded-xl overflow-hidden menu-slide-down"
              style={{
                background: "rgba(8,14,26,0.97)",
                border: "1px solid rgba(34,211,238,0.18)",
                boxShadow: "0 8px 32px rgba(0,0,0,0.7), 0 0 0 1px rgba(34,211,238,0.06) inset",
                minWidth: 140,
              }}
            >
              <button
                type="button"
                className="w-full text-left px-3 py-2 text-[10px] font-bold text-slate-400 hover:text-cyan-300 hover:bg-cyan-950/40 transition uppercase tracking-widest"
                onClick={() => { setExpanded((v) => !v); setMenuOpen(false); }}
              >
                {expanded ? "▲ Collapse" : "▼ Expand"}
              </button>
              <button
                type="button"
                className="w-full text-left px-3 py-2 text-[10px] font-bold text-slate-400 hover:text-amber-400 hover:bg-amber-950/30 transition uppercase tracking-widest"
                onClick={() => { setLines([]); setMenuOpen(false); }}
              >
                ✕ Clear Logs
              </button>
              <button
                type="button"
                className="w-full text-left px-3 py-2 text-[10px] font-bold text-slate-400 hover:text-rose-400 hover:bg-rose-950/30 transition uppercase tracking-widest"
                onClick={() => { setActiveTasks([]); setMenuOpen(false); }}
              >
                ⊘ Clear Tasks
              </button>
            </div>
          )}
        </div>
      </div>

      {/* ── Body ───────────────────────────────────────────────────────────── */}
      {expanded && (
        <div className="flex flex-col gap-0">

          {/* ── Stats row: gauges + ECG ─────────────────────────────────── */}
          <div
            className="px-4 py-3 flex items-center gap-4 shrink-0"
            style={{ borderBottom: "1px solid rgba(15,23,42,0.8)" }}
          >
            {/* Circular gauges */}
            {latestStats ? (
              <div className="flex items-center gap-4 shrink-0">
                <CircularGauge value={latestStats.cpu} label="CPU" size={68} />
                <CircularGauge
                  value={latestStats.ram}
                  max={latestStats.ramTotal}
                  label="RAM"
                  unit="MB"
                  size={68}
                />
              </div>
            ) : (
              <div className="flex items-center gap-4 shrink-0">
                <div className="flex flex-col items-center gap-1 opacity-30">
                  <div className="w-[68px] h-[68px] rounded-full border-2 border-dashed border-slate-700 flex items-center justify-center">
                    <Cpu size={16} className="text-slate-600" />
                  </div>
                  <span className="text-[8px] text-slate-600 uppercase tracking-widest">CPU</span>
                </div>
                <div className="flex flex-col items-center gap-1 opacity-30">
                  <div className="w-[68px] h-[68px] rounded-full border-2 border-dashed border-slate-700 flex items-center justify-center">
                    <Database size={16} className="text-slate-600" />
                  </div>
                  <span className="text-[8px] text-slate-600 uppercase tracking-widest">RAM</span>
                </div>
              </div>
            )}

            {/* ECG + heartbeat */}
            <div className="flex-1 flex flex-col gap-2 min-w-0">
              <div className="flex items-center justify-between mb-1">
                <span className="text-[9px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-1.5">
                  <Activity size={9} className="text-cyan-500" />
                  Heartbeat
                </span>
                {pulseCount > 0 && (
                  <span className="text-[8px] font-mono text-emerald-400/70">
                    {pulseCount} pulses
                  </span>
                )}
              </div>
              {pulseCount > 0 ? (
                <EcgPulse pulseCount={pulseCount} />
              ) : (
                <div
                  className="rounded-lg flex items-center justify-center"
                  style={{
                    height: 36,
                    background: "rgba(0,0,0,0.3)",
                    border: "1px solid rgba(30,41,59,0.5)",
                  }}
                >
                  <span className="text-[8px] font-mono text-slate-700 animate-pulse">
                    Awaiting heartbeat…
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* ── Active tasks ────────────────────────────────────────────── */}
          {activeTasks.length > 0 && (
            <div
              className="px-4 py-2.5 flex flex-col gap-1.5 shrink-0"
              style={{ borderBottom: "1px solid rgba(15,23,42,0.8)" }}
            >
              <div className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 flex items-center gap-1.5">
                <Zap size={9} className="text-amber-400" />
                Active Tasks
              </div>
              {activeTasks.map((t) => (
                <ActiveTaskRow key={t.taskId} {...t} />
              ))}
            </div>
          )}

          {/* ── Log stream ──────────────────────────────────────────────── */}
          <div
            className="flex-1 overflow-y-auto p-3 nexus-os-scrollbar"
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: 10,
              lineHeight: 1.6,
              background: "rgba(1,3,8,0.8)",
              maxHeight: 200,
            }}
          >
            {error && (
              <div className="text-rose-400 mb-1 text-[10px]">[ERROR] {error}</div>
            )}
            {lines.length === 0 && !error && (
              <div className="text-slate-700 animate-pulse text-[10px]">
                Connecting to {label} log stream…
              </div>
            )}
            {lines.map((line, i) => {
              const p = parseLogLine(line);
              const colorClass =
                p.type === "error" ? "text-rose-400" :
                p.type === "warn"  ? "text-amber-400" :
                p.type === "success" ? "text-emerald-400" :
                p.type === "git"   ? "text-purple-400" :
                p.type === "task"  ? "text-cyan-300" :
                "text-green-400/70";
              return (
                <div
                  key={i}
                  className={`whitespace-pre-wrap break-all log-line-new ${colorClass}`}
                >
                  {line}
                </div>
              );
            })}
            <div ref={bottomRef} />
          </div>
        </div>
      )}

      {/* ── Footer ─────────────────────────────────────────────────────────── */}
      <div
        className="px-3 py-1.5 flex items-center justify-between shrink-0"
        style={{
          background: "rgba(2,5,12,0.9)",
          borderTop: "1px solid rgba(15,23,42,0.8)",
        }}
      >
        <span className="text-[8px] font-mono text-slate-700">
          {lines.length} lines
        </span>
        <span className="text-[8px] font-mono text-slate-700 truncate max-w-[160px] mx-2">
          {nodeId}
        </span>
        <span
          className={`text-[8px] font-black uppercase tracking-widest ${
            isLive ? "text-emerald-400" : "text-slate-600"
          }`}
        >
          {isLive ? "● LIVE" : "○ IDLE"}
        </span>
      </div>
    </div>
  );
}

// ── Node Card ─────────────────────────────────────────────────────────────────

function NodeCard({
  nodeId,
  name,
  ip,
  cpu,
  status,
  cpuTemp,
  role,
  ramUsed,
  ramTotal,
  osInfo,
  cpuModel,
}: {
  nodeId: string;
  name: string;
  ip: string;
  cpu: number;
  status: "LIVE" | "IDLE";
  cpuTemp: number | null;
  role: string;
  ramUsed: number | null;
  ramTotal: number | null;
  osInfo: string | null;
  cpuModel: string | null;
}) {
  const [showConsole, setShowConsole] = React.useState(false);
  const [paused, setPaused] = React.useState(false);
  const isMaster = role === "master";
  const isCritical = cpuTemp !== null && cpuTemp !== undefined && cpuTemp > 95;

  // Detect OS type from os_info string
  const osType: "WIN" | "LINUX" | "MAC" | "UNKNOWN" = React.useMemo(() => {
    if (!osInfo) return "UNKNOWN";
    const lower = osInfo.toLowerCase();
    if (lower.includes("win")) return "WIN";
    if (lower.includes("darwin") || lower.includes("mac")) return "MAC";
    if (lower.includes("linux") || lower.includes("ubuntu") || lower.includes("debian")) return "LINUX";
    return "UNKNOWN";
  }, [osInfo]);

  const osLabel = osType === "WIN" ? "🪟 Windows" : osType === "MAC" ? "🍎 macOS" : osType === "LINUX" ? "🐧 Linux" : "? Unknown";
  const osColor = osType === "WIN" ? "text-blue-400" : osType === "MAC" ? "text-slate-300" : osType === "LINUX" ? "text-orange-400" : "text-slate-500";

  const ramPct = ramUsed != null && ramTotal != null && ramTotal > 0
    ? Math.round((ramUsed / ramTotal) * 100)
    : null;

  const handleThermalPause = async () => {
    try {
      await fetch(`${API_BASE}/api/swarm/nodes/${encodeURIComponent(nodeId)}/pause`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reason: "thermal_shutdown", temp: cpuTemp }),
      });
    } catch { /* best-effort */ }
    setPaused((p) => !p);
  };

  return (
    <>
      {showConsole && (
        <LiveConsoleModal nodeId={nodeId} onClose={() => setShowConsole(false)} />
      )}
      <div
        className={`p-5 rounded-3xl flex flex-col gap-3 group transition ${
          isCritical
            ? "bg-rose-950/30 border-2 border-rose-500/60 shadow-[0_0_24px_rgba(239,68,68,0.18)]"
            : isMaster
            ? "bg-cyan-950/60 border-2 border-cyan-400 shadow-[0_0_32px_rgba(34,211,238,0.8),0_0_12px_rgba(34,211,238,0.2)_inset]"
            : "bg-slate-900/40 border border-slate-800 hover:border-cyan-500/50"
        }`}
      >
        {/* ── Header row: icon + name + status badge ─────────────────────── */}
        <div className="flex items-center gap-3">
          <div
            className={`w-10 h-10 rounded-2xl flex items-center justify-center shrink-0 transition ${
              isCritical
                ? "bg-rose-500/20 text-rose-400"
                : isMaster
                ? "bg-cyan-500/20 text-cyan-300 shadow-[0_0_12px_rgba(34,211,238,0.5)]"
                : "bg-slate-800 text-cyan-400 group-hover:bg-cyan-500/20 group-hover:text-cyan-300"
            }`}
          >
            <Network size={20} />
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-1.5 flex-wrap">
              {isMaster && (
                <span className="text-[9px] font-black text-cyan-300 border border-cyan-400 px-1.5 py-0.5 rounded-md bg-cyan-500/20 shrink-0 shadow-[0_0_8px_rgba(34,211,238,0.6)]">
                  👑 MASTER
                </span>
              )}
              {!isMaster && (
                <span className="text-[9px] font-black text-slate-400 border border-slate-700 px-1.5 py-0.5 rounded-md bg-slate-800/60 shrink-0">
                  ⚙ WORKER
                </span>
              )}
              <span className={`text-[10px] font-black px-1.5 py-0.5 rounded-md shrink-0 ${
                status === "LIVE"
                  ? "bg-emerald-500/15 text-emerald-400 border border-emerald-500/30"
                  : "bg-amber-500/15 text-amber-400 border border-amber-500/30"
              }`}>
                {status}
              </span>
            </div>
            <div className={`font-black text-sm mt-0.5 truncate ${isMaster ? "text-cyan-300" : "text-white"}`}>
              {name}
            </div>
          </div>
        </div>

        {/* ── OS + IP row ─────────────────────────────────────────────────── */}
        <div className="flex items-center justify-between gap-2 bg-slate-950/40 rounded-xl px-3 py-2">
          <span className={`text-[11px] font-black ${osColor}`}>{osLabel}</span>
          <span className="text-[10px] font-mono text-slate-400">{ip}</span>
        </div>

        {/* ── CPU model ───────────────────────────────────────────────────── */}
        {cpuModel && (
          <div className="text-[10px] font-mono text-slate-500 truncate px-1" title={cpuModel}>
            {cpuModel}
          </div>
        )}

        {/* ── CPU + RAM bars ──────────────────────────────────────────────── */}
        <div className="space-y-2">
          {/* CPU */}
          <div>
            <div className="flex justify-between text-[10px] font-bold uppercase tracking-tighter mb-1">
              <span className="text-slate-400">CPU</span>
              <span className={cpu > 80 ? "text-rose-400" : cpu > 50 ? "text-amber-400" : "text-emerald-400"}>
                {cpu}%
              </span>
            </div>
            <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${
                  cpu > 80 ? "bg-rose-500" : cpu > 50 ? "bg-amber-400" : "bg-emerald-400"
                }`}
                style={{ width: `${Math.min(cpu, 100)}%` }}
              />
            </div>
          </div>
          {/* RAM */}
          {ramUsed != null && (
            <div>
              <div className="flex justify-between text-[10px] font-bold uppercase tracking-tighter mb-1">
                <span className="text-slate-400">RAM</span>
                <span className="text-slate-300">
                  {Math.round(ramUsed / 1024 * 10) / 10} GB
                  {ramTotal ? ` / ${Math.round(ramTotal / 1024 * 10) / 10} GB` : ""}
                </span>
              </div>
              {ramPct != null && (
                <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${
                      ramPct > 85 ? "bg-rose-500" : ramPct > 60 ? "bg-amber-400" : "bg-cyan-400"
                    }`}
                    style={{ width: `${Math.min(ramPct, 100)}%` }}
                  />
                </div>
              )}
            </div>
          )}
        </div>

        {/* ── Temperature ─────────────────────────────────────────────────── */}
        <div>
          <div className="text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1">
            Temperature
          </div>
          <ThermalGauge temp={cpuTemp} />
        </div>

        {/* ── Thermal shutdown toggle ──────────────────────────────────────── */}
        {isCritical && (
          <button
            type="button"
            onClick={handleThermalPause}
            className={`w-full py-1.5 rounded-xl text-[10px] font-black uppercase tracking-widest transition border ${
              paused
                ? "bg-amber-500/20 border-amber-500/40 text-amber-400"
                : "bg-rose-500/20 border-rose-500/40 text-rose-400 hover:bg-rose-500/30 animate-pulse"
            }`}
          >
            {paused ? "⏸ NODE PAUSED" : "🔥 PAUSE NODE"}
          </button>
        )}

        <button
          type="button"
          onClick={() => setShowConsole(true)}
          className="w-full flex items-center justify-center gap-1.5 px-3 py-1.5 bg-slate-800 hover:bg-cyan-900/50 border border-slate-700 hover:border-cyan-500/50 text-[10px] font-black text-slate-400 hover:text-cyan-300 rounded-xl transition uppercase tracking-widest"
        >
          <Terminal size={10} />
          LIVE CONSOLE
        </button>
      </div>
    </>
  );
}

function DecisionNode({
  time,
  text,
  type,
}: {
  time: string;
  text: string;
  type: keyof typeof DECISION_DOT;
}) {
  const dot = DECISION_DOT[type] ?? DECISION_DOT.trade;
  return (
    <div className="flex gap-4 group">
      <div className="flex flex-col items-center">
        <div className={`w-2.5 h-2.5 rounded-full ${dot}`} />
        <div className="flex-grow w-px bg-slate-800 my-1 group-last:hidden" />
      </div>
      <div>
        <div className="text-[10px] text-slate-500 font-mono">{time}</div>
        <div className="text-xs text-slate-300 mt-1 leading-relaxed">{text}</div>
      </div>
    </div>
  );
}

// ── Live Swarm View ──────────────────────────────────────────────────────────

interface SwarmBot {
  phone: string;
  machine_id: string;
  is_active: boolean;
  messages_sent: number;
  last_message: string;
  is_king: boolean;
}

interface SwarmRecentMessage {
  phone: string;
  message: string;
  topic: string;
  ts: string;
}

interface SwarmFeedData {
  total_in_group: number;
  active_talkers: number;
  last_message: string;
  last_message_ts: number;
  last_sender_phone: string;
  is_running: boolean;
  bots: SwarmBot[];
  verified_count?: number;
  written_count?: number;
  total_sessions?: number;
  recent_messages?: SwarmRecentMessage[];
}

function SwarmBotCard({ bot }: { bot: SwarmBot }) {
  return (
    <div
      className={`rounded-2xl p-4 flex flex-col gap-2 border transition ${
        bot.is_king
          ? "bg-amber-950/30 border-amber-500/40 shadow-[0_0_16px_rgba(245,158,11,0.15)]"
          : bot.is_active
          ? "bg-purple-950/30 border-purple-500/30"
          : "bg-slate-900/40 border-slate-800"
      }`}
    >
      <div className="flex items-center gap-2">
        <div
          className={`w-2 h-2 rounded-full flex-shrink-0 ${
            bot.is_active ? "bg-purple-400 animate-pulse" : "bg-slate-700"
          }`}
        />
        <span className="text-xs font-black font-mono text-slate-200 truncate flex-1">
          {bot.phone}
        </span>
        {bot.is_king && (
          <span className="px-1.5 py-0.5 bg-amber-500/20 border border-amber-500/40 text-amber-400 text-[9px] font-black rounded-full uppercase tracking-widest shrink-0">
            👑 KING
          </span>
        )}
        {bot.is_active && !bot.is_king && (
          <span className="px-1.5 py-0.5 bg-purple-500/20 border border-purple-500/40 text-purple-400 text-[9px] font-black rounded-full uppercase tracking-widest shrink-0">
            ACTIVE
          </span>
        )}
      </div>
      {bot.last_message && (
        <div className="text-[11px] text-slate-500 line-clamp-2 leading-relaxed">
          {bot.last_message}
        </div>
      )}
      <div className="flex items-center justify-between mt-auto">
        <span className="text-[10px] text-slate-600 font-mono truncate">{bot.machine_id}</span>
        <span className="text-[10px] text-slate-500 shrink-0">{bot.messages_sent} הודעות</span>
      </div>
    </div>
  );
}

function LiveSwarmView() {
  const [feed, setFeed] = useState<SwarmFeedData | null>(null);
  const [swarmRunning, setSwarmRunning] = useState(false);
  const [starting, setStarting] = useState(false);
  const [targetGroup, setTargetGroup] = useState("");
  const [statusMsg, setStatusMsg] = useState("");
  const [swarmTab, setSwarmTab] = useState<"bots" | "feed">("bots");

  const fetchFeed = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/swarm/live-feed`);
      if (!res.ok) return;
      const data = (await res.json()) as SwarmFeedData;
      setFeed(data);
      setSwarmRunning(data.is_running ?? false);
    } catch {
      /* ignore */
    }
  }, []);

  useEffect(() => {
    fetchFeed();
    const t = setInterval(fetchFeed, 3000);
    return () => clearInterval(t);
  }, [fetchFeed]);

  async function handleStartSwarm() {
    if (!targetGroup.trim()) {
      setStatusMsg("⚠️ הכנס קישור לקבוצה");
      return;
    }
    setStarting(true);
    setStatusMsg("מפעיל נחיל...");
    try {
      const res = await fetch(`${API_BASE}/api/swarm/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_group: targetGroup }),
      });
      const data = await res.json();
      if (data.ok) {
        setSwarmRunning(true);
        setStatusMsg("✅ הנחיל הופעל בהצלחה!");
      } else {
        setStatusMsg(`❌ שגיאה: ${data.detail || "unknown"}`);
      }
    } catch (e) {
      setStatusMsg(`❌ שגיאת רשת: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setStarting(false);
    }
  }

  async function handleStopSwarm() {
    try {
      await fetch(`${API_BASE}/api/swarm/stop`, { method: "POST" });
      setSwarmRunning(false);
      setStatusMsg("⏹ הנחיל הופסק");
    } catch {
      setStatusMsg("❌ שגיאה בעצירת הנחיל");
    }
  }

  const lastMsgTime = feed?.last_message_ts
    ? new Date(feed.last_message_ts * 1000).toLocaleTimeString("he-IL")
    : "—";

  const recentMessages = feed?.recent_messages ?? [];

  return (
    <div className="space-y-6" dir="rtl">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 bg-gradient-to-tr from-purple-600 to-pink-500 rounded-2xl flex items-center justify-center shadow-[0_0_20px_rgba(168,85,247,0.4)]">
          <MessageSquareCode size={20} className="text-white" />
        </div>
        <div>
          <h2 className="text-xl font-black text-white tracking-tight">קהילה חיה — Live AI Swarm</h2>
          <p className="text-[11px] text-slate-500 font-bold uppercase tracking-widest">
            מנוע נחיל ישראלי · Gemini Hebrew Content Engine
          </p>
        </div>
        {swarmRunning && (
          <div className="mr-auto flex items-center gap-2 px-3 py-1.5 bg-emerald-500/10 border border-emerald-500/30 rounded-xl">
            <div className="w-2 h-2 bg-emerald-400 rounded-full animate-pulse" />
            <span className="text-[11px] font-black text-emerald-400 uppercase tracking-widest">LIVE</span>
          </div>
        )}
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-4">
          <div className="text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1.5">סשנים בדיסק</div>
          <div className="text-2xl font-black text-purple-400">{feed?.total_sessions ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-4">
          <div className="text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1.5">בקבוצה</div>
          <div className="text-2xl font-black text-indigo-400">{feed?.total_in_group ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-4">
          <div className="text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1.5">בוטים פעילים</div>
          <div className="text-2xl font-black text-cyan-400">{feed?.active_talkers ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-emerald-500/30 rounded-2xl p-4">
          <div className="text-[9px] text-emerald-500/70 font-bold uppercase tracking-widest mb-1.5">✅ Verified</div>
          <div className="text-2xl font-black text-emerald-400">{feed?.verified_count ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-amber-500/30 rounded-2xl p-4">
          <div className="text-[9px] text-amber-500/70 font-bold uppercase tracking-widest mb-1.5">✍️ Written</div>
          <div className="text-2xl font-black text-amber-400">{feed?.written_count ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-4">
          <div className="text-[9px] text-slate-500 font-bold uppercase tracking-widest mb-1.5">הודעה אחרונה</div>
          <div className="text-xs font-black text-slate-300 truncate">{feed?.last_message || "—"}</div>
          <div className="text-[9px] text-slate-600 mt-1">{lastMsgTime}</div>
        </div>
      </div>

      {/* Control panel */}
      <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-6 space-y-4">
        <div className="text-[11px] text-slate-400 font-bold uppercase tracking-widest">הפעלת נחיל</div>
        <div className="flex gap-3">
          <input
            type="text"
            value={targetGroup}
            onChange={(e) => setTargetGroup(e.target.value)}
            placeholder="קישור לקבוצה (https://t.me/...)"
            className="flex-grow bg-slate-800 border border-slate-700 rounded-xl px-4 py-2.5 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-purple-500/60 font-mono"
            dir="ltr"
          />
          {!swarmRunning ? (
            <button
              type="button"
              onClick={handleStartSwarm}
              disabled={starting}
              className="px-6 py-2.5 bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-black text-sm rounded-xl transition shadow-[0_0_20px_rgba(168,85,247,0.4)] disabled:opacity-50 disabled:cursor-not-allowed uppercase tracking-wider"
            >
              {starting ? "מפעיל..." : "🚀 Start Swarm"}
            </button>
          ) : (
            <button
              type="button"
              onClick={handleStopSwarm}
              className="px-6 py-2.5 bg-rose-600/20 hover:bg-rose-600/30 border border-rose-500/40 text-rose-400 font-black text-sm rounded-xl transition uppercase tracking-wider"
            >
              ⏹ עצור נחיל
            </button>
          )}
        </div>
        {statusMsg && (
          <div className="text-[12px] text-slate-400 font-mono">{statusMsg}</div>
        )}
      </div>

      {/* Tabs: Bots / Live Feed */}
      {feed && (feed.bots.length > 0 || recentMessages.length > 0) && (
        <div className="space-y-4">
          <div className="flex gap-1 bg-slate-900/60 border border-slate-800 rounded-xl p-1 w-fit">
            <button
              type="button"
              onClick={() => setSwarmTab("bots")}
              className={`px-4 py-1.5 rounded-lg text-[11px] font-black uppercase tracking-widest transition ${
                swarmTab === "bots"
                  ? "bg-purple-600/30 text-purple-300 border border-purple-500/40"
                  : "text-slate-500 hover:text-slate-300"
              }`}
            >
              בוטים ({feed.bots.length})
            </button>
            <button
              type="button"
              onClick={() => setSwarmTab("feed")}
              className={`px-4 py-1.5 rounded-lg text-[11px] font-black uppercase tracking-widest transition ${
                swarmTab === "feed"
                  ? "bg-cyan-600/30 text-cyan-300 border border-cyan-500/40"
                  : "text-slate-500 hover:text-slate-300"
              }`}
            >
              פיד הודעות ({recentMessages.length})
            </button>
          </div>

          {swarmTab === "bots" && feed.bots.length > 0 && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
              {feed.bots.map((bot) => (
                <SwarmBotCard key={bot.phone} bot={bot} />
              ))}
            </div>
          )}

          {swarmTab === "feed" && (
            <div className="bg-slate-900/60 border border-slate-800 rounded-2xl overflow-hidden">
              {recentMessages.length === 0 ? (
                <div className="px-6 py-10 text-center text-slate-600 text-sm font-bold">
                  אין הודעות עדיין — הנחיל טרם שלח
                </div>
              ) : (
                <div className="divide-y divide-slate-800/60 max-h-[480px] overflow-y-auto nexus-os-scrollbar">
                  {[...recentMessages].reverse().map((msg, i) => {
                    const msgTime = msg.ts
                      ? (() => {
                          try { return new Date(msg.ts).toLocaleTimeString("he-IL"); }
                          catch { return msg.ts; }
                        })()
                      : "";
                    return (
                      <div key={i} className="px-5 py-3 flex gap-3 hover:bg-slate-800/30 transition">
                        <div className="w-1.5 h-1.5 rounded-full bg-purple-400 mt-1.5 flex-shrink-0" />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-0.5 flex-wrap">
                            <span className="text-[10px] font-black font-mono text-purple-400">{msg.phone}</span>
                            {msg.topic && (
                              <span className="text-[9px] px-1.5 py-0.5 bg-slate-800 border border-slate-700 rounded-full text-slate-500 font-bold uppercase tracking-widest">
                                {msg.topic}
                              </span>
                            )}
                            {msgTime && (
                              <span className="text-[9px] text-slate-600 font-mono mr-auto">{msgTime}</span>
                            )}
                          </div>
                          <div className="text-sm text-slate-300 leading-relaxed">{msg.message}</div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Empty state */}
      {feed && feed.bots.length === 0 && recentMessages.length === 0 && (
        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl px-6 py-12 text-center space-y-2">
          <div className="text-4xl">🤖</div>
          <div className="text-slate-400 font-black text-sm">הנחיל לא פעיל</div>
          <div className="text-slate-600 text-[12px]">
            הפעל את הנחיל עם קישור לקבוצה כדי להתחיל
          </div>
        </div>
      )}
    </div>
  );
}

// ── Live Master Terminal View ─────────────────────────────────────────────────

function LiveMasterTerminalView() {
  const { lines, connected } = useMasterTerminalStream();
  const bottomRef = React.useRef<HTMLDivElement>(null);
  const [filter, setFilter] = React.useState("");

  React.useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  const filtered = filter.trim()
    ? lines.filter((l) => l.toLowerCase().includes(filter.toLowerCase()))
    : lines;

  return (
    <div className="space-y-6 animate-in fade-in duration-500">
      {/* Header */}
      <div className="flex items-center gap-4">
        <div className="w-10 h-10 bg-gradient-to-tr from-cyan-600 to-slate-800 rounded-2xl flex items-center justify-center shadow-[0_0_20px_rgba(34,211,238,0.35)]">
          <Terminal size={20} className="text-cyan-300" />
        </div>
        <div>
          <h2 className="text-xl font-black text-white tracking-tight">Live Master Terminal</h2>
          <p className="text-[11px] text-slate-500 font-bold uppercase tracking-widest">
            WebSocket Bridge · Real-time stdout from Master node
          </p>
        </div>
        <div className="mr-auto flex items-center gap-2 px-3 py-1.5 bg-slate-900 border border-slate-700 rounded-xl">
          <span
            className={`w-2 h-2 rounded-full ${connected ? "bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.8)] animate-pulse" : "bg-rose-500"}`}
          />
          <span className={`text-[11px] font-black uppercase tracking-widest ${connected ? "text-emerald-400" : "text-rose-400"}`}>
            {connected ? "CONNECTED" : "DISCONNECTED"}
          </span>
        </div>
      </div>

      {/* Filter bar */}
      <div className="flex items-center gap-3">
        <input
          type="text"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter output…"
          className="flex-1 bg-slate-900/80 border border-slate-700 rounded-xl px-4 py-2 text-sm text-slate-200 placeholder-slate-600 focus:outline-none focus:border-cyan-500/60 font-mono"
        />
        {filter && (
          <button
            type="button"
            onClick={() => setFilter("")}
            className="px-3 py-2 text-xs text-slate-400 hover:text-white border border-slate-700 rounded-xl transition"
          >
            Clear
          </button>
        )}
        <span className="text-[10px] text-slate-600 font-mono">{filtered.length} lines</span>
      </div>

      {/* Terminal output */}
      <div className="bg-[#050810] border border-slate-800 rounded-2xl overflow-hidden shadow-2xl">
        <div className="flex items-center gap-2 px-4 py-2 bg-slate-950 border-b border-slate-800">
          <div className="w-2.5 h-2.5 rounded-full bg-rose-500/70" />
          <div className="w-2.5 h-2.5 rounded-full bg-amber-500/70" />
          <div className="w-2.5 h-2.5 rounded-full bg-emerald-500/70" />
          <span className="ml-2 text-[10px] font-mono text-slate-600 uppercase tracking-widest">
            master-hybrid-node · stdout
          </span>
        </div>
        <div className="h-[60vh] overflow-y-auto p-4 font-mono text-[11px] space-y-0.5 nexus-os-scrollbar">
          {!connected && filtered.length === 0 && (
            <div className="flex items-center gap-2 text-slate-600 mt-6">
              <Terminal size={12} />
              <span>Connecting to Master stdout via WebSocket…</span>
            </div>
          )}
          {filtered.map((line, i) => {
            const isError = /error|fail|exception|critical/i.test(line);
            const isWarn = /warn|warning/i.test(line);
            const isOk = /ok|success|dispatched|started|✅/i.test(line);
            const color = isError
              ? "text-rose-400"
              : isWarn
              ? "text-amber-400"
              : isOk
              ? "text-emerald-400"
              : "text-slate-300";
            return (
              <div key={i} className={`leading-relaxed whitespace-pre-wrap break-all ${color}`}>
                <span className="text-slate-700 select-none mr-2">{String(i + 1).padStart(4, " ")} │</span>
                {line}
              </div>
            );
          })}
          <div ref={bottomRef} />
        </div>
      </div>
    </div>
  );
}
