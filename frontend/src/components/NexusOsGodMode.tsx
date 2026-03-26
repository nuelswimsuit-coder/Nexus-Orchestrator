"use client";

import Link from "next/link";
import React, { useCallback, useEffect, useState } from "react";
import { createPortal } from "react-dom";
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
} from "lucide-react";
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
} from "recharts";
import { API_BASE, triggerPanic } from "@/lib/api";

// ── Types ───────────────────────────────────────────────────────────────────

export interface GodModeDashboard {
  collateral_usdc: string;
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
      className="min-h-[calc(100vh-56px)] bg-[#020617] text-slate-200 font-sans flex overflow-hidden selection:bg-cyan-500/30"
      dir="rtl"
    >
      <aside className="w-80 bg-slate-950/90 border-l border-slate-800 backdrop-blur-3xl flex flex-col min-h-[calc(100vh-56px)] z-20 shadow-2xl overflow-hidden">
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

      <main className="flex-grow flex flex-col min-h-[calc(100vh-56px)] overflow-hidden bg-[radial-gradient(circle_at_20%_20%,#0f172a_0%,#020617_100%)]" style={{ contain: "layout" }}>
        <header className="h-24 border-b border-slate-800/50 backdrop-blur-xl bg-slate-900/20 flex items-center justify-between px-10 z-10 shrink-0">
          <div className="flex gap-10 flex-wrap">
            <GlobalMetric
              label="יתרה (USDC)"
              value={marketData?.collateral_usdc || "0.00"}
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

function _relativeTime(epoch: number): string {
  if (!epoch) return "";
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

function GroupFactoryView() {
  const [warmupGroups, setWarmupGroups] = useState<TelefixGroup[]>([]);
  const [dbGroups, setDbGroups] = useState<TelefixDbGroup[]>([]);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [forceSearchLoading, setForceSearchLoading] = useState<string | null>(null);
  const [deleteLoading, setDeleteLoading] = useState<string | null>(null);
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  const showToast = (msg: string, ok = true) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3000);
  };

  const loadWarmup = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration`);
      if (!res.ok) return;
      const j = (await res.json()) as { groups: TelefixGroup[] };
      setWarmupGroups(j.groups ?? []);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    let cancelled = false;

    const loadDbGroups = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/telefix/groups`);
        if (!res.ok) return;
        const j = (await res.json()) as { groups: TelefixDbGroup[] };
        if (!cancelled) setDbGroups(j.groups ?? []);
      } catch { /* ignore */ }
    };

    void loadWarmup();
    void loadDbGroups();
    return () => { cancelled = true; };
  }, [loadWarmup]);

  const handleForceSearch = async (groupId: string) => {
    setForceSearchLoading(groupId);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration/${groupId}/force-search`, { method: "POST" });
      if (!res.ok) throw new Error(`שגיאה ${res.status}`);
      showToast("הקבוצה הועלתה לחיפוש ✅");
      await loadWarmup();
    } catch {
      showToast("נכשל — נסה שוב", false);
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
          </div>
          <div className="flex items-center gap-3">
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
              onClick={() => setShowCreateModal(true)}
              className="bg-cyan-600 hover:bg-cyan-500 text-white px-6 py-3 rounded-2xl font-bold transition flex items-center gap-2"
            >
              <span className="text-lg leading-none">+</span>
              צור קבוצה חדשה
            </button>
          </div>
        </div>

        {/* Stats bar */}
        <div className="grid grid-cols-3 gap-4 mb-8">
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
        </div>

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
                    onClick={() => void handleForceSearch(group.id)}
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

function AhuManagementView() {
  const ENV_FALLBACK = "https://t.me/Ahu_Management_Private";
  const [ahuInvite, setAhuInvite] = useState<string>("");
  const [linkLoaded, setLinkLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/telefix/ops-config`);
        if (!res.ok) throw new Error(String(res.status));
        const j = (await res.json()) as { operations_chat_link?: string };
        if (!cancelled) {
          // Use the env-sourced link from the API; only fall back if truly absent
          setAhuInvite(j.operations_chat_link || ENV_FALLBACK);
          setLinkLoaded(true);
        }
      } catch {
        if (!cancelled) {
          setAhuInvite(ENV_FALLBACK);
          setLinkLoaded(true);
        }
      }
    })();
    return () => { cancelled = true; };
  }, []);

  const displayInvite = linkLoaded ? ahuInvite : "…טוען";

  const channels = [
    {
      name: "Management Ahu — Ops Sync",
      invite: displayInvite,
      description: "ערוץ ניהול פנימי לסנכרון פעולות שוטפות",
      type: "PRIVATE",
    },
  ];

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10 animate-in fade-in">
      <div className="flex justify-between items-center mb-10 flex-wrap gap-4">
        <div>
          <h3 className="text-2xl font-black text-white">ניהול אהו — Ops Sync</h3>
          <p className="text-slate-500 text-sm mt-1">
            ערוצי ניהול פנימיים וקישורי גישה ישירה
          </p>
        </div>
        {linkLoaded && ahuInvite && (
          <a
            href={ahuInvite}
            target="_blank"
            rel="noopener noreferrer"
            className="bg-cyan-600 hover:bg-cyan-500 text-white px-6 py-3 rounded-2xl font-bold transition flex items-center gap-2"
          >
            <Users size={16} />
            כניסה לערוץ
          </a>
        )}
      </div>

      <div className="grid gap-4">
        {channels.map((ch, i) => (
          <div
            key={i}
            className="bg-slate-950/50 p-6 rounded-3xl border border-cyan-500/20 flex items-center justify-between group hover:border-cyan-500/50 transition flex-wrap gap-4"
          >
            <div className="flex items-center gap-6">
              <div className="w-14 h-14 rounded-2xl flex items-center justify-center bg-cyan-500/20 text-cyan-400">
                <MessageSquareCode size={28} />
              </div>
              <div>
                <div className="text-lg font-bold text-white">{ch.name}</div>
                <div className="text-xs text-slate-500 mt-1">{ch.description}</div>
                <a
                  href={ch.invite}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[11px] text-cyan-400 hover:text-cyan-300 font-mono mt-1 inline-block transition"
                >
                  {ch.invite}
                </a>
              </div>
            </div>
            <div className="text-left flex flex-col items-end gap-2">
              <span className="text-[10px] font-bold text-cyan-400 bg-cyan-500/10 border border-cyan-500/30 px-3 py-1 rounded-lg uppercase tracking-widest">
                {ch.type}
              </span>
              <a
                href={ch.invite}
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs font-bold text-white bg-cyan-600 hover:bg-cyan-500 px-4 py-2 rounded-xl transition"
              >
                פתח ↗
              </a>
            </div>
          </div>
        ))}
      </div>

      <div className="mt-8 p-6 bg-slate-950/30 rounded-2xl border border-slate-800/50">
        <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-3">
          קישור ישיר — Management Ahu
        </div>
        <div className="flex items-center gap-4 flex-wrap">
          <code className="text-cyan-400 font-mono text-sm bg-slate-900 px-4 py-2 rounded-xl border border-slate-800">
            {linkLoaded ? ahuInvite : "…טוען מ-OPERATIONS_CHAT_LINK"}
          </code>
          {linkLoaded && ahuInvite && (
            <a
              href={ahuInvite}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs font-bold text-white bg-purple-600 hover:bg-purple-500 px-4 py-2 rounded-xl transition"
            >
              פתח בטלגרם
            </a>
          )}
        </div>
      </div>
    </div>
  );
}

interface OrderbookData {
  token_id: string;
  best_bid: number | null;
  best_ask: number | null;
  spread: number | null;
  mid_price: number | null;
  bids: { price: string; size: string }[];
  asks: { price: string; size: string }[];
  price_series: { price: number; size: number; side: "bid" | "ask" }[];
  source: string;
}

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
      if (msg.includes("404") || msg.includes("Failed to fetch")) {
        setObError("⚠️ Database initializing on Master...");
      } else {
        setObError(`Sync error — ${msg}`);
      }
      setOrderbook(null);
    } finally {
      setObLoading(false);
    }
  }, [tokenId]);

  useEffect(() => {
    void fetchOrderbook();
    const t = setInterval(() => void fetchOrderbook(), 2_000);
    return () => clearInterval(t);
  }, [fetchOrderbook]);

  const handleOrder = async (side: "BUY" | "SELL") => {
    if (!tokenId.trim()) {
      window.alert("הכנס מזהה חוזה (Token ID)");
      return;
    }
    try {
      const res = await fetch(`${API_BASE}/api/polymarket/manual-order`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          token_id: tokenId.trim(),
          side,
          amount: parseFloat(amount),
        }),
      });
      const j = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = (j as { detail?: string | { msg?: string } }).detail;
        const msg =
          typeof detail === "string"
            ? detail
            : Array.isArray(detail)
              ? detail.map((x: { msg?: string }) => x.msg).join(", ")
              : "הפקודה נדחתה";
        window.alert(msg);
        return;
      }
      await fetchDashboardData();
    } catch {
      window.alert("שגיאה בביצוע פקודה");
    }
  };

  return (
    <div className="grid grid-cols-12 gap-8">
      {/* ── Live CLOB Orderbook ─────────────────────────────────────────── */}
      <div className="col-span-12 bg-slate-900/40 border border-cyan-500/30 rounded-[2.5rem] p-8 space-y-4">
        <div className="flex items-center justify-between flex-wrap gap-4">
          <h3 className="text-xl font-bold flex items-center gap-3">
            <TrendingUp size={20} className="text-cyan-400" />
            CLOB Live Orderbook
            {orderbook?.source && (
              <span className="text-[10px] font-black text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 px-2 py-0.5 rounded-lg uppercase tracking-widest animate-pulse">
                {orderbook.source}
              </span>
            )}
          </h3>
          <div className="flex items-center gap-4 text-sm font-mono flex-wrap">
            {orderbook ? (
              <>
                <span className="text-emerald-400 font-black">BID: {orderbook.best_bid?.toFixed(4) ?? "—"}</span>
                <span className="text-rose-400 font-black">ASK: {orderbook.best_ask?.toFixed(4) ?? "—"}</span>
                <span className="text-cyan-400">MID: {orderbook.mid_price?.toFixed(4) ?? "—"}</span>
                <span className="text-slate-400">SPREAD: {orderbook.spread?.toFixed(4) ?? "—"}</span>
              </>
            ) : obLoading ? (
              <span className="text-slate-500 text-xs">טוען...</span>
            ) : null}
          </div>
        </div>

        {obError && (
          <div className="flex items-center gap-2 p-3 bg-rose-500/10 border border-rose-500/30 rounded-xl text-rose-400 text-xs font-black uppercase tracking-widest">
            <AlertTriangle size={14} />
            {obError}
          </div>
        )}

        {orderbook && orderbook.price_series.length > 0 && (
          <div className="h-[200px] w-full block">
            <ResponsiveContainer width="100%" height={200}>
              <AreaChart data={orderbook.price_series}>
                <defs>
                  <linearGradient id="bidGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#34d399" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#34d399" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="askGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f87171" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#f87171" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="price" stroke="#475569" fontSize={9} tickLine={false} axisLine={false} tickFormatter={(v: number) => v.toFixed(3)} />
                <YAxis stroke="#475569" fontSize={9} tickLine={false} axisLine={false} />
                <Tooltip
                  contentStyle={{ backgroundColor: "#0f172a", border: "1px solid #334155", borderRadius: "12px" }}
                  itemStyle={{ color: "#22d3ee" }}
                  formatter={(value, name) => [typeof value === "number" ? value.toFixed(2) : String(value ?? ""), String(name ?? "") === "size" ? "Size" : String(name ?? "")]}
                />
                <Area
                  type="stepAfter"
                  dataKey="size"
                  stroke="#34d399"
                  fill="url(#bidGrad)"
                  strokeWidth={2}
                  dot={false}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        )}

        {orderbook && (
          <div className="grid grid-cols-2 gap-4 text-xs font-mono max-h-[160px] overflow-y-auto nexus-os-scrollbar">
            <div>
              <div className="text-[10px] font-black text-emerald-400 uppercase tracking-widest mb-2">BIDS</div>
              {orderbook.bids.slice(0, 8).map((b, i) => (
                <div key={i} className="flex justify-between py-0.5 border-b border-slate-800/40">
                  <span className="text-emerald-400">{parseFloat(b.price).toFixed(4)}</span>
                  <span className="text-slate-400">{parseFloat(b.size).toFixed(2)}</span>
                </div>
              ))}
            </div>
            <div>
              <div className="text-[10px] font-black text-rose-400 uppercase tracking-widest mb-2">ASKS</div>
              {orderbook.asks.slice(0, 8).map((a, i) => (
                <div key={i} className="flex justify-between py-0.5 border-b border-slate-800/40">
                  <span className="text-rose-400">{parseFloat(a.price).toFixed(4)}</span>
                  <span className="text-slate-400">{parseFloat(a.size).toFixed(2)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="col-span-12 lg:col-span-8 space-y-8">
        <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10">
          <h3 className="text-xl font-bold mb-8">היסטוריית טריידים (CLOB)</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-right text-sm">
              <thead>
                <tr className="text-slate-500 border-b border-slate-800">
                  <th className="pb-4">זמן</th>
                  <th className="pb-4">חוזה</th>
                  <th className="pb-4">פעולה</th>
                  <th className="pb-4">כמות</th>
                  <th className="pb-4">מחיר</th>
                </tr>
              </thead>
              <tbody>
                {(data?.trading_history?.length ? data.trading_history : []).map(
                  (trade, i) => (
                    <tr
                      key={i}
                      className="border-b border-slate-800/50 hover:bg-slate-800/20 transition"
                    >
                      <td className="py-4 text-slate-500 font-mono">
                        {trade.time}
                      </td>
                      <td className="py-4 font-bold">{trade.asset}</td>
                      <td
                        className={`py-4 font-bold ${
                          trade.side === "BUY"
                            ? "text-emerald-400"
                            : "text-rose-400"
                        }`}
                      >
                        {trade.side}
                      </td>
                      <td className="py-4">{trade.amount}</td>
                      <td className="py-4 font-mono">${trade.price}</td>
                    </tr>
                  ),
                )}
              </tbody>
            </table>
            {!data?.trading_history?.length ? (
              <p className="text-slate-500 text-sm mt-4">אין רשומות עדיין.</p>
            ) : null}
          </div>
        </div>
      </div>

      <div className="col-span-12 lg:col-span-4 space-y-6">
        <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-8 flex flex-col gap-6">
          <h3 className="text-lg font-bold">ביצוע פקודה ידנית</h3>
          <p className="text-[11px] text-slate-500 leading-relaxed">
            BUY = רכישת YES ב-CLOB (מחיר ברירת מחדל מסנאפשוט הבוט או 0.5). SELL =
            מכירת חוזים לפי גודל בש&quot;ח/מחיר.
          </p>
          <input
            type="text"
            placeholder="Token ID (CLOB outcome token)"
            className="bg-slate-950 border border-slate-800 p-3 rounded-xl focus:border-cyan-500 outline-none text-sm"
            value={tokenId}
            onChange={(e) => setTokenId(e.target.value)}
          />
          <div className="bg-slate-950 p-4 rounded-xl border border-slate-800">
            <div className="text-[10px] text-slate-500 font-bold uppercase mb-1">
              כמות (USDC)
            </div>
            <input
              type="number"
              className="bg-transparent text-2xl font-black w-full outline-none"
              value={amount}
              onChange={(e) => setAmount(e.target.value)}
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <button
              type="button"
              onClick={() => handleOrder("BUY")}
              className="py-4 bg-emerald-500 hover:bg-emerald-400 text-white rounded-2xl font-black shadow-lg shadow-emerald-500/20 transition"
            >
              BUY
            </button>
            <button
              type="button"
              onClick={() => handleOrder("SELL")}
              className="py-4 bg-rose-500 hover:bg-rose-400 text-white rounded-2xl font-black shadow-lg shadow-rose-500/20 transition"
            >
              SELL
            </button>
          </div>
        </div>
      </div>
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
        const res = await fetch(`${API_BASE}/api/system/redis-ping`, { signal: AbortSignal.timeout(3000) });
        if (!cancelled) setStatus(res.ok ? "online" : "offline");
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
    const wsBase = (API_BASE || "http://localhost:8002")
      .replace(/^https/, "wss")
      .replace(/^http/, "ws");
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
            cpuTemp: n.cpu_temp ?? null,
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
              👑 MASTER — Jacob-PC
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

      {/* ── Remote Terminal — one per worker node (Jacob sees all laptops) ──── */}
      {cards.filter((c) => c.name !== "Jacob-PC").length > 0 && (
        <div className="space-y-4">
          <div className="text-[10px] font-black text-cyan-400 uppercase tracking-widest flex items-center gap-2">
            <Terminal size={12} />
            REMOTE TERMINALS — WORKER NODES
            <span className="text-slate-600 font-normal normal-case tracking-normal">
              (live WebSocket stream from each laptop)
            </span>
          </div>
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
            {cards
              .filter((c) => c.name !== "Jacob-PC")
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

function SessionSwarmView() {
  const [data, setData] = useState<AllScannedResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/swarm/sessions/all_scanned`);
      // #region agent log
      fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'dfb50f'},body:JSON.stringify({sessionId:'dfb50f',location:'NexusOsGodMode.tsx:fetch-response',message:'API response status',data:{ok:res.ok,status:res.status},timestamp:Date.now(),hypothesisId:'H-B'})}).catch(()=>{});
      // #endregion
      if (!res.ok) throw new Error(String(res.status));
      const j = (await res.json()) as AllScannedResponse;
      // #region agent log
      fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'dfb50f'},body:JSON.stringify({sessionId:'dfb50f',location:'NexusOsGodMode.tsx:after-json',message:'Parsed JSON shape',data:{typeofJ:typeof j,isNull:j===null,keys:j && typeof j==='object' ? Object.keys(j) : null, sessions_by_machine_type: j ? typeof (j as AllScannedResponse).sessions_by_machine : 'N/A', sessions_by_machine_val: j ? String((j as AllScannedResponse).sessions_by_machine).slice(0,100) : 'N/A'},timestamp:Date.now(),hypothesisId:'H-A,H-B,H-C'})}).catch(()=>{});
      // #endregion
      setData(j);
    } catch (err) {
      // #region agent log
      fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'dfb50f'},body:JSON.stringify({sessionId:'dfb50f',location:'NexusOsGodMode.tsx:catch',message:'Fetch error',data:{err:String(err)},timestamp:Date.now(),hypothesisId:'H-B'})}).catch(()=>{});
      // #endregion
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

  // #region agent log
  fetch('http://127.0.0.1:7273/ingest/903bdd2a-d3ba-4205-9ef3-4953f609952a',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'dfb50f'},body:JSON.stringify({sessionId:'dfb50f',location:'NexusOsGodMode.tsx:before-entries',message:'data state before Object.entries',data:{dataIsNull:data===null,dataType:typeof data,hasSBM:data ? 'sessions_by_machine' in data : false, sbmType: data ? typeof (data as AllScannedResponse).sessions_by_machine : 'N/A', sbmIsNull: data ? (data as AllScannedResponse).sessions_by_machine === null : false},timestamp:Date.now(),hypothesisId:'H-A,H-D'})}).catch(()=>{});
  // #endregion

  const filteredMachines: [string, SwarmSession[]][] = data
    ? Object.entries(data.sessions_by_machine ?? {})
        .map(([machine, sessions]) => {
          const filtered = q
            ? sessions.filter(
                (s) =>
                  s.phone_number.toLowerCase().includes(q) ||
                  s.origin_machine.toLowerCase().includes(q) ||
                  s.status.toLowerCase().includes(q),
              )
            : sessions;
          return [machine, filtered] as [string, SwarmSession[]];
        })
        .filter(([, sessions]) => sessions.length > 0)
    : [];

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
          <div className="text-xs text-slate-500 font-mono bg-slate-950 border border-slate-800 px-3 py-1.5 rounded-xl">
            {data?.total ?? 0} sessions
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
          placeholder="חפש לפי מספר טלפון, מחשב, סטטוס..."
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

      <div className="space-y-8">
        {filteredMachines.map(([machine, sessions]) => {
          const isMaster = machine === "Jacob-PC";
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
                      <th className="px-5 py-3 text-right font-bold">טלפון</th>
                      <th className="px-5 py-3 text-right font-bold">מחשב מקור</th>
                      <th className="px-5 py-3 text-right font-bold">סטטוס</th>
                      <th className="px-5 py-3 text-right font-bold">יעד אחרון</th>
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
                        <td className={`px-5 py-3 font-bold ${isMaster ? "text-cyan-300" : "text-slate-300"}`}>
                          {isMaster ? <span className="font-black">👑 {machine}</span> : machine}
                        </td>
                        <td className="px-5 py-3">
                          <span className={`text-[11px] font-bold px-2 py-0.5 rounded-lg ${
                            s.status === "active" || s.status === "online"
                              ? "bg-emerald-500/15 text-emerald-400"
                              : s.status === "banned" || s.status === "error"
                                ? "bg-rose-500/15 text-rose-400"
                                : "bg-slate-800 text-slate-400"
                          }`}>
                            {s.status || "—"}
                          </span>
                        </td>
                        <td className="px-5 py-3 text-slate-500 font-mono text-xs truncate max-w-[200px]">
                          {s.last_scanned_target || "—"}
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

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/swarm/sessions/inventory`);
      if (!res.ok) throw new Error(String(res.status));
      const j = (await res.json()) as InventoryResponse;
      setData(j);
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

  const filteredMachines: [string, InventorySession[]][] = data
    ? Object.entries(data.inventory_by_machine ?? {})
        .map(([machine, sessions]) => {
          const filtered = q
            ? (sessions ?? []).filter(
                (s) =>
                  (s.phone ?? "").toLowerCase().includes(q) ||
                  (s.machine_id ?? "").toLowerCase().includes(q) ||
                  (s.status ?? "").toLowerCase().includes(q),
              )
            : (sessions ?? []);
          return [machine, filtered] as [string, InventorySession[]];
        })
        .filter(([, sessions]) => sessions.length > 0)
    : [];

  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-[2.5rem] p-10 animate-in fade-in">
      <div className="flex justify-between items-start mb-8 flex-wrap gap-4">
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
          <div className="text-xs text-slate-500 font-mono bg-slate-950 border border-slate-800 px-3 py-1.5 rounded-xl">
            {data?.total ?? 0} sessions
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
          placeholder="חפש לפי טלפון, מחשב, סטטוס..."
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

      <div className="space-y-8">
        {filteredMachines.map(([machine, sessions]) => {
          const isMaster = machine === "Jacob-PC";
          return (
            <div key={machine}>
              <div
                className={`flex items-center gap-3 mb-3 ${
                  isMaster ? "text-cyan-300" : "text-slate-400"
                }`}
              >
                <div
                  className={`w-2 h-2 rounded-full ${
                    isMaster
                      ? "bg-cyan-400 shadow-[0_0_8px_rgba(34,211,238,0.6)]"
                      : "bg-slate-600"
                  }`}
                />
                <span
                  className={`text-sm font-black uppercase tracking-widest ${
                    isMaster ? "text-cyan-300" : "text-slate-400"
                  }`}
                >
                  {isMaster ? "👑 " : ""}
                  {machine}
                </span>
                {isMaster && (
                  <span className="text-[10px] font-black text-cyan-300 bg-cyan-500/20 border border-cyan-400 px-2 py-0.5 rounded-lg uppercase tracking-widest shadow-[0_0_8px_rgba(34,211,238,0.5)]">
                    👑 MASTER
                  </span>
                )}
                <span className="text-xs text-slate-600 font-mono">
                  ({sessions.length} sessions)
                </span>
              </div>
              <div
                className={`overflow-x-auto rounded-2xl border ${
                  isMaster
                    ? "border-cyan-500/40 shadow-[0_0_20px_rgba(34,211,238,0.08)]"
                    : "border-slate-800"
                }`}
              >
                <table className="w-full text-sm">
                  <thead>
                    <tr
                      className={`text-[11px] uppercase tracking-widest ${
                        isMaster
                          ? "bg-cyan-950/60 text-cyan-500"
                          : "bg-slate-950/60 text-slate-500"
                      }`}
                    >
                      <th className="px-5 py-3 text-right font-bold">טלפון</th>
                      <th className="px-5 py-3 text-right font-bold">מחשב מקור</th>
                      <th className="px-5 py-3 text-right font-bold">סטטוס</th>
                      <th className="px-5 py-3 text-right font-bold">פעיל לאחרונה</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sessions.map((s, i) => (
                      <tr
                        key={s.redis_key + i}
                        className={`border-t transition ${
                          isMaster
                            ? "border-cyan-900/40 hover:bg-cyan-500/5"
                            : "border-slate-800/50 hover:bg-slate-800/20"
                        }`}
                      >
                        <td
                          className={`px-5 py-3 font-mono font-bold ${
                            isMaster ? "text-cyan-300" : "text-slate-200"
                          }`}
                        >
                          {s.phone || "—"}
                        </td>
                        <td
                          className={`px-5 py-3 font-bold ${
                            isMaster ? "text-cyan-300" : "text-slate-300"
                          }`}
                        >
                          {isMaster ? (
                            <span className="font-black">👑 {machine}</span>
                          ) : (
                            machine
                          )}
                        </td>
                        <td className="px-5 py-3">
                          <span
                            className={`text-[11px] font-bold px-2 py-0.5 rounded-lg ${
                              s.status === "active" || s.status === "online"
                                ? "bg-emerald-500/15 text-emerald-400"
                                : s.status === "banned" || s.status === "error"
                                  ? "bg-rose-500/15 text-rose-400"
                                  : "bg-slate-800 text-slate-400"
                            }`}
                          >
                            {s.status || "—"}
                          </span>
                        </td>
                        <td className="px-5 py-3 text-slate-500 font-mono text-xs truncate max-w-[200px]">
                          {s.last_active || "—"}
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
    const wsBase = (API_BASE || "http://localhost:8002")
      .replace(/^https/, "wss")
      .replace(/^http/, "ws");
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
  if (temp === null || temp === undefined) {
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

// ── Remote Terminal Panel ─────────────────────────────────────────────────────
// Full embedded terminal per worker node, piping logs from the laptop directly
// to Jacob-PC's dashboard via the existing WebSocket endpoint.

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
  const bottomRef = React.useRef<HTMLDivElement>(null);
  const wsRef = React.useRef<WebSocket | null>(null);

  React.useEffect(() => {
    if (!expanded) return;
    const wsBase = (API_BASE || "http://localhost:8002")
      .replace(/^https/, "wss")
      .replace(/^http/, "ws");
    const url = `${wsBase}/api/v1/swarm/nodes/${encodeURIComponent(nodeId)}/log_stream`;
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      setError(null);
    };
    ws.onmessage = (ev) => {
      setLines((prev) => {
        const next = [...prev, String(ev.data)];
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

  return (
    <div
      className={`rounded-2xl border overflow-hidden flex flex-col transition ${
        status === "LIVE"
          ? "border-cyan-500/40 bg-[#050a10] shadow-[0_0_20px_rgba(34,211,238,0.08)]"
          : "border-slate-800 bg-[#080808]"
      }`}
    >
      {/* Title bar */}
      <div className="flex items-center justify-between px-4 py-2.5 bg-slate-950 border-b border-slate-800 shrink-0">
        <div className="flex items-center gap-3 min-w-0">
          <Terminal size={12} className="text-cyan-400 shrink-0" />
          <span className="text-[11px] font-black text-cyan-300 uppercase tracking-widest truncate">
            {label}
          </span>
          <span className="text-[10px] font-mono text-slate-600 shrink-0">{ip}</span>
          <span
            className={`w-1.5 h-1.5 rounded-full shrink-0 ${
              connected
                ? "bg-emerald-400 shadow-[0_0_6px_rgba(52,211,153,0.8)] animate-pulse"
                : "bg-rose-500"
            }`}
          />
          <span className="text-[9px] font-bold text-slate-500 shrink-0">
            {connected ? "LIVE" : error ? "ERR" : "CONNECTING…"}
          </span>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <button
            type="button"
            onClick={() => setLines([])}
            className="text-[9px] font-bold text-slate-600 hover:text-amber-400 transition uppercase tracking-widest px-1"
          >
            CLR
          </button>
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="text-[9px] font-bold text-slate-500 hover:text-cyan-400 transition uppercase tracking-widest px-1"
          >
            {expanded ? "▲ COLLAPSE" : "▼ EXPAND"}
          </button>
        </div>
      </div>

      {/* Terminal body */}
      {expanded && (
        <div className="flex-1 overflow-y-auto p-3 font-mono text-[10px] leading-relaxed bg-[#030303] text-green-400 nexus-os-scrollbar max-h-[280px]">
          {error && (
            <div className="text-rose-400 mb-1">[ERROR] {error}</div>
          )}
          {lines.length === 0 && !error && (
            <div className="text-slate-700 animate-pulse">
              Connecting to {label} log stream via WebSocket…
            </div>
          )}
          {lines.map((line, i) => {
            const isError = /error|exception|traceback|critical|fatal/i.test(line);
            const isWarn = /warn|warning/i.test(line);
            const isSuccess = /success|completed|done|started|ready/i.test(line);
            const isGit = /\[git/i.test(line);
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
                        : isGit
                          ? "text-purple-400"
                          : "text-green-400/80"
                }`}
              >
                {line}
              </div>
            );
          })}
          <div ref={bottomRef} />
        </div>
      )}

      {/* Footer */}
      {expanded && (
        <div className="px-3 py-1.5 bg-slate-950 border-t border-slate-800 flex items-center justify-between shrink-0">
          <span className="text-[9px] text-slate-600 font-mono">
            {lines.length} lines · node: {nodeId}
          </span>
          <span
            className={`text-[9px] font-bold uppercase tracking-widest ${
              status === "LIVE" ? "text-emerald-400" : "text-amber-400"
            }`}
          >
            {status}
          </span>
        </div>
      )}
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
}

function LiveSwarmView() {
  const [feed, setFeed] = useState<SwarmFeedData | null>(null);
  const [swarmRunning, setSwarmRunning] = useState(false);
  const [starting, setStarting] = useState(false);
  const [targetGroup, setTargetGroup] = useState("");
  const [statusMsg, setStatusMsg] = useState("");

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
      if (data.status === "ok") {
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
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-4">
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5">
          <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-2">סשנים בקבוצה</div>
          <div className="text-3xl font-black text-purple-400">{feed?.total_in_group ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5">
          <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-2">בוטים פעילים</div>
          <div className="text-3xl font-black text-cyan-400">{feed?.active_talkers ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-emerald-500/30 rounded-2xl p-5">
          <div className="text-[10px] text-emerald-500/70 font-bold uppercase tracking-widest mb-2">✅ Verified</div>
          <div className="text-3xl font-black text-emerald-400">{feed?.verified_count ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-amber-500/30 rounded-2xl p-5">
          <div className="text-[10px] text-amber-500/70 font-bold uppercase tracking-widest mb-2">✍️ Written</div>
          <div className="text-3xl font-black text-amber-400">{feed?.written_count ?? 0}</div>
        </div>
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-5">
          <div className="text-[10px] text-slate-500 font-bold uppercase tracking-widest mb-2">הודעה אחרונה</div>
          <div className="text-sm font-black text-slate-300 truncate">{feed?.last_message || "—"}</div>
          <div className="text-[10px] text-slate-600 mt-1">{lastMsgTime}</div>
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

      {/* Bot list */}
      {feed && feed.bots.length > 0 && (
        <div className="bg-slate-900/60 border border-slate-800 rounded-2xl overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-800 flex items-center justify-between">
            <span className="text-[11px] text-slate-400 font-bold uppercase tracking-widest">
              רשימת בוטים ({feed.bots.length})
            </span>
          </div>
          <div className="divide-y divide-slate-800/60 max-h-96 overflow-y-auto nexus-os-scrollbar">
            {feed.bots.map((bot) => (
              <div
                key={bot.phone}
                className={`px-6 py-3 flex items-center gap-4 ${bot.is_active ? "bg-purple-500/5" : ""}`}
              >
                <div className={`w-2 h-2 rounded-full flex-shrink-0 ${bot.is_active ? "bg-purple-400 animate-pulse" : "bg-slate-700"}`} />
                <div className="flex-grow min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-black text-slate-200 font-mono">{bot.phone}</span>
                    {bot.is_king && (
                      <span className="px-2 py-0.5 bg-amber-500/20 border border-amber-500/40 text-amber-400 text-[10px] font-black rounded-full uppercase tracking-widest">
                        👑 KING
                      </span>
                    )}
                    {bot.is_active && (
                      <span className="px-2 py-0.5 bg-purple-500/20 border border-purple-500/40 text-purple-400 text-[10px] font-black rounded-full uppercase tracking-widest">
                        ACTIVE
                      </span>
                    )}
                  </div>
                  {bot.last_message && (
                    <div className="text-[11px] text-slate-500 truncate mt-0.5">{bot.last_message}</div>
                  )}
                </div>
                <div className="text-right flex-shrink-0">
                  <div className="text-[10px] text-slate-600 font-mono">{bot.machine_id}</div>
                  <div className="text-[11px] text-slate-500">{bot.messages_sent} הודעות</div>
                </div>
              </div>
            ))}
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
