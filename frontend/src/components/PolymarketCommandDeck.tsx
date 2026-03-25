"use client";

/**
 * Polymarket Command & Control — consulting-style executive deck.
 * Data: live Nexus prediction / paper / evolution APIs.
 */

import useSWR from "swr";
import { useMemo, type ReactNode } from "react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Activity, Cable, KeyRound, Layers, Radio, Server, type LucideIcon } from "lucide-react";
import { swrFetcher } from "@/lib/api";
import type { PaperTradesResponse, VirtualTradeEntry } from "@/lib/api";

// ── API shapes ───────────────────────────────────────────────────────────────

type PolymarketBotPnLResponse = {
  available: boolean;
  realized_pnl_usd: number;
  unrealized_pnl_usd: number;
  total_pnl_usd: number;
  btc_spot: number | null;
  yes_price: number | null;
  market_question: string | null;
  session_active: boolean;
  session_stage: string;
  updated_at: string;
};

type PaperPerformanceResponse = {
  total_trades: number;
  wins: number;
  losses: number;
  virtual_pnl: number;
  win_rate: number;
  updated_at?: string | null;
};

type ChartPoint = {
  timestamp: string;
  binance_price: number | null;
  poly_price: number | null;
  pred_mid: number | null;
};

type ArbitrageChartDataResponse = { data: ChartPoint[]; total: number };

type TradeLogEntry = {
  timestamp: string;
  side: string;
  price: number;
  shares: number;
  spent_usd: number;
  market_question: string;
  status: string;
  log_text: string;
  paper: boolean;
};

type TradeLogResponse = {
  entries: TradeLogEntry[];
  total: number;
  paper_trading: boolean;
  kill_switch_balance_usd: number;
};

type Poly5mResponse = {
  wins: number;
  losses: number;
  decision: string | null;
  trading_halted: boolean;
};

type IncubatorProject = {
  name: string;
  status: string;
  created_at: string;
  updated_at: string;
  ai_logic: string;
};

type IncubatorResponse = {
  projects: IncubatorProject[];
  total: number;
  queried_at: string;
};

type EvolutionStateResponse = {
  state: string;
  updated_at: string;
};

const NAVY = "#0f172a";
const INK = "#1e293a";
const MUTED = "#64748b";
const GOLD = "#b45309";
const GOLD_LIGHT = "#d4a574";
const PAPER_BG = "#faf8f5";
const RULE = "rgba(15, 23, 42, 0.12)";

function fmtUsd(n: number) {
  const sign = n >= 0 ? "" : "−";
  return `${sign}$${Math.abs(n).toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

function fmtTime(ts: string) {
  try {
    return new Date(ts).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
  } catch {
    return ts.slice(11, 16);
  }
}

function SlideShell({
  kicker,
  title,
  subtitle,
  children,
}: {
  kicker: string;
  title: string;
  subtitle?: string;
  children: ReactNode;
}) {
  return (
    <section
      className="min-h-[520px] border-b py-10"
      style={{
        borderColor: RULE,
        background: PAPER_BG,
        color: INK,
      }}
    >
      <div className="mx-auto max-w-5xl px-6">
        <p
          className="mb-2 text-[0.65rem] font-semibold uppercase tracking-[0.28em]"
          style={{ color: GOLD }}
        >
          {kicker}
        </p>
        <h1
          className="mb-1 text-3xl font-normal leading-tight md:text-[2.15rem]"
          style={{ fontFamily: "Georgia, 'Times New Roman', serif", color: NAVY }}
        >
          {title}
        </h1>
        {subtitle ? (
          <p className="mb-8 max-w-3xl text-sm leading-relaxed" style={{ color: MUTED }}>
            {subtitle}
          </p>
        ) : (
          <div className="mb-8" />
        )}
        {children}
      </div>
    </section>
  );
}

function ConfigTile({
  icon: Icon,
  label,
  value,
  hint,
}: {
  icon: LucideIcon;
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div
      className="flex gap-3 rounded-sm border p-4"
      style={{ borderColor: RULE, background: "#fff" }}
    >
      <div className="mt-0.5 shrink-0 text-amber-800/80">
        <Icon size={22} strokeWidth={1.35} />
      </div>
      <div>
        <p className="text-[0.6rem] font-bold uppercase tracking-[0.2em]" style={{ color: MUTED }}>
          {label}
        </p>
        <p className="mt-1 font-mono text-sm font-medium" style={{ color: NAVY }}>
          {value}
        </p>
        {hint ? (
          <p className="mt-1 text-[0.7rem] leading-snug" style={{ color: MUTED }}>
            {hint}
          </p>
        ) : null}
      </div>
    </div>
  );
}

function useCumulativePaperCurve(trades: VirtualTradeEntry[] | undefined) {
  return useMemo(() => {
    if (!trades?.length) return [{ i: 0, v: 0 }];
    const sorted = [...trades].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime(),
    );
    let acc = 0;
    return sorted.map((t, i) => {
      acc += t.potential_profit_usd;
      return { i: i + 1, v: acc, t: fmtTime(t.timestamp) };
    });
  }, [trades]);
}

export default function PolymarketCommandDeck() {
  const { data: bot } = useSWR<PolymarketBotPnLResponse>("/api/prediction/polymarket-bot", swrFetcher, {
    refreshInterval: 5_000,
  });
  const { data: perf } = useSWR<PaperPerformanceResponse>("/api/prediction/performance", swrFetcher, {
    refreshInterval: 12_000,
  });
  const { data: chart } = useSWR<ArbitrageChartDataResponse>("/api/prediction/chart-data", swrFetcher, {
    refreshInterval: 4_000,
  });
  const { data: trades } = useSWR<PaperTradesResponse>("/api/prediction/paper-trades", swrFetcher, {
    refreshInterval: 15_000,
  });
  const { data: log } = useSWR<TradeLogResponse>("/api/prediction/trade-log", swrFetcher, {
    refreshInterval: 6_000,
  });
  const { data: poly5m } = useSWR<Poly5mResponse>("/api/prediction/poly5m-scalper", swrFetcher, {
    refreshInterval: 10_000,
  });
  const { data: evo } = useSWR<EvolutionStateResponse>("/api/evolution/state", swrFetcher, {
    refreshInterval: 30_000,
  });
  const { data: inc } = useSWR<IncubatorResponse>("/api/evolution/incubator", swrFetcher, {
    refreshInterval: 30_000,
  });

  const cumPaper = useCumulativePaperCurve(trades?.trades);

  const areaData = useMemo(() => {
    const rows = chart?.data ?? [];
    if (!rows.length) return [];
    let cum = 0;
    let prev: number | null = null;
    return rows.map((r, idx) => {
      const p = r.poly_price;
      if (p != null && prev != null) cum += p - prev;
      if (p != null) prev = p;
      return {
        idx,
        label: fmtTime(r.timestamp),
        poly: p != null ? p : 0,
        cum: cum,
      };
    });
  }, [chart?.data]);

  const doughnutData = useMemo(() => {
    const w = perf?.wins ?? 0;
    const l = perf?.losses ?? 0;
    const open = Math.max(0, (trades?.total ?? 0) - w - l);
    const parts = [
      { name: "Wins", value: w, fill: "#0f766e" },
      { name: "Losses", value: l, fill: "#b91c1c" },
      { name: "Open / other", value: open || 0.001, fill: "#94a3b8" },
    ].filter((p) => p.value > 0);
    return parts.length ? parts : [{ name: "No data", value: 1, fill: "#cbd5e1" }];
  }, [perf, trades?.total]);

  const forecastVsObs = useMemo(() => {
    const rows = chart?.data ?? [];
    const last = rows.slice(-12);
    return last.map((r, i) => ({
      i: String(i + 1),
      forecast: r.pred_mid != null ? r.pred_mid * 100 : 0,
      observed: r.poly_price != null ? r.poly_price * 100 : 0,
    }));
  }, [chart?.data]);

  const timelineItems = useMemo(() => {
    const items: { t: string; title: string; body: string }[] = [];
    if (evo?.updated_at) {
      items.push({
        t: evo.updated_at.slice(0, 16).replace("T", " "),
        title: "Evolution engine",
        body: `State: ${evo.state}`,
      });
    }
    for (const p of (inc?.projects ?? []).slice(0, 5)) {
      items.push({
        t: p.updated_at.slice(0, 16).replace("T", " "),
        title: p.name,
        body: `${p.status} — ${p.ai_logic.slice(0, 120)}${p.ai_logic.length > 120 ? "…" : ""}`,
      });
    }
    if (!items.length) {
      items.push({
        t: "—",
        title: "Evolution timeline",
        body: "Connect master with evolution engine enabled to populate autonomous improvement events.",
      });
    }
    return items;
  }, [evo, inc]);

  return (
    <div className="min-h-screen" style={{ background: PAPER_BG }}>
      {/* Cover */}
      <header
        className="border-b py-16"
        style={{
          borderColor: RULE,
          background: `linear-gradient(145deg, ${NAVY} 0%, #1e3a5f 55%, #0f172a 100%)`,
          color: "#f8fafc",
        }}
      >
        <div className="mx-auto max-w-5xl px-6">
          <p className="text-[0.65rem] font-semibold uppercase tracking-[0.35em]" style={{ color: GOLD_LIGHT }}>
            Nexus OS
          </p>
          <h1
            className="mt-4 text-4xl font-light md:text-[2.75rem]"
            style={{ fontFamily: "Georgia, 'Times New Roman', serif" }}
          >
            Polymarket Command &amp; Control
          </h1>
          <p className="mt-4 max-w-2xl text-sm leading-relaxed opacity-85">
            Real-time execution telemetry, CLOB connectivity context, and performance analytics for the Polymarket
            engine — executive briefing format.
          </p>
          <div className="mt-8 flex flex-wrap gap-6 font-mono text-[0.65rem] uppercase tracking-widest opacity-70">
            <span>Live refresh</span>
            <span>·</span>
            <span>API-backed</span>
            <span>·</span>
            <span>{bot?.session_active ? "Worker session active" : "Session idle"}</span>
          </div>
        </div>
      </header>

      <SlideShell
        kicker="01 · Architecture"
        title="Connection &amp; execution fabric"
        subtitle="Public endpoints and execution path — credentials remain in environment (never exposed here)."
      >
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <ConfigTile
            icon={Server}
            label="Gamma API"
            value="https://gamma-api.polymarket.com"
            hint="Market discovery & metadata"
          />
          <ConfigTile
            icon={Cable}
            label="CLOB host"
            value="https://clob.polymarket.com"
            hint="Order placement & order book (py-clob-client)"
          />
          <ConfigTile
            icon={Layers}
            label="Chain"
            value="Polygon · chainId 137"
            hint="USDC collateral context"
          />
          <ConfigTile
            icon={KeyRound}
            label="L2 credentials"
            value="●●●● ●●●● (env)"
            hint="POLYMARKET_API_* · builder keys optional"
          />
          <ConfigTile
            icon={Radio}
            label="Kill switch"
            value={fmtUsd(log?.kill_switch_balance_usd ?? 90)}
            hint="Trading halts below balance threshold"
          />
          <ConfigTile
            icon={Activity}
            label="Paper mode"
            value={log?.paper_trading ? "Simulation ON" : "Live path"}
            hint="Virtual trades & paper stats when enabled"
          />
        </div>
      </SlideShell>

      <SlideShell
        kicker="02 · Operations"
        title="Command journal"
        subtitle="Latest automated actions (newest first) — buys, sells, and halts from the trade log."
      >
        <div className="overflow-x-auto rounded-sm border" style={{ borderColor: RULE, background: "#fff" }}>
          <table className="w-full text-left text-sm">
            <thead>
              <tr style={{ borderBottom: `1px solid ${RULE}` }}>
                <th className="px-4 py-3 font-mono text-[0.6rem] uppercase tracking-wider" style={{ color: MUTED }}>
                  Time
                </th>
                <th className="px-4 py-3 font-mono text-[0.6rem] uppercase tracking-wider" style={{ color: MUTED }}>
                  Side
                </th>
                <th className="px-4 py-3 font-mono text-[0.6rem] uppercase tracking-wider" style={{ color: MUTED }}>
                  Price
                </th>
                <th className="px-4 py-3 font-mono text-[0.6rem] uppercase tracking-wider" style={{ color: MUTED }}>
                  Status
                </th>
                <th className="px-4 py-3 font-mono text-[0.6rem] uppercase tracking-wider" style={{ color: MUTED }}>
                  Detail
                </th>
              </tr>
            </thead>
            <tbody>
              {(log?.entries ?? []).slice(0, 12).map((e, idx) => (
                <tr key={`${e.timestamp}-${idx}`} style={{ borderBottom: `1px solid ${RULE}` }}>
                  <td className="px-4 py-2.5 font-mono text-xs" style={{ color: INK }}>
                    {fmtTime(e.timestamp)}
                  </td>
                  <td className="px-4 py-2.5 font-mono text-xs">{e.side}</td>
                  <td className="px-4 py-2.5 font-mono text-xs">{e.price.toFixed(4)}</td>
                  <td className="px-4 py-2.5 font-mono text-xs">{e.status}</td>
                  <td className="max-w-md truncate px-4 py-2.5 text-xs" style={{ color: MUTED }}>
                    {e.log_text || e.market_question || "—"}
                  </td>
                </tr>
              ))}
              {!(log?.entries ?? []).length ? (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-sm" style={{ color: MUTED }}>
                    No trade log entries yet.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </SlideShell>

      <SlideShell
        kicker="03 · Performance"
        title="ROI &amp; P&amp;L snapshot"
        subtitle="Polymarket bot telemetry plus aggregated paper performance and 5m scalper win count."
      >
        <div className="grid gap-6 md:grid-cols-3">
          {[
            {
              label: "Bot total P&L",
              value: bot?.available ? fmtUsd(bot.total_pnl_usd) : "—",
              sub: bot?.available
                ? `Realized ${fmtUsd(bot.realized_pnl_usd)} · Unreal ${fmtUsd(bot.unrealized_pnl_usd)}`
                : "Awaiting worker tick / Redis snapshot",
            },
            {
              label: "Paper virtual P&L",
              value: fmtUsd(perf?.virtual_pnl ?? 0),
              sub: `Win rate ${(perf?.win_rate ?? 0).toFixed(1)}% · ${perf?.total_trades ?? 0} trades`,
            },
            {
              label: "Poly 5m scalper",
              value: `${poly5m?.wins ?? 0}W / ${poly5m?.losses ?? 0}L`,
              sub: poly5m?.trading_halted ? "Halted" : (poly5m?.decision ?? "—"),
            },
          ].map((k) => (
            <div
              key={k.label}
              className="rounded-sm border p-6"
              style={{ borderColor: RULE, background: "#fff" }}
            >
              <p
                className="text-[0.6rem] font-bold uppercase tracking-[0.22em]"
                style={{ color: MUTED }}
              >
                {k.label}
              </p>
              <p
                className="mt-3 text-3xl font-light"
                style={{ fontFamily: "Georgia, serif", color: NAVY }}
              >
                {k.value}
              </p>
              <p className="mt-2 text-xs leading-relaxed" style={{ color: MUTED }}>
                {k.sub}
              </p>
            </div>
          ))}
        </div>
      </SlideShell>

      <SlideShell
        kicker="04 · Markets"
        title="Market trends"
        subtitle="Left: cumulative change in Polymarket YES from tick stream. Right: cumulative paper exposure curve (potential profit stack)."
      >
        <div className="grid gap-8 lg:grid-cols-2">
          <div
            className="h-72 min-h-[288px] min-w-0 rounded-sm border p-4"
            style={{ borderColor: RULE, background: "#fff" }}
          >
            <p className="mb-2 font-mono text-[0.6rem] uppercase tracking-widest" style={{ color: MUTED }}>
              YES momentum (arb collector)
            </p>
            <ResponsiveContainer width="100%" height={240}>
              <AreaChart data={areaData}>
                <CartesianGrid strokeDasharray="3 3" stroke={RULE} />
                <XAxis dataKey="label" tick={{ fontSize: 9, fill: MUTED }} />
                <YAxis tick={{ fontSize: 9, fill: MUTED }} width={36} />
                <Tooltip
                  contentStyle={{ fontSize: 11, border: `1px solid ${RULE}` }}
                  formatter={(v) => [Number(v ?? 0).toFixed(4), "cum Δ YES"]}
                />
                <Area type="monotone" dataKey="cum" stroke={GOLD} fill={GOLD} fillOpacity={0.12} strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <div className="h-72 rounded-sm border p-4" style={{ borderColor: RULE, background: "#fff" }}>
            <p className="mb-2 font-mono text-[0.6rem] uppercase tracking-widest" style={{ color: MUTED }}>
              Paper exposure build-up
            </p>
            <ResponsiveContainer width="100%" height="90%" minHeight={300}>
              <AreaChart data={cumPaper}>
                <CartesianGrid strokeDasharray="3 3" stroke={RULE} />
                <XAxis dataKey="t" tick={{ fontSize: 9, fill: MUTED }} />
                <YAxis tick={{ fontSize: 9, fill: MUTED }} width={44} />
                <Tooltip contentStyle={{ fontSize: 11, border: `1px solid ${RULE}` }} />
                <Area type="monotone" dataKey="v" stroke="#0f766e" fill="#0f766e" fillOpacity={0.1} strokeWidth={2} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      </SlideShell>

      <SlideShell
        kicker="05 · Positions"
        title="Outcome distribution"
        subtitle="Settlement-style split from paper stats (wins / losses / remainder)."
      >
        <div className="mx-auto flex max-w-xl min-w-0 justify-center">
          <div className="h-80 min-h-[320px] w-full min-w-0">
            <ResponsiveContainer width="100%" height={320}>
              <PieChart>
                <Pie
                  data={doughnutData}
                  dataKey="value"
                  nameKey="name"
                  innerRadius={68}
                  outerRadius={110}
                  paddingAngle={2}
                >
                  {doughnutData.map((entry, i) => (
                    <Cell key={i} fill={entry.fill} stroke="#fff" strokeWidth={1} />
                  ))}
                </Pie>
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </SlideShell>

      <SlideShell
        kicker="06 · Intelligence"
        title="Forecast vs observed (recent ticks)"
        subtitle="AI fair-value mid (×100) vs Polymarket YES (×100) on the last ~12 arb snapshots — directional sense-check."
      >
        <div
          className="h-80 min-h-[320px] min-w-0 rounded-sm border p-4"
          style={{ borderColor: RULE, background: "#fff" }}
        >
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={forecastVsObs.length ? forecastVsObs : [{ i: "—", forecast: 0, observed: 0 }]}>
              <CartesianGrid strokeDasharray="3 3" stroke={RULE} />
              <XAxis dataKey="i" tick={{ fontSize: 10, fill: MUTED }} />
              <YAxis tick={{ fontSize: 10, fill: MUTED }} />
              <Legend />
              <Tooltip />
              <Bar dataKey="forecast" name="Forecast (pred_mid×100)" fill="#1e3a5f" radius={[4, 4, 0, 0]} />
              <Bar dataKey="observed" name="Observed (YES×100)" fill={GOLD} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </SlideShell>

      <SlideShell
        kicker="07 · Evolution"
        title="Autonomous improvement log"
        subtitle="Evolution engine state and recent incubator projects — proxy for code/strategy iteration pipeline."
      >
        <div className="relative border-l-2 pl-8" style={{ borderColor: GOLD }}>
          {timelineItems.map((it, idx) => (
            <div key={idx} className="relative mb-10 last:mb-0">
              <div
                className="absolute -left-[1.15rem] top-1.5 h-2.5 w-2.5 rounded-full border-2 border-white"
                style={{ background: GOLD, boxShadow: `0 0 0 2px ${GOLD_LIGHT}` }}
              />
              <p className="font-mono text-[0.65rem] uppercase tracking-widest" style={{ color: MUTED }}>
                {it.t}
              </p>
              <p className="mt-1 text-lg font-normal" style={{ fontFamily: "Georgia, serif", color: NAVY }}>
                {it.title}
              </p>
              <p className="mt-1 text-sm leading-relaxed" style={{ color: MUTED }}>
                {it.body}
              </p>
            </div>
          ))}
        </div>
      </SlideShell>

      <footer className="border-t py-8 text-center text-[0.65rem] uppercase tracking-[0.2em]" style={{ color: MUTED, borderColor: RULE }}>
        Nexus OS · Polymarket C&amp;C Deck · Confidential — internal use
      </footer>
    </div>
  );
}
