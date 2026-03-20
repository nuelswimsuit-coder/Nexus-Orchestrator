"use client";

import { motion } from "framer-motion";
import {
  Clock4,
  Cpu,
  Gem,
  Headphones,
  Keyboard,
  Monitor,
  Mouse,
  Smartphone,
  Watch,
} from "lucide-react";
import { useMemo } from "react";
import type { NodeStatus } from "@/lib/api";
import { useNexus } from "@/lib/nexus-context";

const FAN_POSITIONS = [
  "left-4 top-4",
  "left-20 top-4",
  "left-36 top-4",
  "left-4 top-20",
  "left-20 top-20",
  "left-36 top-20",
  "left-20 top-36",
];

function contains(value: string | undefined, needle: string): boolean {
  return (value ?? "").toLowerCase().includes(needle);
}

function pickGamingWorker(workers: NodeStatus[]): NodeStatus | null {
  return (
    workers.find(
      (worker) =>
        contains(worker.node_id, "gaming")
        || contains(worker.node_id, "beast")
        || contains(worker.os_info, "windows"),
    )
    ?? workers[0]
    ?? null
  );
}

function pickHpWorker(workers: NodeStatus[], excludeId?: string): NodeStatus | null {
  return (
    workers.find(
      (worker) =>
        worker.node_id !== excludeId
        && (contains(worker.node_id, "hp") || contains(worker.os_info, "hp")),
    )
    ?? workers.find((worker) => worker.node_id !== excludeId)
    ?? null
  );
}

function Ring({
  value,
  color,
  glow,
  offline,
}: {
  value: number;
  color: string;
  glow: string;
  offline: boolean;
}) {
  const safeValue = Math.max(0, Math.min(100, value));
  return (
    <div className="relative grid h-20 w-20 place-items-center">
      <svg className="-rotate-90" width="80" height="80" viewBox="0 0 80 80">
        <circle cx="40" cy="40" r="34" stroke="rgba(148,163,184,0.2)" strokeWidth="8" fill="none" />
        <circle
          cx="40"
          cy="40"
          r="34"
          stroke={offline ? "#6b7280" : color}
          strokeWidth="8"
          strokeLinecap="round"
          strokeDasharray={213.6}
          strokeDashoffset={213.6 - (213.6 * safeValue) / 100}
          fill="none"
          style={{
            transition: "stroke-dashoffset 0.6s ease",
            filter: offline ? "none" : `drop-shadow(0 0 10px ${glow})`,
          }}
        />
      </svg>
      <span className={`absolute text-[11px] font-semibold ${offline ? "text-slate-500" : "text-slate-200"}`}>
        {safeValue}%
      </span>
    </div>
  );
}

function NodeBadge({
  title,
  node,
  ringColor,
  glow,
  power,
}: {
  title: string;
  node: NodeStatus | null;
  ringColor: string;
  glow: string;
  power: number;
}) {
  const online = !!node?.online;
  return (
    <div
      className={`rounded-xl border px-3 py-2 ${
        online
          ? "border-cyan-300/40 bg-slate-950/70 text-cyan-50"
          : "border-slate-700/80 bg-slate-900/90 text-slate-400"
      }`}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-[10px] font-semibold uppercase tracking-[0.2em]">{title}</span>
        <span className={`text-[10px] font-bold uppercase tracking-wider ${online ? "text-emerald-300" : "text-slate-500"}`}>
          {online ? "ONLINE" : "OFFLINE"}
        </span>
      </div>
      <div className="mt-2 flex items-center gap-3">
        <Ring value={online ? power : 0} color={ringColor} glow={glow} offline={!online} />
        <div className="space-y-1 text-[11px] text-slate-300">
          <p className="max-w-[180px] truncate font-mono text-cyan-200/90">
            {node?.node_id ?? "node_unassigned"}
          </p>
          <p className="font-mono text-slate-400">
            CPU {node?.cpu_percent?.toFixed(1) ?? "0.0"}% | JOBS {node?.active_jobs ?? 0}
          </p>
        </div>
      </div>
    </div>
  );
}

export default function TopologyVisual() {
  const { cluster } = useNexus();

  const { master, gamingWorker, hpWorker, liveNodes } = useMemo(() => {
    const masterNode = cluster?.nodes.find((node) => node.role === "master") ?? null;
    const workers = cluster?.nodes.filter((node) => node.role === "worker") ?? [];
    const gaming = pickGamingWorker(workers);
    const hp = pickHpWorker(workers, gaming?.node_id);
    const onlineCount = [masterNode, gaming, hp].filter((node) => node?.online).length;
    return { master: masterNode, gamingWorker: gaming, hpWorker: hp, liveNodes: onlineCount };
  }, [cluster]);

  const masterOnline = !!master?.online;
  const gamingOnline = !!gamingWorker?.online;
  const hpOnline = !!hpWorker?.online;

  return (
    <div className="relative w-full overflow-hidden rounded-2xl border border-cyan-300/20 bg-black p-5 text-white shadow-[0_0_60px_rgba(14,165,233,0.14)] md:p-7">
      <div
        className="pointer-events-none absolute inset-0 opacity-[0.05]"
        style={{
          backgroundImage:
            "linear-gradient(rgba(34,211,238,0.35) 1px, transparent 1px), linear-gradient(90deg, rgba(34,211,238,0.35) 1px, transparent 1px)",
          backgroundSize: "44px 44px",
        }}
      />

      <div className="relative z-20 mb-6 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="font-mono text-lg font-bold uppercase tracking-[0.28em] text-cyan-200 md:text-xl">
            LIVE OPS - REAL-TIME EXECUTION
          </h2>
          <p className="mt-1 text-xs uppercase tracking-[0.24em] text-slate-400">
            Nexus Physical Topology Vision 2026
          </p>
        </div>
        <div className="rounded-lg border border-cyan-400/25 bg-slate-950/70 px-3 py-2 text-right">
          <p className="text-[10px] uppercase tracking-[0.18em] text-slate-500">Live Nodes</p>
          <p className="font-mono text-sm font-bold text-cyan-200">{liveNodes}/3 ONLINE</p>
        </div>
      </div>

      <div className="relative z-10 h-[760px] w-full overflow-hidden rounded-2xl border border-slate-800 bg-black md:h-[640px]">
        <svg className="pointer-events-none absolute inset-0 z-10 h-full w-full" viewBox="0 0 1200 640">
          <defs>
            <linearGradient id="fiberLeft" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="#f59e0b" stopOpacity="0.9" />
              <stop offset="55%" stopColor="#67e8f9" stopOpacity="0.95" />
              <stop offset="100%" stopColor="#a855f7" stopOpacity="0.25" />
            </linearGradient>
            <linearGradient id="fiberRight" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="#f59e0b" stopOpacity="0.9" />
              <stop offset="50%" stopColor="#67e8f9" stopOpacity="1" />
              <stop offset="100%" stopColor="#06b6d4" stopOpacity="0.28" />
            </linearGradient>
          </defs>

          <path
            d="M 530 300 C 690 270, 820 255, 935 230"
            fill="none"
            stroke={masterOnline && gamingOnline ? "url(#fiberLeft)" : "rgba(100,116,139,0.42)"}
            strokeWidth={masterOnline && gamingOnline ? 6 : 3}
            strokeLinecap="round"
            className={masterOnline && gamingOnline ? "animate-[fiberFlow_1.8s_linear_infinite]" : ""}
            strokeDasharray={masterOnline && gamingOnline ? "14 18" : "8 10"}
          />
          <path
            d="M 530 320 C 705 320, 850 330, 955 360"
            fill="none"
            stroke={masterOnline && hpOnline ? "url(#fiberRight)" : "rgba(100,116,139,0.42)"}
            strokeWidth={masterOnline && hpOnline ? 6 : 3}
            strokeLinecap="round"
            className={masterOnline && hpOnline ? "animate-[fiberFlow_1.8s_linear_infinite]" : ""}
            strokeDasharray={masterOnline && hpOnline ? "14 18" : "8 10"}
          />
        </svg>

        <motion.div
          initial={{ opacity: 0.7, y: 0 }}
          animate={{ opacity: 1, y: [0, -6, 0] }}
          transition={{ duration: 6, repeat: Infinity, ease: "easeInOut" }}
          className={`absolute left-5 top-20 z-20 h-[420px] w-[65%] rounded-3xl border border-slate-700/60 p-6 shadow-[0_30px_70px_rgba(0,0,0,0.65)] md:left-8 ${
            masterOnline ? "bg-gradient-to-br from-zinc-100/95 via-zinc-300/75 to-zinc-200/65" : "bg-slate-700/50"
          }`}
        >
          <div className="pointer-events-none absolute inset-x-8 bottom-5 h-4 rounded-full bg-black/40 blur-md" />
          <div className="mb-4 flex items-center justify-between">
            <p className="text-xs font-bold uppercase tracking-[0.22em] text-slate-900">Main Station - White Desk</p>
            <p className="text-[11px] font-mono font-semibold text-slate-700">MASTER / ASUS VG10Q 27"</p>
          </div>

          <div className="grid grid-cols-[1.1fr_0.9fr] gap-4">
            <div className="relative h-[300px] rounded-2xl border border-cyan-300/30 bg-slate-950/80 p-4">
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-xs font-bold uppercase tracking-[0.18em] text-cyan-200">Crystal Clear Gaming Case</p>
                  <p className="mt-1 text-[11px] font-mono text-slate-400">ASUS Z490-WIFI • RTX 3080 TUF</p>
                </div>
                <Cpu className="h-5 w-5 text-cyan-300" />
              </div>

              <div className="relative mt-3 h-[220px] rounded-xl border border-cyan-500/25 bg-black/80">
                {FAN_POSITIONS.map((position, index) => (
                  <motion.div
                    key={position}
                    className={`absolute ${position} h-12 w-12 rounded-full border border-cyan-300/35 bg-slate-900/90`}
                    animate={{ boxShadow: ["0 0 6px rgba(34,211,238,0.35)", "0 0 20px rgba(168,85,247,0.6)", "0 0 10px rgba(14,165,233,0.55)"] }}
                    transition={{ duration: 2.2 + index * 0.12, repeat: Infinity, ease: "linear" }}
                  >
                    <div className="grid h-full w-full place-items-center text-cyan-200/80">
                      <div className="h-3 w-3 rounded-full bg-cyan-200/80" />
                    </div>
                  </motion.div>
                ))}
              </div>
              {!masterOnline && <div className="absolute inset-0 rounded-2xl bg-slate-600/55 backdrop-blur-[1px]" />}
            </div>

            <div className="space-y-3">
              <div className="rounded-xl border border-cyan-400/20 bg-black/70 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-cyan-200">Peripherals</p>
                <div className="mt-2 space-y-2 text-[11px] text-slate-300">
                  <div className="flex items-center gap-2"><Keyboard className="h-4 w-4 text-cyan-300" /> SteelSeries Apex Pro</div>
                  <div className="flex items-center gap-2"><Mouse className="h-4 w-4 text-purple-300" /> Razer Basilisk + Dock</div>
                  <div className="flex items-center gap-2"><Monitor className="h-4 w-4 text-amber-300" /> ASUS VG10Q 27"</div>
                </div>
              </div>

              <div className="rounded-xl border border-emerald-400/20 bg-black/75 p-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-emerald-200">Charging Station</p>
                <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-slate-200">
                  <motion.span
                    className="inline-flex items-center gap-1 rounded-md bg-emerald-400/10 px-2 py-1"
                    animate={{ boxShadow: ["0 0 0 rgba(16,185,129,0)", "0 0 12px rgba(16,185,129,0.55)", "0 0 0 rgba(16,185,129,0)"] }}
                    transition={{ duration: 1.8, repeat: Infinity }}
                  >
                    <Smartphone className="h-4 w-4 text-emerald-300" /> iPhone 17 Pro
                  </motion.span>
                  <span className="inline-flex items-center gap-1 rounded-md bg-slate-700/60 px-2 py-1"><Watch className="h-4 w-4 text-amber-200" /> Rolex</span>
                  <span className="inline-flex items-center gap-1 rounded-md bg-slate-700/60 px-2 py-1"><Headphones className="h-4 w-4 text-cyan-200" /> AirPods</span>
                </div>
              </div>
            </div>
          </div>
        </motion.div>

        <motion.div
          initial={{ opacity: 0.8 }}
          animate={{ opacity: 1, y: [0, -4, 0] }}
          transition={{ duration: 4.4, repeat: Infinity, ease: "easeInOut" }}
          className="absolute right-7 top-[180px] z-20 h-[320px] w-[31%] rounded-full border border-cyan-200/25 bg-gradient-to-br from-slate-100/25 to-slate-300/5 p-4 backdrop-blur-sm"
        >
          <div className="mb-2 text-center text-[11px] font-bold uppercase tracking-[0.2em] text-cyan-100">
            Secondary Station - Round Glass Table
          </div>
          <div className="grid h-[260px] grid-cols-2 gap-3 rounded-full border border-cyan-100/15 bg-slate-950/35 p-4">
            <div className="relative rounded-2xl border border-purple-400/35 bg-black/70 p-3">
              <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-purple-200">Gaming Beast</p>
              <p className="mt-1 text-[10px] font-mono text-slate-400">High Performance Node</p>
              <div className="mt-3 rounded-lg border border-purple-400/25 bg-slate-900/90 p-2">
                <div className="mb-2 h-8 rounded bg-gradient-to-r from-slate-800 to-slate-700" />
                <motion.div
                  className="h-3 rounded bg-gradient-to-r from-purple-500/65 via-pink-500/70 to-cyan-400/70"
                  animate={{ opacity: [0.45, 1, 0.5] }}
                  transition={{ duration: 1.2, repeat: Infinity }}
                />
              </div>
              {!gamingOnline && <div className="absolute inset-0 rounded-2xl bg-slate-600/55 backdrop-blur-[1px]" />}
            </div>

            <div className="relative rounded-2xl border border-cyan-400/35 bg-black/70 p-3">
              <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-cyan-200">HP Silver</p>
              <p className="mt-1 text-[10px] font-mono text-slate-400">Office/Home Node</p>
              <div className="mt-3 rounded-lg border border-cyan-400/25 bg-gradient-to-br from-slate-300/35 to-slate-100/15 p-2">
                <div className="mb-2 h-8 rounded bg-slate-800/80" />
                <div className="h-3 rounded bg-cyan-400/55" />
              </div>
              {!hpOnline && <div className="absolute inset-0 rounded-2xl bg-slate-600/55 backdrop-blur-[1px]" />}
            </div>
          </div>
        </motion.div>

        <div className="absolute inset-x-4 bottom-4 z-30 grid gap-2 md:grid-cols-3">
          <NodeBadge title="Master Node" node={master} ringColor="#f59e0b" glow="rgba(245,158,11,0.75)" power={50} />
          <NodeBadge title="Gaming Beast" node={gamingWorker} ringColor="#a855f7" glow="rgba(168,85,247,0.8)" power={95} />
          <NodeBadge title="HP Silver" node={hpWorker} ringColor="#06b6d4" glow="rgba(6,182,212,0.75)" power={90} />
        </div>

        <div className="absolute left-4 top-4 z-30 rounded-lg border border-emerald-400/25 bg-slate-950/70 px-3 py-2 text-xs text-emerald-200">
          <p className="font-mono uppercase tracking-[0.18em]">Execution Feed</p>
          <p className="mt-1 inline-flex items-center gap-1 text-[11px] text-slate-300">
            <Clock4 className="h-3.5 w-3.5 text-emerald-300" />
            {cluster?.timestamp ?? "awaiting cluster heartbeat"}
          </p>
        </div>
      </div>

      <style>{`
        @keyframes fiberFlow {
          0% { stroke-dashoffset: 60; }
          100% { stroke-dashoffset: 0; }
        }
      `}</style>
    </div>
  );
}
