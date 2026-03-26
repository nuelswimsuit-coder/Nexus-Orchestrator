"use client";

import { useState, useEffect, useRef } from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { useNexus } from "@/lib/nexus-context";
import { useI18n } from "@/lib/i18n";
import type {
  ClusterStatusResponse,
  EngineStateResponse,
  EngineStateValue,
  FactoryActiveResponse,
  NodeStatus,
  QueueStats,
  ReportStatusResponse,
  ScrapeStatusResponse,
} from "@/lib/api";

// ─────────────────────────────────────────────────────────────────────────────
// RGB colour system — all glow values run through stealth-aware helpers
// ─────────────────────────────────────────────────────────────────────────────

const C_ON  = "#00ff88";
const C_OFF = "#ff2244";
const C_ON_DIM  = "#00cc66";
const C_OFF_DIM = "#cc1133";

// Engine-state override colours
const ENGINE_COLORS: Record<EngineStateValue, string | null> = {
  idle:        null,           // use normal online/offline colour
  calculating: "#6366f1",      // Deep Indigo
  dispatching: "#f59e0b",      // Gold/Yellow
  warning:     "#ef4444",      // Red
};

// Neon Blue — used when the daily profit report is being sent
const REPORT_BLUE = "#00b4ff";

const rgb  = (on: boolean) => on ? C_ON  : C_OFF;
const rgbD = (on: boolean) => on ? C_ON_DIM : C_OFF_DIM;

// Purple — used when a node is receiving a code deployment
const DEPLOY_PURPLE = "#a855f7";

function useRgb(
  on: boolean,
  engineState?: EngineStateValue,
  reportSending?: boolean,
  deploying?: boolean,
) {
  const { stealth } = useStealth();
  // Priority: deploying (purple) > reportSending (blue) > engineState
  const engineOverride = deploying
    ? DEPLOY_PURPLE
    : reportSending
    ? REPORT_BLUE
    : engineState
    ? ENGINE_COLORS[engineState]
    : null;
  const c  = engineOverride ?? rgb(on);
  const cd = engineOverride ?? rgbD(on);
  const glow  = (px = 16) => stealth ? "none" : `0 0 ${px}px ${c}, 0 0 ${px*2}px ${c}44`;
  const glowS = (px = 10) => stealth ? "none" : `drop-shadow(0 0 ${px}px ${c}88)`;
  const border = stealth ? "#1e293b" : `${c}44`;
  const boxShadow = (px = 20) => stealth ? "none" : `0 0 ${px}px ${c}22`;
  return { c, cd, glow, glowS, border, boxShadow, stealth };
}

// ─────────────────────────────────────────────────────────────────────────────
// 60-second CPU sparkline (one sample per SWR refresh ~10 s → 6 samples/min)
// ─────────────────────────────────────────────────────────────────────────────

const MAX_SAMPLES = 60;

function useCpuHistory(nodeId: string, current: number) {
  const ref = useRef<Map<string, number[]>>(new Map());
  if (!ref.current.has(nodeId)) ref.current.set(nodeId, []);
  const history = ref.current.get(nodeId)!;

  useEffect(() => {
    const next = [...history.slice(-(MAX_SAMPLES - 1)), current];
    ref.current.set(nodeId, next);
  });

  return ref.current.get(nodeId) ?? [];
}

function Sparkline({
  nodeId,
  percent,
  online,
  width = 120,
  height = 40,
}: {
  nodeId: string;
  percent: number;
  online: boolean;
  width?: number;
  height?: number;
}) {
  const { stealth } = useStealth();
  const history = useCpuHistory(nodeId, percent);
  const c = rgb(online);
  const W = width, H = height, pad = 2;

  const pts =
    history.length < 2
      ? [[0, H / 2], [W, H / 2]]
      : history.map((v, i) => [
          pad + (i / (history.length - 1)) * (W - pad * 2),
          pad + (1 - v / 100) * (H - pad * 2),
        ]);

  const line = pts
    .map((p, i) => `${i === 0 ? "M" : "L"}${p[0].toFixed(1)},${p[1].toFixed(1)}`)
    .join(" ");

  const fill =
    [...pts, [pts[pts.length - 1][0], H], [pts[0][0], H]]
      .map((p, i) => `${i === 0 ? "M" : "L"}${p[0].toFixed(1)},${p[1].toFixed(1)}`)
      .join(" ") + "Z";

  const gradId = `sg-${nodeId.replace(/[^a-z0-9]/gi, "")}`;

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ overflow: "visible" }}>
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={c} stopOpacity={stealth ? 0.1 : 0.35} />
          <stop offset="100%" stopColor={c} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={fill} fill={`url(#${gradId})`} />
      <path
        d={line}
        fill="none"
        stroke={c}
        strokeWidth="1.5"
        style={stealth ? {} : { filter: `drop-shadow(0 0 3px ${c})` }}
      />
      {pts.length > 0 && (
        <circle
          cx={pts[pts.length - 1][0]}
          cy={pts[pts.length - 1][1]}
          r="2.5"
          fill={c}
          style={stealth ? {} : { filter: `drop-shadow(0 0 4px ${c})` }}
        />
      )}
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Hardware Health Modal
// ─────────────────────────────────────────────────────────────────────────────

function HardwareModal({ node, onClose }: { node: NodeStatus; onClose: () => void }) {
  const { c, glow, boxShadow, stealth } = useRgb(node.online);

  const tempStr = (node.cpu_temp_c != null && node.cpu_temp_c >= 0)
    ? `${node.cpu_temp_c.toFixed(1)} °C`
    : "N/A";
  const tempColor = (node.cpu_temp_c != null && node.cpu_temp_c >= 0)
    ? node.cpu_temp_c >= 85 ? "#ef4444"
      : node.cpu_temp_c >= 70 ? "#f59e0b"
      : "#22c55e"
    : "#475569";

  const rows: [string, string, string?][] = [
    ["Node ID",      node.node_id],
    ["Name",         node.display_name || node.node_id],
    ["Role",         node.role.toUpperCase()],
    ["Status",       node.online ? "ONLINE" : "OFFLINE"],
    ["IP Address",   node.local_ip ?? "—"],
    ["OS",           node.os_info ?? "—"],
    ["Motherboard",  node.motherboard ?? "N/A"],
    ["CPU",          node.cpu_model ?? "—"],
    ["CPU Temp",     tempStr, tempColor],
    ["GPU",          node.gpu_model ?? "N/A"],
    ["RAM Used",     `${node.ram_used_mb.toFixed(0)} / ${(node.ram_total_mb ?? 0).toFixed(0)} MB`],
    ["CPU Load",     `${node.cpu_percent.toFixed(1)}%`],
    ["Active Jobs",  String(node.active_jobs)],
    ["Last Seen",    new Date(node.last_seen).toLocaleTimeString()],
  ];

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      style={{ background: "rgba(2,6,23,0.88)", backdropFilter: "blur(10px)" }}
      onClick={onClose}
    >
      <div
        className="relative w-full max-w-xl rounded-2xl p-6 flex flex-col gap-4"
        style={{
          background: "linear-gradient(145deg, #0f172a, #0d1117)",
          border: `1px solid ${stealth ? "#1e293b" : `${c}55`}`,
          boxShadow: stealth ? "none" : `0 0 40px ${c}22, 0 0 80px ${c}11`,
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Top accent line */}
        <div
          className="absolute top-0 left-6 right-6 h-px rounded-full"
          style={{
            background: stealth
              ? "#1e293b"
              : `linear-gradient(90deg, transparent, ${c}, transparent)`,
          }}
        />

        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span
              className="rgb-led rounded-full shrink-0"
              style={{
                width: 10,
                height: 10,
                background: c,
                boxShadow: stealth ? "none" : glow(6),
                display: "block",
                animation: stealth ? "none" : "rgb-pulse 2s infinite",
              }}
            />
            <span
              className="rgb-text font-mono text-sm font-bold tracking-widest"
              style={{ color: c, textShadow: stealth ? "none" : `0 0 8px ${c}` }}
            >
              HARDWARE HEALTH
            </span>
          </div>
          <button
            onClick={onClose}
            className="rounded-lg px-3 py-1 font-mono text-xs"
            style={{ color: "#475569", border: "1px solid #1e293b" }}
          >
            ✕ CLOSE
          </button>
        </div>

        {/* CPU sparkline */}
        <div
          className="rounded-xl p-3"
          style={{ background: "#020617", border: `1px solid ${stealth ? "#0f172a" : `${c}22`}` }}
        >
          <div className="font-mono text-[9px] tracking-widest mb-1" style={{ color: "#475569" }}>
            CPU LOAD — LAST 60 s
          </div>
          <Sparkline nodeId={node.node_id} percent={node.cpu_percent} online={node.online} width={480} height={48} />
          <div className="font-mono text-xs font-bold mt-1" style={{ color: c }}>
            {node.cpu_percent.toFixed(1)}%
          </div>
        </div>

        {/* Spec table */}
        <div className="grid grid-cols-1 gap-0">
          {rows.map(([k, v, vc]) => (
            <div
              key={k}
              className="flex items-start justify-between gap-4 py-1.5 px-2 rounded"
              style={{ borderBottom: "1px solid #0f172a" }}
            >
              <span className="font-mono text-[10px] tracking-widest shrink-0" style={{ color: "#475569" }}>
                {k}
              </span>
              <span
                className="font-mono text-[11px] font-semibold text-right break-all"
                style={{ color: vc ?? "#94a3b8", wordBreak: "break-word" }}
              >
                {v}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Floating Hardware HUD
// ─────────────────────────────────────────────────────────────────────────────

const ENGINE_STATE_LABELS: Record<EngineStateValue, string> = {
  idle:        "",
  calculating: "◈ CALCULATING...",
  dispatching: "⚡ DISPATCHING",
  warning:     "⚠ NEEDS APPROVAL",
};

function NodeHud({
  node,
  onOpen,
  scrapeStatus,
  engineState,
  contentFactoryActive,
  reportSending,
  deploying,
}: {
  node: NodeStatus;
  onOpen: () => void;
  scrapeStatus?: ScrapeStatusResponse;
  engineState?: EngineStateValue;
  contentFactoryActive?: boolean;
  reportSending?: boolean;
  deploying?: boolean;
}) {
  const isMaster = node.role === "master";
  const activeEngine = isMaster && engineState && engineState !== "idle"
    ? engineState
    : undefined;
  const { c, glow, stealth } = useRgb(
    node.online,
    activeEngine,
    isMaster ? reportSending : undefined,
    deploying,
  );
  const isScanning =
    isMaster &&
    (scrapeStatus?.status === "running" || scrapeStatus?.status === "pending");
  const engineLabel = isMaster && engineState ? ENGINE_STATE_LABELS[engineState] : "";
  const showThinking = isMaster && contentFactoryActive;

  const tempC = node.cpu_temp_c;
  const hasTempData = tempC != null && tempC >= 0;
  const tempColor = hasTempData
    ? tempC >= 85 ? "#ef4444"
      : tempC >= 70 ? "#f59e0b"
      : "#22c55e"
    : "#334155";
  const tempLabel = hasTempData ? `${tempC.toFixed(1)}°C` : "N/A";

  const machineName = node.display_name || node.node_id;

  return (
    <button
      onClick={onOpen}
      className="rgb-glow flex flex-col gap-1 rounded-xl px-3 py-2.5 text-left w-full transition-all duration-200"
      style={{
        background: "rgba(2,6,23,0.9)",
        border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
        boxShadow: stealth ? "none" : `0 0 14px ${c}18`,
        cursor: "pointer",
        minWidth: "160px",
      }}
      title="Click for Hardware Health"
    >
      {/* LED + machine name */}
      <div className="flex items-center gap-2">
        <span
          className="rgb-led shrink-0 rounded-full"
          style={{
            width: 7,
            height: 7,
            background: c,
            boxShadow: stealth ? "none" : glow(5),
            display: "block",
            animation: stealth ? "none" : "rgb-pulse 2s infinite",
          }}
        />
        <span
          className="rgb-text font-mono text-[10px] font-bold tracking-widest truncate"
          style={{ color: c, textShadow: stealth ? "none" : `0 0 6px ${c}` }}
        >
          {machineName.toUpperCase()}
        </span>
      </div>

      {/* [IP] */}
      <div className="font-mono text-[9px] truncate" style={{ color: "#475569" }}>
        <span style={{ color: "#334155" }}>[IP]</span>{" "}
        {node.local_ip ?? "—"}
      </div>

      {/* [CPU] */}
      <div className="font-mono text-[9px] truncate" style={{ color: "#475569" }}>
        <span style={{ color: "#334155" }}>[CPU]</span>{" "}
        {node.cpu_model ?? "—"} @{" "}
        <span style={{ color: stealth ? "#475569" : c }}>{node.cpu_percent.toFixed(0)}%</span>
      </div>

      {/* [TEMP] */}
      <div className="font-mono text-[9px] truncate" style={{ color: "#475569" }}>
        <span style={{ color: "#334155" }}>[TEMP]</span>{" "}
        <span style={{ color: stealth ? "#475569" : tempColor }}>{tempLabel}</span>
      </div>

      {/* [GPU] */}
      <div className="font-mono text-[9px] truncate" style={{ color: "#475569" }}>
        <span style={{ color: "#334155" }}>[GPU]</span>{" "}
        {node.gpu_model ?? "N/A"}
      </div>

      {/* [MB] motherboard */}
      <div className="font-mono text-[9px] truncate" style={{ color: "#334155" }}>
        <span>[MB]</span>{" "}
        <span style={{ color: "#475569" }}>{node.motherboard ?? "N/A"}</span>
      </div>

      {/* [OS] */}
      <div className="font-mono text-[9px] truncate" style={{ color: "#334155" }}>
        <span>[OS]</span> {node.os_info ?? "—"}
      </div>

      {/* Scanning indicator — always visible, even in stealth mode */}
      {isScanning && (
        <div
          className="flex items-center gap-1.5 rounded px-2 py-1 mt-0.5"
          style={{
            background: stealth ? "#0f172a" : "#6366f115",
            border: `1px solid ${stealth ? "#1e293b" : "#6366f144"}`,
          }}
        >
          <span
            className="rounded-full shrink-0"
            style={{
              width: 5,
              height: 5,
              background: "#6366f1",
              display: "inline-block",
              // Pulse animation always on for scanning — regardless of stealth
              animation: "rgb-pulse 0.8s ease-in-out infinite",
            }}
          />
          <span className="font-mono text-[8px] font-bold tracking-widest" style={{ color: "#6366f1" }}>
            SCANNING...
          </span>
        </div>
      )}

      {/* Mini metrics */}
      <div className="flex gap-3 mt-1">
        {[
          ["RAM", `${node.ram_used_mb.toFixed(0)}M`],
          ["JOBS", String(node.active_jobs)],
          ["ROLE", node.role.slice(0, 3).toUpperCase()],
        ].map(([k, v]) => (
          <div key={k} className="flex flex-col">
            <span className="text-[7px] tracking-widest" style={{ color: "#1e293b" }}>{k}</span>
            <span className="font-mono text-[9px] font-bold" style={{ color: "#475569" }}>{v}</span>
          </div>
        ))}
      </div>

      {/* Report sending indicator — Neon Blue flash */}
      {isMaster && reportSending && (
        <div
          className="flex items-center gap-1.5 rounded px-2 py-1 mt-0.5"
          style={{
            background: stealth ? "#0f172a" : "#00b4ff12",
            border: `1px solid ${stealth ? "#1e293b" : "#00b4ff44"}`,
          }}
        >
          <span
            className="rounded-full shrink-0"
            style={{
              width: 5,
              height: 5,
              background: REPORT_BLUE,
              display: "inline-block",
              animation: "rgb-pulse 0.5s ease-in-out infinite",
              boxShadow: stealth ? "none" : `0 0 6px ${REPORT_BLUE}`,
            }}
          />
          <span
            className="font-mono text-[8px] font-bold tracking-widest"
            style={{ color: stealth ? "#334155" : REPORT_BLUE }}
          >
            📊 SENDING REPORT...
          </span>
        </div>
      )}

      {/* Content Factory "Thinking" indicator */}
      {showThinking && (
        <div
          className="flex items-center gap-1.5 rounded px-2 py-1 mt-0.5"
          style={{
            background: stealth ? "#0f172a" : "#6366f112",
            border: `1px solid ${stealth ? "#1e293b" : "#6366f144"}`,
          }}
        >
          <span
            className="rounded-full shrink-0"
            style={{
              width: 5,
              height: 5,
              background: "#6366f1",
              display: "inline-block",
              animation: "rgb-pulse 0.6s ease-in-out infinite",
              boxShadow: stealth ? "none" : "0 0 5px #6366f1",
            }}
          />
          <span
            className="font-mono text-[8px] font-bold tracking-widest"
            style={{ color: stealth ? "#334155" : "#6366f1" }}
          >
            🤖 THINKING...
          </span>
        </div>
      )}

      {/* Engine state indicator — always visible for master */}
      {engineLabel && (
        <div
          className="flex items-center gap-1.5 rounded px-2 py-1 mt-0.5"
          style={{
            background: stealth ? "#0f172a" : `${c}12`,
            border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
          }}
        >
          <span
            className="rounded-full shrink-0"
            style={{
              width: 5,
              height: 5,
              background: stealth ? "#334155" : c,
              display: "inline-block",
              animation: "rgb-pulse 0.8s ease-in-out infinite",
              boxShadow: stealth ? "none" : `0 0 5px ${c}`,
            }}
          />
          <span
            className="font-mono text-[8px] font-bold tracking-widest"
            style={{ color: stealth ? "#334155" : c }}
          >
            {engineLabel}
          </span>
        </div>
      )}

      {/* Deploy indicator — purple pulse when receiving update */}
      {deploying && (
        <div
          className="flex items-center gap-1.5 rounded px-2 py-1 mt-0.5"
          style={{
            background: stealth ? "#0f172a" : "#a855f712",
            border: `1px solid ${stealth ? "#1e293b" : "#a855f744"}`,
          }}
        >
          <span
            className="rounded-full shrink-0"
            style={{
              width: 5,
              height: 5,
              background: DEPLOY_PURPLE,
              display: "inline-block",
              animation: "rgb-pulse 0.6s ease-in-out infinite",
              boxShadow: stealth ? "none" : `0 0 6px ${DEPLOY_PURPLE}`,
            }}
          />
          <span
            className="font-mono text-[8px] font-bold tracking-widest"
            style={{ color: stealth ? "#334155" : DEPLOY_PURPLE }}
          >
            ⬆ DEPLOYING UPDATE...
          </span>
        </div>
      )}

      <div className="font-mono text-[8px] tracking-widest mt-0.5" style={{ color: `${c}55` }}>
        ▸ HARDWARE DETAILS
      </div>
    </button>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Isometric Gaming PC Tower — glass chassis with internal components
// ─────────────────────────────────────────────────────────────────────────────

function IsometricPC({
  online,
  engineState,
  reportSending,
}: {
  online: boolean;
  engineState?: EngineStateValue;
  reportSending?: boolean;
}) {
  const { c, cd, stealth } = useRgb(online, engineState, reportSending);
  const sf = stealth ? "none" : `drop-shadow(0 0 12px ${c}88)`;

  return (
    <svg
      viewBox="0 0 150 230"
      width="150"
      height="230"
      aria-label="Gaming PC Tower"
      style={{ filter: sf, overflow: "visible" }}
    >
      <defs>
        {/* Tempered glass gradient — left-to-right highlight */}
        <linearGradient id="tg-glass" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%"   stopColor="#ffffff" stopOpacity="0.22" />
          <stop offset="40%"  stopColor="#ffffff" stopOpacity="0.06" />
          <stop offset="100%" stopColor="#ffffff" stopOpacity="0" />
        </linearGradient>
        {/* Inner glow for RGB strips */}
        <filter id="rgb-inner" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>
      </defs>

      {/* ── Isometric body ── */}
      {/* Front face */}
      <polygon points="25,65 115,65 115,210 25,210"
        fill="#080d18" stroke={stealth ? "#1e293b" : c} strokeWidth="1.2" />
      {/* Top face */}
      <polygon points="15,55 105,55 115,65 25,65"
        fill="#1a2236" stroke={stealth ? "#1e293b" : c} strokeWidth="1" />
      {/* Right depth face */}
      <polygon points="115,65 125,55 125,200 115,210"
        fill="#060b14" stroke={stealth ? "#1e293b" : c} strokeWidth="0.8" />

      {/* ── Tempered glass panel (front) — shows internals ── */}
      <rect x="29" y="69" width="82" height="137" rx="3"
        fill="url(#tg-glass)" />
      {/* Glass edge highlight */}
      <rect x="29" y="69" width="82" height="137" rx="3"
        fill="none" stroke="#ffffff" strokeWidth="0.4" opacity="0.12" />
      {/* Second glass reflection streak */}
      <rect x="31" y="71" width="14" height="133" rx="2"
        fill="#ffffff" opacity="0.04" />

      {/* ── GPU (visible through glass) ── */}
      <rect x="33" y="76" width="70" height="24" rx="2"
        fill="#111827" stroke={stealth ? "#1e293b" : cd} strokeWidth="0.8" />
      <rect x="37" y="80" width="44" height="7" rx="1.5" fill="#0a0f1e" />
      {/* GPU fans */}
      {[48, 63, 78].map((cx) => (
        <g key={cx}>
          <circle cx={cx} cy={88} r={6.5} fill="#0a0f1e"
            stroke={stealth ? "#1e293b" : cd} strokeWidth="0.7" />
          <circle cx={cx} cy={88} r={2.5} fill={stealth ? "#1e293b" : cd} opacity="0.7" />
          {[0,60,120,180,240,300].map((deg) => {
            const r = (deg * Math.PI) / 180;
            return <line key={deg} x1={cx} y1={88}
              x2={cx + Math.cos(r) * 5} y2={88 + Math.sin(r) * 5}
              stroke={stealth ? "#1e293b" : cd} strokeWidth="0.5" opacity="0.5" />;
          })}
        </g>
      ))}

      {/* ── RAM sticks (4 × visible through glass) ── */}
      {[33, 44, 55, 66].map((x, i) => (
        <g key={i}>
          <rect x={x} y="106" width="8" height="36" rx="1"
            fill="#111827" stroke={stealth ? "#1e293b" : c} strokeWidth="0.5" />
          {/* RGB cap on each RAM stick */}
          {!stealth && (
            <rect x={x + 1} y="108" width="6" height="3" rx="0.5"
              fill={c} opacity="0.9"
              style={{ filter: `drop-shadow(0 0 3px ${c})` }} />
          )}
        </g>
      ))}

      {/* ── Motherboard PCB outline ── */}
      <rect x="31" y="104" width="80" height="62" rx="2"
        fill="none" stroke={stealth ? "#0f172a" : "#1a2a1a"} strokeWidth="0.6" />

      {/* ── Case fans (large, centre) ── */}
      {[152, 183].map((cy) => (
        <g key={cy}>
          <circle cx="70" cy={cy} r="15" fill="#080d18"
            stroke={stealth ? "#1e293b" : c} strokeWidth="1"
            style={stealth ? {} : { filter: `drop-shadow(0 0 4px ${c})` }} />
          <circle cx="70" cy={cy} r="5" fill={stealth ? "#1e293b" : c} opacity={stealth ? 0.3 : 0.65} />
          {[0,45,90,135,180,225,270,315].map((deg) => {
            const r = (deg * Math.PI) / 180;
            return <line key={deg} x1={70} y1={cy}
              x2={70 + Math.cos(r) * 12} y2={cy + Math.sin(r) * 12}
              stroke={stealth ? "#1e293b" : c} strokeWidth="0.7" opacity="0.55" />;
          })}
          {/* Fan RGB ring */}
          {!stealth && (
            <circle cx="70" cy={cy} r="13" fill="none" stroke={c}
              strokeWidth="1.2" strokeDasharray="3 3" opacity="0.45"
              style={{ filter: `drop-shadow(0 0 3px ${c})` }} />
          )}
        </g>
      ))}

      {/* ── Vertical RGB strips (left & right inside case) ── */}
      {!stealth && [31, 103].map((x) => (
        <rect key={x} x={x} y="71" width="3" height="134" rx="1.5"
          fill={c} opacity="0.5"
          style={{ filter: `drop-shadow(0 0 7px ${c})` }} />
      ))}

      {/* ── Front panel ── */}
      <circle cx="35" cy="205" r="3" fill={stealth ? "#1e293b" : c}
        style={stealth ? {} : { filter: `drop-shadow(0 0 5px ${c})` }} />
      <rect x="42" y="203" width="20" height="4" rx="2"
        fill={stealth ? "#1e293b" : c} opacity={stealth ? 0.2 : 0.3} />
      <circle cx="104" cy="205" r="4.5" fill="#111827"
        stroke={stealth ? "#1e293b" : c} strokeWidth="0.8" />
      <circle cx="104" cy="205" r="2" fill={stealth ? "#1e293b" : c} opacity={stealth ? 0.3 : 0.85}
        style={stealth ? {} : { filter: `drop-shadow(0 0 4px ${c})` }} />

      {/* ── RGB bottom underglow ── */}
      {!stealth && (
        <rect x="27" y="208" width="88" height="3.5" rx="1.75"
          fill={c} opacity="0.7"
          style={{ filter: `drop-shadow(0 0 10px ${c})` }} />
      )}
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Curved Monitor — live CPU sparkline on screen
// ─────────────────────────────────────────────────────────────────────────────

function CurvedMonitor({
  node,
  engineState,
  reportSending,
}: {
  node: NodeStatus;
  engineState?: EngineStateValue;
  reportSending?: boolean;
}) {
  const { c, stealth } = useRgb(node.online, engineState, reportSending);
  const sf = stealth ? "none" : `drop-shadow(0 0 8px ${c}55)`;

  return (
    <svg viewBox="0 0 210 135" width="210" height="135" aria-label="Curved Monitor"
      style={{ filter: sf, overflow: "visible" }}>

      {/* Curved bezel */}
      <path d="M8,22 Q105,12 202,22 L202,112 Q105,122 8,112 Z"
        fill="#080d18" stroke={stealth ? "#1e293b" : c} strokeWidth="1.5" />

      {/* Screen */}
      <path d="M14,26 Q105,17 196,26 L196,108 Q105,117 14,108 Z"
        fill="#020617" />

      {/* Screen interior */}
      <path d="M18,30 Q105,22 192,30 L192,104 Q105,112 18,104 Z"
        fill="#040a14" />

      {/* Grid lines */}
      {[42, 56, 70, 84, 98].map((y) => (
        <line key={y} x1="22" y1={y} x2="188" y2={y}
          stroke={c} strokeWidth="0.3" opacity={stealth ? 0.04 : 0.1} />
      ))}
      {[50, 80, 105, 130, 160].map((x) => (
        <line key={x} x1={x} y1="32" x2={x} y2="102"
          stroke={c} strokeWidth="0.3" opacity={stealth ? 0.04 : 0.1} />
      ))}

      {/* CPU sparkline on screen */}
      <g transform="translate(22, 32)">
        <text x="0" y="-2" fontSize="5" fontFamily="monospace"
          fill={c} opacity={stealth ? 0.2 : 0.5}>
          CPU LOAD — 60s
        </text>
        <Sparkline
          nodeId={`${node.node_id}-monitor`}
          percent={node.cpu_percent}
          online={node.online}
          width={166}
          height={58}
        />
      </g>

      {/* Current % readout */}
      <text x="188" y="98" textAnchor="end" fontSize="7"
        fontFamily="monospace" fill={c} opacity={stealth ? 0.2 : 0.6} fontWeight="bold">
        {node.cpu_percent.toFixed(1)}%
      </text>

      {/* NEXUS watermark */}
      <text x="105" y="107" textAnchor="middle" fontSize="6"
        fontFamily="monospace" fill={c} opacity={stealth ? 0.05 : 0.18}
        fontWeight="bold" letterSpacing="4">
        NEXUS ORCHESTRATOR
      </text>

      {/* RGB bottom bezel */}
      {!stealth && (
        <path d="M10,111 Q105,121 200,111 L200,114 Q105,124 10,114 Z"
          fill={c} opacity="0.65"
          style={{ filter: `drop-shadow(0 0 6px ${c})` }} />
      )}

      {/* Stand */}
      <rect x="96" y="114" width="18" height="13" rx="2" fill="#111827" />
      <rect x="72" y="126" width="66" height="6" rx="3" fill="#111827"
        stroke={stealth ? "#1e293b" : c} strokeWidth="0.5" />
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Mechanical Keyboard
// ─────────────────────────────────────────────────────────────────────────────

function Keyboard({ online }: { online: boolean }) {
  const { c, stealth } = useRgb(online);
  return (
    <svg viewBox="0 0 155 52" width="155" height="52" aria-label="Mechanical Keyboard"
      style={{ filter: stealth ? "none" : `drop-shadow(0 0 6px ${c}66)` }}>
      <rect x="2" y="2" width="151" height="48" rx="6"
        fill="#080d18" stroke={stealth ? "#1e293b" : c} strokeWidth="1.2" />
      {[[6,8,15],[6,21,14],[6,34,13]].map(([sx,y,n], row) =>
        Array.from({length:n}).map((_,i) => (
          <rect key={`${row}-${i}`} x={sx+i*9.8} y={y} width="7.8" height="9" rx="1.5"
            fill="#111827" stroke={stealth ? "#1e293b" : c} strokeWidth="0.4" opacity="0.9" />
        ))
      )}
      <rect x="36" y="40" width="83" height="9" rx="2"
        fill="#111827" stroke={stealth ? "#1e293b" : c} strokeWidth="0.4" />
      {/* Per-key RGB highlights */}
      {!stealth && [[6,8],[36,8],[66,8],[96,21],[46,34]].map(([x,y],i) => (
        <rect key={i} x={x} y={y} width="7.8" height="9" rx="1.5"
          fill={c} opacity="0.4"
          style={{ filter: `drop-shadow(0 0 3px ${c})` }} />
      ))}
      {/* RGB underglow */}
      {!stealth && (
        <rect x="4" y="46" width="147" height="2.5" rx="1.25"
          fill={c} opacity="0.7"
          style={{ filter: `drop-shadow(0 0 6px ${c})` }} />
      )}
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Mouse
// ─────────────────────────────────────────────────────────────────────────────

function Mouse({ online }: { online: boolean }) {
  const { c, stealth } = useRgb(online);
  return (
    <svg viewBox="0 0 42 64" width="42" height="64" aria-label="Gaming Mouse"
      style={{ filter: stealth ? "none" : `drop-shadow(0 0 6px ${c}55)` }}>
      <path d="M5,23 Q5,5 21,5 Q37,5 37,23 L37,50 Q37,60 21,60 Q5,60 5,50 Z"
        fill="#080d18" stroke={stealth ? "#1e293b" : c} strokeWidth="1.2" />
      <rect x="18" y="12" width="6" height="14" rx="3"
        fill="#111827" stroke={stealth ? "#1e293b" : c} strokeWidth="0.8" />
      <line x1="21" y1="5" x2="21" y2="31"
        stroke={stealth ? "#1e293b" : c} strokeWidth="0.6" opacity="0.5" />
      {!stealth && (
        <path d="M5,34 Q5,57 21,60" fill="none" stroke={c} strokeWidth="2.5" opacity="0.7"
          style={{ filter: `drop-shadow(0 0 4px ${c})` }} />
      )}
      <circle cx="21" cy="42" r="3.5" fill={stealth ? "#1e293b" : c} opacity={stealth ? 0.2 : 0.65}
        style={stealth ? {} : { filter: `drop-shadow(0 0 5px ${c})` }} />
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Laptop
// ─────────────────────────────────────────────────────────────────────────────

function Laptop({ node, variant }: { node: NodeStatus; variant: "gaming" | "standard" }) {
  const { c, stealth } = useRgb(node.online);
  const isGaming = variant === "gaming";
  const sf = stealth ? "none" : `drop-shadow(0 0 ${isGaming ? 10 : 5}px ${c}${isGaming ? "88" : "44"})`;

  return (
    <svg viewBox="0 0 168 115" width="168" height="115" aria-label={`${variant} laptop`}
      style={{ filter: sf }}>
      {/* Lid */}
      <rect x="8" y="4" width="152" height="84" rx="7"
        fill="#080d18" stroke={stealth ? "#1e293b" : c} strokeWidth={isGaming ? 1.8 : 1} />
      {/* Screen */}
      <rect x="14" y="10" width="140" height="72" rx="3" fill="#020617" />
      <rect x="18" y="14" width="132" height="64" rx="2" fill="#040a14" />

      {isGaming ? (
        <>
          {[0,20,40,60,80,100].map((o) => (
            <line key={o} x1={18+o} y1={14} x2={18+o-40} y2={78}
              stroke={c} strokeWidth="1" opacity={stealth ? 0.03 : 0.07} />
          ))}
          <g transform="translate(22, 18)">
            <Sparkline
              nodeId={`${node.node_id}-laptop`}
              percent={node.cpu_percent}
              online={node.online}
              width={124}
              height={44}
            />
          </g>
          <text x="84" y="72" textAnchor="middle" fontSize="7"
            fontFamily="monospace" fill={c} opacity={stealth ? 0.15 : 0.45} fontWeight="bold">
            {node.node_id.toUpperCase()}
          </text>
          {/* RGB lid edge */}
          {!stealth && (
            <rect x="10" y="86" width="148" height="3" rx="1.5"
              fill={c} opacity="0.8"
              style={{ filter: `drop-shadow(0 0 6px ${c})` }} />
          )}
        </>
      ) : (
        <>
          {[22,34,46,58,66].map((y) => (
            <rect key={y} x="22" y={y} width={58 + (y%20)} height="1.5"
              rx="0.75" fill="#1e293b" opacity="0.5" />
          ))}
          <text x="84" y="60" textAnchor="middle" fontSize="7"
            fontFamily="monospace" fill="#334155" opacity="0.7">
            {node.node_id}
          </text>
        </>
      )}

      <circle cx="84" cy="8" r="2.5"
        fill={isGaming && !stealth ? c : "#1e293b"} opacity={isGaming && !stealth ? 0.8 : 0.4} />

      {/* Base */}
      <rect x="6" y="88" width="156" height="18" rx="4"
        fill="#080d18" stroke={stealth ? "#1e293b" : isGaming ? c : "#1e293b"}
        strokeWidth={isGaming ? 1.2 : 0.8} />

      {/* Keys */}
      {Array.from({length:14}).map((_,i) => (
        <rect key={i} x={12+i*10.3} y={91} width="8" height="6" rx="1.2"
          fill={isGaming ? "#111827" : "#0a0f1e"}
          stroke={stealth ? "#0f172a" : isGaming ? c : "#1e293b"}
          strokeWidth="0.4" opacity="0.85" />
      ))}

      {/* Hinge */}
      <rect x="6" y="87" width="156" height="3" rx="1"
        fill={!stealth && isGaming ? c : "#111827"}
        opacity={!stealth && isGaming ? 0.55 : 1}
        style={!stealth && isGaming ? { filter: `drop-shadow(0 0 4px ${c})` } : {}} />
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Desk surface
// ─────────────────────────────────────────────────────────────────────────────

function Desk({
  children,
  label,
  online,
  wide,
  engineState,
  reportSending,
  deploying,
}: {
  children: React.ReactNode;
  label: string;
  online: boolean;
  wide?: boolean;
  engineState?: EngineStateValue;
  reportSending?: boolean;
  deploying?: boolean;
}) {
  const { c, stealth } = useRgb(online, engineState, reportSending, deploying);
  return (
    <div className={`flex flex-col items-center gap-3 ${wide ? "w-full max-w-xl" : "w-full max-w-sm"}`}>
      <span
        className="rgb-text font-mono text-[10px] tracking-[0.2em] font-bold uppercase"
        style={{
          color: stealth ? "#334155" : c,
          textShadow: stealth ? "none" : `0 0 10px ${c}`,
        }}
      >
        {label}
      </span>

      <div
        className="rgb-glow relative w-full rounded-2xl flex flex-col items-center gap-4 px-5 pt-6 pb-5"
        style={{
          background: "linear-gradient(160deg, #0d1525 0%, #080d18 100%)",
          border: `1px solid ${stealth ? "#1e293b" : `${c}33`}`,
          boxShadow: stealth ? "none" : `0 0 30px ${c}18, 0 0 60px ${c}0a, inset 0 1px 0 ${c}15`,
        }}
      >
        {/* Glass reflection */}
        <div className="absolute top-0 left-8 right-8 h-px rounded-full"
          style={{
            background: stealth
              ? "transparent"
              : `linear-gradient(90deg, transparent, ${c}55, transparent)`,
          }} />
        {/* Desk underglow */}
        <div className="absolute bottom-0 left-8 right-8 h-px rounded-full"
          style={{
            background: stealth
              ? "transparent"
              : `linear-gradient(90deg, transparent, ${c}44, transparent)`,
          }} />

        {children}
      </div>

      <div className="w-4/5 h-2.5 rounded-b-xl"
        style={{ background: "linear-gradient(180deg, #1e293b, #080d18)", boxShadow: "0 6px 16px #00000099" }} />
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection line
// ─────────────────────────────────────────────────────────────────────────────

function ConnectionLine({ online }: { online: boolean }) {
  const { c, stealth } = useRgb(online);
  return (
    <svg width="80" height="48" viewBox="0 0 80 48" className="hidden lg:block self-center shrink-0">
      <defs>
        <linearGradient id="cg" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%"   stopColor={c} stopOpacity={stealth ? 0.2 : 0.9} />
          <stop offset="50%"  stopColor={c} stopOpacity={stealth ? 0.05 : 0.25} />
          <stop offset="100%" stopColor={c} stopOpacity={stealth ? 0.2 : 0.9} />
        </linearGradient>
      </defs>
      <line x1="0" y1="24" x2="80" y2="24"
        stroke="url(#cg)" strokeWidth="2" strokeDasharray="6 4" />
      {!stealth && (
        <line x1="0" y1="24" x2="80" y2="24"
          stroke={c} strokeWidth="5" opacity="0.12" />
      )}
      <circle cx="5" cy="24" r="4.5" fill={c}
        style={stealth ? { opacity: 0.2 } : { filter: `drop-shadow(0 0 5px ${c})` }} />
      <circle cx="75" cy="24" r="4.5" fill={c}
        style={stealth ? { opacity: 0.2 } : { filter: `drop-shadow(0 0 5px ${c})` }} />
      <text x="40" y="16" textAnchor="middle" fontSize="7"
        fontFamily="monospace" fill={c} opacity={stealth ? 0.2 : 0.65}>
        REDIS
      </text>
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Queue bar
// ─────────────────────────────────────────────────────────────────────────────

function QueueBar({ queue }: { queue: QueueStats }) {
  const hasJobs = queue.pending_jobs > 0;
  return (
    <div className="flex items-center justify-between rounded-lg px-3 py-2"
      style={{ background: "#0f172a", border: "1px solid #1e293b" }}>
      <span className="font-mono text-[11px] text-slate-400">{queue.queue_name}</span>
      <span className="font-mono text-[11px] font-bold"
        style={{ color: hasJobs ? "#f59e0b" : "#22c55e" }}>
        {queue.pending_jobs} pending
      </span>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Skeleton
// ─────────────────────────────────────────────────────────────────────────────

function Skeleton() {
  return (
    <div className="flex flex-col lg:flex-row gap-8 items-start justify-center animate-pulse">
      {[420, 340].map((w, i) => (
        <div key={i} className="rounded-2xl"
          style={{ width: w, height: 320, background: "#0f172a", border: "1px solid #1e293b" }} />
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared UI primitives (re-exported for HitlManager)
// ─────────────────────────────────────────────────────────────────────────────

export function SectionHeader({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div className="mb-5">
      <h2 className="text-[10px] font-bold tracking-[0.15em] uppercase text-slate-600 mb-0.5">{title}</h2>
      {subtitle && <p className="text-[11px] text-slate-600">{subtitle}</p>}
    </div>
  );
}

export function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      className="rounded-xl px-4 py-3 flex items-center gap-3"
      style={{
        background: "rgba(239,68,68,0.08)",
        border: "1.5px solid #ef4444",
        color: "#ef4444",
        fontSize: "0.78rem",
        fontFamily: "var(--font-mono)",
        fontWeight: 600,
        letterSpacing: "0.02em",
      }}
    >
      <span style={{
        background: "rgba(239,68,68,0.18)",
        border: "1px solid #ef444440",
        padding: "2px 7px",
        borderRadius: "5px",
        fontWeight: 800,
        fontSize: "0.68rem",
        flexShrink: 0,
      }}>
        [ERROR]
      </span>
      {message}
    </div>
  );
}

// ── System self-repair banner ─────────────────────────────────────────────────

type RepairPhase = "none" | "error" | "repairing" | "resolved";

export function SystemRepairBanner({
  hasError,
  isRTL = false,
  isHighContrast = false,
}: {
  hasError: boolean;
  isRTL?: boolean;
  isHighContrast?: boolean;
}) {
  const [phase, setPhase] = useState<RepairPhase>("none");
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => {
    if (hasError) {
      clearTimeout(timer.current);
      if (phase === "none" || phase === "resolved") {
        setPhase("error");
        timer.current = setTimeout(() => setPhase("repairing"), 10_000);
      }
    } else if (phase === "error" || phase === "repairing") {
      clearTimeout(timer.current);
      setPhase("resolved");
      timer.current = setTimeout(() => setPhase("none"), 6_000);
    }
    return () => clearTimeout(timer.current);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hasError]);

  if (phase === "none") return null;

  const phaseConfig = {
    error:     { border: isHighContrast ? "#AA0000" : "#ff3355", color: isHighContrast ? "#AA0000" : "#ff3355", bg: isHighContrast ? "#FFE4E4" : "rgba(255,51,85,0.09)", badgeBg: isHighContrast ? "rgba(170,0,0,0.12)" : "rgba(255,51,85,0.18)", animation: "error-pulse-cs 1.5s ease-in-out infinite" },
    repairing: { border: isHighContrast ? "#7c4e00" : "#ffb800", color: isHighContrast ? "#7c4e00" : "#ffb800", bg: isHighContrast ? "#FFF8DC" : "rgba(255,184,0,0.08)", badgeBg: isHighContrast ? "rgba(124,78,0,0.12)" : "rgba(255,184,0,0.18)",   animation: "none" },
    resolved:  { border: isHighContrast ? "#0a6640" : "#00e096", color: isHighContrast ? "#0a6640" : "#00e096", bg: isHighContrast ? "#DCFCE7" : "rgba(0,224,150,0.07)", badgeBg: isHighContrast ? "rgba(10,102,64,0.12)" : "rgba(0,224,150,0.15)",   animation: "none" },
  };
  const cfg       = phaseConfig[phase];
  const phaseBadge = { error: "[ERROR]", repairing: "[REPAIRING]", resolved: "[RESOLVED]" }[phase];
  const heMessage  = {
    error:     "שגיאת מערכת — פרוטוקול תיקון עצמי הופעל",
    repairing: "שגיאת מערכת — פרוטוקול תיקון עצמי פעיל",
    resolved:  "המערכת שוחזרה — פרוטוקול תיקון הושלם",
  }[phase];
  const enMessage  = {
    error:     "SYSTEM ERROR — Self-Repair Protocol Activated",
    repairing: "SYSTEM ERROR — Self-Repair Protocol Active",
    resolved:  "SYSTEM RECOVERED — Self-Repair Protocol Complete",
  }[phase];

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "0.75rem",
        padding: "0.7rem 1rem",
        borderRadius: "10px",
        background: cfg.bg,
        border: `1.5px solid ${cfg.border}`,
        animation: cfg.animation,
        marginBottom: "1rem",
        flexDirection: isRTL ? "row-reverse" : "row",
      }}
    >
      <style>{`
        @keyframes error-pulse-cs {
          0%, 100% { box-shadow: 0 0 0 0   rgba(255, 51, 85, 0.5); }
          50%       { box-shadow: 0 0 0 8px rgba(255, 51, 85, 0); }
        }
      `}</style>
      <span style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.68rem",
        fontWeight: 800,
        letterSpacing: "0.08em",
        color: cfg.color,
        background: cfg.badgeBg,
        padding: "2px 7px",
        borderRadius: "5px",
        border: `1px solid ${cfg.border}40`,
        flexShrink: 0,
      }}>
        {phaseBadge}
      </span>
      <span style={{
        fontFamily: "var(--font-sans)",
        fontSize: "0.82rem",
        fontWeight: 700,
        color: cfg.color,
        direction: isRTL ? "rtl" : "ltr",
        flex: 1,
      }}>
        {isRTL ? heMessage : enMessage}
      </span>
      {phase !== "resolved" && (
        <span style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.58rem",
          color: cfg.color,
          opacity: 0.55,
          flexShrink: 0,
          letterSpacing: "0.03em",
          marginLeft: isRTL ? 0 : "auto",
          marginRight: isRTL ? "auto" : 0,
        }}>
          [ERROR] → [REPAIRING] → [RESOLVED]
        </span>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────────────────────────────────

export default function ClusterStatus() {
  const { data, error, isLoading } = useSWR<ClusterStatusResponse>(
    "/api/cluster/status",
    swrFetcher<ClusterStatusResponse>,
    { refreshInterval: 10_000 }
  );

  const [modalNode, setModalNode] = useState<NodeStatus | null>(null);
  const { stealth } = useStealth();
  const { deployingNodes } = useNexus();
  const { isRTL } = useI18n();

  const { data: scrapeStatus } = useSWR<ScrapeStatusResponse>(
    "/api/business/scrape-status",
    swrFetcher<ScrapeStatusResponse>,
    { refreshInterval: 8_000 }
  );

  // Engine state for RGB sync — fast poll so colour changes feel immediate
  const { data: engineState } = useSWR<EngineStateResponse>(
    "/api/business/engine-state",
    swrFetcher<EngineStateResponse>,
    { refreshInterval: 3_000 }
  );
  const activeEngineState = engineState?.state ?? "idle";

  // Content factory active — drives indigo "Thinking" glow on monitor
  const { data: factoryActive } = useSWR<FactoryActiveResponse>(
    "/api/content/factory-active",
    swrFetcher<FactoryActiveResponse>,
    { refreshInterval: 5_000 }
  );
  const contentFactoryRunning = factoryActive?.active ?? false;

  // Report sending — drives Neon Blue flash for 10 s
  const { data: reportStatus } = useSWR<ReportStatusResponse>(
    "/api/business/report-status",
    swrFetcher<ReportStatusResponse>,
    { refreshInterval: 3_000 }
  );
  const reportSending = reportStatus?.sending ?? false;

  const master = data?.nodes.find((n) => n.role === "master");
  const workers = data?.nodes.filter((n) => n.role === "worker") ?? [];

  const masterNode: NodeStatus = master ?? {
    node_id: "master", role: "master", cpu_percent: 0, ram_used_mb: 0,
    active_jobs: 0, last_seen: new Date().toISOString(), online: false,
    local_ip: "—", cpu_model: "—", gpu_model: "N/A",
    ram_total_mb: 0, active_tasks_count: 0, os_info: "—",
    motherboard: "N/A", cpu_temp_c: -1, display_name: "",
  };

  // Use real workers when available; show placeholder slots only when no data yet
  const workerNodes: NodeStatus[] = workers.length > 0 ? workers : (data ? [] : [
    { node_id: "worker-1", role: "worker", cpu_percent: 0, ram_used_mb: 0,
      active_jobs: 0, last_seen: new Date().toISOString(), online: false,
      local_ip: "—", cpu_model: "—", gpu_model: "N/A",
      ram_total_mb: 0, active_tasks_count: 0, os_info: "Linux",
      motherboard: "N/A", cpu_temp_c: -1, display_name: "Worker 1" },
    { node_id: "worker-2", role: "worker", cpu_percent: 0, ram_used_mb: 0,
      active_jobs: 0, last_seen: new Date().toISOString(), online: false,
      local_ip: "—", cpu_model: "—", gpu_model: "N/A",
      ram_total_mb: 0, active_tasks_count: 0, os_info: "Linux",
      motherboard: "N/A", cpu_temp_c: -1, display_name: "Worker 2" },
  ]);

  const anyOnline = masterNode.online || workerNodes.some((w) => w.online);
  // Only flag error when we have data and master is down
  const hasSystemError = !!data && !masterNode.online;

  // Desk labels use display_name when available
  const masterLabel = masterNode.display_name || masterNode.node_id || "Master Station";
  const workerLabel = workerNodes.length === 1 && workerNodes[0].display_name
    ? workerNodes[0].display_name
    : "Worker Station";

  return (
    <section>
      <SectionHeader
        title="Workspace Topology"
        subtitle={stealth
          ? "STEALTH MODE — RGB suppressed"
          : "Digital Twin — auto-refreshes every 10 s"}
      />

      {error && <ErrorBanner message="Could not reach the API. Is the master running?" />}
      {!stealth && <SystemRepairBanner hasError={hasSystemError || !!error} isRTL={isRTL} />}
      {isLoading && !data && <Skeleton />}

      {/* ── Scene ── */}
      <div className="flex flex-col lg:flex-row items-start justify-center gap-6 lg:gap-10">

        {/* Master Station — RGB driven by engine state / report sending */}
        {/* Priority: reportSending (blue) > contentFactory (indigo) > engineState */}
        <Desk
          label={masterLabel}
          online={masterNode.online}
          wide
          engineState={activeEngineState !== "idle" ? activeEngineState : undefined}
          reportSending={reportSending}
        >
          <CurvedMonitor
            node={masterNode}
            engineState={
              reportSending
                ? "calculating"   // blue override handled via reportSending prop
                : contentFactoryRunning
                ? "calculating"
                : activeEngineState
            }
            reportSending={reportSending}
          />
          <div className="flex items-end gap-5 justify-center w-full">
            <IsometricPC
              online={masterNode.online}
              engineState={activeEngineState !== "idle" ? activeEngineState : undefined}
              reportSending={reportSending}
            />
            <div className="flex flex-col items-center gap-3 pb-2">
              <Keyboard online={masterNode.online} />
              <Mouse online={masterNode.online} />
            </div>
          </div>
          <NodeHud
            node={masterNode}
            onOpen={() => setModalNode(masterNode)}
            scrapeStatus={scrapeStatus}
            engineState={contentFactoryRunning ? "calculating" : activeEngineState}
            contentFactoryActive={contentFactoryRunning}
            reportSending={reportSending}
          />
        </Desk>

        <ConnectionLine online={anyOnline} />

        {/* Worker Station */}
        {workerNodes.length > 0 && (
        <Desk
          label={workerLabel}
          online={workerNodes[0]?.online ?? false}
          deploying={workerNodes.some((w) => deployingNodes.has(w.node_id))}
        >
          <div className="flex flex-wrap gap-5 justify-center">
            {workerNodes.slice(0, 2).map((w, i) => (
              <div key={w.node_id} className="flex flex-col items-center gap-2">
                <Laptop node={w} variant={i === 0 ? "gaming" : "standard"} />
                <NodeHud
                  node={w}
                  onOpen={() => setModalNode(w)}
                  deploying={deployingNodes.has(w.node_id)}
                />
              </div>
            ))}
          </div>
        </Desk>
        )}
      </div>

      {/* Queue stats */}
      {data && data.queues.length > 0 && (
        <div className="mt-6 flex flex-col gap-2">
          <span className="text-[10px] font-mono font-bold tracking-widest uppercase text-slate-600">
            Task Queues
          </span>
          {data.queues.map((q) => <QueueBar key={q.queue_name} queue={q} />)}
        </div>
      )}

      {/* Hardware Health Modal */}
      {modalNode && (
        <HardwareModal node={modalNode} onClose={() => setModalNode(null)} />
      )}

      <style>{`
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.4; }
        }
      `}</style>
    </section>
  );
}
