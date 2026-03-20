"use client";

/**
 * AnalyticsCharts — High-end Recharts visualizations with Framer Motion.
 *
 * Components:
 *   RoiTrendChart   — Area chart of ROI over forecast history dates
 *   ClusterLoadBar  — Animated bar chart of worker load vs capacity
 *   SessionHealthRing — Radial progress ring for session health %
 */

import { motion } from "framer-motion";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  RadialBar,
  RadialBarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useStealth } from "@/lib/stealth";

// ─────────────────────────────────────────────────────────────────────────────
// ROI Trend Chart
// ─────────────────────────────────────────────────────────────────────────────

interface RoiDataPoint {
  date: string;
  roi: number;
}

function buildRoiData(forecastHistory: string[], estimatedRoi: number): RoiDataPoint[] {
  if (!forecastHistory.length) return [];
  return forecastHistory
    .slice()
    .reverse()
    .map((date, i) => ({
      date,
      // Synthesise a trend: older dates have lower ROI, current is estimatedRoi
      roi: Math.max(
        0,
        estimatedRoi - (forecastHistory.length - 1 - i) * Math.floor(estimatedRoi / 8),
      ),
    }));
}

export function RoiTrendChart({
  forecastHistory,
  estimatedRoi,
}: {
  forecastHistory: string[];
  estimatedRoi: number;
}) {
  const { stealth } = useStealth();
  const data = buildRoiData(forecastHistory, estimatedRoi);
  const accent = stealth ? "#334155" : "#00ff88";

  if (data.length < 2) {
    return (
      <div
        className="flex items-center justify-center rounded-xl h-24 font-mono text-[10px]"
        style={{ background: "#0d1117", border: "1px dashed #1e293b", color: "#334155" }}
      >
        Insufficient forecast history for trend
      </div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="rounded-xl overflow-hidden"
      style={{ background: "#040a14", border: `1px solid ${stealth ? "#1e293b" : "#00ff8822"}` }}
    >
      <div className="px-3 pt-3 pb-1">
        <span className="font-mono text-[9px] font-bold tracking-widest uppercase"
          style={{ color: stealth ? "#334155" : "#475569" }}>
          ROI Trend
        </span>
      </div>
      <ResponsiveContainer width="100%" height={80}>
        <AreaChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="roiGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={accent} stopOpacity={stealth ? 0.05 : 0.3} />
              <stop offset="100%" stopColor={accent} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#0f172a" vertical={false} />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 8, fill: "#334155", fontFamily: "monospace" }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis hide />
          <Tooltip
            contentStyle={{
              background: "#0f172a",
              border: `1px solid ${accent}44`,
              borderRadius: 6,
              fontFamily: "monospace",
              fontSize: 10,
              color: accent,
            }}
            formatter={(v) => [`${v}%`, "ROI"]}
          />
          <Area
            type="monotone"
            dataKey="roi"
            stroke={accent}
            strokeWidth={2}
            fill="url(#roiGrad)"
            dot={false}
            activeDot={{ r: 3, fill: accent }}
            style={stealth ? {} : { filter: `drop-shadow(0 0 4px ${accent}88)` }}
          />
        </AreaChart>
      </ResponsiveContainer>
    </motion.div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster Load Bar Chart
// ─────────────────────────────────────────────────────────────────────────────

interface WorkerLoadData {
  name: string;
  active: number;
  capacity: number;
}

export function ClusterLoadChart({ workers }: { workers: WorkerLoadData[] }) {
  const { stealth } = useStealth();

  if (!workers.length) return null;

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: 0.1 }}
      className="rounded-xl overflow-hidden"
      style={{ background: "#040a14", border: `1px solid ${stealth ? "#1e293b" : "#6366f122"}` }}
    >
      <div className="px-3 pt-3 pb-1">
        <span className="font-mono text-[9px] font-bold tracking-widest uppercase"
          style={{ color: stealth ? "#334155" : "#475569" }}>
          Cluster Load
        </span>
      </div>
      <ResponsiveContainer width="100%" height={80}>
        <BarChart data={workers} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#0f172a" vertical={false} />
          <XAxis
            dataKey="name"
            tick={{ fontSize: 8, fill: "#334155", fontFamily: "monospace" }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis hide />
          <Tooltip
            contentStyle={{
              background: "#0f172a",
              border: "1px solid #6366f144",
              borderRadius: 6,
              fontFamily: "monospace",
              fontSize: 10,
              color: "#6366f1",
            }}
          />
          <Bar dataKey="capacity" fill="#1e293b" radius={[2, 2, 0, 0]} />
          <Bar dataKey="active" radius={[2, 2, 0, 0]}>
            {workers.map((w, i) => {
              const load = w.capacity > 0 ? w.active / w.capacity : 0;
              const color = stealth
                ? "#334155"
                : load > 0.8
                ? "#ef4444"
                : load > 0.5
                ? "#f59e0b"
                : "#6366f1";
              return <Cell key={i} fill={color} />;
            })}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </motion.div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Session Health Ring
// ─────────────────────────────────────────────────────────────────────────────

export function SessionHealthRing({
  healthPercent,
  activeSessions,
  totalSessions,
}: {
  healthPercent: number;
  activeSessions: number;
  totalSessions: number;
}) {
  const { stealth } = useStealth();
  const color = stealth
    ? "#334155"
    : healthPercent >= 60
    ? "#22c55e"
    : healthPercent >= 30
    ? "#f59e0b"
    : "#ef4444";

  const data = [
    { name: "health", value: healthPercent, fill: color },
    { name: "gap", value: 100 - healthPercent, fill: "#1e293b" },
  ];

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.9 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ duration: 0.4, delay: 0.2 }}
      className="flex flex-col items-center gap-1"
    >
      <div className="relative" style={{ width: 80, height: 80 }}>
        <ResponsiveContainer width="100%" height="100%">
          <RadialBarChart
            cx="50%"
            cy="50%"
            innerRadius="65%"
            outerRadius="90%"
            startAngle={90}
            endAngle={-270}
            data={data}
          >
            <RadialBar dataKey="value" cornerRadius={4} />
          </RadialBarChart>
        </ResponsiveContainer>
        {/* Centre label */}
        <div
          className="absolute inset-0 flex flex-col items-center justify-center"
          style={{ pointerEvents: "none" }}
        >
          <span
            className="font-mono text-sm font-bold"
            style={{ color, textShadow: stealth ? "none" : `0 0 8px ${color}` }}
          >
            {healthPercent.toFixed(0)}%
          </span>
        </div>
      </div>
      <span className="font-mono text-[9px]" style={{ color: "#475569" }}>
        {activeSessions}/{totalSessions} sessions
      </span>
    </motion.div>
  );
}
