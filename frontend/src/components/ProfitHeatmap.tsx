"use client";

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { SectionHeader } from "@/components/ClusterStatus";
import type { BusinessStatsResponse, DecisionsResponse } from "@/lib/api";

// ─────────────────────────────────────────────────────────────────────────────
// Colour helpers
// ─────────────────────────────────────────────────────────────────────────────

function roiColor(roi: number, stealth: boolean): string {
  if (stealth) return "#1e293b";
  if (roi >= 80) return "#00ff88";
  if (roi >= 50) return "#22d3ee";
  if (roi >= 20) return "#f59e0b";
  if (roi >= 0)  return "#6366f1";
  return "#ef4444";
}

function roiGlow(roi: number, stealth: boolean): string {
  if (stealth) return "none";
  const c = roiColor(roi, false);
  return `0 0 16px ${c}55, 0 0 32px ${c}22`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Group tile
// ─────────────────────────────────────────────────────────────────────────────

interface GroupTileProps {
  title: string;
  link: string;
  role: "source" | "target";
  estimatedRoi: number;   // 0–100
  userCount: number;
  stealth: boolean;
}

function GroupTile({ title, link, role, estimatedRoi, userCount, stealth }: GroupTileProps) {
  const c = roiColor(estimatedRoi, stealth);
  const isSource = role === "source";

  return (
    <div
      className="relative flex flex-col gap-1.5 rounded-xl p-3 overflow-hidden cursor-default"
      style={{
        background: stealth
          ? "#0d1117"
          : `linear-gradient(135deg, ${c}10 0%, #080d18 100%)`,
        border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
        boxShadow: roiGlow(estimatedRoi, stealth),
        transition: "box-shadow 0.3s",
        minWidth: "140px",
      }}
      title={link}
    >
      {/* Top accent */}
      <div
        className="absolute top-0 left-0 right-0 h-0.5 rounded-t-xl"
        style={{
          background: stealth
            ? "transparent"
            : `linear-gradient(90deg, ${c}, transparent)`,
        }}
      />

      {/* Role badge */}
      <span
        className="font-mono text-[8px] font-bold tracking-widest self-start px-1.5 py-0.5 rounded"
        style={{
          color: stealth ? "#334155" : (isSource ? "#6366f1" : "#22d3ee"),
          background: stealth ? "transparent" : (isSource ? "#6366f115" : "#22d3ee15"),
          border: `1px solid ${stealth ? "#1e293b" : (isSource ? "#6366f133" : "#22d3ee33")}`,
        }}
      >
        {isSource ? "SOURCE" : "TARGET"}
      </span>

      {/* Group name */}
      <span
        className="font-mono text-[10px] font-semibold truncate"
        style={{ color: stealth ? "#334155" : "#94a3b8" }}
        title={title}
      >
        {title || link.replace("https://t.me/", "@")}
      </span>

      {/* ROI bar */}
      <div className="flex flex-col gap-0.5">
        <div className="flex items-center justify-between">
          <span className="font-mono text-[8px]" style={{ color: "#334155" }}>ROI</span>
          <span
            className="font-mono text-[10px] font-bold"
            style={{ color: stealth ? "#475569" : c }}
          >
            {estimatedRoi}%
          </span>
        </div>
        <div
          className="h-1 rounded-full overflow-hidden"
          style={{ background: "#1e293b" }}
        >
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(100, estimatedRoi)}%`,
              background: stealth ? "#1e293b" : c,
              boxShadow: stealth ? "none" : `0 0 6px ${c}`,
            }}
          />
        </div>
      </div>

      {/* User count */}
      <span className="font-mono text-[8px]" style={{ color: "#334155" }}>
        {userCount.toLocaleString()} users
      </span>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Decision badge
// ─────────────────────────────────────────────────────────────────────────────

function DecisionBadge({
  title,
  confidence,
  roi_impact,
  requires_approval,
  stealth,
}: {
  title: string;
  confidence: number;
  roi_impact: string;
  requires_approval: boolean;
  stealth: boolean;
}) {
  const c = stealth ? "#334155" : (requires_approval ? "#f59e0b" : "#00ff88");

  return (
    <div
      className="flex items-start gap-3 rounded-xl px-3 py-2.5"
      style={{
        background: stealth ? "#0d1117" : `${c}0a`,
        border: `1px solid ${stealth ? "#1e293b" : `${c}33`}`,
      }}
    >
      {/* Confidence ring */}
      <div className="flex flex-col items-center gap-0.5 shrink-0">
        <span
          className="font-mono text-sm font-bold"
          style={{ color: stealth ? "#334155" : c }}
        >
          {confidence}
        </span>
        <span className="font-mono text-[7px]" style={{ color: "#334155" }}>CONF</span>
      </div>

      <div className="flex flex-col gap-0.5 min-w-0">
        <span
          className="font-mono text-[10px] font-bold truncate"
          style={{ color: stealth ? "#334155" : "#f1f5f9" }}
        >
          {title}
        </span>
        <span className="font-mono text-[9px] truncate" style={{ color: "#475569" }}>
          {roi_impact}
        </span>
        {requires_approval && (
          <span
            className="font-mono text-[8px] font-bold tracking-widest"
            style={{ color: stealth ? "#334155" : "#f59e0b" }}
          >
            ⚠ HITL REQUIRED
          </span>
        )}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────────────────────────────────

export default function ProfitHeatmap() {
  const { stealth } = useStealth();

  const { data: biz } = useSWR<BusinessStatsResponse>(
    "/api/business/stats",
    swrFetcher<BusinessStatsResponse>,
    { refreshInterval: 30_000 }
  );

  const { data: decisions } = useSWR<DecisionsResponse>(
    "/api/business/decisions",
    swrFetcher<DecisionsResponse>,
    { refreshInterval: 30_000 }
  );

  // Build synthetic group tiles from the DB data.
  // In a full implementation these would come from a per-group stats endpoint.
  // For now we generate tiles from the aggregate counts.
  const totalUsers = biz?.total_scraped_users ?? 0;
  const sourceCount = biz?.source_groups ?? 0;
  const targetCount = biz?.target_groups ?? 0;

  const sourceTiles = Array.from({ length: Math.min(sourceCount, 8) }, (_, i) => ({
    title: `Source Group ${i + 1}`,
    link: `https://t.me/source_${i + 1}`,
    role: "source" as const,
    estimatedRoi: Math.max(20, 90 - i * 8),
    userCount: Math.floor(totalUsers / Math.max(sourceCount, 1)),
  }));

  const targetTiles = Array.from({ length: Math.min(targetCount, 12) }, (_, i) => ({
    title: `Target Group ${i + 1}`,
    link: `https://t.me/target_${i + 1}`,
    role: "target" as const,
    estimatedRoi: Math.max(10, 75 - i * 5),
    userCount: Math.floor((biz?.total_users_pipeline ?? 0) / Math.max(targetCount, 1)),
  }));

  const allTiles = [...sourceTiles, ...targetTiles];

  return (
    <section>
      <SectionHeader
        title="Profit Heatmap"
        subtitle="Group ROI visualization — refreshes every 30 s"
      />

      {/* ── Heatmap grid ── */}
      {allTiles.length > 0 ? (
        <div className="flex flex-wrap gap-3 mb-5">
          {allTiles.map((tile, i) => (
            <GroupTile key={i} {...tile} stealth={stealth} />
          ))}
        </div>
      ) : (
        <div
          className="rounded-xl p-6 text-center font-mono text-[11px] mb-5"
          style={{ background: "#0d1117", border: "1px dashed #1e293b", color: "#334155" }}
        >
          No group data yet — run a scrape to populate the heatmap
        </div>
      )}

      {/* ── Decision Engine recommendations ── */}
      {decisions && decisions.decisions.length > 0 && (
        <div className="flex flex-col gap-2">
          <span
            className="font-mono text-[9px] font-bold tracking-[0.15em] uppercase"
            style={{ color: "#334155" }}
          >
            AI Recommendations
          </span>
          {decisions.decisions.slice(0, 4).map((d, i) => (
            <DecisionBadge
              key={i}
              title={d.title}
              confidence={d.confidence}
              roi_impact={d.roi_impact}
              requires_approval={d.requires_approval}
              stealth={stealth}
            />
          ))}
        </div>
      )}
    </section>
  );
}
