"use client";

import { useState } from "react";
import useSWR from "swr";
import { swrFetcher, resolveContent } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { SectionHeader } from "@/components/ClusterStatus";
import type {
  ContentPreviewItem,
  ContentPreviewsResponse,
  FactoryActiveResponse,
} from "@/lib/api";

// ─────────────────────────────────────────────────────────────────────────────
// Status colours
// ─────────────────────────────────────────────────────────────────────────────

const STATUS_COLOR: Record<string, string> = {
  pending_approval: "#f59e0b",
  ready:            "#22c55e",
  completed:        "#22c55e",
  failed:           "#ef4444",
  awaiting_approval:"#6366f1",
};

const STATUS_LABEL: Record<string, string> = {
  pending_approval:  "NEEDS APPROVAL",
  ready:             "READY",
  completed:         "POSTED",
  failed:            "FAILED",
  awaiting_approval: "AWAITING HITL",
};

// ─────────────────────────────────────────────────────────────────────────────
// Single preview card
// ─────────────────────────────────────────────────────────────────────────────

function PreviewCard({
  item,
  onAction,
}: {
  item: ContentPreviewItem;
  onAction: (id: string, action: "approve" | "reject" | "regenerate") => Promise<void>;
}) {
  const { stealth } = useStealth();
  const [loading, setLoading] = useState<string | null>(null);
  const [flash, setFlash] = useState<string | null>(null);

  const statusColor = STATUS_COLOR[item.status] ?? "#475569";
  const isHitl = item.requires_hitl || item.status === "pending_approval" || item.status === "awaiting_approval";

  async function act(action: "approve" | "reject" | "regenerate") {
    setLoading(action);
    try {
      await onAction(item.preview_id, action);
      setFlash(action === "approve" ? "✓ Approved" : action === "reject" ? "✗ Rejected" : "↻ Regenerating");
    } catch (err) {
      setFlash("Error");
    } finally {
      setLoading(null);
    }
  }

  const ts = item.created_at
    ? new Date(item.created_at).toLocaleTimeString("en-US", { hour12: false })
    : "—";

  return (
    <div
      className="relative flex flex-col gap-3 rounded-2xl p-4 overflow-hidden"
      style={{
        background: stealth
          ? "#0d1117"
          : "linear-gradient(135deg, rgba(15,23,42,0.9) 0%, rgba(8,13,24,0.95) 100%)",
        backdropFilter: "blur(12px)",
        border: `1px solid ${stealth ? "#1e293b" : (isHitl ? "#f59e0b44" : `${statusColor}33`)}`,
        boxShadow: stealth
          ? "none"
          : isHitl
          ? "0 0 20px #f59e0b18, 0 0 40px #f59e0b0a"
          : "none",
      }}
    >
      {/* HITL top bar */}
      {isHitl && !stealth && (
        <div
          className="absolute top-0 left-0 right-0 h-0.5"
          style={{ background: "linear-gradient(90deg, #f59e0b, transparent)" }}
        />
      )}

      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1 min-w-0">
          <div className="flex items-center gap-2">
            <span
              className="font-mono text-[9px] font-bold tracking-widest px-2 py-0.5 rounded-full"
              style={{
                color: stealth ? "#334155" : statusColor,
                background: stealth ? "transparent" : `${statusColor}15`,
                border: `1px solid ${stealth ? "#1e293b" : `${statusColor}44`}`,
              }}
            >
              {STATUS_LABEL[item.status] ?? item.status.toUpperCase()}
            </span>
            {isHitl && !stealth && (
              <span
                className="font-mono text-[8px] font-bold tracking-widest"
                style={{ color: "#f59e0b" }}
              >
                ⚠ HITL
              </span>
            )}
          </div>
          <span className="font-mono text-[9px] truncate" style={{ color: "#334155" }}>
            {item.target_group_id} · {item.niche} · {ts}
          </span>
        </div>

        {/* Project badge */}
        <span
          className="font-mono text-[8px] px-2 py-0.5 rounded shrink-0"
          style={{ color: "#475569", border: "1px solid #1e293b", background: "#0d1117" }}
        >
          {item.project_id}
        </span>
      </div>

      {/* Post text */}
      <div
        className="rounded-xl p-3 font-mono text-[12px] leading-relaxed"
        style={{
          background: stealth ? "#080d18" : "#020617",
          border: `1px solid ${stealth ? "#0f172a" : "#0f172a"}`,
          color: stealth ? "#334155" : "#94a3b8",
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {item.post_text}
      </div>

      {/* HITL reason */}
      {item.hitl_reason && (
        <div
          className="font-mono text-[9px] px-2 py-1 rounded"
          style={{
            color: stealth ? "#334155" : "#f59e0b",
            background: stealth ? "transparent" : "#f59e0b0a",
            border: `1px solid ${stealth ? "#1e293b" : "#f59e0b22"}`,
          }}
        >
          ⚠ {item.hitl_reason}
        </div>
      )}

      {/* Flash message */}
      {flash && (
        <div className="font-mono text-[10px] font-bold" style={{ color: "#22c55e" }}>
          {flash}
        </div>
      )}

      {/* Action buttons — hidden in stealth mode */}
      {!stealth && (
        <div className="flex gap-2">
          <button
            onClick={() => act("approve")}
            disabled={loading !== null}
            className="flex-1 py-1.5 rounded-lg font-mono text-[10px] font-bold tracking-widest transition-all"
            style={{
              background: loading === null ? "#22c55e" : "#1e293b",
              color: loading === null ? "#fff" : "#475569",
              border: "none",
              cursor: loading !== null ? "not-allowed" : "pointer",
              opacity: loading === "reject" || loading === "regenerate" ? 0.4 : 1,
            }}
          >
            {loading === "approve" ? "POSTING..." : "✓ APPROVE"}
          </button>

          <button
            onClick={() => act("regenerate")}
            disabled={loading !== null}
            className="flex-1 py-1.5 rounded-lg font-mono text-[10px] font-bold tracking-widest transition-all"
            style={{
              background: "transparent",
              color: loading === null ? "#6366f1" : "#334155",
              border: `1px solid ${loading === null ? "#6366f144" : "#1e293b"}`,
              cursor: loading !== null ? "not-allowed" : "pointer",
              opacity: loading === "approve" || loading === "reject" ? 0.4 : 1,
            }}
          >
            {loading === "regenerate" ? "..." : "↻ REGEN"}
          </button>

          <button
            onClick={() => act("reject")}
            disabled={loading !== null}
            className="py-1.5 px-3 rounded-lg font-mono text-[10px] font-bold tracking-widest transition-all"
            style={{
              background: "transparent",
              color: loading === null ? "#ef4444" : "#334155",
              border: `1px solid ${loading === null ? "#ef444433" : "#1e293b"}`,
              cursor: loading !== null ? "not-allowed" : "pointer",
              opacity: loading === "approve" || loading === "regenerate" ? 0.4 : 1,
            }}
          >
            {loading === "reject" ? "..." : "✗"}
          </button>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory status badge
// ─────────────────────────────────────────────────────────────────────────────

function FactoryBadge({ factoryData }: { factoryData: FactoryActiveResponse | undefined }) {
  const { stealth } = useStealth();
  const isActive = factoryData?.active ?? false;
  const statusStr = factoryData?.status ?? "idle";
  const c = stealth ? "#334155" : (isActive ? "#6366f1" : "#334155");

  return (
    <span
      className="inline-flex items-center gap-1.5 font-mono text-[9px] font-bold tracking-widest px-2 py-1 rounded-full"
      style={{
        color: c,
        background: stealth ? "transparent" : (isActive ? "#6366f112" : "transparent"),
        border: `1px solid ${stealth ? "#1e293b" : (isActive ? "#6366f144" : "#1e293b")}`,
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
      {isActive ? statusStr.replace("_", " ").toUpperCase() : "IDLE"}
    </span>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────────────────────────────────────

export default function ContentPreview() {
  const { stealth } = useStealth();

  const { data: previewsData, mutate: mutatePreviews } = useSWR<ContentPreviewsResponse>(
    "/api/content/previews",
    swrFetcher<ContentPreviewsResponse>,
    { refreshInterval: 8_000 }
  );

  const { data: factoryData } = useSWR<FactoryActiveResponse>(
    "/api/content/factory-active",
    swrFetcher<FactoryActiveResponse>,
    { refreshInterval: 5_000 }
  );

  const previews = previewsData?.previews ?? [];
  const hitlPreviews = previews.filter((p) => p.requires_hitl || p.status === "pending_approval");
  const recentPreviews = previews.filter((p) => !p.requires_hitl && p.status !== "pending_approval");

  async function handleAction(
    id: string,
    action: "approve" | "reject" | "regenerate",
  ) {
    await resolveContent(id, action);
    await mutatePreviews();
  }

  return (
    <section>
      <div className="flex items-center justify-between mb-5 flex-wrap gap-3">
        <SectionHeader
          title="Content Previews"
          subtitle={stealth ? "STEALTH MODE — previews hidden" : "AI-generated content — approve before publishing"}
        />
        <FactoryBadge factoryData={factoryData} />
      </div>

      {/* In stealth mode, show nothing visual but keep the section mount */}
      {stealth ? (
        <div
          className="rounded-xl p-4 font-mono text-[10px] text-center"
          style={{ background: "#0d1117", border: "1px dashed #1e293b", color: "#1e293b" }}
        >
          Content previews hidden in Stealth Mode.
          Check the Agent Thinking Log for generation activity.
        </div>
      ) : (
        <>
          {/* HITL queue — needs approval */}
          {hitlPreviews.length > 0 && (
            <div className="flex flex-col gap-3 mb-5">
              <span
                className="font-mono text-[9px] font-bold tracking-widest uppercase"
                style={{ color: "#f59e0b" }}
              >
                ⚠ Requires Approval ({hitlPreviews.length})
              </span>
              {hitlPreviews.map((item) => (
                <PreviewCard key={item.preview_id} item={item} onAction={handleAction} />
              ))}
            </div>
          )}

          {/* Recent posts */}
          {recentPreviews.length > 0 && (
            <div className="flex flex-col gap-3">
              <span
                className="font-mono text-[9px] font-bold tracking-widest uppercase"
                style={{ color: "#334155" }}
              >
                Recent ({recentPreviews.length})
              </span>
              {recentPreviews.slice(0, 5).map((item) => (
                <PreviewCard key={item.preview_id} item={item} onAction={handleAction} />
              ))}
            </div>
          )}

          {/* Empty state */}
          {previews.length === 0 && (
            <div
              className="rounded-xl p-6 text-center font-mono text-[11px]"
              style={{
                background: "#0d1117",
                border: "1px dashed #1e293b",
                color: "#334155",
              }}
            >
              No content previews yet.
              <br />
              Trigger a generation from the Operational Intelligence panel.
            </div>
          )}
        </>
      )}

      <style>{`
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </section>
  );
}
