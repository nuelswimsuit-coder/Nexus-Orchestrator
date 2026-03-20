"use client";

/**
 * MobileHitl — One-thumb HITL approval for phone/tablet (Tailscale VPN use-case).
 *
 * Renders a full-screen bottom sheet on mobile (< 768px) with:
 *   - Large touch targets (min 56px height)
 *   - Swipe-up animation via Framer Motion
 *   - Single-thumb reachable Approve/Reject buttons at the bottom
 *   - Haptic feedback hint via navigator.vibrate()
 *
 * On desktop it falls back to the standard HitlManager card layout.
 */

import { useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import useSWR from "swr";
import { swrFetcher, resolveHitl } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import type { HitlPendingItem, HitlPendingResponse } from "@/lib/api";

// ─────────────────────────────────────────────────────────────────────────────
// Mobile bottom sheet for a single HITL item
// ─────────────────────────────────────────────────────────────────────────────

function MobileHitlSheet({
  item,
  onResolved,
  onDismiss,
}: {
  item: HitlPendingItem;
  onResolved: () => void;
  onDismiss: () => void;
}) {
  const [loading, setLoading] = useState<"approve" | "reject" | null>(null);
  const [result, setResult] = useState<string | null>(null);

  async function act(approved: boolean) {
    const action = approved ? "approve" : "reject";
    setLoading(action);
    // Haptic feedback on mobile
    if (typeof navigator !== "undefined" && navigator.vibrate) {
      navigator.vibrate(approved ? [10, 50, 10] : [30]);
    }
    try {
      await resolveHitl({
        request_id: item.request_id,
        approved,
        reviewer_id: "mobile-dashboard",
      });
      setResult(approved ? "Approved ✓" : "Rejected ✗");
      setTimeout(onResolved, 800);
    } catch (err) {
      setResult("Error — try again");
    } finally {
      setLoading(null);
    }
  }

  return (
    <motion.div
      initial={{ y: "100%" }}
      animate={{ y: 0 }}
      exit={{ y: "100%" }}
      transition={{ type: "spring", damping: 30, stiffness: 300 }}
      className="fixed inset-x-0 bottom-0 z-50 rounded-t-3xl flex flex-col"
      style={{
        background: "linear-gradient(180deg, #0f172a, #080d18)",
        border: "1px solid #f59e0b44",
        borderBottom: "none",
        maxHeight: "85vh",
        paddingBottom: "env(safe-area-inset-bottom, 16px)",
      }}
    >
      {/* Drag handle */}
      <div className="flex justify-center pt-3 pb-2">
        <div className="w-10 h-1 rounded-full" style={{ background: "#334155" }} />
      </div>

      {/* Header */}
      <div className="flex items-center justify-between px-5 pb-3"
        style={{ borderBottom: "1px solid #1e293b" }}>
        <div className="flex items-center gap-2">
          <span className="text-lg">⚠️</span>
          <div>
            <div className="font-mono text-xs font-bold tracking-widest" style={{ color: "#f59e0b" }}>
              ACTION REQUIRED
            </div>
            <div className="font-mono text-[10px]" style={{ color: "#475569" }}>
              {item.task_type}
            </div>
          </div>
        </div>
        <button
          onClick={onDismiss}
          className="rounded-full w-8 h-8 flex items-center justify-center font-mono text-xs"
          style={{ background: "#1e293b", color: "#475569" }}
        >
          ✕
        </button>
      </div>

      {/* Context */}
      <div className="flex-1 overflow-y-auto px-5 py-4">
        <div
          className="rounded-2xl p-4 font-mono text-sm leading-relaxed"
          style={{
            background: "#040a14",
            border: "1px solid #f59e0b22",
            color: "#94a3b8",
          }}
        >
          {item.context}
        </div>

        <div className="mt-3 flex flex-wrap gap-2">
          <span className="font-mono text-[9px] px-2 py-1 rounded-lg"
            style={{ background: "#1e293b", color: "#475569" }}>
            {item.task_id.slice(0, 8)}
          </span>
          <span className="font-mono text-[9px] px-2 py-1 rounded-lg"
            style={{ background: "#1e293b", color: "#475569" }}>
            {new Date(item.requested_at).toLocaleTimeString()}
          </span>
        </div>

        {result && (
          <motion.div
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            className="mt-4 rounded-2xl p-4 text-center font-mono text-sm font-bold"
            style={{
              background: result.includes("✓") ? "#052e16" : "#1c0000",
              color: result.includes("✓") ? "#22c55e" : "#ef4444",
              border: `1px solid ${result.includes("✓") ? "#22c55e44" : "#ef444444"}`,
            }}
          >
            {result}
          </motion.div>
        )}
      </div>

      {/* Action buttons — large touch targets at bottom */}
      {!result && (
        <div className="px-5 pb-4 pt-2 flex gap-3" style={{ borderTop: "1px solid #1e293b" }}>
          <motion.button
            whileTap={{ scale: 0.96 }}
            onClick={() => act(true)}
            disabled={loading !== null}
            className="flex-1 rounded-2xl font-mono text-sm font-bold tracking-widest"
            style={{
              minHeight: 56,
              background: loading === null ? "#22c55e" : "#1e293b",
              color: loading === null ? "#fff" : "#475569",
              border: "none",
              cursor: loading !== null ? "not-allowed" : "pointer",
              boxShadow: loading === null ? "0 0 20px #22c55e44" : "none",
            }}
          >
            {loading === "approve" ? "..." : "✓ APPROVE"}
          </motion.button>

          <motion.button
            whileTap={{ scale: 0.96 }}
            onClick={() => act(false)}
            disabled={loading !== null}
            className="flex-1 rounded-2xl font-mono text-sm font-bold tracking-widest"
            style={{
              minHeight: 56,
              background: "transparent",
              color: loading === null ? "#ef4444" : "#334155",
              border: `2px solid ${loading === null ? "#ef4444" : "#1e293b"}`,
              cursor: loading !== null ? "not-allowed" : "pointer",
            }}
          >
            {loading === "reject" ? "..." : "✗ REJECT"}
          </motion.button>
        </div>
      )}
    </motion.div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Mobile HITL trigger button
// ─────────────────────────────────────────────────────────────────────────────

export function MobileHitlButton() {
  const [activeItem, setActiveItem] = useState<HitlPendingItem | null>(null);
  const { stealth } = useStealth();

  const { data, mutate } = useSWR<HitlPendingResponse>(
    "/api/hitl/pending",
    swrFetcher<HitlPendingResponse>,
    { refreshInterval: 5_000 }
  );

  const count = data?.total ?? 0;
  if (count === 0) return null;

  const firstItem = data?.items[0];

  return (
    <>
      {/* Floating action button — bottom right, thumb-reachable */}
      <motion.button
        initial={{ scale: 0 }}
        animate={{ scale: 1 }}
        whileTap={{ scale: 0.9 }}
        onClick={() => firstItem && setActiveItem(firstItem)}
        className="fixed bottom-6 right-4 z-40 rounded-full flex items-center gap-2 px-4 font-mono text-xs font-bold tracking-widest"
        style={{
          height: 52,
          background: stealth ? "#1e293b" : "#f59e0b",
          color: stealth ? "#475569" : "#000",
          boxShadow: stealth ? "none" : "0 0 30px #f59e0b66",
          border: "none",
          cursor: "pointer",
        }}
      >
        <span className="text-base">⚠️</span>
        <span>{count} PENDING</span>
      </motion.button>

      {/* Bottom sheet */}
      <AnimatePresence>
        {activeItem && (
          <>
            {/* Backdrop */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              className="fixed inset-0 z-40"
              style={{ background: "rgba(2,6,23,0.7)", backdropFilter: "blur(4px)" }}
              onClick={() => setActiveItem(null)}
            />
            <MobileHitlSheet
              item={activeItem}
              onResolved={() => { setActiveItem(null); mutate(); }}
              onDismiss={() => setActiveItem(null)}
            />
          </>
        )}
      </AnimatePresence>
    </>
  );
}
