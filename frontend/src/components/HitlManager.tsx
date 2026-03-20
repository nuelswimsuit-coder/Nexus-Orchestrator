"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import useSWR from "swr";
import { swrFetcher, resolveHitl, forceRunTask } from "@/lib/api";
import type {
  HitlPendingResponse,
  HitlPendingItem,
  StuckStateResponse,
  ThresholdInfoResponse,
} from "@/lib/api";
import { SectionHeader, ErrorBanner } from "@/components/ClusterStatus";
import { useI18n } from "@/lib/i18n";

const HARD_RESET_MS = 5 * 60 * 1000;

// ── Threshold gap badge ───────────────────────────────────────────────────────

function ThresholdGap({ taskType }: { taskType: string }) {
  const { t } = useI18n();

  const ACTION_MAP: Record<string, string> = {
    "telegram.auto_scrape":  "scale_scrape",
    "telegram.auto_add":     "scale_add",
    "telegram.run_warmup":   "emergency_warmup",
    "telegram.super_scrape": "scale_scrape",
    "nexus.scale_worker":    "scale_workers",
  };
  const actionType = ACTION_MAP[taskType] ?? taskType.replace("telegram.", "").replace(".", "_");

  const { data } = useSWR<ThresholdInfoResponse>(
    `/api/business/threshold-info/${actionType}`,
    swrFetcher<ThresholdInfoResponse>,
    { refreshInterval: 10_000 }
  );

  if (!data) return null;

  return (
    <div style={{
      display: "flex",
      flexWrap: "wrap",
      gap: "0.5rem",
      padding: "0.6rem 0.85rem",
      background: "#0a0e1a",
      borderRadius: "8px",
      border: "1px solid #1e293b",
    }}>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#475569" }}>
        📊 {t("threshold_label")}:
      </span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#f59e0b", fontWeight: 700 }}>
        {data.effective_threshold}
        {data.effective_threshold < data.default_threshold && (
          <span style={{ color: "#22c55e", marginLeft: "0.3rem" }}>
            ↓ (was {data.default_threshold})
          </span>
        )}
      </span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#334155" }}>·</span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#475569" }}>
        {t("approval_streak_label")}:
      </span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#6366f1", fontWeight: 700 }}>
        {data.approval_streak}/3
      </span>
      {data.streak_needed > 0 && (
        <>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#334155" }}>·</span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#475569" }}>
            {t("approve")} {data.streak_needed}× more to lower threshold by 5
          </span>
        </>
      )}
    </div>
  );
}

// ── Stuck state banner ────────────────────────────────────────────────────────

function StuckBanner({ onForceRun }: { onForceRun: () => void }) {
  const { t } = useI18n();
  const { data } = useSWR<StuckStateResponse>(
    "/api/business/stuck-state",
    swrFetcher<StuckStateResponse>,
    { refreshInterval: 10_000 }
  );
  const [running, setRunning] = useState(false);
  const [done, setDone] = useState(false);

  if (!data?.stuck) return null;

  async function handleForce() {
    if (!data) return;
    setRunning(true);
    try {
      await forceRunTask(data.task_type, data.task_params);
      setDone(true);
      setTimeout(onForceRun, 1500);
    } catch {
      setRunning(false);
    }
  }

  return (
    <div style={{
      background: "linear-gradient(135deg, #1c0000, #0d1117)",
      border: "2px solid #ef4444",
      borderRadius: "12px",
      padding: "1.1rem 1.25rem",
      marginBottom: "1.25rem",
      display: "flex",
      flexDirection: "column",
      gap: "0.75rem",
      boxShadow: "0 0 24px #ef444433",
      animation: "card-appear 0.3s ease-out",
    }}>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "1rem" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            <span style={{ fontSize: "1rem" }}>🚨</span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.85rem", fontWeight: 700, color: "#ef4444", letterSpacing: "0.04em" }}>
              {t("stuck_loop")}
            </span>
          </div>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#475569" }}>
            Agent blocked for ~15 min on [{data.action_type}]
          </span>
        </div>
        <div style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          padding: "3px 10px",
          borderRadius: "999px",
          background: "#1c0000",
          color: "#ef4444",
          border: "1px solid #ef444466",
          whiteSpace: "nowrap",
          flexShrink: 0,
        }}>
          Need +{data.gap} confidence
        </div>
      </div>

      {/* Confidence vs threshold bar */}
      <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem" }}>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#334155" }}>
            Confidence: {data.confidence}
          </span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#334155" }}>
            {t("threshold_label")}: {data.threshold}
          </span>
        </div>
        <div style={{ height: 6, background: "#0f172a", borderRadius: 3, overflow: "hidden", position: "relative" }}>
          <div style={{
            position: "absolute", left: 0, top: 0, bottom: 0,
            width: `${Math.min(100, data.confidence)}%`,
            background: "#f59e0b",
            borderRadius: 3,
            boxShadow: "0 0 6px #f59e0b",
          }} />
          <div style={{
            position: "absolute", top: 0, bottom: 0,
            left: `${data.threshold}%`,
            width: 2,
            background: "#ef4444",
            boxShadow: "0 0 4px #ef4444",
          }} />
        </div>
        <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#ef4444" }}>
          Gap: need +{data.gap} points OR click Force Run below
        </span>
      </div>

      {/* Force Run button */}
      {done ? (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.82rem", color: "#22c55e", fontWeight: 700 }}>
          ✓ {t("force_run_done")}
        </div>
      ) : (
        <button
          onClick={handleForce}
          disabled={running}
          style={{
            padding: "0.75rem 1rem",
            borderRadius: "8px",
            border: "none",
            cursor: running ? "not-allowed" : "pointer",
            fontFamily: "var(--font-mono)",
            fontWeight: 800,
            fontSize: "0.9rem",
            letterSpacing: "0.06em",
            background: running ? "#1e293b" : "linear-gradient(135deg, #b91c1c, #ef4444, #f87171)",
            color: running ? "#475569" : "#fff",
            boxShadow: running ? "none" : "0 0 20px #ef444466",
            transition: "all 0.15s",
          }}
        >
          {running ? `⏳ ${t("dispatching")}` : `⚡ ${t("force_run_label")}`}
        </button>
      )}
    </div>
  );
}

// ── Single HITL card ──────────────────────────────────────────────────────────

function HitlCard({
  item,
  onResolved,
}: {
  item: HitlPendingItem;
  onResolved: () => void;
}) {
  const { t, isRTL } = useI18n();
  const [reason, setReason] = useState("");
  const [submitting, setSubmitting] = useState<"approve" | "reject" | null>(null);
  const [flash, setFlash] = useState<{ ok: boolean; msg: string } | null>(null);

  async function handleResolve(approved: boolean) {
    const action = approved ? "approve" : "reject";
    setSubmitting(action);
    setFlash(null);
    try {
      const res = await resolveHitl({
        request_id: item.request_id,
        approved,
        reviewer_id: "dashboard",
        reason: reason.trim() || undefined,
      });
      setFlash({ ok: true, msg: res.message });
      setTimeout(onResolved, 900);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Unknown error — check API logs";
      setFlash({ ok: false, msg });
    } finally {
      setSubmitting(null);
    }
  }

  const timeAgo = formatTimeAgo(item.requested_at);
  const isActing = submitting !== null;

  const waitMs = Date.now() - new Date(item.requested_at).getTime();
  const isStale = waitMs > 10 * 60 * 1000;

  return (
    <div
      style={{
        background: "linear-gradient(135deg, #0f172a 0%, #0d1117 100%)",
        border: `2px solid ${isStale ? "#ef4444" : "#f59e0b"}`,
        borderRadius: "14px",
        overflow: "hidden",
        boxShadow: isStale
          ? "0 0 32px #ef444433, 0 0 64px #ef444411"
          : "0 0 32px #f59e0b33, 0 0 64px #f59e0b11",
        animation: "card-appear 0.3s ease-out",
      }}
    >
      {/* Top bar */}
      <div style={{
        height: "4px",
        background: isStale
          ? "linear-gradient(90deg, #ef4444, #f87171, #ef444444, transparent)"
          : "linear-gradient(90deg, #f59e0b, #fbbf24, #f59e0b44, transparent)",
      }} />

      <div style={{ padding: "1.5rem", display: "flex", flexDirection: "column", gap: "1rem" }}>

        {/* ── Header ── */}
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: "1rem", flexDirection: isRTL ? "row-reverse" : "row" }}>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem", minWidth: 0 }}>
            <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
              <span style={{ fontSize: "1.1rem" }}>{isStale ? "🔴" : "⚠️"}</span>
              <span style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.9rem",
                fontWeight: 700,
                color: isStale ? "#f87171" : "#fbbf24",
                letterSpacing: "0.04em",
              }}>
                {item.task_type}
              </span>
              {isStale && (
                <span style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.62rem",
                  padding: "1px 6px",
                  borderRadius: "4px",
                  background: "#1c0000",
                  color: "#ef4444",
                  border: "1px solid #ef444466",
                }}>
                  STUCK
                </span>
              )}
            </div>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#475569" }}>
              Task: {item.task_id}
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#334155" }}>
              Request: {item.request_id}
            </span>
          </div>
          <span style={{
            fontSize: "0.7rem",
            color: isStale ? "#ef4444" : "#475569",
            whiteSpace: "nowrap",
            flexShrink: 0,
            fontFamily: "var(--font-mono)",
            fontWeight: isStale ? 700 : 400,
          }}>
            {timeAgo}
          </span>
        </div>

        {/* ── Threshold gap ── */}
        <ThresholdGap taskType={item.task_type} />

        {/* ── Context ── */}
        <div style={{
          background: "#020617",
          borderRadius: "8px",
          padding: "1rem 1.1rem",
          fontSize: "0.88rem",
          color: "#f1f5f9",
          lineHeight: 1.7,
          borderLeft: isRTL ? "none" : `4px solid ${isStale ? "#ef4444" : "#f59e0b"}`,
          borderRight: isRTL ? `4px solid ${isStale ? "#ef4444" : "#f59e0b"}` : "none",
          wordBreak: "break-word",
          direction: isRTL ? "rtl" : "ltr",
        }}>
          {item.context}
        </div>

        {/* ── Optional note ── */}
        <div>
          <label
            htmlFor={`reason-${item.request_id}`}
            style={{
              display: "block",
              fontSize: "0.68rem",
              fontWeight: 600,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              color: "#475569",
              marginBottom: "0.35rem",
              fontFamily: "var(--font-mono)",
              textAlign: isRTL ? "right" : "left",
            }}
          >
            {t("note_for_audit")}
          </label>
          <textarea
            id={`reason-${item.request_id}`}
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder={t("add_context")}
            rows={2}
            disabled={isActing}
            style={{
              width: "100%",
              resize: "vertical",
              background: "#0a0e1a",
              border: "1px solid #1e293b",
              borderRadius: "6px",
              padding: "0.5rem 0.75rem",
              fontSize: "0.82rem",
              color: "#f1f5f9",
              fontFamily: "var(--font-sans)",
              outline: "none",
              opacity: isActing ? 0.5 : 1,
              boxSizing: "border-box",
              direction: isRTL ? "rtl" : "ltr",
            }}
          />
        </div>

        {/* ── Flash message ── */}
        {flash && (
          <div style={{
            padding: "0.6rem 1rem",
            borderRadius: "8px",
            fontSize: "0.84rem",
            fontWeight: 600,
            background: flash.ok ? "#052e16" : "#1c0000",
            color: flash.ok ? "#22c55e" : "#ef4444",
            border: `1px solid ${flash.ok ? "#22c55e" : "#ef4444"}`,
            fontFamily: "var(--font-mono)",
          }}>
            {flash.ok ? "✓" : "✗"} {flash.msg}
          </div>
        )}

        {/* ── Action buttons ── */}
        <div style={{ display: "flex", gap: "1rem", flexDirection: isRTL ? "row-reverse" : "row" }}>
          {/* ✓ APPROVE — Neon Green */}
          <button
            onClick={() => handleResolve(true)}
            disabled={isActing}
            title={t("approve_action")}
            style={{
              flex: 1,
              padding: "0.9rem 1rem",
              borderRadius: "10px",
              border: "none",
              cursor: isActing ? "not-allowed" : "pointer",
              fontFamily: "var(--font-mono)",
              fontWeight: 800,
              fontSize: "1rem",
              letterSpacing: "0.06em",
              background: isActing
                ? "#1e293b"
                : "linear-gradient(135deg, #15803d, #22c55e, #4ade80)",
              color: isActing ? "#475569" : "#fff",
              boxShadow: isActing ? "none" : "0 0 24px #22c55e77, 0 4px 16px #22c55e44",
              opacity: submitting === "reject" ? 0.35 : 1,
              transition: "all 0.15s",
              textShadow: isActing ? "none" : "0 0 10px #ffffff99",
            }}
          >
            {submitting === "approve"
              ? `⏳ ${t("approving")}`
              : `✓ ${t("approve_action")}`}
          </button>

          {/* ✗ REJECT — Neon Red */}
          <button
            onClick={() => handleResolve(false)}
            disabled={isActing}
            title={t("reject_action")}
            style={{
              flex: 1,
              padding: "0.9rem 1rem",
              borderRadius: "10px",
              border: "2px solid #ef4444",
              cursor: isActing ? "not-allowed" : "pointer",
              fontFamily: "var(--font-mono)",
              fontWeight: 800,
              fontSize: "1rem",
              letterSpacing: "0.06em",
              background: isActing ? "transparent" : "#1c000088",
              color: isActing ? "#475569" : "#f87171",
              boxShadow: isActing ? "none" : "0 0 18px #ef444455",
              opacity: submitting === "approve" ? 0.35 : 1,
              transition: "all 0.15s",
            }}
          >
            {submitting === "reject"
              ? `⏳ ${t("rejecting")}`
              : `✗ ${t("reject_action")}`}
          </button>
        </div>

      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function HitlManager() {
  const { t, isRTL } = useI18n();
  const { data, error, isLoading, mutate } = useSWR<HitlPendingResponse>(
    "/api/hitl/pending",
    swrFetcher<HitlPendingResponse>,
    {
      refreshInterval: 2_000,
      revalidateOnFocus: true,
      dedupingInterval: 500,
    }
  );

  const count = data?.total ?? 0;
  const hasPending = count > 0;

  // ── Hard reset: if list unchanged for 5 min, force a full remount ─────────
  const lastChangeRef  = useRef<number>(Date.now());
  const lastIdsRef     = useRef<string>("");
  const [resetKey, setResetKey] = useState(0);

  useEffect(() => {
    const currentIds = (data?.items ?? []).map(i => i.request_id).sort().join(",");
    if (currentIds !== lastIdsRef.current) {
      lastIdsRef.current  = currentIds;
      lastChangeRef.current = Date.now();
    }
    const staleness = Date.now() - lastChangeRef.current;
    if (staleness > HARD_RESET_MS && count > 0) {
      setResetKey(k => k + 1);
      lastChangeRef.current = Date.now();
      mutate();
    }
  }, [data, count, mutate]);

  // ── Audio + visual pulse when new tasks arrive ────────────────────────────
  const prevCountRef = useRef(0);
  const [newAlert, setNewAlert] = useState(false);

  useEffect(() => {
    if (count > prevCountRef.current && prevCountRef.current >= 0) {
      setNewAlert(true);
      setTimeout(() => setNewAlert(false), 3000);
      try {
        const ctx = new (window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        osc.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.3);
        gain.gain.setValueAtTime(0.3, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
        osc.start(ctx.currentTime);
        osc.stop(ctx.currentTime + 0.4);
      } catch {
        // Audio not available
      }
    }
    prevCountRef.current = count;
  }, [count]);

  const handleForceRunDone = useCallback(() => {
    mutate();
  }, [mutate]);

  return (
    <section
      key={resetKey}
      style={{
        outline: newAlert ? "2px solid #f59e0b" : "none",
        borderRadius: "12px",
        transition: "outline 0.3s",
        animation: newAlert ? "section-alert 0.5s ease-in-out 3" : "none",
        direction: isRTL ? "rtl" : "ltr",
      }}
    >
      {/* ── Header ── */}
      <div style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        marginBottom: "1.1rem",
        gap: "1rem",
        flexWrap: "wrap",
        flexDirection: isRTL ? "row-reverse" : "row",
      }}>
        <SectionHeader
          title={`⚡ ${t("action_required")}`}
          subtitle={t("action_required_sub")}
        />
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          {hasPending && (
            <span style={{
              fontSize: "0.75rem",
              fontWeight: 700,
              fontFamily: "var(--font-mono)",
              padding: "4px 14px",
              borderRadius: "999px",
              background: "#1c1400",
              color: "#fbbf24",
              border: "1px solid #f59e0b",
              letterSpacing: "0.08em",
              animation: "hitl-pulse 1s ease-in-out infinite",
              boxShadow: "0 0 14px #f59e0b55",
            }}>
              {count} {t("pending")}
            </span>
          )}
          <button
            onClick={() => mutate()}
            title={t("refresh")}
            style={{
              background: "transparent",
              border: "1px solid #1e293b",
              borderRadius: "6px",
              color: "#475569",
              cursor: "pointer",
              fontSize: "0.75rem",
              padding: "3px 8px",
              fontFamily: "var(--font-mono)",
            }}
          >
            ↻ {t("refresh")}
          </button>
        </div>
      </div>

      {/* ── Stuck banner ── */}
      <StuckBanner onForceRun={handleForceRunDone} />

      {/* ── Loading ── */}
      {isLoading && !data && (
        <div style={{ padding: "2rem", textAlign: "center", color: "#475569", fontSize: "0.85rem" }}>
          {t("checking_approvals")}
        </div>
      )}

      {/* ── Error ── */}
      {error && (
        <div style={{
          padding: "1rem",
          borderRadius: "10px",
          background: "#1c0000",
          border: "1px solid #ef4444",
          color: "#f87171",
          fontSize: "0.83rem",
          fontFamily: "var(--font-mono)",
          lineHeight: 1.6,
        }}>
          <div style={{ fontWeight: 700, marginBottom: "0.4rem" }}>
            ✗ Could not reach /api/hitl/pending
          </div>
          <div style={{ color: "#ef444499" }}>{error.message ?? "API unreachable"}</div>
          <div style={{ color: "#475569", marginTop: "0.5rem", fontSize: "0.75rem" }}>
            Make sure <code>python scripts/start_api.py</code> is running on port 8001.
          </div>
        </div>
      )}

      {/* ── Empty state ── */}
      {!isLoading && !error && data && data.items.length === 0 && (
        <div style={{
          padding: "2.5rem",
          textAlign: "center",
          background: "#0f172a",
          borderRadius: "10px",
          border: "1px dashed #1e293b",
          color: "#475569",
          fontSize: "0.85rem",
        }}>
          <div style={{ fontSize: "1.5rem", marginBottom: "0.5rem" }}>✓</div>
          {t("no_tasks_pending")}
        </div>
      )}

      {/* ── Pending cards ── */}
      {data && data.items.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: "1.5rem" }}>
          {data.items.map((item) => (
            <HitlCard
              key={item.request_id}
              item={item}
              onResolved={() => mutate()}
            />
          ))}
        </div>
      )}

      <style>{`
        @keyframes hitl-pulse {
          0%, 100% { opacity: 1; box-shadow: 0 0 14px #f59e0b55; }
          50%       { opacity: 0.75; box-shadow: 0 0 28px #f59e0b99; }
        }
        @keyframes card-appear {
          from { opacity: 0; transform: translateY(-8px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        @keyframes section-alert {
          0%, 100% { outline-color: #f59e0b; }
          50%       { outline-color: #fbbf24; box-shadow: 0 0 20px #f59e0b66; }
        }
      `}</style>
    </section>
  );
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatTimeAgo(isoString: string): string {
  const diff = Date.now() - new Date(isoString).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  return `${h}h ago`;
}
