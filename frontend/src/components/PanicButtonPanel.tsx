"use client";

import React, { useState } from "react";
import useSWR from "swr";
import { swrFetcher, triggerPanic, resetPanic, type PanicStateResponse } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

/**
 * Soft panic panel — reversible emergency stop.
 * Sets SYSTEM_STATE:PANIC in Redis and broadcasts TERMINATE to all workers.
 * Can be reset immediately via the Resume button.
 */
export default function PanicButtonPanel() {
  const { stealth } = useStealth();
  const [armed, setArmed]     = useState(false);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const { data: panicState } = useSWR<PanicStateResponse>(
    "/api/system/panic/state",
    swrFetcher<PanicStateResponse>,
    { refreshInterval: 3_000 },
  );

  const [localPanic, setLocalPanic] = useState(false);
  const isPanic = localPanic || Boolean(panicState?.panic);

  async function handlePanic() {
    if (!armed) { setArmed(true); return; }
    setLoading(true);
    setMessage(null);
    try {
      const json = await triggerPanic();
      setMessage(`🚨 PANIC ACTIVATED — ${json.workers_terminated?.length ?? 0} workers terminated`);
      setLocalPanic(true);
    } catch {
      setMessage("שגיאה בהפעלת PANIC");
    } finally {
      setLoading(false);
      setArmed(false);
      setTimeout(() => setMessage(null), 8_000);
    }
  }

  async function handleReset() {
    setLoading(true);
    try {
      await resetPanic();
      setLocalPanic(false);
      setMessage("✅ המערכת הופעלה בהצלחה — System resumed");
    } catch {
      setLocalPanic(false);
      setMessage("✅ מצב חירום בוטל — Panic cleared");
    } finally {
      setLoading(false);
      setTimeout(() => setMessage(null), 5_000);
    }
  }

  const PANIC_RED  = "#ff3333";
  const SAFE_GREEN = "#00ff88";
  const accent     = isPanic ? PANIC_RED : (armed ? "#fbbf24" : SAFE_GREEN);

  return (
    <div
      dir="rtl"
      style={{
        background: "linear-gradient(160deg, #0a0e1a 0%, #080d18 100%)",
        backdropFilter: "blur(20px)",
        WebkitBackdropFilter: "blur(20px)",
        border: `1px solid ${stealth ? "#1e293b" : `${accent}33`}`,
        borderRadius: "16px",
        padding: "1.5rem",
        boxShadow: stealth
          ? "none"
          : isPanic
            ? `0 0 40px ${PANIC_RED}40, 0 0 80px ${PANIC_RED}18`
            : `0 0 30px ${accent}12`,
        transition: "all 0.4s",
        display: "flex",
        flexDirection: "column",
        gap: "1.25rem",
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Top accent line */}
      <div style={{
        position: "absolute", top: 0, left: "1.5rem", right: "1.5rem", height: "1px",
        background: stealth
          ? "transparent"
          : `linear-gradient(90deg, transparent, ${accent}55, transparent)`,
      }} />

      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
        <div style={{
          width: 8, height: 8, borderRadius: "50%",
          background: isPanic ? PANIC_RED : (stealth ? "#334155" : SAFE_GREEN),
          boxShadow: stealth ? "none" : isPanic ? `0 0 12px ${PANIC_RED}` : `0 0 8px ${SAFE_GREEN}`,
          animation: isPanic && !stealth ? "panic-blink 0.6s infinite" : "none",
        }} />
        <span style={{
          fontFamily: "var(--font-mono)", fontSize: "0.72rem", fontWeight: 700,
          letterSpacing: "0.1em", textTransform: "uppercase",
          color: stealth ? "#2a3450" : "#6b8fab",
        }}>
          {isPanic ? "⚠ עצירת חירום פעילה" : "עצירת חירום (PANIC)"}
        </span>
      </div>

      {/* Status row */}
      <div style={{
        fontFamily: "var(--font-mono)", fontSize: "0.62rem",
        color: stealth ? "#1e293b" : (isPanic ? PANIC_RED : "#334155"),
        lineHeight: 1.5,
      }}>
        {isPanic
          ? `מערכת הופסקה · ${(panicState?.activated_at ?? "").slice(0, 19).replace("T", " ") || "—"} UTC`
          : armed
            ? "לחץ שוב לאישור — כל המשימות יופסקו"
            : "מערכת פעילה · לחץ להפעלת חירום"}
      </div>

      {/* Main button */}
      {!isPanic ? (
        <button
          onClick={handlePanic}
          disabled={loading}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.78rem",
            fontWeight: 800,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
            color: loading ? "#475569" : (stealth ? "#334155" : (armed ? "#fbbf24" : PANIC_RED)),
            background: armed && !stealth
              ? "#fbbf2410"
              : (!stealth ? `${PANIC_RED}10` : "transparent"),
            border: `2px solid ${
              loading ? "#1e293b"
              : stealth ? "#1e293b"
              : armed ? "#fbbf2466"
              : `${PANIC_RED}66`
            }`,
            borderRadius: "12px",
            padding: "0.85rem 1rem",
            cursor: loading ? "not-allowed" : "pointer",
            width: "100%",
            boxShadow: stealth || loading ? "none"
              : armed ? `0 0 20px #fbbf2422`
              : `0 0 16px ${PANIC_RED}20`,
            transition: "all 0.2s",
            animation: armed && !stealth && !loading ? "panic-blink 0.8s infinite" : "none",
          }}
        >
          {loading ? "מפעיל..." : armed ? "⚠ אשר הפסקה" : "🛑 PANIC — עצור הכל"}
        </button>
      ) : (
        <button
          onClick={handleReset}
          disabled={loading}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.75rem",
            fontWeight: 800,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            color: loading ? "#475569" : (stealth ? "#334155" : SAFE_GREEN),
            background: stealth ? "transparent" : `${SAFE_GREEN}10`,
            border: `2px solid ${loading ? "#1e293b" : stealth ? "#1e293b" : `${SAFE_GREEN}55`}`,
            borderRadius: "12px",
            padding: "0.85rem 1rem",
            cursor: loading ? "not-allowed" : "pointer",
            width: "100%",
            boxShadow: stealth || loading ? "none" : `0 0 16px ${SAFE_GREEN}18`,
            transition: "all 0.2s",
          }}
        >
          {loading ? "מאפס..." : "✓ אפס מערכת / RESUME"}
        </button>
      )}

      {/* Cancel armed state */}
      {armed && !loading && (
        <button
          onClick={() => setArmed(false)}
          style={{
            fontFamily: "var(--font-mono)", fontSize: "0.6rem", fontWeight: 600,
            color: "#475569", background: "transparent", border: "none",
            cursor: "pointer", textAlign: "center", letterSpacing: "0.08em",
          }}
        >
          ✕ ביטול
        </button>
      )}

      {/* Feedback message */}
      {message && (
        <div style={{
          fontFamily: "var(--font-mono)", fontSize: "0.62rem",
          color: isPanic ? PANIC_RED : SAFE_GREEN,
          textAlign: "center",
          letterSpacing: "0.06em",
        }}>
          {message}
        </div>
      )}

      {/* Description */}
      <div style={{
        fontFamily: "var(--font-mono)", fontSize: "0.58rem",
        color: stealth ? "#1e293b" : "#1e3a5f",
        lineHeight: 1.6,
        borderTop: `1px solid ${stealth ? "#0f172a" : "#0f172a"}`,
        paddingTop: "0.75rem",
        direction: "rtl",
      }}>
        מפסיק את כל פעולות המעבד (Worker) ומשדר TERMINATE לכל הצמתים. ניתן לאיפוס מיידי.
      </div>

      <style>{`
        @keyframes panic-blink {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}
