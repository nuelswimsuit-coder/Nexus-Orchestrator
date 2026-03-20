"use client";

import { AnimatePresence, motion } from "framer-motion";
import { useCallback, useEffect, useRef, useState } from "react";

// ── Types ─────────────────────────────────────────────────────────────────────

interface FlightModeState {
  active: boolean;
  triggered_at?: string;
  score?: number;
  reason?: string;
}

interface StabilityState {
  score: number;
  threshold: number;
  critical: boolean;
  updated_at: string | null;
}

interface FlightModeStatus {
  flight_mode: FlightModeState;
  stability: StabilityState;
  timestamp: string;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const API_BASE    = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8001";
const POLL_INTERVAL_MS = 3_000;

// ── Component ─────────────────────────────────────────────────────────────────

export default function FlightModeOverlay() {
  const [status, setStatus]       = useState<FlightModeStatus | null>(null);
  const [recovering, setRecovering] = useState(false);
  const [recoveryDone, setRecoveryDone] = useState(false);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── Polling ──────────────────────────────────────────────────────────────────

  const poll = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/flight-mode/status`, {
        cache: "no-store",
      });
      if (res.ok) {
        const data: FlightModeStatus = await res.json();
        setStatus(data);
        if (!data.flight_mode.active) {
          setRecoveryDone(false);
        }
      }
    } catch {
      // Silently ignore — API might be restarting under flight mode
    }
  }, []);

  useEffect(() => {
    poll();
    timerRef.current = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [poll]);

  // ── Recovery action ───────────────────────────────────────────────────────────

  const handleRecover = async () => {
    if (recovering) return;
    setRecovering(true);
    try {
      const res = await fetch(`${API_BASE}/api/flight-mode/recover`, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ operator: "dashboard" }),
      });
      if (res.ok) {
        setRecoveryDone(true);
        // Final poll to confirm deactivation
        await poll();
      }
    } catch {
      // Recovery will be retried on next click
    } finally {
      setRecovering(false);
    }
  };

  // ── Render: invisible when flight mode is not active ─────────────────────────

  const isActive = status?.flight_mode?.active === true;

  return (
    <AnimatePresence>
      {isActive && (
        <motion.div
          key="flight-mode-overlay"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.4 }}
          style={{
            position:       "fixed",
            inset:          0,
            zIndex:         9999,
            display:        "flex",
            flexDirection:  "column",
            alignItems:     "center",
            justifyContent: "center",
            backdropFilter: "blur(18px) brightness(0.35)",
            WebkitBackdropFilter: "blur(18px) brightness(0.35)",
            backgroundColor: "rgba(30, 0, 0, 0.72)",
          }}
        >
          {/* ── Pulsing status text ────────────────────────────────────────── */}
          <motion.div
            animate={{
              textShadow: [
                "0 0 20px rgba(255,30,30,0.8), 0 0 60px rgba(255,30,30,0.4)",
                "0 0 45px rgba(255,60,60,1),   0 0 100px rgba(255,60,60,0.7)",
                "0 0 20px rgba(255,30,30,0.8), 0 0 60px rgba(255,30,30,0.4)",
              ],
              color: ["#ff3030", "#ff6060", "#ff3030"],
            }}
            transition={{ duration: 1.6, repeat: Infinity, ease: "easeInOut" }}
            style={{
              fontFamily:    "var(--font-sans, sans-serif)",
              fontSize:      "clamp(1.5rem, 4vw, 2.6rem)",
              fontWeight:    900,
              letterSpacing: "0.04em",
              textAlign:     "center",
              color:         "#ff3030",
              lineHeight:    1.3,
              padding:       "0 2rem",
              maxWidth:      "800px",
              userSelect:    "none",
              direction:     "rtl",
            }}
          >
            מצב טיסה אוטונומי פעיל
            <br />
            <span style={{ fontSize: "0.65em", opacity: 0.85 }}>
              המערכת בשיחזור — הרצה בבדיקה בלבד
            </span>
          </motion.div>

          {/* ── Score badge ───────────────────────────────────────────────── */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            dir="rtl"
            style={{
              marginTop:       "1.6rem",
              padding:         "0.55rem 1.4rem",
              borderRadius:    "999px",
              border:          "1.5px solid rgba(255,60,60,0.5)",
              background:      "rgba(80,0,0,0.55)",
              color:           "#ffaaaa",
              fontFamily:      "var(--font-sans, monospace)",
              fontSize:        "0.95rem",
              fontWeight:      600,
              letterSpacing:   "0.06em",
              display:         "flex",
              alignItems:      "center",
              gap:             "0.4rem",
            }}
          >
            <span>מדד חוסן מערכת:</span>
            <span dir="ltr" style={{ color: "#ff6060", fontWeight: 800 }}>
              {status.flight_mode.score ?? "—"}/100
            </span>
            {status.flight_mode.triggered_at && (
              <span dir="ltr" style={{ opacity: 0.6, fontWeight: 400, fontSize: "0.85rem" }}>
                {new Date(status.flight_mode.triggered_at).toLocaleTimeString()}
              </span>
            )}
          </motion.div>

          {/* ── Sub-text ─────────────────────────────────────────────────── */}
          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.7 }}
            transition={{ delay: 0.35 }}
            style={{
              marginTop:   "1rem",
              color:       "#ffbbbb",
              fontFamily:  "var(--font-sans, sans-serif)",
              fontSize:    "1rem",
              fontWeight:  400,
              textAlign:   "center",
              direction:   "rtl",
              maxWidth:    "520px",
              lineHeight:  1.6,
              padding:     "0 1rem",
            }}
          >
            זיכרון Redis נוקה · המערכת פועלת בסימולציה (Sandbox) ·{" "}
            מסחר אמיתי חסום עד לאישור ידני
          </motion.p>

          {/* ── Recovery button ───────────────────────────────────────────── */}
          <motion.button
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.5, type: "spring", stiffness: 200 }}
            whileHover={{ scale: 1.05, boxShadow: "0 0 30px rgba(0,200,100,0.55)" }}
            whileTap={{ scale: 0.97 }}
            onClick={handleRecover}
            disabled={recovering}
            style={{
              marginTop:       "2.4rem",
              padding:         "0.9rem 2.8rem",
              borderRadius:    "14px",
              border:          "2px solid rgba(0,220,110,0.7)",
              background:      recovering
                ? "rgba(0,80,40,0.6)"
                : "linear-gradient(135deg, rgba(0,160,80,0.75) 0%, rgba(0,100,50,0.75) 100%)",
              color:           "#b0ffdb",
              fontFamily:      "var(--font-sans, sans-serif)",
              fontSize:        "1.15rem",
              fontWeight:      700,
              letterSpacing:   "0.03em",
              cursor:          recovering ? "not-allowed" : "pointer",
              transition:      "background 0.2s",
              direction:       "rtl",
              textAlign:       "center",
              minWidth:        "280px",
            }}
          >
            {recovering ? (
              <span style={{ opacity: 0.7 }}>מאתחל...</span>
            ) : recoveryDone ? (
              "✅ המערכת הופעלה בהצלחה"
            ) : (
              "✅ שחזור מערכת / System Recovery"
            )}
          </motion.button>

          {/* ── Warning footer ────────────────────────────────────────────── */}
          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.45 }}
            transition={{ delay: 0.7 }}
            style={{
              marginTop:  "2rem",
              color:      "#ff9090",
              fontFamily: "var(--font-sans, sans-serif)",
              fontSize:   "0.78rem",
              fontWeight: 400,
              textAlign:  "center",
              direction:  "ltr",
            }}
          >
            AUTONOMOUS FLIGHT MODE · NEXUS ORCHESTRATOR ·{" "}
            {status.flight_mode.reason ?? "Stability threshold breached"}
          </motion.p>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
