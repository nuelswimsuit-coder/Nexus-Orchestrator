"use client";

import { type ReactNode, useEffect, useState } from "react";
import { motion } from "framer-motion";
import useSWR from "swr";
import CyberGrid from "@/components/CyberGrid";
import StabilityGauge from "@/components/StabilityGauge";
import AgentThinkingLog from "@/components/AgentThinkingLog";
import ArbitrageGraph from "@/components/ArbitrageGraph";
import { patchConfig, swrFetcher } from "@/lib/api";
import type { ConfigResponse, SentinelStatusResponse } from "@/lib/api";

// ── Animation variants ────────────────────────────────────────────────────────

const stagger = {
  hidden: {},
  visible: {
    transition: { staggerChildren: 0.11, delayChildren: 0.18 },
  },
};

const fadeUp = {
  hidden:  { opacity: 0, y: 28, filter: "blur(4px)" },
  visible: {
    opacity: 1,
    y: 0,
    filter: "blur(0px)",
    transition: { duration: 0.6, ease: "easeOut" as const },
  },
};

// ── Status Pill ───────────────────────────────────────────────────────────────

function StatusPill({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color: string;
}) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: "0.45rem",
        fontFamily: "var(--font-mono)",
        fontSize: "0.6rem",
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase" as const,
        color,
        background: `${color}14`,
        border: `1px solid ${color}44`,
        borderRadius: "999px",
        padding: "0.28rem 0.8rem",
        whiteSpace: "nowrap" as const,
      }}
    >
      <span
        style={{
          width: 5,
          height: 5,
          borderRadius: "50%",
          background: color,
          boxShadow: `0 0 8px ${color}`,
          display: "inline-block",
          flexShrink: 0,
          animation: "nexus-dot-pulse 2.2s infinite",
        }}
      />
      <span style={{ color: "#4a7a9b", marginRight: "0.15rem" }}>{label}</span>
      {value}
    </span>
  );
}

// ── Section label with accent bar ─────────────────────────────────────────────

function SectionLabel({
  icon,
  label,
  sub,
}: {
  icon: string;
  label: string;
  sub: string;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "0.9rem",
        marginBottom: "1.75rem",
      }}
    >
      <div
        style={{
          width: 3,
          height: 38,
          borderRadius: 2,
          background:
            "linear-gradient(180deg, #00b4ff 0%, rgba(155,77,255,0.6) 100%)",
          boxShadow: "0 0 14px rgba(0,180,255,0.5)",
          flexShrink: 0,
        }}
      />
      <div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.55rem",
            marginBottom: "0.2rem",
          }}
        >
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.72rem",
              opacity: 0.75,
            }}
          >
            {icon}
          </span>
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.05rem",
              fontWeight: 800,
              letterSpacing: "0.13em",
              textTransform: "uppercase" as const,
              color: "#e8f2ff",
              textShadow: "0 0 24px rgba(0,180,255,0.2)",
            }}
          >
            {label}
          </span>
        </div>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.67rem",
            color: "#2d5470",
            letterSpacing: "0.06em",
          }}
        >
          {sub}
        </span>
      </div>
    </div>
  );
}

// ── Glassmorphism panel ───────────────────────────────────────────────────────

function GlassPanel({
  children,
  accent = false,
}: {
  children: ReactNode;
  accent?: boolean;
}) {
  return (
    <motion.div
      variants={fadeUp}
      style={{
        background: accent
          ? "rgba(2, 6, 20, 0.84)"
          : "rgba(3, 7, 19, 0.78)",
        backdropFilter: "blur(36px) saturate(1.9)",
        WebkitBackdropFilter: "blur(36px) saturate(1.9)",
        border: `1.5px solid rgba(0, 180, 255, ${accent ? "0.26" : "0.13"})`,
        borderRadius: "22px",
        padding: "2.25rem",
        boxShadow: accent
          ? `0 0 0 1px rgba(0,180,255,0.07) inset,
             0 20px 80px rgba(0,0,0,0.80),
             0 0 80px rgba(0,180,255,0.08),
             0 1px 0 rgba(0,180,255,0.28) inset`
          : `0 0 0 1px rgba(0,180,255,0.04) inset,
             0 10px 50px rgba(0,0,0,0.70),
             0 0 30px rgba(0,180,255,0.04)`,
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Top-edge shimmer line */}
      <div
        style={{
          position: "absolute",
          top: 0,
          left: "2.5rem",
          right: "2.5rem",
          height: "1px",
          background: `linear-gradient(90deg, transparent, rgba(0,180,255,${accent ? "0.45" : "0.22"}), rgba(155,77,255,${accent ? "0.25" : "0.10"}), transparent)`,
          pointerEvents: "none",
        }}
      />
      {/* Corner radial glow (accent only) */}
      {accent && (
        <div
          style={{
            position: "absolute",
            top: 0,
            right: 0,
            width: 200,
            height: 200,
            background:
              "radial-gradient(circle, rgba(155,77,255,0.08) 0%, transparent 70%)",
            transform: "translate(25%, -25%)",
            pointerEvents: "none",
          }}
        />
      )}
      {children}
    </motion.div>
  );
}

function QuickSettingsModal({
  open,
  onClose,
  initialPowerLimit,
  initialMaxWorkers,
  onSaved,
}: {
  open: boolean;
  onClose: () => void;
  initialPowerLimit: number;
  initialMaxWorkers: number;
  onSaved: (cfg: ConfigResponse) => void;
}) {
  const [powerLimit, setPowerLimit] = useState(initialPowerLimit);
  const [maxWorkers, setMaxWorkers] = useState(initialMaxWorkers);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!open) return;
    setPowerLimit(initialPowerLimit);
    setMaxWorkers(initialMaxWorkers);
    setError("");
  }, [open, initialPowerLimit, initialMaxWorkers]);

  if (!open) return null;

  return (
    <div
      onClick={(e) => {
        if (e.target === e.currentTarget && !saving) onClose();
      }}
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 100,
        background: "rgba(3,7,19,0.85)",
        backdropFilter: "blur(8px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "1rem",
      }}
    >
      <div
        style={{
          width: "100%",
          maxWidth: 520,
          borderRadius: 16,
          border: "1px solid rgba(0,180,255,0.28)",
          background: "rgba(3,7,19,0.94)",
          padding: "1.25rem",
          boxShadow: "0 20px 80px rgba(0,0,0,0.65)",
        }}
      >
        <h3
          style={{
            margin: 0,
            marginBottom: "0.85rem",
            color: "#e8f2ff",
            fontFamily: "var(--font-mono)",
            letterSpacing: "0.08em",
            fontSize: "0.9rem",
            textTransform: "uppercase",
          }}
        >
          Settings - Normal Mode
        </h3>
        <p
          style={{
            margin: 0,
            marginBottom: "1rem",
            color: "#4a7a9b",
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
          }}
        >
          Keep this machine light: 30% power and 2-3 workers.
        </p>
        <div style={{ display: "grid", gap: "0.8rem" }}>
          <label style={{ color: "#94a3b8", fontFamily: "var(--font-mono)", fontSize: "0.72rem" }}>
            Power Limit (%)
            <input
              type="number"
              min={0}
              max={100}
              value={powerLimit}
              onChange={(e) => setPowerLimit(Number(e.target.value))}
              style={{
                marginTop: "0.35rem",
                width: "100%",
                borderRadius: 8,
                border: "1px solid #1e293b",
                background: "#0f172a",
                color: "#e8f2ff",
                padding: "0.45rem 0.6rem",
              }}
            />
          </label>
          <label style={{ color: "#94a3b8", fontFamily: "var(--font-mono)", fontSize: "0.72rem" }}>
            Max Workers (2-3)
            <input
              type="number"
              min={2}
              max={3}
              value={maxWorkers}
              onChange={(e) => setMaxWorkers(Number(e.target.value))}
              style={{
                marginTop: "0.35rem",
                width: "100%",
                borderRadius: 8,
                border: "1px solid #1e293b",
                background: "#0f172a",
                color: "#e8f2ff",
                padding: "0.45rem 0.6rem",
              }}
            />
          </label>
        </div>
        {error && (
          <p style={{ color: "#ef4444", fontSize: "0.7rem", marginTop: "0.75rem", marginBottom: 0 }}>
            {error}
          </p>
        )}
        <div style={{ marginTop: "1rem", display: "flex", justifyContent: "flex-end", gap: "0.5rem" }}>
          <button
            onClick={onClose}
            disabled={saving}
            style={{
              border: "1px solid #334155",
              background: "transparent",
              color: "#94a3b8",
              borderRadius: 8,
              padding: "0.45rem 0.8rem",
            }}
          >
            Cancel
          </button>
          <button
            onClick={async () => {
              setSaving(true);
              setError("");
              try {
                const clampedWorkers = Math.max(2, Math.min(3, maxWorkers || 3));
                const next = await patchConfig({
                  power_limit: Math.max(0, Math.min(100, powerLimit || 30)),
                  max_workers: clampedWorkers,
                });
                onSaved(next);
                onClose();
              } catch (err) {
                setError(err instanceof Error ? err.message : String(err));
              } finally {
                setSaving(false);
              }
            }}
            disabled={saving}
            style={{
              border: "1px solid rgba(0,180,255,0.5)",
              background: "rgba(0,180,255,0.18)",
              color: "#c4f0ff",
              borderRadius: 8,
              padding: "0.45rem 0.8rem",
            }}
          >
            {saving ? "Saving..." : "Apply"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function HomePage() {
  const { data: sentinel } = useSWR<SentinelStatusResponse>(
    "/api/sentinel/status",
    swrFetcher<SentinelStatusResponse>,
    { refreshInterval: 5_000 },
  );
  const { data: cfg, mutate: mutateCfg } = useSWR<ConfigResponse>(
    "/api/config",
    swrFetcher<ConfigResponse>,
    { refreshInterval: 15_000 },
  );
  const [settingsOpen, setSettingsOpen] = useState(false);

  const isActive  = sentinel?.state === "active";
  const latencyMs = sentinel?.latency_ms;
  const ramPct    = sentinel?.ram_pct;

  return (
    <>
      {/* Three.js WebGL grid — placed fixed behind everything */}
      <CyberGrid opacity={0.62} speed={0.88} color="#00b4ff" glowColor="#9b4dff" />

      <motion.div
        dir="rtl"
        variants={stagger}
        initial="hidden"
        animate="visible"
        style={{
          position: "relative",
          zIndex: 1,
          maxWidth: "1340px",
          margin: "0 auto",
          padding: "2.5rem 1.5rem 5rem",
          display: "flex",
          flexDirection: "column",
          gap: "1.75rem",
        }}
      >
        {/* ── Hero header ───────────────────────────────────────────────── */}
        <motion.div variants={fadeUp}>
          <div
            style={{
              display: "flex",
              alignItems: "flex-end",
              justifyContent: "space-between",
              flexWrap: "wrap",
              gap: "1rem",
            }}
          >
            {/* Branding */}
            <div>
              <div
                style={{
                  fontFamily: "var(--font-assistant), var(--font-mono)",
                  fontSize: "0.65rem",
                  fontWeight: 700,
                  letterSpacing: "0.08em",
                  color: "#00b4ff",
                  marginBottom: "0.45rem",
                  textShadow: "0 0 22px rgba(0,180,255,0.6)",
                }}
              >
                ⬡ NEXUS-OS · v2.0-ALPHA · משטח שליטה
              </div>
              <h1
                style={{
                  fontFamily: "var(--font-assistant), var(--font-mono)",
                  fontSize: "clamp(1.75rem, 4vw, 3.2rem)",
                  fontWeight: 900,
                  letterSpacing: "0.02em",
                  color: "#e8f2ff",
                  lineHeight: 1.1,
                  margin: 0,
                  textShadow:
                    "0 0 40px rgba(0,180,255,0.18), 0 0 80px rgba(155,77,255,0.10)",
                }}
              >
                מרכז שליטה אופרטיבי — Nexus
              </h1>
              <p
                style={{
                  fontFamily: "var(--font-assistant), var(--font-mono)",
                  fontSize: "0.78rem",
                  color: "#2d5470",
                  marginTop: "0.5rem",
                  letterSpacing: "0.02em",
                }}
              >
                מערכת זרימת עבודה אוטונומית מבוזרת — שכבת אורקסטרציה חכמה
              </p>
            </div>

            {/* Live status pills */}
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: "0.45rem",
                alignItems: "center",
              }}
            >
              <StatusPill
                label="סנטינל"
                value={
                  sentinel
                    ? isActive
                      ? "פעיל"
                      : "לא מחובר"
                    : "מתחבר..."
                }
                color={
                  !sentinel
                    ? "#4a7a9b"
                    : isActive
                      ? "#00ff88"
                      : "#ff3333"
                }
              />
              {latencyMs != null && (
                <StatusPill
                  label="השהייה"
                  value={`${latencyMs.toFixed(0)}ms`}
                  color={
                    latencyMs < 200
                      ? "#00b4ff"
                      : latencyMs < 500
                        ? "#fbbf24"
                        : "#ff3333"
                  }
                />
              )}
              {ramPct != null && (
                <StatusPill
                  label="זיכרון"
                  value={`${ramPct.toFixed(0)}%`}
                  color={
                    ramPct < 70
                      ? "#00b4ff"
                      : ramPct < 85
                        ? "#fbbf24"
                        : "#ff3333"
                  }
                />
              )}
              <button
                onClick={() => setSettingsOpen(true)}
                style={{
                  border: "1px solid rgba(0,180,255,0.38)",
                  background: "rgba(0,180,255,0.10)",
                  color: "#9bdfff",
                  borderRadius: "999px",
                  padding: "0.35rem 0.85rem",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.62rem",
                  fontWeight: 700,
                  letterSpacing: "0.1em",
                  textTransform: "uppercase",
                  cursor: "pointer",
                }}
              >
                Settings
              </button>
            </div>
          </div>

          {/* Divider */}
          <div
            style={{
              marginTop: "1.75rem",
              height: "1px",
              background:
                "linear-gradient(270deg, rgba(0,180,255,0.4), rgba(155,77,255,0.22), transparent 80%)",
            }}
          />
        </motion.div>

        {/* ── Section 1 — מדד חוסן מערכת ────────────────────────────────── */}
        <GlassPanel accent>
          <SectionLabel
            icon="◎"
            label="מדד חוסן המערכת"
            sub="מוניטור בריאות אורביטלי · Sentinel AI · עדכון כל 5 שניות"
          />
          <StabilityGauge />
        </GlassPanel>

        {/* ── Section 2 — יומן אירועים חי ──────────────────────────────── */}
        <GlassPanel>
          <SectionLabel
            icon="⟁"
            label="יומן אירועים חי"
            sub="זרם פעילות סוכנים · פלט אורקסטרטור בזמן אמת · גלילה אוטומטית"
          />
          <AgentThinkingLog />
        </GlassPanel>

        {/* ── Section 3 — סורק ארביטראז' ───────────────────────────────── */}
        <GlassPanel accent>
          <SectionLabel
            icon="⬡"
            label="סורק ארביטראז'"
            sub="מצב סימולציה (Sandbox) · רשת ניורל בין-בורסאית · Binance × Polymarket · עדכון כל 2.5 שניות"
          />
          <ArbitrageGraph />
        </GlassPanel>
      </motion.div>

      <style>{`
        @keyframes nexus-dot-pulse {
          0%,  100% { opacity: 1;   transform: scale(1);   }
          50%        { opacity: 0.4; transform: scale(1.6); }
        }
      `}</style>
      <QuickSettingsModal
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        initialPowerLimit={cfg?.power_limit ?? 30}
        initialMaxWorkers={cfg?.max_workers ?? 3}
        onSaved={(next) => {
          void mutateCfg(next, false);
        }}
      />
    </>
  );
}
