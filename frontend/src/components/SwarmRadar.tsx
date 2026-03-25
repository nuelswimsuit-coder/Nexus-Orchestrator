"use client";

/**
 * SwarmRadar — circular “ping” map of online workers (laptops) with ripple pulses.
 */

import { useMemo } from "react";
import { motion } from "framer-motion";
import { useNexus } from "@/lib/nexus-context";
import { useStealth } from "@/lib/stealth";

const CYAN = "var(--neon-binance, #00e5ff)";
const LIME = "var(--neon-poly, #b8ff3d)";

export default function SwarmRadar() {
  const { cluster } = useNexus();
  const { stealth } = useStealth();

  const workers = useMemo(
    () =>
      (cluster?.nodes ?? []).filter(
        (n) => n.role === "worker" && n.online,
      ),
    [cluster?.nodes],
  );

  const masterOnline =
    cluster?.nodes?.some((n) => n.role === "master" && n.online) ?? false;

  const blips = useMemo(() => {
    const n = Math.max(workers.length, 1);
    return workers.map((w, i) => {
      const angle = (i / n) * Math.PI * 2 - Math.PI / 2;
      const r = 0.62;
      return {
        id: w.node_id,
        x: 50 + Math.cos(angle) * r * 50,
        y: 50 + Math.sin(angle) * r * 50,
        load: w.cpu_percent ?? 0,
      };
    });
  }, [workers]);

  return (
    <div
      dir="rtl"
      style={{
        background: "var(--glass-command, rgba(6,12,28,0.55))",
        backdropFilter: "blur(22px) saturate(1.5)",
        WebkitBackdropFilter: "blur(22px) saturate(1.5)",
        border: "1px solid var(--glass-command-border, rgba(0,229,255,0.14))",
        borderRadius: 20,
        padding: "1.25rem 1.35rem",
        boxShadow: stealth
          ? "none"
          : `0 0 40px ${CYAN}12, inset 0 1px 0 rgba(255,255,255,0.04)`,
        position: "relative",
        overflow: "hidden",
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.58rem",
          fontWeight: 700,
          letterSpacing: "0.14em",
          color: stealth ? "#475569" : "#5ee7df",
          marginBottom: "0.75rem",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          gap: "0.5rem",
        }}
      >
        <span>SWARM RADAR</span>
        <span style={{ opacity: 0.75, fontWeight: 600 }}>
          {workers.length} פעילים · master {masterOnline ? "ON" : "OFF"}
        </span>
      </div>

      <div
        style={{
          position: "relative",
          aspectRatio: "1",
          maxHeight: 280,
          margin: "0 auto",
        }}
      >
        <svg
          viewBox="0 0 100 100"
          style={{
            width: "100%",
            height: "100%",
            display: "block",
          }}
          aria-label="Swarm radar"
        >
          <defs>
            <radialGradient id="sr-grid" cx="50%" cy="50%" r="55%">
              <stop offset="0%" stopColor={stealth ? "#1e293b" : "#0a1628"} />
              <stop offset="100%" stopColor={stealth ? "#0f172a" : "#050a12"} />
            </radialGradient>
            <filter id="sr-glow" x="-40%" y="-40%" width="180%" height="180%">
              <feGaussianBlur stdDeviation="1.2" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>
          <circle cx="50" cy="50" r="48" fill="url(#sr-grid)" opacity={0.95} />
          {[16, 32, 48].map((r) => (
            <circle
              key={r}
              cx="50"
              cy="50"
              r={r}
              fill="none"
              stroke={stealth ? "#334155" : `${CYAN}`}
              strokeOpacity={stealth ? 0.2 : 0.14}
              strokeWidth={0.35}
            />
          ))}
          <line
            x1="50"
            y1="2"
            x2="50"
            y2="98"
            stroke={stealth ? "#334155" : `${CYAN}`}
            strokeOpacity={0.12}
            strokeWidth={0.25}
          />
          <line
            x1="2"
            y1="50"
            x2="98"
            y2="50"
            stroke={stealth ? "#334155" : `${CYAN}`}
            strokeOpacity={0.12}
            strokeWidth={0.25}
          />

          {/* Center — command */}
          <circle
            cx="50"
            cy="50"
            r="4"
            fill={masterOnline ? CYAN : "#64748b"}
            opacity={stealth ? 0.35 : 0.9}
            filter={stealth ? undefined : "url(#sr-glow)"}
          />
          {!stealth && masterOnline && (
            <motion.circle
              cx="50"
              cy="50"
              r="4"
              fill="none"
              stroke={CYAN}
              strokeWidth="0.5"
              initial={{ r: 4, opacity: 0.55 }}
              animate={{ r: 22, opacity: 0 }}
              transition={{ duration: 2.4, repeat: Infinity, ease: "easeOut" }}
            />
          )}

          {blips.map((b, idx) => (
            <g key={b.id} filter={stealth ? undefined : "url(#sr-glow)"}>
              {!stealth && (
                <motion.circle
                  cx={b.x}
                  cy={b.y}
                  r="1.8"
                  fill="none"
                  stroke={LIME}
                  strokeWidth="0.45"
                  initial={{ r: 2, opacity: 0.5 }}
                  animate={{ r: 10 + (idx % 3) * 2, opacity: 0 }}
                  transition={{
                    duration: 1.8 + idx * 0.15,
                    repeat: Infinity,
                    ease: "easeOut",
                    delay: idx * 0.25,
                  }}
                />
              )}
              <circle
                cx={b.x}
                cy={b.y}
                r={1.2 + Math.min(b.load / 100, 1) * 0.9}
                fill={stealth ? "#475569" : LIME}
                opacity={stealth ? 0.5 : 0.95}
              />
            </g>
          ))}
        </svg>
      </div>

      <div
        style={{
          marginTop: "0.65rem",
          fontFamily: "var(--font-mono)",
          fontSize: "0.55rem",
          color: "#64748b",
          textAlign: "center",
          lineHeight: 1.5,
        }}
      >
        פעימות חיות מסנכרנות עם דופק ה־CPU של כל worker
      </div>
    </div>
  );
}
