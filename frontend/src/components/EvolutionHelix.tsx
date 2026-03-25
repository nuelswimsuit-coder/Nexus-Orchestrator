"use client";

/**
 * EvolutionHelix — dual-strand DNA-style animation; speeds up when the engine
 * is busy or a deploy is in flight (learning / self-patch metaphor).
 */

import { useMemo } from "react";
import { useNexus } from "@/lib/nexus-context";

const CYAN = "var(--neon-binance, #00e5ff)";
const LIME = "var(--neon-poly, #b8ff3d)";

export default function EvolutionHelix() {
  const { engineState, deployPhase, deployLiveStep } = useNexus();

  const busy =
    engineState?.state === "calculating" ||
    engineState?.state === "dispatching" ||
    deployPhase === "running" ||
    Boolean(deployLiveStep);

  const periodSec = busy ? 2.2 : 5.5;
  const stateLabel = engineState?.state ?? "idle";

  const pathA = useMemo(
    () =>
      "M 10,50 Q 30,20 50,50 T 90,50 Q 70,80 50,50 T 10,50",
    [],
  );
  const pathB = useMemo(
    () =>
      "M 10,50 Q 30,80 50,50 T 90,50 Q 70,20 50,50 T 10,50",
    [],
  );

  return (
    <div
      dir="rtl"
      style={{
        background: "var(--glass-command, rgba(6,12,28,0.55))",
        backdropFilter: "blur(20px)",
        WebkitBackdropFilter: "blur(20px)",
        border: "1px solid var(--glass-command-border, rgba(0,229,255,0.14))",
        borderRadius: 18,
        padding: "1rem 1.15rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.65rem",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          gap: "0.5rem",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "#7dd3fc",
          }}
        >
          EVOLUTION TRACKER
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.52rem",
            color: busy ? LIME : "#64748b",
          }}
        >
          {stateLabel.toUpperCase()}
          {deployLiveStep ? ` · ${deployLiveStep}` : ""}
        </span>
      </div>

      <svg
        viewBox="0 0 100 100"
        preserveAspectRatio="xMidYMid meet"
        style={{
          width: "100%",
          height: 72,
          display: "block",
        }}
        aria-hidden
      >
        <defs>
          <linearGradient id="eh-a" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor={CYAN} stopOpacity="0.2" />
            <stop offset="50%" stopColor={CYAN} stopOpacity="0.95" />
            <stop offset="100%" stopColor={CYAN} stopOpacity="0.2" />
          </linearGradient>
          <linearGradient id="eh-b" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor={LIME} stopOpacity="0.2" />
            <stop offset="50%" stopColor={LIME} stopOpacity="0.95" />
            <stop offset="100%" stopColor={LIME} stopOpacity="0.2" />
          </linearGradient>
        </defs>
        <path
          d={pathA}
          fill="none"
          stroke="url(#eh-a)"
          strokeWidth="2.2"
          strokeLinecap="round"
          style={{
            animation: `evolution-dash ${periodSec}s linear infinite`,
          }}
        />
        <path
          d={pathB}
          fill="none"
          stroke="url(#eh-b)"
          strokeWidth="2.2"
          strokeLinecap="round"
          style={{
            animation: `evolution-dash ${periodSec}s linear infinite reverse`,
            animationDelay: `${periodSec * -0.5}s`,
          }}
        />
        {[20, 50, 80].map((x) => (
          <circle
            key={x}
            cx={x}
            cy={50 + Math.sin(x / 8) * 18}
            r="2.2"
            fill={x % 40 === 0 ? CYAN : LIME}
            opacity={0.85}
          />
        ))}
      </svg>

      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.52rem",
          color: "#475569",
          lineHeight: 1.45,
        }}
      >
        מהירות הסליל מוגברת כשהמנוע מחשב / משגר משימות או כשפריסת קוד פעילה.
      </div>

      <style>{`
        @keyframes evolution-dash {
          from { stroke-dashoffset: 0; }
          to { stroke-dashoffset: -240; }
        }
        svg path {
          stroke-dasharray: 36 24;
        }
      `}</style>
    </div>
  );
}
