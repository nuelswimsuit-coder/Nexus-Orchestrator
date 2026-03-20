"use client";

/**
 * GlobalErrorOverlay — System-wide offline / self-repair notification banner.
 *
 * Mounts as a fixed overlay when the cluster Master is OFFLINE.
 * Displays a prominent Hebrew status indicator:
 *   "מערכת בעצירה - מנסה לתקן אוטומטית..."
 *
 * Fades out automatically when the master comes back online.
 */

import { useState, useEffect } from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import type { ClusterStatusResponse } from "@/lib/api";

export default function GlobalErrorOverlay() {
  const { data, isLoading } = useSWR<ClusterStatusResponse>(
    "/api/cluster/status",
    swrFetcher<ClusterStatusResponse>,
    { refreshInterval: 5_000, revalidateOnFocus: true }
  );

  const [visible, setVisible] = useState(false);
  const [animateOut, setAnimateOut] = useState(false);

  const masterNode  = data?.nodes.find(n => n.role === "master");
  const masterOnline = masterNode?.online ?? true; // assume online during initial load
  const isOffline   = !isLoading && data !== undefined && !masterOnline;

  useEffect(() => {
    if (isOffline) {
      setAnimateOut(false);
      setVisible(true);
    } else if (visible) {
      // Animate out before unmounting
      setAnimateOut(true);
      const t = setTimeout(() => setVisible(false), 600);
      return () => clearTimeout(t);
    }
  }, [isOffline]);

  if (!visible) return null;

  return (
    <div
      role="alert"
      aria-live="assertive"
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 9998,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "rgba(4, 4, 12, 0.82)",
        backdropFilter: "blur(10px)",
        WebkitBackdropFilter: "blur(10px)",
        animation: animateOut
          ? "overlay-fade-out 0.6s ease forwards"
          : "overlay-fade-in 0.45s cubic-bezier(0.22,1,0.36,1) forwards",
        pointerEvents: isOffline ? "auto" : "none",
      }}
    >
      {/* Modal card */}
      <div
        style={{
          background: "rgba(15, 4, 8, 0.92)",
          border: "2.5px solid rgba(255, 30, 60, 0.65)",
          borderRadius: "20px",
          padding: "3.25rem 4.5rem",
          maxWidth: "680px",
          width: "90%",
          textAlign: "center",
          backdropFilter: "blur(28px)",
          WebkitBackdropFilter: "blur(28px)",
          animation: "critical-glow-pulse 2s ease-in-out infinite",
          boxShadow: `
            0 0 0 1px rgba(255,30,60,0.18) inset,
            0 8px 64px rgba(0,0,0,0.8),
            0 0 80px rgba(255,30,60,0.18)
          `,
        }}
      >
        {/* Warning icon */}
        <div
          style={{
            fontSize: "3.25rem",
            marginBottom: "1.25rem",
            lineHeight: 1,
            animation: "blink-critical 1s step-end infinite",
            userSelect: "none",
          }}
        >
          ⚠
        </div>

        {/* Primary Hebrew status — always visible, always large */}
        <h1
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: "2.5rem",
            fontWeight: 900,
            color: "#ff3355",
            margin: "0 0 0.6rem",
            lineHeight: 1.2,
            direction: "rtl",
            textShadow: "0 0 24px rgba(255,50,80,0.5)",
            letterSpacing: "-0.01em",
          }}
        >
          מערכת בעצירה
        </h1>

        {/* Secondary Hebrew repair status */}
        <p
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: "1.5rem",
            fontWeight: 700,
            color: "#ffaa44",
            margin: "0 0 2.25rem",
            direction: "rtl",
            letterSpacing: "0.02em",
            textShadow: "0 0 16px rgba(255,170,68,0.35)",
          }}
        >
          מנסה לתקן אוטומטית...
        </p>

        {/* Animated repair progress bar */}
        <div
          style={{
            width: "100%",
            height: "7px",
            borderRadius: "4px",
            background: "rgba(255, 50, 80, 0.13)",
            overflow: "hidden",
            marginBottom: "2rem",
            border: "1px solid rgba(255,50,80,0.2)",
          }}
        >
          <div
            style={{
              height: "100%",
              borderRadius: "4px",
              background: "linear-gradient(90deg, #ff2244, #ff8844, #ff2244)",
              backgroundSize: "200% 100%",
              animation: "repair-scan 2.4s ease-in-out infinite, repair-shimmer 1.8s linear infinite",
            }}
          />
        </div>

        {/* Status pills row */}
        <div
          style={{
            display: "flex",
            gap: "0.75rem",
            justifyContent: "center",
            flexWrap: "wrap",
            marginBottom: "1.75rem",
          }}
        >
          {[
            { label: "MASTER", value: "OFFLINE", color: "#ff3355", bg: "rgba(255,50,80,0.08)" },
            { label: "REDIS",  value: "CHECKING", color: "#ffb800", bg: "rgba(255,184,0,0.08)" },
            { label: "WORKERS", value: "PENDING",  color: "#6b8fab", bg: "rgba(107,143,171,0.08)" },
          ].map(({ label, value, color, bg }) => (
            <div
              key={label}
              style={{
                padding: "6px 14px",
                borderRadius: "8px",
                background: bg,
                border: `1px solid ${color}40`,
                display: "flex",
                gap: "6px",
                alignItems: "center",
              }}
            >
              <span style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
                color: "#7da8cc",
                letterSpacing: "0.08em",
              }}>
                {label}
              </span>
              <span style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
                fontWeight: 700,
                color,
                letterSpacing: "0.06em",
              }}>
                {value}
              </span>
            </div>
          ))}
        </div>

        {/* English subtitle for non-Hebrew users */}
        <p
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.82rem",
            color: "rgba(255, 100, 120, 0.65)",
            margin: 0,
            letterSpacing: "0.09em",
            textTransform: "uppercase",
          }}
        >
          SYSTEM OFFLINE — AUTO-REPAIR IN PROGRESS
        </p>
      </div>

      <style>{`
        @keyframes overlay-fade-in {
          from { opacity: 0; backdrop-filter: blur(0px); }
          to   { opacity: 1; backdrop-filter: blur(10px); }
        }
        @keyframes overlay-fade-out {
          from { opacity: 1; }
          to   { opacity: 0; transform: scale(1.02); }
        }
        @keyframes repair-shimmer {
          0%   { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
      `}</style>
    </div>
  );
}
