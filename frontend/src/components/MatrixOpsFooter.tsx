"use client";

import { useEffect, useRef } from "react";
import useSWR from "swr";
import { API_BASE, swrFetcher, type ClusterHealthResponse } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

const MX_BG = "#0a0a0a";
const MX_GREEN = "#00ff41";
const MX_RED = "#ff3e3e";

/**
 * Bottom dashboard strip: pre-configured target heatmap + scrolling swarm terminal
 * fed by GET /api/cluster/health (SWR dedupes with FleetStatus).
 */
export default function MatrixOpsFooter() {
  const { stealth } = useStealth();
  const termRef = useRef<HTMLDivElement>(null);
  const { data } = useSWR<ClusterHealthResponse>(
    `${API_BASE}/api/cluster/health`,
    swrFetcher<ClusterHealthResponse>,
    { refreshInterval: 4_000 },
  );

  const lines = data?.swarm_activity ?? [];
  const targets = data?.targets ?? [];

  useEffect(() => {
    const el = termRef.current;
    if (!el) return;
    el.scrollTop = 0;
  }, [lines.length, data?.timestamp]);

  useEffect(() => {
    const el = termRef.current;
    if (!el) return;
    const id = window.setInterval(() => {
      el.scrollTop += 1;
      if (el.scrollTop + el.clientHeight >= el.scrollHeight - 2) {
        el.scrollTop = 0;
      }
    }, 420);
    return () => clearInterval(id);
  }, [lines.length]);

  return (
    <div
      style={{
        marginTop: "0.5rem",
        padding: "1.25rem 1.35rem",
        borderRadius: 16,
        background: MX_BG,
        border: `1px solid ${stealth ? "#1e293b" : `${MX_GREEN}28`}`,
        boxShadow: stealth ? "none" : `0 0 40px ${MX_GREEN}08, inset 0 1px 0 ${MX_GREEN}12`,
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
        gap: "1.25rem",
        alignItems: "stretch",
      }}
    >
      <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            fontWeight: 800,
            letterSpacing: "0.18em",
            color: stealth ? "#334155" : MX_GREEN,
          }}
        >
          TARGET HEATMAP
        </span>
        <p
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.68rem",
            color: stealth ? "#334155" : "#6b7280",
            margin: 0,
            lineHeight: 1.45,
          }}
        >
          BTC Regulation & Whale Alerts — intensity from war-room cache + swarm hits.
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: "0.65rem" }}>
          {targets.length === 0 && (
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#525252" }}>
              No target data yet.
            </div>
          )}
          {targets.map((t) => (
            <div key={t.id}>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "baseline",
                  marginBottom: 4,
                }}
              >
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.62rem",
                    fontWeight: 700,
                    color: stealth ? "#475569" : "#d1d5db",
                    letterSpacing: "0.06em",
                  }}
                >
                  {t.label}
                </span>
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.58rem",
                    color: stealth ? "#334155" : MX_GREEN,
                  }}
                >
                  {t.intensity.toFixed(0)}%
                </span>
              </div>
              <div
                style={{
                  height: 8,
                  borderRadius: 4,
                  background: stealth ? "#0f172a" : "#050805",
                  border: `1px solid ${stealth ? "#1e293b" : `${MX_GREEN}20`}`,
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    height: "100%",
                    width: `${Math.min(100, t.intensity)}%`,
                    background: stealth
                      ? "#334155"
                      : t.intensity > 70
                        ? `linear-gradient(90deg, ${MX_RED}, #ff6b6b)`
                        : `linear-gradient(90deg, #0d2a12, ${MX_GREEN})`,
                    boxShadow: stealth ? "none" : `0 0 12px ${MX_GREEN}33`,
                    transition: "width 0.4s ease",
                  }}
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", minHeight: 160 }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 8,
          }}
        >
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              fontWeight: 800,
              letterSpacing: "0.18em",
              color: stealth ? "#334155" : MX_GREEN,
            }}
          >
            GLOBAL SWARM · AGENT FEED
          </span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.52rem", color: "#525252" }}>
            {lines.length} events buffered
          </span>
        </div>
        <div
          ref={termRef}
          style={{
            flex: 1,
            overflow: "auto",
            fontFamily: "var(--font-mono), ui-monospace, monospace",
            fontSize: "0.62rem",
            lineHeight: 1.55,
            padding: "0.65rem 0.75rem",
            borderRadius: 10,
            background: "#030303",
            border: `1px solid ${stealth ? "#1e293b" : `${MX_GREEN}18`}`,
            color: stealth ? "#334155" : `${MX_GREEN}cc`,
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {lines.length === 0 ? (
            <span style={{ color: stealth ? "#1e293b" : "#4b5563" }}>
              &gt; Listening for keyword hits (whale, flash, breaking…) from connected agents…
            </span>
          ) : (
            lines
              .slice()
              .reverse()
              .map((line, i) => (
                <div key={`${i}-${line.slice(0, 24)}`}>
                  <span style={{ color: stealth ? "#334155" : "#22c55e", opacity: 0.65 }}>{">"} </span>
                  {line}
                </div>
              ))
          )}
        </div>
      </div>
    </div>
  );
}
