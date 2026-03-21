"use client";

import React, { useEffect, useRef, useState } from "react";
import { useStealth } from "@/lib/stealth";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8001";

type FleetPhase = "started" | "progress" | "ended" | string;

interface FleetScanPayload {
  phase?: FleetPhase;
  task_type?: string;
  detail?: string;
  managed_members_total?: number;
  premium_members_total?: number;
  timestamp?: string;
}

function parsePayload(raw: string): FleetScanPayload | null {
  try {
    return JSON.parse(raw) as FleetScanPayload;
  } catch {
    return null;
  }
}

/**
 * Subscribes to GET /api/cluster/fleet/scan/stream (SSE) and shows a slim
 * progress bar while a fleet mapper / scrape run is active.
 */
export default function FleetScanProgress() {
  const { stealth } = useStealth();
  const esRef = useRef<EventSource | null>(null);
  const [phase, setPhase] = useState<FleetPhase | null>(null);
  const [detail, setDetail] = useState<string>("");
  const [managed, setManaged] = useState<number>(0);
  const [premium, setPremium] = useState<number>(0);

  useEffect(() => {
    const es = new EventSource(`${API_BASE}/api/cluster/fleet/scan/stream`);
    esRef.current = es;

    es.onmessage = (ev) => {
      const p = parsePayload(ev.data);
      if (!p?.phase) return;
      setPhase(p.phase);
      setDetail(p.detail ?? "");
      if (typeof p.managed_members_total === "number") {
        setManaged(p.managed_members_total);
      }
      if (typeof p.premium_members_total === "number") {
        setPremium(p.premium_members_total);
      }
    };

    es.onerror = () => {
      /* EventSource auto-reconnects; keep UI as-is */
    };

    return () => {
      es.close();
      esRef.current = null;
    };
  }, []);

  const active = phase === "started" || phase === "progress";
  const pct = phase === "ended" ? 100 : active ? 35 : 0;

  return (
    <div
      style={{
        marginTop: "1rem",
        padding: "0.85rem 1rem",
        borderRadius: "12px",
        background: stealth ? "rgba(15,23,42,0.5)" : "rgba(8,14,28,0.85)",
        border: stealth ? "1px solid #1e293b" : "1px solid rgba(0,180,255,0.25)",
        boxShadow: stealth ? "none" : "0 0 20px rgba(0,180,255,0.08)",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          marginBottom: "0.5rem",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            fontWeight: 700,
            letterSpacing: "0.14em",
            textTransform: "uppercase",
            color: stealth ? "#334155" : "#6b8fab",
          }}
        >
          Fleet scan
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            color: stealth ? "#1e293b" : "#475569",
          }}
        >
          {managed} managed / {premium} premium
        </span>
      </div>

      <div
        style={{
          height: "6px",
          borderRadius: "4px",
          background: "#0f172a",
          overflow: "hidden",
          marginBottom: "0.45rem",
        }}
      >
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            borderRadius: "4px",
            background: active
              ? "linear-gradient(90deg, #00b4ff, #7c3aed)"
              : phase === "ended"
                ? "linear-gradient(90deg, #22c55e, #00b4ff)"
                : "transparent",
            transition: "width 0.45s ease, background 0.35s ease",
          }}
        />
      </div>

      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.62rem",
          color: stealth ? "#1e293b" : "#94a3b8",
          lineHeight: 1.45,
        }}
      >
        {phase
          ? `${phase.toUpperCase()}${detail ? ` — ${detail}` : ""}`
          : "Listening for mapper / scrape events…"}
      </div>
    </div>
  );
}
