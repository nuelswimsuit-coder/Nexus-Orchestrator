"use client";

import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";

// ── Architecture SVG ──────────────────────────────────────────────────────────
function ArchitectureSvg({ stealth }: { stealth: boolean }) {
  const c = (hex: string) => stealth ? "#1e293b" : hex;
  const t = (hex: string) => stealth ? "#334155" : hex;

  const box = (x: number, y: number, w: number, h: number, fill: string, label: string, sub?: string) => (
    <g key={label}>
      <rect x={x} y={y} width={w} height={h} rx={6} fill={c(fill)} stroke={c(fill)} strokeWidth={1} opacity={stealth ? 0.3 : 0.15} />
      <rect x={x} y={y} width={w} height={h} rx={6} fill="none" stroke={c(fill)} strokeWidth={1} />
      <text x={x + w / 2} y={y + h / 2 - (sub ? 6 : 0)} textAnchor="middle" dominantBaseline="middle" fontSize={10} fontFamily="monospace" fill={t(fill)} fontWeight="bold">{label}</text>
      {sub && <text x={x + w / 2} y={y + h / 2 + 10} textAnchor="middle" dominantBaseline="middle" fontSize={8} fontFamily="monospace" fill={t("#475569")}>{sub}</text>}
    </g>
  );

  const arrow = (x1: number, y1: number, x2: number, y2: number, color: string) => (
    <line key={`${x1}-${y1}`} x1={x1} y1={y1} x2={x2} y2={y2} stroke={c(color)} strokeWidth={1} strokeDasharray="4 3" markerEnd={`url(#arr-${color.replace("#","")})`} />
  );

  return (
    <svg viewBox="0 0 560 280" width="100%" style={{ maxHeight: 280 }}>
      <defs>
        {["6366f1","22c55e","f59e0b","22d3ee"].map(col => (
          <marker key={col} id={`arr-${col}`} markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" fill={c(`#${col}`)} />
          </marker>
        ))}
      </defs>

      {/* Master */}
      {box(20, 100, 120, 80, "#6366f1", "MASTER", "Windows Desktop")}
      {/* Redis */}
      {box(220, 110, 120, 60, "#f59e0b", "REDIS", "Broker + Pub/Sub")}
      {/* Workers */}
      {box(420, 60, 120, 50, "#22c55e", "WORKER-1", "Linux")}
      {box(420, 130, 120, 50, "#22d3ee", "WORKER-2", "Windows")}
      {box(420, 200, 120, 50, "#22c55e", "WORKER-N", "Docker")}
      {/* API */}
      {box(20, 200, 120, 60, "#22d3ee", "FASTAPI", "Control Center")}
      {/* Dashboard */}
      {box(220, 200, 120, 60, "#f59e0b", "NEXT.JS", "Dashboard")}

      {/* Arrows */}
      {arrow(140, 140, 220, 140, "#6366f1")}
      {arrow(340, 140, 420, 85, "#f59e0b")}
      {arrow(340, 140, 420, 155, "#f59e0b")}
      {arrow(340, 140, 420, 225, "#f59e0b")}
      {arrow(140, 230, 220, 230, "#22d3ee")}
      {arrow(20, 180, 20, 200, "#6366f1")}

      {/* Labels */}
      <text x={175} y={130} textAnchor="middle" fontSize={8} fontFamily="monospace" fill={t("#475569")}>enqueue</text>
      <text x={175} y={220} textAnchor="middle" fontSize={8} fontFamily="monospace" fill={t("#475569")}>HTTP API</text>
    </svg>
  );
}

// ── Credit row ────────────────────────────────────────────────────────────────
function Credit({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "0.5rem 0", borderBottom: "1px solid #0f172a" }}>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#475569" }}>{label}</span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#64748b" }}>{value}</span>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AboutPage() {
  const { stealth } = useStealth();
  const c = (hex: string) => stealth ? "#334155" : hex;

  return (
    <PageTransition>
      <div style={{ maxWidth: "900px", margin: "0 auto", padding: "2rem 1.5rem" }}>

        {/* Hero */}
        <div style={{ textAlign: "center", marginBottom: "2.5rem" }}>
          <div style={{ fontSize: "3rem", marginBottom: "0.5rem" }}>⚡</div>
          <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.4rem", fontWeight: 800, letterSpacing: "0.12em", textTransform: "uppercase", color: c("#f1f5f9"), marginBottom: "0.25rem" }}>
            Nexus Orchestrator
          </h1>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.8rem", color: c("#6366f1"), letterSpacing: "0.08em" }}>
            v2.0-Alpha
          </div>
          <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569", marginTop: "0.75rem", maxWidth: 480, margin: "0.75rem auto 0" }}>
            Distributed Agentic Workflow System — autonomous profit engine for Telegram growth operations.
          </p>
        </div>

        {/* Architecture map */}
        <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.5rem", marginBottom: "1.5rem" }}>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", marginBottom: "1rem" }}>
            🗺️ System Architecture
          </div>
          <ArchitectureSvg stealth={stealth} />
        </div>

        {/* Stack */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.25rem", marginBottom: "1.5rem" }}>
          <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.25rem" }}>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", marginBottom: "0.75rem" }}>
              🐍 Backend Stack
            </div>
            {[
              ["Runtime",     "Python 3.10+"],
              ["Queue",       "ARQ + Redis"],
              ["API",         "FastAPI + uvicorn"],
              ["DB Bridge",   "aiosqlite (read-only)"],
              ["Telegram",    "aiogram 3.x"],
              ["Encryption",  "Fernet (cryptography)"],
              ["Rate Limit",  "slowapi"],
            ].map(([k, v]) => <Credit key={k} label={k} value={v} />)}
          </div>
          <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.25rem" }}>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", marginBottom: "0.75rem" }}>
              ⚛️ Frontend Stack
            </div>
            {[
              ["Framework",   "Next.js 16 (App Router)"],
              ["Language",    "TypeScript 5"],
              ["Styling",     "Tailwind CSS v4"],
              ["Animation",   "Framer Motion"],
              ["Charts",      "Recharts"],
              ["Data",        "SWR"],
              ["Fonts",       "Geist Sans + Mono"],
            ].map(([k, v]) => <Credit key={k} label={k} value={v} />)}
          </div>
        </div>

        {/* Version info */}
        <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: "12px", padding: "1.25rem" }}>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: "#475569", marginBottom: "0.75rem" }}>
            📋 Version Info
          </div>
          <Credit label="Version"      value="2.0-Alpha" />
          <Credit label="Architecture" value="Multi-page Nexus OS" />
          <Credit label="Pages"        value="Dashboard · Operations · Fleet · Treasury · Automation · Settings" />
          <Credit label="Data Bridge"  value="Mangement Ahu → telefix.db (read-only)" />
          <Credit label="Notifications" value="Telegram Bot + WhatsApp (mock/evolution/twilio)" />
          <Credit label="HITL"         value="Redis pub/sub + durable key store" />
        </div>

      </div>
    </PageTransition>
  );
}
