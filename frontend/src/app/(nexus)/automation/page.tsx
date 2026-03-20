"use client";

import { useState } from "react";
import { motion } from "framer-motion";
import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";

// ── Rule card ─────────────────────────────────────────────────────────────────
interface Rule {
  id: string;
  name: string;
  type: "scraper" | "content" | "adder" | "warmup";
  enabled: boolean;
  schedule: string;
  lastRun: string;
  status: "idle" | "running" | "error";
}

const INITIAL_RULES: Rule[] = [
  { id: "r1", name: "Nightly Super-Scrape",  type: "scraper",  enabled: true,  schedule: "02:00 daily",  lastRun: "Yesterday 02:00", status: "idle" },
  { id: "r2", name: "Content Factory — AI",  type: "content",  enabled: false, schedule: "Manual",       lastRun: "Never",           status: "idle" },
  { id: "r3", name: "Auto-Adder Pipeline",   type: "adder",    enabled: true,  schedule: "Every 6h",     lastRun: "6h ago",          status: "idle" },
  { id: "r4", name: "Session Warmup",        type: "warmup",   enabled: false, schedule: "Manual",       lastRun: "3 days ago",      status: "idle" },
];

const TYPE_META: Record<Rule["type"], { icon: string; color: string; desc: string }> = {
  scraper: { icon: "🔍", color: "#6366f1", desc: "Hunts new Telegram groups by niche keyword" },
  content: { icon: "🤖", color: "#f59e0b", desc: "Generates AI posts via Gemini + Imagen" },
  adder:   { icon: "➕", color: "#22c55e", desc: "Adds scraped users to target groups" },
  warmup:  { icon: "🔥", color: "#ef4444", desc: "Warms up frozen Telethon sessions" },
};

function RuleCard({ rule, onToggle }: { rule: Rule; onToggle: (id: string) => void }) {
  const { stealth } = useStealth();
  const meta = TYPE_META[rule.type];
  const c = stealth ? "#334155" : meta.color;

  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      style={{
        background: "linear-gradient(135deg, #0f172a, #0a0e1a)",
        border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
        borderRadius: "12px",
        padding: "1.25rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.75rem",
        boxShadow: stealth ? "none" : `0 0 16px ${c}18`,
      }}
    >
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ fontSize: "1.1rem" }}>{meta.icon}</span>
          <div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.82rem", fontWeight: 700, color: stealth ? "#475569" : "#f1f5f9" }}>
              {rule.name}
            </div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#334155" }}>
              {meta.desc}
            </div>
          </div>
        </div>

        {/* Toggle */}
        <button
          onClick={() => onToggle(rule.id)}
          style={{
            width: 40,
            height: 22,
            borderRadius: 11,
            background: rule.enabled ? (stealth ? "#1e293b" : c) : "#1e293b",
            border: "none",
            cursor: "pointer",
            position: "relative",
            transition: "background 0.2s",
            boxShadow: rule.enabled && !stealth ? `0 0 8px ${c}88` : "none",
          }}
        >
          <span style={{
            position: "absolute",
            top: 3,
            left: rule.enabled ? 20 : 3,
            width: 16,
            height: 16,
            borderRadius: "50%",
            background: rule.enabled ? "#fff" : "#475569",
            transition: "left 0.2s",
          }} />
        </button>
      </div>

      {/* Details */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "0.5rem" }}>
        {[
          ["Schedule", rule.schedule],
          ["Last Run",  rule.lastRun],
          ["Status",   rule.status.toUpperCase()],
        ].map(([k, v]) => (
          <div key={k}>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.58rem", color: "#334155", textTransform: "uppercase", letterSpacing: "0.08em" }}>{k}</div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", color: stealth ? "#334155" : (k === "Status" && v === "RUNNING" ? "#22c55e" : "#64748b") }}>{v}</div>
          </div>
        ))}
      </div>

      {/* Action row */}
      <div style={{ display: "flex", gap: "0.5rem" }}>
        <button style={{
          flex: 1,
          padding: "5px 0",
          background: "transparent",
          border: `1px solid ${stealth ? "#1e293b" : `${c}44`}`,
          borderRadius: "6px",
          color: stealth ? "#334155" : c,
          cursor: "pointer",
          fontFamily: "var(--font-mono)",
          fontSize: "0.68rem",
          fontWeight: 600,
        }}>
          ▶ Run Now
        </button>
        <button style={{
          flex: 1,
          padding: "5px 0",
          background: "transparent",
          border: "1px solid #1e293b",
          borderRadius: "6px",
          color: "#334155",
          cursor: "pointer",
          fontFamily: "var(--font-mono)",
          fontSize: "0.68rem",
        }}>
          ✏️ Edit Rule
        </button>
      </div>
    </motion.div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function AutomationPage() {
  const { stealth } = useStealth();
  const [rules, setRules] = useState<Rule[]>(INITIAL_RULES);

  function toggleRule(id: string) {
    setRules(rs => rs.map(r => r.id === id ? { ...r, enabled: !r.enabled } : r));
  }

  const active = rules.filter(r => r.enabled).length;

  return (
    <PageTransition>
      <div style={{ maxWidth: "1400px", margin: "0 auto", padding: "2rem 1.5rem" }}>
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: "1.5rem", flexWrap: "wrap", gap: "1rem" }}>
          <div>
            <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.1rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: stealth ? "#334155" : "#f1f5f9", marginBottom: "0.25rem" }}>
              🤖 Automation
            </h1>
            <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569" }}>
              Visual rule builder — Super-Scraper & Content Factory
            </p>
          </div>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", padding: "4px 12px", borderRadius: "999px", background: stealth ? "transparent" : "#22c55e18", color: stealth ? "#334155" : "#22c55e", border: `1px solid ${stealth ? "#1e293b" : "#22c55e44"}` }}>
            {active}/{rules.length} rules active
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))", gap: "1.25rem" }}>
          {rules.map(rule => (
            <RuleCard key={rule.id} rule={rule} onToggle={toggleRule} />
          ))}

          {/* Add rule placeholder */}
          <div style={{
            background: "#0a0e1a",
            border: "1px dashed #1e293b",
            borderRadius: "12px",
            padding: "1.25rem",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            gap: "0.5rem",
            cursor: "pointer",
            minHeight: 160,
            color: "#334155",
          }}>
            <span style={{ fontSize: "1.5rem" }}>＋</span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem" }}>Add New Rule</span>
          </div>
        </div>
      </div>
    </PageTransition>
  );
}
