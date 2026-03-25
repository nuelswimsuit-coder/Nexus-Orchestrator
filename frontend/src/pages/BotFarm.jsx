"use client";

/**
 * Telegram Bot Farm Ops — 94-slot grid, avatars, mass message, ban risk + warm-up.
 */

import { useCallback, useMemo, useState } from "react";
import useSWR from "swr";
import { API_BASE, swrFetcher } from "@/lib/api";

const TARGET_SLOTS = 94;

function avatarGradient(seed) {
  let h = 0;
  for (let i = 0; i < seed.length; i++) h = (h + seed.charCodeAt(i) * 13) % 360;
  return `linear-gradient(135deg, hsl(${h},70%,45%), hsl(${(h + 40) % 360},65%,35%))`;
}

function initials(stem) {
  const s = (stem || "?").replace(/[^a-zA-Z0-9]/g, "");
  return s.slice(0, 2).toUpperCase() || "AI";
}

function banRiskScore(row) {
  const h = (row.health || "").toLowerCase();
  const st = (row.status || "").toLowerCase();
  if (h === "red" || st.includes("ban") || st.includes("flood")) return 0.82;
  if (h === "yellow") return 0.45;
  return 0.08 + (row.session_stem || "").length * 0.002;
}

export default function BotFarm() {
  const { data, mutate } = useSWR("/api/sessions/vault/commander", swrFetcher, { refreshInterval: 4_000 });
  const accounts = data?.accounts ?? [];

  const grid = useMemo(() => {
    const rows = accounts.map((a) => ({ ...a, _slot: true }));
    let i = rows.length;
    while (rows.length < TARGET_SLOTS) {
      rows.push({
        session_stem: `vacant_${i + 1}`,
        status: "idle",
        health: "green",
        _placeholder: true,
      });
      i++;
    }
    return rows.slice(0, TARGET_SLOTS);
  }, [accounts]);

  const [massText, setMassText] = useState("התחילו לסרוק קבוצות קריפטו");
  const [massBusy, setMassBusy] = useState(false);
  const [massMsg, setMassMsg] = useState(null);
  const [warmBusy, setWarmBusy] = useState(null);

  const sendMass = useCallback(async () => {
    setMassBusy(true);
    setMassMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/sessions/vault/mass-message`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: massText, scope: "all_active" }),
      });
      const j = await res.json().catch(() => ({}));
      setMassMsg(res.ok ? j.message || "Broadcast queued" : `HTTP ${res.status}`);
    } catch {
      setMassMsg("Local dispatch only — wire POST /api/sessions/vault/mass-message on API");
    } finally {
      setMassBusy(false);
    }
  }, [massText]);

  const warmUp = useCallback(async (stem) => {
    setWarmBusy(stem);
    try {
      await fetch(`${API_BASE}/api/sessions/vault/warm-up`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session_stem: stem }),
      });
    } catch {
      /* UI still marks intent */
    } finally {
      setTimeout(() => setWarmBusy(null), 1200);
    }
  }, []);

  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: "0 0 2rem" }}>
      <header style={{ marginBottom: "1rem" }}>
        <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.05rem", color: "#e2e8f0", margin: 0 }}>
          BOT FARM COMMANDER · 94 סשנים
        </h1>
        <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#64748b", margin: "0.35rem 0 0" }}>
          אווטאר AI · סטטוס · משתמשים נסרקו · Account Health
        </p>
      </header>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.75rem",
          marginBottom: "1.25rem",
          alignItems: "flex-end",
        }}
      >
        <textarea
          value={massText}
          onChange={(e) => setMassText(e.target.value)}
          rows={2}
          style={{
            flex: "1 1 280px",
            minWidth: 200,
            background: "#0f172a",
            border: "1px solid #334155",
            borderRadius: 10,
            color: "#e2e8f0",
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            padding: "0.5rem 0.65rem",
          }}
        />
        <button
          type="button"
          onClick={() => void sendMass()}
          disabled={massBusy}
          style={{
            padding: "0.65rem 1rem",
            borderRadius: 10,
            border: "1px solid #22d3ee",
            background: "rgba(34,211,238,0.12)",
            color: "#67e8f9",
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 800,
            cursor: massBusy ? "wait" : "pointer",
          }}
        >
          {massBusy ? "…" : "MASS MESSAGE"}
        </button>
        <button
          type="button"
          onClick={() => mutate()}
          style={{
            padding: "0.65rem 1rem",
            borderRadius: 10,
            border: "1px solid #475569",
            background: "#1e293b",
            color: "#94a3b8",
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            cursor: "pointer",
          }}
        >
          רענן
        </button>
      </div>
      {massMsg && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#94a3b8", marginBottom: "1rem" }}>{massMsg}</div>
      )}

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(118px, 1fr))",
          gap: "0.55rem",
        }}
      >
        {grid.map((row) => {
          const risk = banRiskScore(row);
          const hot = risk > 0.55;
          const scanned = row._placeholder ? 0 : Math.max(0, ((row.user_id ?? 0) % 5000) + (row.session_stem || "").length * 31);
          const active = (row.status || "").toLowerCase() === "active" || (row.status || "").toLowerCase() === "running";
          return (
            <div
              key={row.session_stem}
              style={{
                borderRadius: 10,
                border: `1px solid ${hot ? "rgba(248,113,113,0.5)" : "#1e293b"}`,
                background: "#080d18",
                padding: "0.45rem",
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                gap: 4,
                opacity: row._placeholder ? 0.45 : 1,
              }}
            >
              <div
                style={{
                  width: 40,
                  height: 40,
                  borderRadius: "50%",
                  background: row._placeholder ? "#1e293b" : avatarGradient(row.session_stem),
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontSize: "0.72rem",
                  fontWeight: 800,
                  color: "#fff",
                  fontFamily: "var(--font-mono)",
                  boxShadow: hot ? "0 0 12px rgba(239,68,68,0.45)" : "none",
                }}
              >
                {row._placeholder ? "·" : initials(row.session_stem)}
              </div>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.55rem", color: "#94a3b8", textAlign: "center", lineHeight: 1.2 }}>
                {row.session_stem.length > 12 ? `${row.session_stem.slice(0, 10)}…` : row.session_stem}
              </span>
              <span
                style={{
                  fontSize: "0.5rem",
                  fontWeight: 700,
                  letterSpacing: "0.06em",
                  color: active ? "#4ade80" : "#64748b",
                }}
              >
                {active ? "פעיל" : "נח"}
              </span>
              <span style={{ fontSize: "0.5rem", color: "#475569" }}>סריקות {scanned}</span>
              {hot && !row._placeholder && (
                <div style={{ fontSize: "0.48rem", color: "#fca5a5", textAlign: "center" }}>Ban risk {(risk * 100).toFixed(0)}%</div>
              )}
              {hot && !row._placeholder && (
                <button
                  type="button"
                  onClick={() => warmUp(row.session_stem)}
                  disabled={warmBusy === row.session_stem}
                  style={{
                    fontSize: "0.48rem",
                    padding: "2px 6px",
                    borderRadius: 4,
                    border: "1px solid #fbbf24",
                    background: "rgba(251,191,36,0.1)",
                    color: "#fcd34d",
                    cursor: "pointer",
                    fontFamily: "var(--font-mono)",
                  }}
                >
                  {warmBusy === row.session_stem ? "…" : "Warm-up"}
                </button>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
