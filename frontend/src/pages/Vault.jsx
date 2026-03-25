"use client";

/**
 * Sentinel Vault — security map (blocked IPs), Nuke Mode, rotate all proxies.
 */

import { useCallback, useEffect, useState } from "react";
import { API_BASE } from "@/lib/api";

const STORAGE_KEY = "nexus:vault:blocked_ips";

function loadBlocked() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch {
    /* ignore */
  }
  return [
    { ip: "185.220.101.44", reason: "SSH brute-force", ts: "2025-03-18T14:22:00Z" },
    { ip: "45.142.212.61", reason: "API fuzz /api/v1/*", ts: "2025-03-19T09:01:00Z" },
    { ip: "103.152.112.8", reason: "Redis probe", ts: "2025-03-20T11:40:00Z" },
  ];
}

export default function Vault() {
  const [blocked, setBlocked] = useState([]);
  const [nukeMsg, setNukeMsg] = useState(null);
  const [nukeBusy, setNukeBusy] = useState(false);
  const [proxyMsg, setProxyMsg] = useState(null);
  const [proxyBusy, setProxyBusy] = useState(false);

  useEffect(() => {
    setBlocked(loadBlocked());
  }, []);

  const persist = useCallback((rows) => {
    setBlocked(rows);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(rows));
    } catch {
      /* ignore */
    }
  }, []);

  const nukeMode = useCallback(async () => {
    if (!confirm("NUKE MODE: מוחק מפתחות API מהזיכרון ומנתק שרתים. לאשר?")) return;
    setNukeBusy(true);
    setNukeMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/system/vault/nuke`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ confirm: "NUKE_CONFIRM", scope: "memory_and_upstream" }),
      });
      const j = await res.json().catch(() => ({}));
      setNukeMsg(res.ok ? j.message || "Nuke dispatched" : `HTTP ${res.status} — ${JSON.stringify(j)}`);
    } catch {
      setNukeMsg("Local only: clear env session — wire POST /api/system/vault/nuke");
    } finally {
      setNukeBusy(false);
    }
  }, []);

  const rotateProxies = useCallback(async () => {
    setProxyBusy(true);
    setProxyMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/sessions/vault/proxy-rotate-all`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ strategy: "fresh_residential", scope: "swarm" }),
      });
      const j = await res.json().catch(() => ({}));
      setProxyMsg(res.ok ? j.message || "Proxies rotated for swarm" : `HTTP ${res.status}`);
    } catch {
      setProxyMsg("Stub — implement POST /api/sessions/vault/proxy-rotate-all");
    } finally {
      setProxyBusy(false);
    }
  }, []);

  return (
    <div style={{ maxWidth: 960, margin: "0 auto", padding: "0 0 2rem" }}>
      <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.05rem", color: "#f1f5f9", margin: "0 0 0.25rem" }}>
        SENTINEL VAULT
      </h1>
      <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#64748b", marginBottom: "1.25rem" }}>
        Security Map · Nuke Mode · החלפת IP לכל הנחיל
      </p>

      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.75rem", marginBottom: "1.5rem" }}>
        <button
          type="button"
          onClick={() => void nukeMode()}
          disabled={nukeBusy}
          style={{
            padding: "0.65rem 1rem",
            borderRadius: 10,
            border: "2px solid #ef4444",
            background: "rgba(239,68,68,0.12)",
            color: "#fecaca",
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 800,
            cursor: nukeBusy ? "wait" : "pointer",
          }}
        >
          {nukeBusy ? "…" : "NUKE MODE"}
        </button>
        <button
          type="button"
          onClick={() => void rotateProxies()}
          disabled={proxyBusy}
          style={{
            padding: "0.65rem 1rem",
            borderRadius: 10,
            border: "1px solid #38bdf8",
            background: "rgba(56,189,248,0.1)",
            color: "#7dd3fc",
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 700,
            cursor: proxyBusy ? "wait" : "pointer",
          }}
        >
          {proxyBusy ? "…" : "Rotate all proxies (swarm)"}
        </button>
      </div>
      {(nukeMsg || proxyMsg) && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#94a3b8", marginBottom: "1rem" }}>
          {nukeMsg && <div>{nukeMsg}</div>}
          {proxyMsg && <div>{proxyMsg}</div>}
        </div>
      )}

      <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#94a3b8", marginBottom: "0.5rem" }}>
        Security Map — IPs חסומים
      </div>
      <div style={{ border: "1px solid #1e293b", borderRadius: 12, overflow: "hidden" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.72rem" }}>
          <thead>
            <tr style={{ background: "#0f172a", color: "#64748b", textAlign: "left" }}>
              <th style={{ padding: "0.5rem 0.75rem" }}>IP</th>
              <th style={{ padding: "0.5rem 0.75rem" }}>Reason</th>
              <th style={{ padding: "0.5rem 0.75rem" }}>When</th>
              <th style={{ padding: "0.5rem 0.75rem" }} />
            </tr>
          </thead>
          <tbody>
            {blocked.map((row) => (
              <tr key={row.ip} style={{ borderTop: "1px solid #1e293b", color: "#e2e8f0" }}>
                <td style={{ padding: "0.5rem 0.75rem", fontFamily: "var(--font-mono)" }}>{row.ip}</td>
                <td style={{ padding: "0.5rem 0.75rem" }}>{row.reason}</td>
                <td style={{ padding: "0.5rem 0.75rem", color: "#64748b" }}>{row.ts}</td>
                <td style={{ padding: "0.5rem 0.75rem" }}>
                  <button
                    type="button"
                    onClick={() => persist(blocked.filter((b) => b.ip !== row.ip))}
                    style={{
                      fontSize: "0.58rem",
                      border: "1px solid #334155",
                      background: "transparent",
                      color: "#94a3b8",
                      borderRadius: 4,
                      cursor: "pointer",
                    }}
                  >
                    הסר
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
