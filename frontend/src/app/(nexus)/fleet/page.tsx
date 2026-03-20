"use client";

import { useState } from "react";
import { useNexus } from "@/lib/nexus-context";
import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";
import type { NodeStatus } from "@/lib/api";

// ── CPU bar ───────────────────────────────────────────────────────────────────
function CpuBar({ pct, stealth }: { pct: number; stealth: boolean }) {
  const color = pct > 80 ? "#ef4444" : pct > 50 ? "#f59e0b" : "#22c55e";
  const c = stealth ? "#1e293b" : color;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
      <div style={{ flex: 1, height: 6, background: "#0f172a", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: `${Math.min(100, pct)}%`, height: "100%", background: c, borderRadius: 3, boxShadow: stealth ? "none" : `0 0 6px ${c}` }} />
      </div>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: stealth ? "#334155" : c, minWidth: 32, textAlign: "right" }}>
        {pct.toFixed(0)}%
      </span>
    </div>
  );
}

// ── Remote shell placeholder ──────────────────────────────────────────────────
function RemoteShell({ nodeId }: { nodeId: string }) {
  const { stealth } = useStealth();
  return (
    <div style={{ background: "#030810", border: `1px solid ${stealth ? "#0f172a" : "#1e293b"}`, borderRadius: "8px", padding: "1rem", fontFamily: "var(--font-mono)", fontSize: "0.75rem" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.75rem" }}>
        {["#ef4444","#f59e0b","#22c55e"].map((c, i) => (
          <span key={i} style={{ width: 8, height: 8, borderRadius: "50%", background: stealth ? "#1e293b" : c, display: "inline-block" }} />
        ))}
        <span style={{ color: "#334155" }}>nexus@{nodeId} — worker logs</span>
      </div>
      <div style={{ color: "#334155", lineHeight: 1.8 }}>
        <div>$ tail -f /var/log/nexus/worker.log</div>
        <div style={{ color: "#1e293b" }}>─── Remote Shell — Coming Soon ───</div>
        <div style={{ color: "#1e293b" }}>Connect via Tailscale VPN to enable</div>
        <div style={{ color: "#22c55e", animation: "rgb-pulse 1s step-end infinite" }}>▮</div>
      </div>
    </div>
  );
}

// ── Node card ─────────────────────────────────────────────────────────────────
function NodeCard({ node }: { node: NodeStatus }) {
  const { stealth } = useStealth();
  const [showShell, setShowShell] = useState(false);
  const isMaster = node.role === "master";
  const online   = node.online;
  const statusC  = online ? "#22c55e" : "#ef4444";
  const accentC  = stealth ? "#334155" : (isMaster ? "#6366f1" : "#22d3ee");

  return (
    <div style={{
      background: "linear-gradient(135deg, #0f172a, #0a0e1a)",
      border: `1px solid ${stealth ? "#1e293b" : `${accentC}44`}`,
      borderRadius: "14px",
      overflow: "hidden",
      boxShadow: stealth ? "none" : `0 0 20px ${accentC}18`,
    }}>
      {/* Top accent */}
      <div style={{ height: 3, background: stealth ? "#0f172a" : `linear-gradient(90deg, ${accentC}, transparent)` }} />

      <div style={{ padding: "1.25rem", display: "flex", flexDirection: "column", gap: "0.75rem" }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
            <span style={{ fontSize: "1rem" }}>{isMaster ? "👑" : "⚙️"}</span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.85rem", fontWeight: 700, color: stealth ? "#475569" : "#f1f5f9" }}>
              {node.node_id}
            </span>
            <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", padding: "1px 6px", borderRadius: "4px", background: stealth ? "#0f172a" : `${accentC}18`, color: stealth ? "#334155" : accentC, border: `1px solid ${stealth ? "#1e293b" : `${accentC}33`}` }}>
              {node.role.toUpperCase()}
            </span>
          </div>
          <span style={{ display: "flex", alignItems: "center", gap: "0.3rem", fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: stealth ? "#334155" : statusC }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: stealth ? "#334155" : statusC, display: "inline-block", boxShadow: online && !stealth ? `0 0 5px ${statusC}` : "none" }} />
            {online ? "ONLINE" : "OFFLINE"}
          </span>
        </div>

        {/* Specs grid */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.4rem 1rem" }}>
          {[
            ["🌐 IP",  node.local_ip ?? "—"],
            ["🖥️ OS",  node.os_info ?? "—"],
            ["🎮 GPU", node.gpu_model ?? "N/A"],
            ["📋 Jobs", String(node.active_jobs)],
          ].map(([k, v]) => (
            <div key={k}>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155", textTransform: "uppercase", letterSpacing: "0.08em" }}>{k}</div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#64748b", wordBreak: "break-all" }}>{v}</div>
            </div>
          ))}
        </div>

        {/* CPU */}
        <div>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: "0.3rem" }}>CPU Load</div>
          <CpuBar pct={node.cpu_percent} stealth={stealth} />
        </div>

        {/* RAM */}
        <div>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: "0.3rem" }}>
            RAM — {node.ram_used_mb.toFixed(0)} / {(node.ram_total_mb ?? 0).toFixed(0)} MB
          </div>
          <CpuBar pct={node.ram_total_mb ? (node.ram_used_mb / node.ram_total_mb * 100) : 0} stealth={stealth} />
        </div>

        {/* CPU model */}
        {node.cpu_model && (
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155", wordBreak: "break-word" }}>
            🖥️ {node.cpu_model}
          </div>
        )}

        {/* Remote shell toggle */}
        <button
          onClick={() => setShowShell(v => !v)}
          style={{
            background: "transparent",
            border: `1px solid ${stealth ? "#1e293b" : "#1e293b"}`,
            borderRadius: "6px",
            color: stealth ? "#334155" : "#475569",
            cursor: "pointer",
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            padding: "4px 10px",
            textAlign: "left",
          }}
        >
          {showShell ? "▲ Hide Shell" : "▼ Worker Logs (Remote Shell)"}
        </button>
        {showShell && <RemoteShell nodeId={node.node_id} />}
      </div>
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────
export default function FleetPage() {
  const { cluster, clusterLoading } = useNexus();
  const { stealth } = useStealth();

  const nodes = cluster?.nodes ?? [];
  const sorted = [...nodes].sort((a, b) =>
    a.role === "master" ? -1 : b.role === "master" ? 1 : 0
  );

  return (
    <PageTransition>
      <div style={{ maxWidth: "1400px", margin: "0 auto", padding: "2rem 1.5rem" }}>
        <div style={{ marginBottom: "1.5rem" }}>
          <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.1rem", fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: stealth ? "#334155" : "#f1f5f9", marginBottom: "0.25rem" }}>
            🖥️ Fleet Monitor
          </h1>
          <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569" }}>
            Hardware health for all nodes — Master + Workers
          </p>
        </div>

        {clusterLoading && !cluster && (
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.85rem", color: "#334155", padding: "3rem", textAlign: "center" }}>
            Loading cluster data…
          </div>
        )}

        {nodes.length === 0 && !clusterLoading && (
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.85rem", color: "#334155", padding: "3rem", textAlign: "center", border: "1px dashed #1e293b", borderRadius: "12px" }}>
            No nodes reporting heartbeats. Start the master process.
          </div>
        )}

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))", gap: "1.5rem" }}>
          {sorted.map(node => <NodeCard key={node.node_id} node={node} />)}
        </div>
      </div>
    </PageTransition>
  );
}
