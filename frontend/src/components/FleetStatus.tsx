"use client";

import useSWR from "swr";
import { API_BASE, swrFetcher, type ClusterHealthNode, type ClusterHealthResponse } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

const MX_BG = "#0a0a0a";
const MX_GREEN = "#00ff41";
const MX_RED = "#ff3e3e";
const MX_DIM = "#0d2a12";

function cpuTargetPct(role: ClusterHealthNode["role"]): number {
  return role === "master" ? 50 : 90;
}

function heatTier(cpu: number): "cool" | "warm" | "hot" | "critical" {
  if (cpu < 35) return "cool";
  if (cpu < 65) return "warm";
  if (cpu < 85) return "hot";
  return "critical";
}

function tierColor(tier: ReturnType<typeof heatTier>, stealth: boolean): string {
  if (stealth) return "#334155";
  switch (tier) {
    case "cool":
      return "#1a5c28";
    case "warm":
      return "#2d8f3a";
    case "hot":
      return MX_GREEN;
    default:
      return MX_RED;
  }
}

function ConnectionGlyph({
  ok,
  stealth,
}: {
  ok: boolean;
  stealth: boolean;
}) {
  const c = stealth ? "#334155" : ok ? MX_GREEN : MX_RED;
  return (
    <span
      title={ok ? "Redis path OK · heartbeat live" : "Redis or node path failed"}
      style={{
        position: "relative",
        width: 12,
        height: 12,
        borderRadius: "50%",
        background: c,
        boxShadow: stealth ? "none" : ok ? `0 0 14px ${MX_GREEN}` : "none",
        animation: stealth || !ok ? "none" : "mx-pulse 1.4s ease-in-out infinite",
        flexShrink: 0,
      }}
    />
  );
}

function HeatStrip({ cpu, stealth }: { cpu: number; stealth: boolean }) {
  const tier = heatTier(cpu);
  const segments = 4;
  const lit = tier === "cool" ? 1 : tier === "warm" ? 2 : tier === "hot" ? 3 : 4;
  return (
    <div style={{ display: "flex", gap: 4, alignItems: "flex-end" }}>
      {Array.from({ length: segments }, (_, i) => (
        <div
          key={i}
          style={{
            width: 6,
            height: 10 + i * 5,
            borderRadius: 2,
            background: i < lit ? tierColor(tier, stealth) : stealth ? "#1e293b" : MX_DIM,
            opacity: i < lit ? 1 : 0.35,
            boxShadow:
              stealth || i >= lit ? "none" : `0 0 8px ${tierColor(tier, stealth)}44`,
          }}
        />
      ))}
      <span
        style={{
          marginLeft: 6,
          fontFamily: "var(--font-mono)",
          fontSize: "0.58rem",
          letterSpacing: "0.12em",
          color: stealth ? "#334155" : tierColor(tier, stealth),
          fontWeight: 700,
        }}
      >
        {tier === "cool" && "NOMINAL"}
        {tier === "warm" && "WARM"}
        {tier === "hot" && "HOT"}
        {tier === "critical" && "MAX"}
      </span>
    </div>
  );
}

function CpuPowerBar({
  node,
  stealth,
}: {
  node: ClusterHealthNode;
  stealth: boolean;
}) {
  const target = cpuTargetPct(node.role);
  const pct = Math.min(100, Math.max(0, node.cpu_percent));
  const atPower = node.role === "worker" ? pct >= 80 && pct <= 98 : pct >= 35 && pct <= 60;
  const fill = stealth ? "#334155" : atPower ? MX_GREEN : pct > target ? MX_RED : "#2dd4bf";

  return (
    <div style={{ width: "100%" }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          marginBottom: 6,
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            letterSpacing: "0.14em",
            color: stealth ? "#334155" : "#6b7280",
          }}
        >
          CPU LOAD
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.85rem",
            fontWeight: 800,
            color: stealth ? "#475569" : MX_GREEN,
            textShadow: stealth ? "none" : `0 0 12px ${MX_GREEN}44`,
          }}
        >
          {pct.toFixed(1)}%
          <span style={{ opacity: 0.55, fontWeight: 600, fontSize: "0.65rem", marginLeft: 6 }}>
            TARGET {target}%
          </span>
        </span>
      </div>
      <div
        style={{
          position: "relative",
          height: 10,
          borderRadius: 4,
          background: stealth ? "#0f172a" : "#050805",
          border: `1px solid ${stealth ? "#1e293b" : `${MX_GREEN}22`}`,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            borderRadius: 3,
            background: stealth ? "#334155" : `linear-gradient(90deg, ${MX_DIM}, ${fill})`,
            boxShadow: stealth ? "none" : `0 0 16px ${fill}55`,
            transition: "width 0.35s ease",
          }}
        />
        <div
          style={{
            position: "absolute",
            top: 0,
            bottom: 0,
            left: `${target}%`,
            width: 2,
            marginLeft: -1,
            background: stealth ? "#475569" : "#fbbf24",
            opacity: stealth ? 0.4 : 0.95,
            boxShadow: stealth ? "none" : "0 0 6px #fbbf24",
          }}
        />
      </div>
    </div>
  );
}

function FleetCard({
  node,
  redisOk,
  stealth,
}: {
  node: ClusterHealthNode;
  redisOk: boolean;
  stealth: boolean;
}) {
  const linkOk = redisOk && node.online;
  const roleTag = node.role === "master" ? "[MASTER / BRAIN]" : "[WORKER / SCAVENGER]";

  return (
    <div
      style={{
        position: "relative",
        background: `linear-gradient(165deg, ${MX_BG} 0%, #0d0f0d 100%)`,
        border: `1px solid ${stealth ? "#1e293b" : linkOk ? `${MX_GREEN}35` : `${MX_RED}40`}`,
        borderRadius: 14,
        padding: "1.15rem 1.2rem",
        boxShadow: stealth
          ? "none"
          : linkOk
            ? `0 0 0 1px ${MX_GREEN}12 inset, 0 12px 40px rgba(0,0,0,0.65), 0 0 28px ${MX_GREEN}0d`
            : `0 0 0 1px ${MX_RED}10 inset, 0 8px 32px rgba(0,0,0,0.55)`,
        display: "flex",
        flexDirection: "column",
        gap: "0.85rem",
        minHeight: 200,
      }}
    >
      <div
        style={{
          position: "absolute",
          top: 0,
          left: "1.25rem",
          right: "1.25rem",
          height: 1,
          background: stealth ? "transparent" : `linear-gradient(90deg, transparent, ${MX_GREEN}40, transparent)`,
        }}
      />

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <ConnectionGlyph ok={linkOk} stealth={stealth} />
          <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
                fontWeight: 800,
                letterSpacing: "0.1em",
                color: stealth ? "#475569" : "#e5e7eb",
              }}
            >
              {node.display_label}
            </span>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.58rem",
                color: stealth ? "#334155" : `${MX_GREEN}aa`,
                letterSpacing: "0.06em",
              }}
            >
              {node.node_id}
            </span>
          </div>
        </div>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.52rem",
            fontWeight: 800,
            letterSpacing: "0.08em",
            color: stealth ? "#334155" : MX_GREEN,
            border: `1px solid ${stealth ? "#1e293b" : `${MX_GREEN}44`}`,
            padding: "4px 8px",
            borderRadius: 6,
            whiteSpace: "nowrap",
          }}
        >
          {roleTag}
        </span>
      </div>

      <CpuPowerBar node={node} stealth={stealth} />

      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
        <HeatStrip cpu={node.cpu_percent} stealth={stealth} />
        <div
          style={{
            textAlign: "right",
            fontFamily: "var(--font-mono)",
            fontSize: "0.55rem",
            color: stealth ? "#334155" : "#6b7280",
            lineHeight: 1.5,
          }}
        >
          <div>PROBE {node.probe_latency_ms.toFixed(2)} ms</div>
          <div>JOBS {node.active_jobs}</div>
          <div>{node.local_ip ?? "—"}</div>
        </div>
      </div>
    </div>
  );
}

export default function FleetStatus() {
  const { stealth } = useStealth();
  const { data, error, isLoading } = useSWR<ClusterHealthResponse>(
    `${API_BASE}/api/cluster/health`,
    swrFetcher<ClusterHealthResponse>,
    { refreshInterval: 4_000 },
  );

  const redisOk = data?.redis_ok ?? false;
  const nodes = data?.nodes ?? [];
  const workers = data?.workers_online ?? 0;

  return (
    <section style={{ position: "relative" }}>
      <div style={{ marginBottom: "1rem" }}>
        <h2
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            fontWeight: 800,
            letterSpacing: "0.2em",
            color: stealth ? "#334155" : MX_GREEN,
            marginBottom: 6,
          }}
        >
          HARDWARE GRID · FLEET
        </h2>
        <p
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.82rem",
            color: stealth ? "#334155" : "#9ca3af",
            maxWidth: 640,
            lineHeight: 1.5,
          }}
        >
          Live heartbeats only — {workers} worker{workers === 1 ? "" : "s"} online. Redis{" "}
          {redisOk ? (
            <span style={{ color: stealth ? "#475569" : MX_GREEN }}>ONLINE</span>
          ) : (
            <span style={{ color: MX_RED }}>FAILED</span>
          )}
          {data?.redis_ping_ms != null && (
            <span style={{ opacity: 0.7 }}> · {data.redis_ping_ms.toFixed(2)} ms PING</span>
          )}
        </p>
      </div>

      {error && (
        <div
          style={{
            padding: "0.85rem 1rem",
            borderRadius: 10,
            border: `1px solid ${MX_RED}55`,
            color: MX_RED,
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            marginBottom: "1rem",
          }}
        >
          CLUSTER HEALTH UNAVAILABLE — API / Redis down
        </div>
      )}

      {isLoading && !data && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
            gap: "1rem",
          }}
        >
          {[1, 2, 3].map((i) => (
            <div
              key={i}
              style={{
                height: 200,
                borderRadius: 14,
                background: "#0f0f0f",
                border: "1px solid #1a1a1a",
                animation: "pulse 1.2s ease-in-out infinite",
              }}
            />
          ))}
        </div>
      )}

      {nodes.length > 0 && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
            gap: "1.1rem",
          }}
        >
          {nodes.map((n) => (
            <div key={n.node_id} style={{ position: "relative" }}>
              <FleetCard node={n} redisOk={redisOk} stealth={stealth} />
            </div>
          ))}
        </div>
      )}

      {!isLoading && !error && nodes.length === 0 && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: stealth ? "#334155" : "#9ca3af",
            padding: "1.5rem",
            textAlign: "center",
            border: `1px dashed ${stealth ? "#1e293b" : `${MX_GREEN}33`}`,
            borderRadius: 12,
            background: MX_BG,
          }}
        >
          NO ACTIVE HEARTBEATS — Start master / workers to populate the grid.
        </div>
      )}

      <style>{`
        @keyframes mx-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50% { opacity: 0.55; transform: scale(0.92); }
        }
        @keyframes pulse {
          0%, 100% { opacity: 0.45; }
          50% { opacity: 0.9; }
        }
      `}</style>
    </section>
  );
}
