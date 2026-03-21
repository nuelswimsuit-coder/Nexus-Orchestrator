"use client";

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";

type ModuleRuntimeHealth = {
  module: string;
  active: boolean;
  stage: string;
  detail: string;
  node_id: string;
  cpu_percent: number;
  rss_mb: number;
  updated_at: string;
};

type ModuleHealthResponse = {
  modules: Record<string, ModuleRuntimeHealth>;
  queried_at: string;
};

function ModuleHealthCard({ data }: { data: ModuleRuntimeHealth }) {
  const active = data.active;
  const accent = active ? "#22c55e" : "#64748b";
  return (
    <div
      style={{
        background: "linear-gradient(165deg, #0b1220, #0a0f1b)",
        border: `1px solid ${accent}44`,
        borderRadius: "12px",
        padding: "0.9rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.45rem",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ color: "#e2e8f0", fontFamily: "var(--font-mono)", fontSize: "0.72rem", fontWeight: 700 }}>
          {data.module.toUpperCase()}
        </span>
        <span style={{ color: accent, fontFamily: "var(--font-mono)", fontSize: "0.62rem", fontWeight: 700 }}>
          {active ? "ACTIVE" : "IDLE"}
        </span>
      </div>
      <span style={{ color: "#94a3b8", fontFamily: "var(--font-mono)", fontSize: "0.62rem" }}>
        {data.stage || "idle"} {data.node_id ? `@ ${data.node_id}` : ""}
      </span>
      <span style={{ color: "#64748b", fontFamily: "var(--font-sans)", fontSize: "0.68rem" }}>
        {data.detail || "No active workload"}
      </span>
      <div style={{ display: "flex", gap: "1rem", marginTop: "0.15rem" }}>
        <span style={{ color: "#94a3b8", fontFamily: "var(--font-mono)", fontSize: "0.64rem" }}>
          CPU: {Number(data.cpu_percent || 0).toFixed(1)}%
        </span>
        <span style={{ color: "#94a3b8", fontFamily: "var(--font-mono)", fontSize: "0.64rem" }}>
          RAM: {Number(data.rss_mb || 0).toFixed(1)} MB
        </span>
      </div>
    </div>
  );
}

export default function ModuleHealth() {
  const { data, isLoading } = useSWR<ModuleHealthResponse>(
    "/api/modules/widgets/module-health",
    swrFetcher<ModuleHealthResponse>,
    { refreshInterval: 5000 }
  );

  const modules = data?.modules ?? {};
  const entries = Object.values(modules);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div style={{ color: "#93c5fd", fontFamily: "var(--font-mono)", fontSize: "0.72rem", letterSpacing: "0.08em" }}>
        MODULE HEALTH
      </div>
      {isLoading && entries.length === 0 ? (
        <div style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.68rem" }}>
          Loading module telemetry...
        </div>
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: "0.8rem" }}>
          {entries.map((item) => (
            <ModuleHealthCard key={item.module} data={item} />
          ))}
        </div>
      )}
    </div>
  );
}
