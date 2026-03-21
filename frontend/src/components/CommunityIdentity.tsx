"use client";

import useSWR from "swr";
import { API_BASE, swrFetcher } from "@/lib/api";

type SwarmDashboardRow = {
  group_key: string;
  config: Record<string, unknown>;
  community_identity: string;
  group_description: string;
  emerging_identity: string;
  updated_at?: string;
  next_run_at?: string;
  last_topic?: string;
  last_classify_at?: string;
};

type SwarmDashboardResponse = {
  groups: SwarmDashboardRow[];
  count: number;
};

function IdentityCard({ row }: { row: SwarmDashboardRow }) {
  const title =
    (typeof row.config?.group_title === "string" && row.config.group_title) ||
    `Group ${row.group_key}`;
  return (
    <div
      style={{
        background: "linear-gradient(165deg, #0b1220, #0a0f1b)",
        border: "1px solid rgba(0, 180, 255, 0.22)",
        borderRadius: "12px",
        padding: "1rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.55rem",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "0.75rem" }}>
        <span
          style={{
            color: "#e2e8f0",
            fontFamily: "var(--font-sans)",
            fontSize: "0.85rem",
            fontWeight: 700,
            letterSpacing: "0.04em",
          }}
        >
          {title}
        </span>
        <span
          style={{
            color: "#38bdf8",
            fontFamily: "var(--font-mono)",
            fontSize: "0.62rem",
            fontWeight: 700,
            textTransform: "uppercase",
            letterSpacing: "0.1em",
            textAlign: "right",
            maxWidth: "48%",
            lineHeight: 1.35,
          }}
        >
          {row.community_identity || "—"}
        </span>
      </div>
      <div
        style={{
          color: "#94a3b8",
          fontFamily: "var(--font-sans)",
          fontSize: "0.72rem",
          lineHeight: 1.5,
        }}
      >
        {row.group_description || "No description yet — first 24h classification pending."}
      </div>
      {row.emerging_identity ? (
        <div
          style={{
            color: "#64748b",
            fontFamily: "var(--font-mono)",
            fontSize: "0.62rem",
            lineHeight: 1.45,
            borderTop: "1px solid rgba(30, 41, 59, 0.9)",
            paddingTop: "0.5rem",
          }}
        >
          <span style={{ color: "#475569", letterSpacing: "0.06em" }}>EMERGING IDENTITY · </span>
          {row.emerging_identity}
        </div>
      ) : null}
      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.75rem" }}>
        <span style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.6rem" }}>
          next tick: {row.next_run_at?.slice(0, 16)?.replace("T", " ") || "—"}
        </span>
        <span style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.6rem" }}>
          last topic: {row.last_topic || "—"}
        </span>
      </div>
    </div>
  );
}

export default function CommunityIdentity() {
  const { data, isLoading } = useSWR<SwarmDashboardResponse>(
    `${API_BASE}/api/swarm/dashboard`,
    swrFetcher<SwarmDashboardResponse>,
    { refreshInterval: 15_000 },
  );

  const rows = data?.groups ?? [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.85rem" }}>
      <div
        style={{
          color: "#93c5fd",
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          letterSpacing: "0.08em",
        }}
      >
        SWARM · COMMUNITY IDENTITY
      </div>
      {isLoading && rows.length === 0 ? (
        <div style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.68rem" }}>
          Loading swarm synthesis state…
        </div>
      ) : rows.length === 0 ? (
        <div style={{ color: "#64748b", fontFamily: "var(--font-sans)", fontSize: "0.72rem", lineHeight: 1.5 }}>
          No groups registered. POST to <span style={{ color: "#7dd3fc" }}>/api/swarm/groups/{"{key}"}</span> or set{" "}
          <span style={{ color: "#7dd3fc" }}>SWARM_WARMER_CONFIG</span> with a JSON registry.
        </div>
      ) : (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
            gap: "0.85rem",
          }}
        >
          {rows.map((row) => (
            <IdentityCard key={row.group_key} row={row} />
          ))}
        </div>
      )}
    </div>
  );
}
