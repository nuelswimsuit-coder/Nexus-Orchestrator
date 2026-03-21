"use client";

import useSWR from "swr";
import { API_BASE, swrFetcher } from "@/lib/api";

type RetentionGroupRow = {
  group_id: string;
  label?: string;
  title?: string;
  member_count?: number | null;
  baseline?: number;
  stability_score?: number;
  drop_pct_vs_baseline?: number;
  error?: string;
};

type InviteTrackRow = {
  group_id: string;
  invite_hash: string;
  user_id: number;
  join_date?: string | null;
  still_member?: boolean | null;
};

type RetentionHealthResponse = {
  ok?: boolean;
  empty?: boolean;
  skipped?: boolean;
  groups: RetentionGroupRow[];
  invite_tracking?: InviteTrackRow[];
  checked_at?: string | null;
  message?: string;
};

function scoreColor(score: number): string {
  if (score >= 95) return "#22c55e";
  if (score >= 85) return "#84cc16";
  if (score >= 70) return "#eab308";
  return "#f97316";
}

function GroupHealthCard({ row }: { row: RetentionGroupRow }) {
  const label = row.title || row.label || row.group_id;
  const score = Number(row.stability_score ?? 0);
  const accent = row.error ? "#64748b" : scoreColor(score);
  const members =
    row.member_count != null && row.member_count !== undefined
      ? String(row.member_count)
      : "—";
  const baseline = row.baseline != null ? String(row.baseline) : "—";

  return (
    <div
      style={{
        background: "linear-gradient(165deg, #0b1220, #0a0f1b)",
        border: `1px solid ${accent}55`,
        borderRadius: "12px",
        padding: "0.9rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.5rem",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "0.5rem" }}>
        <span
          style={{
            color: "#e2e8f0",
            fontFamily: "var(--font-mono)",
            fontSize: "0.7rem",
            fontWeight: 700,
            lineHeight: 1.35,
          }}
        >
          {label}
        </span>
        <span
          style={{
            color: accent,
            fontFamily: "var(--font-mono)",
            fontSize: "0.75rem",
            fontWeight: 800,
            letterSpacing: "0.06em",
          }}
        >
          {row.error ? "ERR" : `${score}%`}
        </span>
      </div>
      <span style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.6rem" }}>
        {row.group_id}
      </span>
      {row.error ? (
        <span style={{ color: "#94a3b8", fontFamily: "var(--font-sans)", fontSize: "0.65rem" }}>{row.error}</span>
      ) : (
        <div style={{ display: "flex", flexWrap: "wrap", gap: "0.75rem" }}>
          <span style={{ color: "#94a3b8", fontFamily: "var(--font-mono)", fontSize: "0.62rem" }}>
            Members: {members} / baseline {baseline}
          </span>
          {row.drop_pct_vs_baseline != null && row.drop_pct_vs_baseline > 0 ? (
            <span style={{ color: "#fb923c", fontFamily: "var(--font-mono)", fontSize: "0.62rem" }}>
              Δ {row.drop_pct_vs_baseline.toFixed(1)}% vs baseline
            </span>
          ) : null}
        </div>
      )}
      {!row.error && (
        <div
          style={{
            marginTop: "0.25rem",
            height: "6px",
            borderRadius: "4px",
            background: "#1e293b",
            overflow: "hidden",
          }}
        >
          <div
            style={{
              width: `${Math.min(100, Math.max(0, score))}%`,
              height: "100%",
              background: `linear-gradient(90deg, ${accent}99, ${accent})`,
              borderRadius: "4px",
              transition: "width 0.5s ease",
            }}
          />
        </div>
      )}
    </div>
  );
}

export default function GroupHealth() {
  const { data, isLoading } = useSWR<RetentionHealthResponse>(
    `${API_BASE}/api/system/retention-health`,
    swrFetcher<RetentionHealthResponse>,
    { refreshInterval: 60_000 },
  );

  const groups = data?.groups ?? [];
  const invites = data?.invite_tracking ?? [];
  const empty = Boolean(data?.empty) || groups.length === 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
      <div
        style={{
          color: "#a5b4fc",
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          letterSpacing: "0.08em",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          flexWrap: "wrap",
          gap: "0.35rem",
        }}
      >
        <span>GROUP HEALTH</span>
        {data?.checked_at ? (
          <span style={{ color: "#64748b", fontSize: "0.6rem", fontWeight: 500 }}>
            {data.checked_at.slice(0, 19).replace("T", " ")} UTC
          </span>
        ) : null}
      </div>

      {isLoading && !data ? (
        <div style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.68rem" }}>
          Loading retention snapshot…
        </div>
      ) : empty ? (
        <div style={{ color: "#64748b", fontFamily: "var(--font-mono)", fontSize: "0.65rem", lineHeight: 1.5 }}>
          {data?.message ||
            "Configure RETENTION_GROUPS_JSON on the master and run the worker — stability scores appear after the first check."}
        </div>
      ) : (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
            gap: "0.8rem",
          }}
        >
          {groups.map((g) => (
            <GroupHealthCard key={g.group_id} row={g} />
          ))}
        </div>
      )}

      {!empty && invites.length > 0 && (
        <div style={{ marginTop: "0.35rem" }}>
          <div
            style={{
              color: "#818cf8",
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              letterSpacing: "0.06em",
              marginBottom: "0.45rem",
            }}
          >
            INVITE LINK TRACKING ({invites.length})
          </div>
          <div
            style={{
              maxHeight: "160px",
              overflowY: "auto",
              border: "1px solid #1e293b",
              borderRadius: "8px",
              padding: "0.5rem 0.65rem",
            }}
          >
            {invites.slice(0, 40).map((r, i) => (
              <div
                key={`${r.group_id}-${r.user_id}-${i}`}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: "0.5rem",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.58rem",
                  color: "#94a3b8",
                  padding: "0.2rem 0",
                  borderBottom: i < Math.min(invites.length, 40) - 1 ? "1px solid #0f172a" : undefined,
                }}
              >
                <span>
                  uid {r.user_id}{" "}
                  <span style={{ color: "#475569" }}>· {r.invite_hash.slice(0, 10)}…</span>
                </span>
                <span style={{ color: r.still_member === true ? "#4ade80" : r.still_member === false ? "#f87171" : "#94a3b8" }}>
                  {r.still_member === true ? "in group" : r.still_member === false ? "left" : "unknown"}
                </span>
              </div>
            ))}
            {invites.length > 40 ? (
              <div style={{ color: "#475569", fontSize: "0.55rem", marginTop: "0.35rem" }}>
                +{invites.length - 40} more (see API / Redis snapshot)
              </div>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}
