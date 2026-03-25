"use client";

/**
 * Swarm visualizer data layer — backed by Redis key ``nexus:nodes:all`` via
 * GET /api/cluster/nodes-all (mirror refreshed from heartbeat SCAN on each poll).
 */

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";

/** Redis aggregate key written by the cluster API (see nexus/api/routers/cluster.py). */
export const SWARM_NODES_REDIS_KEY = "nexus:nodes:all";

const NODES_ALL_API = "/api/cluster/nodes-all";

/**
 * @returns {{
 *   nodes: import("@/lib/api").NodeStatus[] | undefined,
 *   redisKey: string | undefined,
 *   timestamp: string | undefined,
 *   isLoading: boolean,
 *   error: Error | undefined,
 *   mutate: () => void,
 * }}
 */
export function useSwarmNodesAll() {
  const { data, error, isLoading, mutate } = useSWR(
    NODES_ALL_API,
    swrFetcher,
    { refreshInterval: 8_000 },
  );

  return {
    nodes: data?.nodes,
    redisKey: data?.redis_key,
    timestamp: data?.timestamp,
    isLoading,
    error,
    mutate,
  };
}

/**
 * Optional standalone table for debugging / future /nodes route.
 */
export default function SwarmNodesTable({ nodes }) {
  const rows = nodes ?? [];
  return (
    <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#e2e8f0" }}>
      <p style={{ color: "#64748b", marginBottom: "0.75rem" }}>
        Source: Redis <code style={{ color: "#22d3ee" }}>{SWARM_NODES_REDIS_KEY}</code>
      </p>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ textAlign: "left", color: "#94a3b8" }}>
            <th style={{ padding: "0.35rem" }}>IP</th>
            <th style={{ padding: "0.35rem" }}>Status</th>
            <th style={{ padding: "0.35rem" }}>CPU %</th>
            <th style={{ padding: "0.35rem" }}>Tasks</th>
            <th style={{ padding: "0.35rem" }}>node_id</th>
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 && (
            <tr>
              <td colSpan={5} style={{ padding: "0.75rem", color: "#475569" }}>
                No nodes in snapshot.
              </td>
            </tr>
          )}
          {rows.map((n) => (
            <tr key={n.node_id} style={{ borderTop: "1px solid #1e293b" }}>
              <td style={{ padding: "0.35rem" }}>{n.local_ip ?? "—"}</td>
              <td style={{ padding: "0.35rem", color: n.online ? "#4ade80" : "#f87171" }}>
                {n.online ? "LIVE" : "OFFLINE"}
              </td>
              <td style={{ padding: "0.35rem" }}>{Number(n.cpu_percent ?? 0).toFixed(1)}</td>
              <td style={{ padding: "0.35rem" }}>{n.active_tasks_count ?? n.active_jobs ?? 0}</td>
              <td style={{ padding: "0.35rem", color: "#64748b" }}>{n.node_id}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
