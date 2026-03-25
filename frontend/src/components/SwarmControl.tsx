"use client";

import { useCallback, useState } from "react";
import useSWR from "swr";
import {
  API_BASE,
  swrFetcher,
  triggerSync,
  type ClusterHealthResponse,
} from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

export default function SwarmControl() {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";
  const [note, setNote] = useState<string | null>(null);

  const { data, mutate } = useSWR<ClusterHealthResponse>(
    `${API_BASE}/api/cluster/health`,
    swrFetcher<ClusterHealthResponse>,
    { refreshInterval: 5000 },
  );

  const nodes = data?.nodes ?? [];

  const onRestart = useCallback(async () => {
    setNote(null);
    try {
      const r = await triggerSync();
      setNote(he ? `סנכרון: ${r.message}` : `Sync: ${r.message}`);
      void mutate();
    } catch {
      setNote(he ? "סנכרון נכשל" : "Sync failed");
    }
  }, [he, mutate]);

  const onPayload = useCallback(() => {
    void onRestart();
  }, [onRestart]);

  const onShell = useCallback((ip: string | undefined) => {
    const host = ip || "WORKER_IP";
    const cmd = `ssh user@${host}`;
    void navigator.clipboard.writeText(cmd).catch(() => {});
    setNote(he ? `הועתק ללוח: ${cmd}` : `Copied: ${cmd}`);
  }, [he]);

  const border = `1px solid ${tokens.borderSubtle}`;

  return (
    <div dir={he ? "rtl" : "ltr"}>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.62rem",
          color: tokens.textMuted,
          marginBottom: "0.75rem",
        }}
      >
        Redis {data?.redis_ok ? "OK" : "—"} · {data?.workers_online ?? 0}{" "}
        {he ? "וורקרים מחוברים" : "workers online"}
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
          gap: "1rem",
        }}
      >
        {nodes.map((n) => (
          <div
            key={n.node_id}
            style={{
              position: "relative",
              background: isHighContrast ? tokens.surface1 : tokens.cardBg,
              border,
              borderRadius: 12,
              padding: "1rem",
              boxShadow: `0 0 16px ${tokens.accentDim}`,
            }}
          >
            <div
              style={{
                position: "absolute",
                top: 8,
                [he ? "left" : "right"]: 10,
                width: 10,
                height: 10,
                borderRadius: "50%",
                background: n.online ? tokens.success : tokens.danger,
                boxShadow: `0 0 8px ${n.online ? tokens.success : tokens.danger}`,
              }}
            />
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontWeight: 700,
                color: tokens.accent,
                marginBottom: "0.35rem",
              }}
            >
              {n.display_label || n.node_id}
            </div>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: tokens.textSecondary }}>
              {n.role} · {n.local_ip ?? "—"} · {n.os_info ?? ""}
            </div>
            <div
              style={{
                marginTop: "0.65rem",
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: "0.35rem",
                fontFamily: "var(--font-mono)",
                fontSize: "0.65rem",
                color: tokens.textPrimary,
              }}
            >
              <span>CPU {n.cpu_percent?.toFixed(0) ?? "—"}%</span>
              <span>
                RAM {n.ram_used_mb != null ? `${Math.round(n.ram_used_mb)} MB` : "—"}
              </span>
              <span>
                {he ? "משימות" : "Tasks"} {n.active_jobs ?? 0}
              </span>
              <span title={n.last_seen}>
                Δ {n.probe_latency_ms != null ? `${n.probe_latency_ms}ms` : "—"}
              </span>
            </div>
            <div style={{ marginTop: "0.65rem", display: "flex", flexWrap: "wrap", gap: 6 }}>
              <button
                type="button"
                onClick={() => void onRestart()}
                style={miniBtn(tokens)}
              >
                {he ? "אתחול וורקר" : "Restart worker"}
              </button>
              <button type="button" onClick={onPayload} style={miniBtn(tokens)}>
                {he ? "עדכן payload" : "Update payload"}
              </button>
              <button
                type="button"
                onClick={() => onShell(n.local_ip)}
                style={miniBtn(tokens)}
              >
                Shell
              </button>
            </div>
          </div>
        ))}
      </div>

      {note && (
        <div
          style={{
            marginTop: "0.75rem",
            fontFamily: "var(--font-mono)",
            fontSize: "0.7rem",
            color: tokens.textSecondary,
          }}
        >
          {note}
        </div>
      )}
    </div>
  );
}

function miniBtn(tokens: { accent: string; surface3: string; textPrimary: string }) {
  return {
    fontFamily: "var(--font-mono)",
    fontSize: "0.58rem",
    padding: "4px 8px",
    borderRadius: 6,
    border: `1px solid ${tokens.accent}44`,
    background: tokens.surface3,
    color: tokens.textPrimary,
    cursor: "pointer",
  } as const;
}
