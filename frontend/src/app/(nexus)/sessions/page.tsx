"use client";

import useSWR from "swr";
import PageTransition from "@/components/PageTransition";
import CyberGrid from "@/components/CyberGrid";
import ProxyStatusPanel from "@/components/ProxyStatusPanel";
import { useStealth } from "@/lib/stealth";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import {
  swrFetcher,
  type SessionCommanderAccount,
  type SessionCommanderResponse,
} from "@/lib/api";

function healthColor(health: string | undefined, stealth: boolean): string {
  if (stealth) return "#334155";
  const h = (health || "").toLowerCase();
  if (h === "green") return "#22c55e";
  if (h === "yellow") return "#eab308";
  if (h === "red") return "#ef4444";
  return "#64748b";
}

function HealthOrb({ health, stealth }: { health?: string; stealth: boolean }) {
  const c = healthColor(health, stealth);
  return (
    <span
      title={health || "unknown"}
      style={{
        width: 12,
        height: 12,
        borderRadius: "50%",
        background: c,
        display: "inline-block",
        boxShadow: stealth ? "none" : `0 0 10px ${c}`,
        flexShrink: 0,
      }}
    />
  );
}

function AccountRow({ row, stealth }: { row: SessionCommanderAccount; stealth: boolean }) {
  const { tokens, isHighContrast } = useTheme();
  const border = isHighContrast ? tokens.borderDefault : stealth ? "#0f172a" : "#1e293b";
  const text = isHighContrast ? tokens.textPrimary : stealth ? "#94a3b8" : "#e2e8f0";
  const muted = isHighContrast ? tokens.textMuted : "#64748b";

  return (
    <tr style={{ borderBottom: `1px solid ${border}` }}>
      <td style={{ padding: "0.65rem 0.5rem", fontFamily: "var(--font-mono)", fontSize: "0.78rem", color: text }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <HealthOrb health={row.health ?? undefined} stealth={stealth} />
          <span style={{ fontWeight: 600 }}>{row.session_stem}</span>
        </div>
      </td>
      <td style={{ padding: "0.65rem 0.5rem", fontSize: "0.78rem", color: muted }}>
        {row.phone ?? "—"}
      </td>
      <td style={{ padding: "0.65rem 0.5rem", fontSize: "0.78rem", color: muted }}>
        {row.proxy_ip || "—"}
      </td>
      <td style={{ padding: "0.65rem 0.5rem", fontSize: "0.72rem", color: muted, textTransform: "uppercase" }}>
        {row.status ?? "—"}
      </td>
      <td style={{ padding: "0.65rem 0.5rem", fontSize: "0.78rem", color: muted, fontFamily: "var(--font-mono)" }}>
        {row.lease_worker_id ? (
          <span>
            {row.lease_worker_id}
            {row.lease_task_id ? (
              <span style={{ display: "block", fontSize: "0.65rem", opacity: 0.85 }}>
                task {row.lease_task_id.slice(0, 8)}…
              </span>
            ) : null}
            {row.lease_ttl_seconds != null ? (
              <span style={{ display: "block", fontSize: "0.62rem", opacity: 0.75 }}>
                TTL {row.lease_ttl_seconds}s
              </span>
            ) : null}
          </span>
        ) : (
          "—"
        )}
      </td>
    </tr>
  );
}

export default function SessionCommanderPage() {
  const { stealth } = useStealth();
  const { t } = useI18n();
  const { tokens, isHighContrast } = useTheme();

  const { data, error, isLoading, mutate } = useSWR<SessionCommanderResponse>(
    "/api/sessions/vault/commander",
    swrFetcher<SessionCommanderResponse>,
    { refreshInterval: 3_000 },
  );

  const accounts = data?.accounts ?? [];

  return (
    <PageTransition>
      <div style={{ position: "relative", minHeight: "100vh", padding: "1.25rem 1.25rem 2rem" }}>
        <CyberGrid />
        <div style={{ position: "relative", zIndex: 1, maxWidth: 1200, margin: "0 auto" }}>
          <header style={{ marginBottom: "1.25rem", display: "flex", flexWrap: "wrap", alignItems: "baseline", justifyContent: "space-between", gap: "0.75rem" }}>
            <div>
              <h1
                style={{
                  fontFamily: "var(--font-sans)",
                  fontSize: "1.35rem",
                  fontWeight: 800,
                  letterSpacing: "0.04em",
                  color: isHighContrast ? tokens.textPrimary : stealth ? "#94a3b8" : "#f8fafc",
                  margin: 0,
                }}
              >
                {t("session_commander")}
              </h1>
              <p style={{ margin: "0.35rem 0 0", fontSize: "0.82rem", color: isHighContrast ? tokens.textMuted : "#64748b" }}>
                {t("nav_desc_session_commander")}
              </p>
            </div>
            <button
              type="button"
              onClick={() => mutate()}
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
                fontWeight: 600,
                padding: "8px 14px",
                borderRadius: 8,
                border: `1px solid ${stealth ? "#334155" : "#334155"}`,
                background: isHighContrast ? tokens.accentSubtle : "#0f172a",
                color: isHighContrast ? tokens.textSecondary : "#cbd5e1",
                cursor: "pointer",
              }}
            >
              {t("refresh")}
            </button>
          </header>

          {error ? (
            <div
              style={{
                padding: "1rem",
                borderRadius: 10,
                background: "#450a0a",
                border: "1px solid #7f1d1d",
                color: "#fecaca",
                fontSize: "0.85rem",
              }}
            >
              {String((error as Error).message || error)}
            </div>
          ) : null}

          {/* Proxy pool status */}
          <div style={{ marginBottom: "1.25rem" }}>
            <ProxyStatusPanel />
          </div>

          <div
            style={{
              borderRadius: 14,
              overflow: "hidden",
              border: `1px solid ${isHighContrast ? tokens.borderDefault : stealth ? "#0f172a" : "#1e293b"}`,
              background: isHighContrast ? tokens.accentSubtle : "linear-gradient(145deg, #0b1120, #070b14)",
              boxShadow: stealth ? "none" : "0 12px 40px #00000055",
            }}
          >
            <div
              style={{
                padding: "0.65rem 1rem",
                borderBottom: `1px solid ${isHighContrast ? tokens.borderFaint : "#0f172a"}`,
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
              }}
            >
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#64748b", textTransform: "uppercase", letterSpacing: "0.12em" }}>
                {t("live")} · {accounts.length} accounts
              </span>
              {isLoading && !data ? (
                <span style={{ fontSize: "0.75rem", color: "#64748b" }}>{t("loading")}</span>
              ) : null}
            </div>

            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ textAlign: "left", fontSize: "0.68rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#64748b" }}>
                    <th style={{ padding: "0.6rem 0.5rem", fontWeight: 700 }}>Account / Health</th>
                    <th style={{ padding: "0.6rem 0.5rem", fontWeight: 700 }}>Phone</th>
                    <th style={{ padding: "0.6rem 0.5rem", fontWeight: 700 }}>Proxy</th>
                    <th style={{ padding: "0.6rem 0.5rem", fontWeight: 700 }}>Status</th>
                    <th style={{ padding: "0.6rem 0.5rem", fontWeight: 700 }}>Worker lease</th>
                  </tr>
                </thead>
                <tbody>
                  {!isLoading && accounts.length === 0 ? (
                    <tr>
                      <td colSpan={5} style={{ padding: "2rem", textAlign: "center", color: "#64748b", fontSize: "0.88rem" }}>
                        No indexed sessions yet. Complete login via the API or POST /api/sessions/vault/sync-disk on the master.
                      </td>
                    </tr>
                  ) : null}
                  {accounts.map((row) => (
                    <AccountRow key={row.session_stem} row={row} stealth={stealth} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </PageTransition>
  );
}
