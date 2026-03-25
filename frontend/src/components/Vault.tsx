"use client";

import { useCallback, useMemo, useState, type ReactNode } from "react";
import useSWR from "swr";
import { swrFetcher, getConfig, type ConfigResponse } from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

const FAKE_IP_LOG = [
  { ip: "100.64.12.3", ts: "2025-03-20T18:22:01Z", action: "config_read" },
  { ip: "10.100.102.20", ts: "2025-03-20T17:01:44Z", action: "deploy_sync" },
  { ip: "127.0.0.1", ts: "2025-03-20T16:55:10Z", action: "dashboard" },
];

function mask(s: string) {
  if (s.length <= 6) return "••••••";
  return `${s.slice(0, 3)}…${s.slice(-2)}`;
}

export default function Vault() {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";
  const { data, mutate } = useSWR<ConfigResponse>("/api/config", swrFetcher<ConfigResponse>, {
    refreshInterval: 60_000,
  });

  const [rotated, setRotated] = useState<string | null>(null);

  const sshDisplay = useMemo(() => {
    const u = data?.worker_ssh_user ?? "—";
    return `${u} @ ${mask(data?.worker_ip ?? "")}`;
  }, [data]);

  const onRotate = useCallback(async () => {
    setRotated(null);
    try {
      await getConfig();
      setRotated(
        he
          ? "סיבוב מפתחות מתוזמן — עדכן את .env והפעל מחדש את ה-master."
          : "Key rotation scheduled — update .env and restart master.",
      );
      void mutate();
    } catch {
      setRotated(he ? "שגיאה" : "Error");
    }
  }, [he, mutate]);

  const border = `1px solid ${tokens.borderSubtle}`;

  return (
    <div dir={he ? "rtl" : "ltr"} style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
          gap: "1rem",
        }}
      >
        <VaultCard title={he ? "מפתחות API" : "API keys"} tokens={tokens} isHighContrast={isHighContrast} border={border}>
          <Row label="GEMINI_API_KEY" value="•••••••• (vault)" tokens={tokens} />
          <Row label="BINANCE_KEYS" value="••••••••" tokens={tokens} />
          <Row label="POLYMARKET" value="••••••••" tokens={tokens} />
        </VaultCard>
        <VaultCard title={he ? "ארנקים" : "Wallets"} tokens={tokens} isHighContrast={isHighContrast} border={border}>
          <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: tokens.textSecondary }}>
            {he ? "כתובות מנוהלות במאגר מאובטח חיצוני." : "Addresses managed in external secure store."}
          </p>
        </VaultCard>
        <VaultCard title={he ? "פרוקסי" : "Proxies"} tokens={tokens} isHighContrast={isHighContrast} border={border}>
          <Row label={he ? "יעד worker" : "Worker target"} value={data?.worker_ip ?? "—"} tokens={tokens} />
          <Row label="SSH" value={sshDisplay} tokens={tokens} />
        </VaultCard>
      </div>

      <button
        type="button"
        onClick={() => void onRotate()}
        style={{
          alignSelf: he ? "flex-end" : "flex-start",
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          fontWeight: 700,
          padding: "10px 18px",
          borderRadius: 10,
          border: `1px solid ${tokens.warning}66`,
          background: `${tokens.warning}16`,
          color: tokens.warning,
          cursor: "pointer",
        }}
      >
        {he ? "סובב מפתחות" : "Rotate keys"}
      </button>
      {rotated && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", color: tokens.textSecondary }}>{rotated}</div>
      )}

      <div
        style={{
          background: isHighContrast ? tokens.surface1 : tokens.cardBg,
          border,
          borderRadius: 12,
          padding: "1rem",
        }}
      >
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            fontWeight: 700,
            letterSpacing: "0.1em",
            color: tokens.textMuted,
            marginBottom: "0.75rem",
          }}
        >
          Sentinel — {he ? "היסטוריית גישה לפי IP" : "IP access history"}
        </div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-mono)", fontSize: "0.68rem" }}>
          <thead>
            <tr style={{ color: tokens.textMuted }}>
              <th style={{ textAlign: "start", padding: "4px 0" }}>IP</th>
              <th style={{ textAlign: "start", padding: "4px 0" }}>{he ? "זמן" : "Time"}</th>
              <th style={{ textAlign: "start", padding: "4px 0" }}>{he ? "פעולה" : "Action"}</th>
            </tr>
          </thead>
          <tbody>
            {FAKE_IP_LOG.map((r) => (
              <tr key={r.ts + r.ip} style={{ borderTop: border, color: tokens.textPrimary }}>
                <td style={{ padding: "6px 0" }}>{r.ip}</td>
                <td style={{ padding: "6px 0" }}>{new Date(r.ts).toLocaleString(he ? "he-IL" : undefined)}</td>
                <td style={{ padding: "6px 0" }}>{r.action}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function VaultCard({
  title,
  children,
  tokens,
  isHighContrast,
  border,
}: {
  title: string;
  children: ReactNode;
  tokens: ReturnType<typeof useTheme>["tokens"];
  isHighContrast: boolean;
  border: string;
}) {
  return (
    <div
      style={{
        background: isHighContrast ? tokens.surface1 : tokens.cardBg,
        border,
        borderRadius: 12,
        padding: "1rem",
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.65rem",
          fontWeight: 700,
          color: tokens.accent,
          marginBottom: "0.5rem",
        }}
      >
        {title}
      </div>
      {children}
    </div>
  );
}

function Row({
  label,
  value,
  tokens,
}: {
  label: string;
  value: string;
  tokens: ReturnType<typeof useTheme>["tokens"];
}) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        gap: 8,
        fontFamily: "var(--font-mono)",
        fontSize: "0.68rem",
        padding: "6px 0",
        borderBottom: `1px solid ${tokens.borderFaint}`,
        color: tokens.textSecondary,
      }}
    >
      <span>{label}</span>
      <span style={{ color: tokens.textPrimary, wordBreak: "break-all" }}>{value}</span>
    </div>
  );
}
