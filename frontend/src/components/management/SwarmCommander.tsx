"use client";

import { useCallback, useEffect, useState } from "react";
import {
  getManagementConfig,
  postManagementScan,
  type ManagementConfigResponse,
} from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

export default function SwarmCommander() {
  const { t } = useI18n();
  const { tokens } = useTheme();
  const [cfg, setCfg] = useState<ManagementConfigResponse | null>(null);
  const [msg, setMsg] = useState<string>("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    getManagementConfig()
      .then(setCfg)
      .catch(() => setCfg(null));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const runHealth = async () => {
    setBusy(true);
    setMsg("");
    try {
      const r = await postManagementScan({ run_health_scan: true, run_sentinel_seo: false });
      if (r.errors.length) setMsg(r.errors.join("; "));
      else setMsg(`${r.enqueued.length} task(s) enqueued (worker must be running).`);
    } catch (e) {
      setMsg(e instanceof Error ? e.message : "Scan failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section
      style={{
        marginBottom: "1.5rem",
        padding: "1rem 1.25rem",
        borderRadius: 12,
        border: `1px solid ${tokens.borderDefault}`,
        background: "rgba(15, 23, 42, 0.45)",
      }}
    >
      <h2
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.85rem",
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          color: tokens.textPrimary,
          margin: "0 0 0.5rem",
        }}
      >
        {t("swarm_commander")}
      </h2>
      <p style={{ fontSize: "0.75rem", color: tokens.textMuted, margin: "0 0 1rem" }}>
        {t("swarm_commander_desc")}
      </p>
      {cfg && (
        <p style={{ fontSize: "0.68rem", color: tokens.textMuted, margin: "0 0 0.75rem" }}>
          Legacy TeleFix UI:{" "}
          <strong style={{ color: cfg.legacy_telefix_bot_enabled ? tokens.warning : tokens.success }}>
            {cfg.legacy_telefix_bot_enabled ? "enabled" : "disabled"}
          </strong>
        </p>
      )}
      <button
        type="button"
        disabled={busy}
        onClick={() => void runHealth()}
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          padding: "0.5rem 1rem",
          borderRadius: 8,
          border: `1px solid ${tokens.accent}`,
          background: busy ? tokens.borderDefault : tokens.accent,
          color: "#0f172a",
          cursor: busy ? "wait" : "pointer",
        }}
      >
        {busy ? t("loading") : t("management_run_health_scan")}
      </button>
      {msg ? (
        <p style={{ fontSize: "0.72rem", color: tokens.textSecondary, marginTop: "0.75rem" }}>{msg}</p>
      ) : null}
    </section>
  );
}
