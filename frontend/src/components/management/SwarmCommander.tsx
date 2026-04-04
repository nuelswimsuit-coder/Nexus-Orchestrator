"use client";

import { useCallback, useEffect, useState } from "react";
import useSWR from "swr";
import {
  getManagementConfig,
  postCommunityFactoryInitiate,
  postManagementScan,
  swrFetcher,
  type CommunityFactoryStatusResponse,
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
  const [factoryBusy, setFactoryBusy] = useState(false);

  const { data: factoryStatus, mutate: mutateFactory } = useSWR<CommunityFactoryStatusResponse>(
    "/api/swarm/community-factory/status",
    swrFetcher<CommunityFactoryStatusResponse>,
    { refreshInterval: 5000 },
  );

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

  const startFactory = async () => {
    setFactoryBusy(true);
    setMsg("");
    try {
      const r = await postCommunityFactoryInitiate({
        phases: ["allocate", "create", "join", "chat"],
        dry_run: false,
        reset: false,
      });
      if (r.ok && r.task_id) {
        setMsg(`Community Factory enqueued: task ${r.task_id.slice(0, 8)}…`);
      } else {
        setMsg("Community Factory request sent.");
      }
      void mutateFactory();
    } catch (e) {
      setMsg(e instanceof Error ? e.message : "Factory initiate failed");
    } finally {
      setFactoryBusy(false);
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

      <div
        style={{
          marginTop: "1.25rem",
          paddingTop: "1rem",
          borderTop: `1px solid ${tokens.borderSubtle}`,
        }}
      >
        <h3
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.8rem",
            fontWeight: 700,
            letterSpacing: "0.06em",
            textTransform: "uppercase",
            color: tokens.textPrimary,
            margin: "0 0 0.35rem",
          }}
        >
          {t("community_factory_title")}
        </h3>
        <p style={{ fontSize: "0.72rem", color: tokens.textMuted, margin: "0 0 0.75rem" }}>
          {t("community_factory_desc")}
        </p>
        {factoryStatus ? (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
              gap: "0.5rem 1rem",
              fontSize: "0.68rem",
              color: tokens.textSecondary,
              marginBottom: "0.75rem",
            }}
          >
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_phase")}: </span>
              <strong style={{ color: tokens.textPrimary }}>{factoryStatus.phase ?? "—"}</strong>
            </div>
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_groups")}: </span>
              <strong style={{ color: tokens.textPrimary }}>{factoryStatus.total_groups}</strong>
            </div>
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_sessions")}: </span>
              <strong style={{ color: tokens.textPrimary }}>{factoryStatus.active_sessions}</strong>
            </div>
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_messages")}: </span>
              <strong style={{ color: tokens.textPrimary }}>{factoryStatus.messages_sent}</strong>
            </div>
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_floods")}: </span>
              <strong style={{ color: tokens.warning }}>{factoryStatus.flood_waits}</strong>
            </div>
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_bans")}: </span>
              <strong style={{ color: tokens.warning }}>{factoryStatus.bans}</strong>
            </div>
            <div>
              <span style={{ color: tokens.textMuted }}>{t("community_factory_error_rate")}: </span>
              <strong style={{ color: tokens.textPrimary }}>{factoryStatus.error_rate}</strong>
            </div>
          </div>
        ) : (
          <p style={{ fontSize: "0.68rem", color: tokens.textMuted, marginBottom: "0.75rem" }}>
            …
          </p>
        )}
        <button
          type="button"
          disabled={factoryBusy}
          onClick={() => void startFactory()}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            padding: "0.5rem 1rem",
            borderRadius: 8,
            border: `1px solid ${tokens.borderDefault}`,
            background: factoryBusy ? tokens.borderDefault : tokens.surface1,
            color: tokens.textPrimary,
            cursor: factoryBusy ? "wait" : "pointer",
          }}
        >
          {factoryBusy ? t("loading") : t("community_factory_start")}
        </button>
      </div>
    </section>
  );
}
