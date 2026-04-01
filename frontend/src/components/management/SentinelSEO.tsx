"use client";

import { useCallback, useEffect, useState } from "react";
import {
  getManagementConfig,
  getManagementGroups,
  postManagementScan,
  type ManagementConfigResponse,
  type ManagementGroupRow,
} from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

export default function SentinelSEO() {
  const { t } = useI18n();
  const { tokens } = useTheme();
  const [rows, setRows] = useState<ManagementGroupRow[]>([]);
  const [cfg, setCfg] = useState<ManagementConfigResponse | null>(null);
  const [phrases, setPhrases] = useState("");
  const [msg, setMsg] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    Promise.all([getManagementGroups(), getManagementConfig()])
      .then(([g, c]) => {
        setRows(g.groups);
        setCfg(c);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const runSeo = async () => {
    const parts = phrases
      .split(/[,;\n]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    for (const p of parts) {
      if (p.split(/\s+/).length < 2) {
        setMsg(t("management_phrase_two_words"));
        return;
      }
    }
    setBusy(true);
    setMsg("");
    try {
      const r = await postManagementScan({
        run_health_scan: false,
        run_sentinel_seo: true,
        seo_keyword_phrases: parts.length ? parts : null,
      });
      if (r.errors.length) setMsg(r.errors.join("; "));
      else {
        setMsg(`${r.enqueued.length} SEO task(s) enqueued.`);
        load();
      }
    } catch (e) {
      setMsg(e instanceof Error ? e.message : "Failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      {cfg && (
        <ul
          style={{
            fontSize: "0.72rem",
            color: tokens.textMuted,
            margin: "0 0 1rem",
            paddingLeft: "1.2rem",
          }}
        >
          <li>
            SEO probe session: {cfg.nexus_seo_probe_session_configured ? "configured" : "not set (env / settings)"}
          </li>
          <li>Auto-rename: {cfg.nexus_seo_auto_rename ? "on" : "off"}</li>
          <li>Target title: {cfg.nexus_seo_target_title_set ? "set" : "not set"}</li>
        </ul>
      )}
      <label style={{ display: "block", fontSize: "0.72rem", color: tokens.textSecondary, marginBottom: "0.35rem" }}>
        {t("management_seo_phrases_label")}
      </label>
      <textarea
        value={phrases}
        onChange={(e) => setPhrases(e.target.value)}
        placeholder="e.g. my group name here"
        rows={3}
        style={{
          width: "100%",
          maxWidth: 480,
          fontFamily: "var(--font-mono)",
          fontSize: "0.75rem",
          padding: "0.5rem",
          borderRadius: 8,
          border: `1px solid ${tokens.borderDefault}`,
          background: "transparent",
          color: tokens.textPrimary,
          marginBottom: "0.75rem",
        }}
      />
      <button
        type="button"
        disabled={busy}
        onClick={() => void runSeo()}
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
        {busy ? t("loading") : t("management_run_sentinel_seo")}
      </button>
      {msg ? (
        <p style={{ fontSize: "0.75rem", color: tokens.textSecondary, marginTop: "0.75rem" }}>{msg}</p>
      ) : null}

      <h3
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.8rem",
          marginTop: "1.5rem",
          color: tokens.textPrimary,
        }}
      >
        {t("management_rank_table")}
      </h3>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.72rem", marginTop: "0.5rem" }}>
          <thead>
            <tr style={{ textAlign: "left", color: tokens.textMuted }}>
              <th style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>Group</th>
              <th style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>Keyword</th>
              <th style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>Rank</th>
              <th style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>Shadow</th>
            </tr>
          </thead>
          <tbody>
            {rows.flatMap((g) =>
              g.rank_tracker.map((rt, i) => (
                <tr key={`${g.id}-${i}-${rt.keyword_phrase}`} style={{ color: tokens.textPrimary }}>
                  <td style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                    {g.title ?? g.username ?? g.group_id}
                  </td>
                  <td style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                    {rt.keyword_phrase}
                  </td>
                  <td style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                    {rt.current_rank ?? "—"}
                  </td>
                  <td style={{ padding: "0.45rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                    {rt.is_shadowbanned ? "yes" : "no"}
                  </td>
                </tr>
              )),
            )}
          </tbody>
        </table>
      </div>
      {!rows.some((g) => g.rank_tracker.length > 0) ? (
        <p style={{ fontSize: "0.75rem", color: tokens.textMuted, marginTop: "0.75rem" }}>
          {t("management_no_ranks")}
        </p>
      ) : null}
    </div>
  );
}
