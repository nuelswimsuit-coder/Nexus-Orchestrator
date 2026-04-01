"use client";

import { useCallback, useEffect, useState } from "react";
import { getManagementGroups, type ManagementGroupRow } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

export default function Incubator() {
  const { t } = useI18n();
  const { tokens } = useTheme();
  const [rows, setRows] = useState<ManagementGroupRow[]>([]);
  const [err, setErr] = useState<string>("");
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(() => {
    setLoading(true);
    getManagementGroups()
      .then((r) => {
        setRows(r.groups);
        setErr("");
      })
      .catch((e) => setErr(e instanceof Error ? e.message : "Load failed"))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", marginBottom: "1rem" }}>
        <button
          type="button"
          onClick={() => refresh()}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            padding: "0.4rem 0.85rem",
            borderRadius: 8,
            border: `1px solid ${tokens.borderDefault}`,
            background: "transparent",
            color: tokens.textPrimary,
            cursor: "pointer",
          }}
        >
          {t("modules.refresh")}
        </button>
        {loading ? (
          <span style={{ fontSize: "0.72rem", color: tokens.textMuted }}>{t("loading")}</span>
        ) : null}
      </div>
      {err ? (
        <p style={{ color: tokens.danger, fontSize: "0.8rem" }}>{err}</p>
      ) : null}
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.75rem" }}>
          <thead>
            <tr style={{ textAlign: "left", color: tokens.textMuted }}>
              <th style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                {t("management_col_session")}
              </th>
              <th style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                {t("management_col_title")}
              </th>
              <th style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                @
              </th>
              <th style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                {t("management_col_members")}
              </th>
              <th style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                Premium
              </th>
              <th style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                {t("management_col_active")}
              </th>
            </tr>
          </thead>
          <tbody>
            {rows.map((g) => (
              <tr key={`${g.session_owner}-${g.group_id}`} style={{ color: tokens.textPrimary }}>
                <td style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                  {g.session_owner}
                </td>
                <td style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                  {g.title ?? "—"}
                </td>
                <td style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                  {g.username ? `@${g.username}` : "—"}
                </td>
                <td style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                  {g.member_stats.total_members}
                </td>
                <td style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                  {g.member_stats.premium_count}
                </td>
                <td style={{ padding: "0.5rem", borderBottom: `1px solid ${tokens.borderDefault}` }}>
                  {g.member_stats.active_real_count}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!loading && rows.length === 0 && !err ? (
          <p style={{ color: tokens.textMuted, fontSize: "0.8rem", marginTop: "1rem" }}>
            {t("management_no_groups")}
          </p>
        ) : null}
      </div>
    </div>
  );
}
