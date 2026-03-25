"use client";

import { useState } from "react";
import useSWR, { useSWRConfig } from "swr";
import PageTransition from "@/components/PageTransition";
import CyberGrid from "@/components/CyberGrid";
import { useStealth } from "@/lib/stealth";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { API_BASE, swrFetcher } from "@/lib/api";

const KEY = "/api/telefix/group-infiltration";

/**
 * מעקב חדירה — קבוצות בעברית, ימי חימום, פרטי/ציבורי, אינדוקס חיפוש, כפתור כפייה ידנית.
 */
export default function GroupInfiltration() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const { isHighContrast, tokens } = useTheme();
  const { mutate } = useSWRConfig();
  const [busyId, setBusyId] = useState(null);
  const [toast, setToast] = useState(null);

  const { data, error, isLoading } = useSWR(KEY, swrFetcher, {
    refreshInterval: 30_000,
  });

  const cardBg = isHighContrast ? tokens.surface2 : "rgba(37, 47, 61, 0.72)";
  const border = `1px solid ${tokens.borderSubtle}`;

  async function forceSearch(groupId) {
    setBusyId(groupId);
    setToast(null);
    try {
      const res = await fetch(
        `${API_BASE}/api/telefix/group-infiltration/${encodeURIComponent(groupId)}/force-search`,
        { method: "POST", headers: { "Content-Type": "application/json" } },
      );
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      setToast({ type: "ok", text: body.detail || t("telefix_manual_search") });
      await mutate(KEY);
    } catch (e) {
      setToast({ type: "err", text: String(e.message || e) });
    } finally {
      setBusyId(null);
    }
  }

  return (
    <PageTransition>
      <div style={{ position: "relative", minHeight: "calc(100vh - 56px)", padding: "1.25rem" }}>
        <CyberGrid />
        <div
          style={{
            position: "relative",
            zIndex: 1,
            maxWidth: 1100,
            margin: "0 auto",
            direction: isRTL ? "rtl" : "ltr",
          }}
        >
          <header style={{ marginBottom: "1.25rem" }}>
            <h1
              style={{
                fontFamily: "var(--font-sans)",
                fontSize: "1.35rem",
                fontWeight: 700,
                color: tokens.textPrimary,
                margin: 0,
              }}
            >
              {t("telefix_infiltration_title")}
            </h1>
            <p
              style={{
                margin: "0.35rem 0 0",
                fontSize: "0.88rem",
                color: tokens.textMuted,
                maxWidth: 640,
              }}
            >
              {t("telefix_infiltration_sub")}
            </p>
          </header>

          {toast && (
            <div
              role="status"
              style={{
                marginBottom: "1rem",
                padding: "0.65rem 0.9rem",
                borderRadius: 10,
                fontSize: "0.82rem",
                background:
                  toast.type === "ok"
                    ? isHighContrast
                      ? tokens.successSubtle
                      : "rgba(57, 255, 20, 0.08)"
                    : tokens.dangerSubtle,
                border,
                color: tokens.textPrimary,
              }}
            >
              {toast.text}
            </div>
          )}

          {isLoading && (
            <div style={{ color: tokens.textMuted }}>{t("loading")}</div>
          )}
          {error && (
            <div style={{ color: tokens.danger }}>
              {t("telefix_load_error")}: {String(error.message || error)}
            </div>
          )}

          {data?.groups && (
            <>
              <div
                style={{
                  fontSize: "0.72rem",
                  fontFamily: "var(--font-mono)",
                  color: tokens.textMuted,
                  marginBottom: "0.75rem",
                }}
              >
                {t("telefix_updated")}: {data.updated_at || "—"}
              </div>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                {data.groups.map((g) => {
                  const neon = g.in_search && !stealth;
                  return (
                    <li
                      key={g.id}
                      className={neon ? "telefix-row-in-search" : undefined}
                      style={{
                        display: "flex",
                        flexWrap: "wrap",
                        alignItems: "center",
                        gap: "0.65rem 1rem",
                        padding: "1rem 1.1rem",
                        borderRadius: 12,
                        background: cardBg,
                        border: neon
                          ? `1px solid rgba(57, 255, 20, 0.55)`
                          : border,
                        boxShadow: stealth ? "none" : undefined,
                      }}
                    >
                      <div style={{ minWidth: 0, flex: "1 1 200px" }}>
                        <div
                          style={{
                            fontWeight: 700,
                            fontSize: "1rem",
                            color: tokens.textPrimary,
                            wordBreak: "break-word",
                          }}
                        >
                          {g.name_he || "—"}
                        </div>
                        <div
                          style={{
                            fontSize: "0.75rem",
                            color: tokens.textMuted,
                            marginTop: 4,
                            fontFamily: "var(--font-mono)",
                          }}
                        >
                          id: {g.id}
                        </div>
                      </div>

                      <div
                        title={t("telefix_warmup_days")}
                        style={{
                          textAlign: "center",
                          padding: "6px 10px",
                          borderRadius: 8,
                          background: tokens.accentSubtle,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.8rem",
                          fontWeight: 600,
                          color: tokens.textPrimary,
                          whiteSpace: "nowrap",
                        }}
                      >
                        {g.warmup_days} / 14
                      </div>

                      <div
                        style={{
                          fontSize: "0.82rem",
                          fontWeight: 600,
                          color: g.is_private ? tokens.warning : tokens.success,
                          whiteSpace: "nowrap",
                        }}
                      >
                        {g.is_private ? t("telefix_private") : t("telefix_public")}
                      </div>

                      <div
                        style={{
                          fontSize: "0.82rem",
                          fontWeight: 600,
                          color: g.in_search ? "#39ff14" : tokens.textMuted,
                          whiteSpace: "nowrap",
                        }}
                      >
                        {g.in_search
                          ? t("telefix_in_search_yes")
                          : t("telefix_in_search_no")}
                      </div>

                      <button
                        type="button"
                        disabled={busyId === g.id}
                        onClick={() => forceSearch(g.id)}
                        style={{
                          padding: "8px 12px",
                          borderRadius: 8,
                          border: `1px solid ${tokens.borderSubtle}`,
                          background: isHighContrast ? tokens.surface3 : "rgba(99,179,237,0.12)",
                          color: tokens.textPrimary,
                          fontSize: "0.78rem",
                          fontWeight: 600,
                          cursor: busyId === g.id ? "wait" : "pointer",
                          whiteSpace: "nowrap",
                          marginInlineStart: isRTL ? 0 : "auto",
                          marginInlineEnd: isRTL ? "auto" : 0,
                        }}
                      >
                        {busyId === g.id ? t("telefix_bulk_running") : t("telefix_manual_search")}
                      </button>
                    </li>
                  );
                })}
              </ul>
            </>
          )}
        </div>
      </div>
    </PageTransition>
  );
}
