"use client";

import { useState } from "react";
import useSWR, { useSWRConfig } from "swr";
import PageTransition from "@/components/PageTransition";
import CyberGrid from "@/components/CyberGrid";
import { useStealth } from "@/lib/stealth";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { API_BASE, swrFetcher } from "@/lib/api";

const KEY = "/api/telefix/bot-factory";

/**
 * ניטור Bot Factory — ספירות, סשנים, חימום, טוקנים מוסתרים, ייצור המוני.
 */
export default function BotGenerator() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const { isHighContrast, tokens } = useTheme();
  const { mutate } = useSWRConfig();
  const [count, setCount] = useState(5);
  const [bulkBusy, setBulkBusy] = useState(false);
  const [bulkMsg, setBulkMsg] = useState(null);

  const { data, error, isLoading } = useSWR(KEY, swrFetcher, {
    refreshInterval: 15_000,
  });

  const cardBg = isHighContrast ? tokens.surface2 : "rgba(37, 47, 61, 0.72)";
  const border = `1px solid ${tokens.borderSubtle}`;

  async function runBulk() {
    setBulkBusy(true);
    setBulkMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/bot-factory/bulk`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ count: Math.min(500, Math.max(1, Number(count) || 1)) }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      setBulkMsg({ ok: true, text: body.message || "" });
      await mutate(KEY);
    } catch (e) {
      setBulkMsg({ ok: false, text: String(e.message || e) });
    } finally {
      setBulkBusy(false);
    }
  }

  const job = data?.bulk_job;

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
              {t("telefix_bot_factory_title")}
            </h1>
            <p
              style={{
                margin: "0.35rem 0 0",
                fontSize: "0.88rem",
                color: tokens.textMuted,
                maxWidth: 640,
              }}
            >
              {t("telefix_bot_factory_sub")}
            </p>
          </header>

          {isLoading && (
            <div style={{ color: tokens.textMuted }}>{t("loading")}</div>
          )}
          {error && (
            <div style={{ color: tokens.danger }}>
              {t("telefix_load_error")}: {String(error.message || error)}
            </div>
          )}

          {data && (
            <>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
                  gap: "0.75rem",
                  marginBottom: "1.25rem",
                }}
              >
                {[
                  { label: t("telefix_bots_created"), value: data.bots_created_total },
                  { label: t("telefix_sessions_bound"), value: data.sessions_bound },
                  { label: t("telefix_warmup_active"), value: data.warmup_active },
                ].map((m) => (
                  <div
                    key={m.label}
                    style={{
                      padding: "1rem",
                      borderRadius: 12,
                      background: cardBg,
                      border,
                    }}
                  >
                    <div style={{ fontSize: "0.72rem", color: tokens.textMuted, marginBottom: 6 }}>
                      {m.label}
                    </div>
                    <div
                      style={{
                        fontSize: "1.5rem",
                        fontWeight: 700,
                        fontFamily: "var(--font-mono)",
                        color: stealth ? tokens.textSecondary : tokens.accent,
                      }}
                    >
                      {m.value}
                    </div>
                  </div>
                ))}
              </div>

              {job && (
                <div
                  style={{
                    marginBottom: "1rem",
                    padding: "0.75rem 1rem",
                    borderRadius: 10,
                    background: cardBg,
                    border,
                    fontSize: "0.82rem",
                    color: tokens.textSecondary,
                  }}
                >
                  <strong style={{ color: tokens.textPrimary }}>{t("telefix_bulk_job_label")}</strong>:{" "}
                  {job.status} · {job.requested} · {job.job_id}
                </div>
              )}

              <section
                style={{
                  padding: "1.1rem",
                  borderRadius: 12,
                  background: cardBg,
                  border,
                  marginBottom: "1.25rem",
                }}
              >
                <h2
                  style={{
                    margin: "0 0 0.5rem",
                    fontSize: "1rem",
                    color: tokens.textPrimary,
                  }}
                >
                  {t("telefix_bulk_title")}
                </h2>
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: "0.65rem",
                    alignItems: "center",
                    flexDirection: isRTL ? "row-reverse" : "row",
                  }}
                >
                  <label style={{ fontSize: "0.82rem", color: tokens.textMuted }}>
                    {t("telefix_bulk_count")}
                    <input
                      type="number"
                      min={1}
                      max={500}
                      value={count}
                      onChange={(e) => setCount(e.target.value)}
                      style={{
                        marginInlineStart: 8,
                        width: 72,
                        padding: "6px 8px",
                        borderRadius: 8,
                        border: `1px solid ${tokens.borderSubtle}`,
                        background: tokens.surface2,
                        color: tokens.textPrimary,
                        fontFamily: "var(--font-mono)",
                      }}
                    />
                  </label>
                  <button
                    type="button"
                    disabled={bulkBusy}
                    onClick={runBulk}
                    style={{
                      padding: "8px 16px",
                      borderRadius: 8,
                      border: "none",
                      background: stealth ? tokens.surface3 : tokens.accent,
                      color: stealth ? tokens.textPrimary : "#0f111a",
                      fontWeight: 700,
                      fontSize: "0.82rem",
                      cursor: bulkBusy ? "wait" : "pointer",
                    }}
                  >
                    {bulkBusy ? t("telefix_bulk_running") : t("telefix_bulk_run")}
                  </button>
                </div>
                {bulkMsg && (
                  <p
                    style={{
                      margin: "0.65rem 0 0",
                      fontSize: "0.8rem",
                      color: bulkMsg.ok ? tokens.success : tokens.danger,
                    }}
                  >
                    {bulkMsg.text}
                  </p>
                )}
              </section>

              <section>
                <h2
                  style={{
                    fontSize: "1rem",
                    color: tokens.textPrimary,
                    margin: "0 0 0.35rem",
                  }}
                >
                  {t("telefix_tokens_bf")}
                </h2>
                <p style={{ margin: "0 0 1rem", fontSize: "0.78rem", color: tokens.textMuted }}>
                  {t("telefix_reveal_hint")}
                </p>
                <div style={{ overflowX: "auto" }}>
                  <table
                    style={{
                      width: "100%",
                      borderCollapse: "collapse",
                      fontSize: "0.82rem",
                      background: cardBg,
                      borderRadius: 12,
                      border,
                    }}
                  >
                    <thead>
                      <tr style={{ textAlign: isRTL ? "right" : "left" }}>
                        <th style={{ padding: "10px 12px", borderBottom: border, color: tokens.textMuted }}>
                          {t("telefix_col_bot")}
                        </th>
                        <th style={{ padding: "10px 12px", borderBottom: border, color: tokens.textMuted }}>
                          {t("telefix_col_token")}
                        </th>
                        <th style={{ padding: "10px 12px", borderBottom: border, color: tokens.textMuted }}>
                          {t("telefix_session_label")}
                        </th>
                        <th style={{ padding: "10px 12px", borderBottom: border, color: tokens.textMuted }}>
                          {t("telefix_warmup_status_col")}
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {(data.tokens || []).map((row) => (
                        <tr key={row.bot_id}>
                          <td
                            style={{
                              padding: "10px 12px",
                              borderBottom: `1px solid ${tokens.borderFaint}`,
                              fontFamily: "var(--font-mono)",
                              wordBreak: "break-all",
                            }}
                          >
                            {row.username ? `@${row.username}` : row.bot_id}
                          </td>
                          <td
                            style={{
                              padding: "10px 12px",
                              borderBottom: `1px solid ${tokens.borderFaint}`,
                              fontFamily: "var(--font-mono)",
                              letterSpacing: "0.04em",
                              userSelect: "none",
                            }}
                          >
                            {row.token_masked}
                          </td>
                          <td
                            style={{
                              padding: "10px 12px",
                              borderBottom: `1px solid ${tokens.borderFaint}`,
                              fontFamily: "var(--font-mono)",
                            }}
                          >
                            {row.session_stem || "—"}
                          </td>
                          <td
                            style={{
                              padding: "10px 12px",
                              borderBottom: `1px solid ${tokens.borderFaint}`,
                              maxWidth: 280,
                              wordBreak: "break-word",
                            }}
                          >
                            {row.warmup_status}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <div
                style={{
                  marginTop: "1rem",
                  fontSize: "0.72rem",
                  fontFamily: "var(--font-mono)",
                  color: tokens.textMuted,
                }}
              >
                {t("telefix_updated")}: {data.updated_at || "—"}
              </div>
            </>
          )}
        </div>
      </div>
    </PageTransition>
  );
}
