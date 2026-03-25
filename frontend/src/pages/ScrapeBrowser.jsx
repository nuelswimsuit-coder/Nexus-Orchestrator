"use client";

import { useMemo, useState } from "react";
import useSWR from "swr";
import PageTransition from "@/components/PageTransition";
import CyberGrid from "@/components/CyberGrid";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { API_BASE, swrFetcher } from "@/lib/api";

const KEY = "/api/telefix/scrapes";

function recordMatches(row, dateSub, kw, minRel) {
  const at = String(row.scraped_at || "");
  if (dateSub && !at.includes(dateSub)) return false;
  const rel = Number(row.ai_relevance);
  if (Number.isFinite(minRel) && rel < minRel) return false;
  const k = (kw || "").trim().toLowerCase();
  if (k) {
    try {
      const blob = JSON.stringify(row).toLowerCase();
      if (!blob.includes(k)) return false;
    } catch {
      return false;
    }
  }
  return true;
}

/**
 * דפדפן תוצאות סריקה מ־vault/data/scrapes — פילטרים וייצוא CSV מהמאסטר.
 */
export default function ScrapeBrowser() {
  const { t, isRTL } = useI18n();
  const { isHighContrast, tokens } = useTheme();
  const [dateSub, setDateSub] = useState("");
  const [keyword, setKeyword] = useState("");
  const [minRel, setMinRel] = useState(0);
  const [exportBusy, setExportBusy] = useState(false);

  const { data, error, isLoading, mutate } = useSWR(KEY, swrFetcher, {
    refreshInterval: 60_000,
  });

  const filtered = useMemo(() => {
    const files = data?.files || [];
    return files.filter((r) => recordMatches(r, dateSub, keyword, minRel));
  }, [data, dateSub, keyword, minRel]);

  const cardBg = isHighContrast ? tokens.surface2 : "rgba(37, 47, 61, 0.72)";
  const border = `1px solid ${tokens.borderSubtle}`;

  async function exportCsv() {
    setExportBusy(true);
    try {
      const p = new URLSearchParams();
      if (dateSub.trim()) p.set("date_from", dateSub.trim());
      if (keyword.trim()) p.set("keyword", keyword.trim());
      if (minRel > 0) p.set("min_relevance", String(minRel));
      const url = `${API_BASE}/api/telefix/scrapes/export?${p.toString()}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const cd = res.headers.get("Content-Disposition");
      let filename = "scrapes_export.csv";
      const m = cd && /filename="?([^";]+)"?/i.exec(cd);
      if (m) filename = m[1];
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      a.click();
      URL.revokeObjectURL(a.href);
    } catch (e) {
      console.error(e);
      alert(String(e.message || e));
    } finally {
      setExportBusy(false);
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
              {t("telefix_scrape_title")}
            </h1>
            <p
              style={{
                margin: "0.35rem 0 0",
                fontSize: "0.88rem",
                color: tokens.textMuted,
                maxWidth: 640,
              }}
            >
              {t("telefix_scrape_sub")}
            </p>
          </header>

          <section
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: "0.75rem",
              alignItems: "flex-end",
              marginBottom: "1.25rem",
              padding: "1rem",
              borderRadius: 12,
              background: cardBg,
              border,
              flexDirection: isRTL ? "row-reverse" : "row",
            }}
          >
            <label style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: "0.78rem", color: tokens.textMuted }}>
              {t("telefix_filter_date")}
              <input
                value={dateSub}
                onChange={(e) => setDateSub(e.target.value)}
                placeholder="2025-03-20"
                style={{
                  padding: "8px 10px",
                  borderRadius: 8,
                  border: `1px solid ${tokens.borderSubtle}`,
                  background: tokens.surface2,
                  color: tokens.textPrimary,
                  minWidth: 160,
                  fontFamily: "var(--font-mono)",
                }}
              />
            </label>
            <label style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: "0.78rem", color: tokens.textMuted }}>
              {t("telefix_filter_keyword")}
              <input
                value={keyword}
                onChange={(e) => setKeyword(e.target.value)}
                placeholder="קריפטו"
                style={{
                  padding: "8px 10px",
                  borderRadius: 8,
                  border: `1px solid ${tokens.borderSubtle}`,
                  background: tokens.surface2,
                  color: tokens.textPrimary,
                  minWidth: 160,
                }}
              />
            </label>
            <label style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: "0.78rem", color: tokens.textMuted, minWidth: 180 }}>
              {t("telefix_filter_relevance")}: {minRel.toFixed(2)}
              <input
                type="range"
                min={0}
                max={1}
                step={0.05}
                value={minRel}
                onChange={(e) => setMinRel(Number(e.target.value))}
                style={{ width: "100%" }}
              />
            </label>
            <button
              type="button"
              onClick={() => mutate()}
              style={{
                padding: "8px 14px",
                borderRadius: 8,
                border: `1px solid ${tokens.borderSubtle}`,
                background: tokens.accentSubtle,
                color: tokens.textPrimary,
                fontWeight: 600,
                fontSize: "0.82rem",
                cursor: "pointer",
              }}
            >
              {t("telefix_refresh")}
            </button>
            <button
              type="button"
              disabled={exportBusy}
              onClick={exportCsv}
              style={{
                padding: "8px 14px",
                borderRadius: 8,
                border: "none",
                background: tokens.success,
                color: "#0f111a",
                fontWeight: 700,
                fontSize: "0.82rem",
                cursor: exportBusy ? "wait" : "pointer",
              }}
            >
              {exportBusy ? t("loading") : t("telefix_export_csv")}
            </button>
          </section>

          {isLoading && (
            <div style={{ color: tokens.textMuted }}>{t("loading")}</div>
          )}
          {error && (
            <div style={{ color: tokens.danger }}>
              {t("telefix_load_error")}: {String(error.message || error)}
            </div>
          )}

          {!isLoading && !error && (data?.count === 0 || !data?.files?.length) && (
            <div style={{ color: tokens.textMuted }}>{t("telefix_no_files")}</div>
          )}

          <div style={{ fontSize: "0.78rem", color: tokens.textMuted, marginBottom: "0.75rem" }}>
            {filtered.length} / {data?.count ?? 0} — {t("telefix_raw_data")}
          </div>

          <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: "1rem" }}>
            {filtered.map((row) => (
              <li
                key={row.file}
                style={{
                  padding: "1rem 1.1rem",
                  borderRadius: 12,
                  background: cardBg,
                  border,
                }}
              >
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    justifyContent: "space-between",
                    gap: "0.5rem",
                    marginBottom: "0.65rem",
                    flexDirection: isRTL ? "row-reverse" : "row",
                  }}
                >
                  <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.8rem", color: tokens.accent }}>
                    {row.file}
                  </span>
                  <span style={{ fontSize: "0.78rem", color: tokens.textMuted }}>
                    {row.scraped_at || "—"} · AI {(Number(row.ai_relevance) || 0).toFixed(2)}
                  </span>
                </div>
                <div style={{ marginBottom: "0.5rem", fontSize: "0.88rem", color: tokens.textPrimary }}>
                  <strong>{t("telefix_source_group")}:</strong> {row.source_group || "—"}
                </div>
                {(row.keywords?.length > 0) && (
                  <div style={{ marginBottom: "0.5rem", fontSize: "0.78rem", color: tokens.textSecondary }}>
                    {row.keywords.join(" · ")}
                  </div>
                )}
                <div style={{ marginBottom: "0.35rem", fontSize: "0.75rem", color: tokens.textMuted }}>
                  {t("telefix_users_col")}
                </div>
                <pre
                  style={{
                    margin: "0 0 0.75rem",
                    padding: "0.65rem",
                    borderRadius: 8,
                    background: tokens.surface0 ?? "#1a202c",
                    fontSize: "0.72rem",
                    overflow: "auto",
                    maxHeight: 160,
                    direction: "ltr",
                    textAlign: "left",
                  }}
                >
                  {JSON.stringify(row.users || [], null, 2)}
                </pre>
                <div style={{ marginBottom: "0.35rem", fontSize: "0.75rem", color: tokens.textMuted }}>
                  {t("telefix_messages_col")}
                </div>
                <pre
                  style={{
                    margin: 0,
                    padding: "0.65rem",
                    borderRadius: 8,
                    background: tokens.surface0 ?? "#1a202c",
                    fontSize: "0.72rem",
                    overflow: "auto",
                    maxHeight: 200,
                    direction: "ltr",
                    textAlign: "left",
                  }}
                >
                  {JSON.stringify(row.selected_messages || [], null, 2)}
                </pre>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </PageTransition>
  );
}
