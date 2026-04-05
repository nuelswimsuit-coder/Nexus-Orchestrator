"use client";

/**
 * Profit & stats — Polymarket + Binance-style balances, order history, ROI, trading actions.
 */

import { useCallback, useMemo, useState, type CSSProperties } from "react";
import useSWR from "swr";
import {
  API_BASE,
  swrFetcher,
  postPredictionHalt,
  postPredictionResume,
  type BusinessStatsResponse,
  type PaperTradesResponse,
  type ProfitReportResponse,
} from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { useStealth } from "@/lib/stealth";

interface ScalperLite {
  virtual_balance_usd: number | null;
  live_balance_usd: number | null;
  simulation_mode: boolean;
  current_balance?: number | null;
  yield_metrics?: {
    profit_usd?: number | null;
    start_balance_usd?: number | null;
    current_balance_usd?: number;
    estimated_daily_profit_usd?: number | null;
  } | null;
}

function fmtUsd(n: number) {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(n);
}

export default function ProfitDashboard() {
  const { t, language } = useI18n();
  const { tokens, isHighContrast } = useTheme();
  const { stealth } = useStealth();
  const [msg, setMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const { data: scalper } = useSWR<ScalperLite>(
    `${API_BASE}/api/scalper/status`,
    swrFetcher<ScalperLite>,
    { refreshInterval: 15_000 },
  );
  const { data: paper } = useSWR<PaperTradesResponse>(
    "/api/prediction/paper-trades",
    swrFetcher<PaperTradesResponse>,
    { refreshInterval: 20_000 },
  );
  const { data: biz } = useSWR<BusinessStatsResponse>(
    "/api/business/stats",
    swrFetcher<BusinessStatsResponse>,
    { refreshInterval: 60_000 },
  );
  const { data: report } = useSWR<ProfitReportResponse>(
    "/api/business/report",
    swrFetcher<ProfitReportResponse>,
    { refreshInterval: 120_000 },
  );

  const polyBal =
    scalper?.simulation_mode
      ? scalper?.virtual_balance_usd ?? 0
      : scalper?.live_balance_usd ?? scalper?.current_balance ?? 0;
  const binanceProxy =
    scalper?.yield_metrics?.current_balance_usd != null
      ? Math.max(0, scalper.yield_metrics.current_balance_usd - polyBal)
      : polyBal * 0.15;
  const totalBal = polyBal + binanceProxy;

  const lifetimeRoi =
    scalper?.yield_metrics?.start_balance_usd &&
    scalper.yield_metrics.start_balance_usd > 0
      ? ((scalper.yield_metrics.current_balance_usd ?? totalBal) /
          scalper.yield_metrics.start_balance_usd -
          1) *
        100
      : report?.estimated_roi ?? 0;

  const dailyRoi =
    scalper?.yield_metrics?.estimated_daily_profit_usd != null && totalBal > 0
      ? (scalper.yield_metrics.estimated_daily_profit_usd / totalBal) * 100
      : (report?.estimated_roi ?? 0) / 7;

  const weeklyRoi = dailyRoi * 7;

  const trades = useMemo(
    () => (paper?.trades ?? []).slice().reverse().slice(0, 40),
    [paper],
  );

  const onWithdraw = useCallback(() => {
    setMsg(
      language === "he"
        ? "פעולת משיכה נשלחה לתור (דמו) — אשר בבורסה."
        : "Withdraw queued (demo) — confirm at exchange.",
    );
  }, [language]);

  const onStop = useCallback(async () => {
    setBusy("stop");
    setMsg(null);
    try {
      await postPredictionHalt();
      setMsg(
        language === "he"
          ? "מסחר הופסק — override ידני הופעל."
          : "Trading halted — manual override engaged.",
      );
    } catch {
      setMsg(language === "he" ? "שגיאה בעצירת מסחר" : "Failed to halt trading");
    } finally {
      setBusy(null);
    }
  }, [language]);

  const onResume = useCallback(async () => {
    setBusy("resume");
    try {
      await postPredictionResume();
      setMsg(
        language === "he" ? "מסחר חודש." : "Trading resumed.",
      );
    } catch {
      setMsg(language === "he" ? "שגיאה בחידוש" : "Resume failed");
    } finally {
      setBusy(null);
    }
  }, [language]);

  const onAggressive = useCallback(() => {
    try {
      localStorage.setItem("nexus-aggressive-mode", "1");
    } catch {
      /* ignore */
    }
    setMsg(
      language === "he"
        ? "מצב אגרסיבי הופעל מקומית — בדוק סף scalper."
        : "Aggressive mode flagged locally — review scalper thresholds.",
    );
  }, [language]);

  const cardBg = isHighContrast ? tokens.surface1 : tokens.cardBg;
  const border = `1px solid ${tokens.borderSubtle}`;
  const glow = stealth ? "none" : `0 0 20px ${tokens.accentDim}`;

  return (
    <div dir={language === "he" ? "rtl" : "ltr"} style={{ marginBottom: "2rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
          gap: "1rem",
          marginBottom: "1.25rem",
        }}
      >
        <div
          className="cyber-card"
          style={{
            background: cardBg,
            border,
            borderRadius: 12,
            padding: "1.1rem",
            boxShadow: glow,
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              color: tokens.textMuted,
              letterSpacing: "0.08em",
              marginBottom: "0.35rem",
            }}
          >
            {language === "he" ? "פולימרקט (פעיל)" : "Polymarket (active)"}
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.45rem",
              fontWeight: 700,
              color: tokens.accentBright,
            }}
          >
            {fmtUsd(polyBal)}
          </div>
        </div>
        <div
          style={{
            background: cardBg,
            border,
            borderRadius: 12,
            padding: "1.1rem",
            boxShadow: glow,
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              color: tokens.textMuted,
              letterSpacing: "0.08em",
              marginBottom: "0.35rem",
            }}
          >
            Binance (proxy)
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.45rem",
              fontWeight: 700,
              color: tokens.success,
            }}
          >
            {fmtUsd(binanceProxy)}
          </div>
        </div>
        <div
          style={{
            background: cardBg,
            border,
            borderRadius: 12,
            padding: "1.1rem",
            boxShadow: glow,
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              color: tokens.textMuted,
              letterSpacing: "0.08em",
              marginBottom: "0.35rem",
            }}
          >
            {language === "he" ? "סה״כ יתרה" : "Total balance"}
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.55rem",
              fontWeight: 800,
              color: tokens.textPrimary,
              textShadow: stealth ? "none" : `0 0 18px ${tokens.accentDim}`,
            }}
          >
            {fmtUsd(totalBal)}
          </div>
        </div>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
          gap: "0.75rem",
          marginBottom: "1.25rem",
        }}
      >
        {(
          [
            {
              k: "d",
              label: language === "he" ? "תשואה יומית" : "Daily ROI",
              v: `${dailyRoi.toFixed(2)}%`,
            },
            {
              k: "w",
              label: language === "he" ? "תשואה שבועית" : "Weekly ROI",
              v: `${weeklyRoi.toFixed(2)}%`,
            },
            {
              k: "l",
              label: language === "he" ? "תשואה מצטברת" : "Lifetime ROI",
              v: `${lifetimeRoi.toFixed(2)}%`,
            },
          ] as const
        ).map((row) => (
          <div
            key={row.k}
            style={{
              background: tokens.surface2,
              border,
              borderRadius: 10,
              padding: "0.65rem 0.85rem",
            }}
          >
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.58rem",
                color: tokens.textMuted,
              }}
            >
              {row.label}
            </div>
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "1.05rem",
                fontWeight: 700,
                color: tokens.accent,
              }}
            >
              {row.v}
            </div>
          </div>
        ))}
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "0.6rem",
          marginBottom: "1rem",
        }}
      >
        <button
          type="button"
          onClick={onWithdraw}
          style={btnStyle(tokens, "primary")}
        >
          {language === "he" ? "משיכת רווחים" : "Withdraw profits"}
        </button>
        <button
          type="button"
          disabled={busy === "stop"}
          onClick={onStop}
          style={btnStyle(tokens, "danger")}
        >
          {language === "he" ? "עצור מסחר" : "Stop trading"}
        </button>
        <button
          type="button"
          disabled={busy === "resume"}
          onClick={onResume}
          style={btnStyle(tokens, "muted")}
        >
          {language === "he" ? "חדש מסחר" : "Resume trading"}
        </button>
        <button
          type="button"
          onClick={onAggressive}
          style={btnStyle(tokens, "warn")}
        >
          {language === "he" ? "מצב אגרסיבי" : "Aggressive mode"}
        </button>
      </div>

      {msg && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: tokens.textSecondary,
            marginBottom: "1rem",
          }}
        >
          {msg}
        </div>
      )}

      <div
        style={{
          background: cardBg,
          border,
          borderRadius: 12,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 700,
            letterSpacing: "0.1em",
            color: tokens.textMuted,
            padding: "0.85rem 1rem",
            borderBottom: border,
          }}
        >
          {language === "he" ? "היסטוריית פקודות (PnL)" : "Order history (PnL)"}
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.72rem" }}>
            <thead>
              <tr style={{ color: tokens.textMuted, fontFamily: "var(--font-mono)" }}>
                <th style={th}>{language === "he" ? "זמן" : "Time"}</th>
                <th style={th}>{language === "he" ? "אות" : "Signal"}</th>
                <th style={th}>PnL</th>
                <th style={th}>{language === "he" ? "שוק" : "Market"}</th>
              </tr>
            </thead>
            <tbody>
              {trades.length === 0 ? (
                <tr>
                  <td colSpan={4} style={{ padding: "1.2rem", color: tokens.textMuted }}>
                    {paper ? (language === "he" ? "אין עסקאות" : "No trades") : t("loading")}
                  </td>
                </tr>
              ) : (
                trades.map((tr) => (
                  <tr
                    key={tr.id}
                    style={{
                      borderTop: border,
                      fontFamily: "var(--font-mono)",
                      color: tokens.textPrimary,
                    }}
                  >
                    <td style={td}>{new Date(tr.timestamp).toLocaleString()}</td>
                    <td style={td}>{tr.signal}</td>
                    <td
                      style={{
                        ...td,
                        color:
                          tr.potential_profit_usd >= 0
                            ? tokens.success
                            : tokens.danger,
                      }}
                    >
                      {fmtUsd(tr.potential_profit_usd)}
                    </td>
                    <td style={{ ...td, maxWidth: 220, wordBreak: "break-word" }}>
                      {tr.market_question?.slice(0, 80) ?? "—"}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {(biz?.active_sessions != null || report?.health_ratio != null) && (
        <div
          style={{
            marginTop: "0.75rem",
            fontFamily: "var(--font-mono)",
            fontSize: "0.62rem",
            color: tokens.textMuted,
          }}
        >
          Telegram sessions: {biz?.active_sessions ?? "—"} · health:{" "}
          {report?.health_ratio != null ? `${report.health_ratio.toFixed(0)}%` : "—"}
        </div>
      )}
    </div>
  );
}

const th: CSSProperties = {
  textAlign: "start",
  padding: "0.5rem 0.75rem",
  fontWeight: 600,
};
const td: CSSProperties = {
  padding: "0.45rem 0.75rem",
  verticalAlign: "top",
};

function btnStyle(
  tokens: { accent: string; danger: string; warning: string; surface3: string; textPrimary: string },
  variant: "primary" | "danger" | "warn" | "muted",
): CSSProperties {
  const base: CSSProperties = {
    fontFamily: "var(--font-mono)",
    fontSize: "0.68rem",
    fontWeight: 700,
    letterSpacing: "0.06em",
    padding: "0.5rem 0.9rem",
    borderRadius: 8,
    cursor: "pointer",
    border: "1px solid transparent",
  };
  if (variant === "primary")
    return {
      ...base,
      background: `${tokens.accent}22`,
      borderColor: `${tokens.accent}55`,
      color: tokens.accent,
    };
  if (variant === "danger")
    return {
      ...base,
      background: `${tokens.danger}18`,
      borderColor: `${tokens.danger}66`,
      color: tokens.danger,
    };
  if (variant === "warn")
    return {
      ...base,
      background: `${tokens.warning}18`,
      borderColor: `${tokens.warning}55`,
      color: tokens.warning,
    };
  return {
    ...base,
    background: tokens.surface3,
    borderColor: `${tokens.accent}33`,
    color: tokens.textPrimary,
  };
}
