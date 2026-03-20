"use client";

/**
 * FinancialPulseWidget — Phase 7: Master ROI Engine
 *
 * Aggregates live financial data from BudgetTracker and CryptoSellsBot,
 * calculating net profit per minute and projected monthly revenue.
 *
 * Data sources:
 *   GET /api/modules/widgets/financial-pulse  — BudgetTracker daily P&L
 *   GET /api/modules/otp_sessions              — session count for cost basis
 */

import { useEffect, useState } from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useStealth } from "@/lib/stealth";
import { useTheme } from "@/lib/theme";

interface FinancialPulseData {
  available: boolean;
  daily_pnl: number;
  currency: string;
  status: string;
}

interface ModuleInfo {
  status: string;
  live_stats: Record<string, unknown>;
}

// Format a number as a currency string
function fmt(n: number, currency = "USD") {
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(2)} ${currency}`;
}

// Spark-bar component for the mini profit chart
function SparkBar({
  values,
  color,
}: {
  values: number[];
  color: string;
}) {
  const max = Math.max(...values.map(Math.abs), 1);
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-end",
        gap: "2px",
        height: "32px",
      }}
    >
      {values.map((v, i) => {
        const h = Math.max(2, (Math.abs(v) / max) * 32);
        const c = v >= 0 ? color : "#ef4444";
        return (
          <div
            key={i}
            style={{
              flex: 1,
              height: `${h}px`,
              background: c,
              borderRadius: "2px 2px 0 0",
              opacity: 0.4 + (i / values.length) * 0.6,
              boxShadow: i === values.length - 1 ? `0 0 6px ${c}` : "none",
            }}
          />
        );
      })}
    </div>
  );
}

export default function FinancialPulseWidget() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const { isHighContrast, tokens } = useTheme();

  const { data: pulse } = useSWR<FinancialPulseData>(
    "/api/modules/widgets/financial-pulse",
    swrFetcher<FinancialPulseData>,
    { refreshInterval: 30_000 }
  );

  const { data: otpModule } = useSWR<ModuleInfo>(
    "/api/modules/otp_sessions",
    swrFetcher<ModuleInfo>,
    { refreshInterval: 60_000 }
  );

  const [history, setHistory] = useState<number[]>(
    Array.from({ length: 12 }, () => 0)
  );

  const dailyPnl = pulse?.daily_pnl ?? 0;
  const currency = pulse?.currency ?? "USD";
  const sessionCount = (otpModule?.live_stats?.session_count as number) ?? 0;

  useEffect(() => {
    if (dailyPnl !== 0) {
      setHistory((prev) => [...prev.slice(-11), dailyPnl]);
    }
  }, [dailyPnl]);

  const profitPerMinute  = dailyPnl / (24 * 60);
  const projectedMonthly = dailyPnl * 30;
  const costPerSession   = sessionCount > 0 ? Math.abs(dailyPnl) / sessionCount : 0;

  const isProfit = dailyPnl > 0;
  const isFlat   = dailyPnl === 0;

  // Main color: high contrast uses accessible saturated colors
  const mainColor = stealth
    ? "#475569"
    : isFlat
      ? (isHighContrast ? tokens.textMuted : "#64748b")
      : isProfit
        ? (isHighContrast ? tokens.success : "#22c55e")
        : (isHighContrast ? tokens.danger  : "#ef4444");

  const accentColor = stealth ? "#334155" : (isHighContrast ? tokens.accent : "#06b6d4");
  const pnlIcon = isFlat ? "⚖️" : isProfit ? "📈" : "📉";

  // Widget background & border
  const widgetBg = isHighContrast
    ? "#FFFFFF"
    : "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)";
  const widgetBorder = isHighContrast
    ? `1px solid ${mainColor}`
    : `1px solid ${stealth ? "#1e293b" : mainColor}33`;
  const widgetShadow = isHighContrast
    ? "0 2px 8px rgba(0,0,0,0.1)"
    : (stealth ? "none" : `0 0 20px ${mainColor}0d`);

  // Spark chart background
  const sparkBg     = isHighContrast ? tokens.terminalBg : "#0f172a";
  const sparkBorder = isHighContrast ? `1px solid ${tokens.borderFaint}` : "1px solid #1e293b";
  const sparkLabel  = isHighContrast ? tokens.textMuted : "#334155";

  // Metric card styles
  const metricCardBg     = isHighContrast ? tokens.surface2 : "rgba(20,24,36,0.6)";
  const metricCardBorder = isHighContrast ? `1px solid ${tokens.borderFaint}` : "1px solid #21293d";
  const metricLabelColor = isHighContrast ? tokens.textMuted : (stealth ? "#334155" : "#64748b");

  return (
    <div
      style={{
        background: widgetBg,
        border: widgetBorder,
        borderRadius: "12px",
        padding: "1.25rem",
        boxShadow: widgetShadow,
        transition: "all 0.3s",
      }}
    >
      {/* ── Header ── */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "1rem",
          flexDirection: isRTL ? "row-reverse" : "row",
        }}
      >
        <h3
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.7rem",
            fontWeight: 700,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            color: stealth ? "#475569" : accentColor,
            margin: 0,
            display: "flex",
            alignItems: "center",
            gap: "0.4rem",
          }}
        >
          {t("financial_pulse")}
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: mainColor,
              display: "inline-block",
              boxShadow: (stealth || isHighContrast) ? "none" : `0 0 8px ${mainColor}`,
              animation: (stealth || isFlat || isHighContrast) ? "none" : "fp-pulse 1.8s ease-in-out infinite",
            }}
          />
        </h3>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            color: isHighContrast ? tokens.textMuted : (stealth ? "#475569" : "#64748b"),
          }}
        >
          {t("refresh_30s")}
        </span>
      </div>

      {/* ── Main P&L ── */}
      <div style={{ textAlign: "center", marginBottom: "1rem" }}>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "1.9rem",
            fontWeight: 700,
            color: mainColor,
            letterSpacing: "0.04em",
            textShadow: (stealth || isHighContrast) ? "none" : `0 0 14px ${mainColor}44`,
          }}
        >
          {pnlIcon} {fmt(dailyPnl, currency)}
        </div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            color: isHighContrast ? tokens.textMuted : (stealth ? "#475569" : "#64748b"),
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            marginTop: "0.25rem",
          }}
        >
          {t("daily_pnl")}
        </div>
      </div>

      {/* ── Spark chart ── */}
      <div
        style={{
          marginBottom: "1rem",
          padding: "0.5rem",
          background: sparkBg,
          borderRadius: "8px",
          border: sparkBorder,
        }}
      >
        <SparkBar values={history} color={mainColor} />
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.52rem",
            color: sparkLabel,
            textAlign: "center",
            marginTop: "4px",
            letterSpacing: "0.06em",
          }}
        >
          {t("history_12")}
        </div>
      </div>

      {/* ── Metrics grid ── */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "0.75rem",
          marginBottom: "0.75rem",
        }}
      >
        {[
          {
            label: t("profit_per_min"),
            value: fmt(profitPerMinute, currency),
            color: accentColor,
          },
          {
            label: t("proj_monthly"),
            value: fmt(projectedMonthly, currency),
            color: mainColor,
          },
          {
            label: t("active_sessions"),
            value: String(sessionCount),
            color: stealth ? "#475569" : (isHighContrast ? "#6D28D9" : "#a78bfa"),
          },
          {
            label: t("cost_per_session"),
            value: costPerSession > 0 ? `${costPerSession.toFixed(3)} ${currency}` : "\u2014",
            color: stealth ? "#475569" : tokens.warning,
          },
        ].map(({ label, value, color }) => (
          <div
            key={label}
            style={{
              background: metricCardBg,
              border: metricCardBorder,
              borderRadius: "8px",
              padding: "0.5rem 0.75rem",
            }}
          >
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.52rem",
                color: metricLabelColor,
                marginBottom: "0.25rem",
              }}
            >
              {label}
            </div>
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.75rem",
                fontWeight: 700,
                color: isHighContrast ? color : (stealth ? "#94a3b8" : color),
                textShadow: (stealth || isHighContrast) ? "none" : `0 0 8px ${color}33`,
              }}
            >
              {value}
            </div>
          </div>
        ))}
      </div>

      {/* ── Status bar ── */}
      <div
        style={{
          background: isHighContrast
            ? (isProfit ? tokens.successSubtle : isFlat ? tokens.surface2 : tokens.dangerSubtle)
            : (stealth ? "rgba(15,23,42,0.5)" : `${mainColor}0d`),
          border: `1px solid ${isHighContrast ? mainColor : `${mainColor}33`}`,
          borderRadius: "8px",
          padding: "0.4rem 0.75rem",
          textAlign: "center",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.62rem",
            fontWeight: 600,
            color: mainColor,
            letterSpacing: "0.04em",
          }}
        >
          {isProfit
            ? `\u{1F4C8} ${t("status_profitable")}`
            : isFlat
            ? `\u2696\uFE0F ${t("status_breakeven")}`
            : `\u{1F4C9} ${t("status_loss")}`}
        </span>
      </div>

      <style>{`
        @keyframes fp-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
      `}</style>
    </div>
  );
}
