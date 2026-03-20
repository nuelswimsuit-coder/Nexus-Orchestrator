"use client";

import { useEffect, useRef, useState } from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { useStealth } from "@/lib/stealth";
import type { PaperTradesResponse, VirtualTradeEntry } from "@/lib/api";

// ── Signal colour map ─────────────────────────────────────────────────────────

const SIGNAL_COLOR: Record<string, string> = {
  HIGH_CONFIDENCE_BUY: "#f59e0b",
  BUY_BIAS:            "#06b6d4",
};

const SIGNAL_ICON: Record<string, string> = {
  HIGH_CONFIDENCE_BUY: "⚡",
  BUY_BIAS:            "↗",
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function fmt(ts: string): string {
  try {
    return new Date(ts).toLocaleTimeString("en-GB", { hour12: false });
  } catch {
    return ts;
  }
}

function fmtUsd(v: number): string {
  return `$${v.toFixed(4)}`;
}

function fmtBtc(v: number): string {
  return `$${v.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

// ── PnL counter ───────────────────────────────────────────────────────────────

function PnlCounter({
  value,
  label,
  stealth,
  isHighContrast,
}: {
  value: number;
  label: string;
  stealth: boolean;
  isHighContrast: boolean;
}) {
  const isPositive = value >= 0;
  const color = stealth
    ? "#1e293b"
    : isHighContrast
    ? isPositive ? "#15803d" : "#b91c1c"
    : isPositive ? "#22c55e" : "#ef4444";

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-end",
        gap: "2px",
        flexShrink: 0,
      }}
    >
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.55rem",
          letterSpacing: "0.12em",
          color: stealth ? "#1e293b" : isHighContrast ? "#6b7280" : "#334155",
          textTransform: "uppercase",
        }}
      >
        {label}
      </span>
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "1.1rem",
          fontWeight: 700,
          color,
          letterSpacing: "0.02em",
          textShadow: stealth || isHighContrast ? "none" : `0 0 12px ${color}88`,
          transition: "color 0.3s, text-shadow 0.3s",
        }}
      >
        {isPositive ? "+" : ""}
        {fmtUsd(value)}
      </span>
    </div>
  );
}

// ── Single trade row ──────────────────────────────────────────────────────────

function TradeRow({
  trade,
  stealth,
  isHighContrast,
  t,
}: {
  trade: VirtualTradeEntry;
  stealth: boolean;
  isHighContrast: boolean;
  t: (k: string) => string;
}) {
  const sigColor = stealth
    ? "#1e293b"
    : isHighContrast
    ? "#374151"
    : SIGNAL_COLOR[trade.signal] ?? "#64748b";

  const icon = SIGNAL_ICON[trade.signal] ?? "●";

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "54px 1fr auto",
        gap: "0.5rem",
        alignItems: "start",
        padding: "0.55rem 0",
        borderBottom: `1px solid ${isHighContrast ? "#e5e7eb" : "#0f172a"}`,
      }}
    >
      {/* Timestamp */}
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.62rem",
          color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#1e3a5f",
          flexShrink: 0,
        }}
      >
        {fmt(trade.timestamp)}
      </span>

      {/* Main content */}
      <div style={{ display: "flex", flexDirection: "column", gap: "2px", minWidth: 0 }}>
        {/* Signal badge + market question */}
        <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", flexWrap: "wrap" }}>
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.6rem",
              fontWeight: 700,
              color: sigColor,
              textShadow: stealth || isHighContrast ? "none" : `0 0 8px ${sigColor}66`,
              whiteSpace: "nowrap",
            }}
          >
            {icon} {trade.signal.replace(/_/g, " ")}
          </span>
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.55rem",
              color: stealth ? "#1e293b" : isHighContrast ? "#4b5563" : "#334155",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
              maxWidth: "260px",
            }}
          >
            {trade.market_question}
          </span>
        </div>

        {/* Price details */}
        <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap" }}>
          <span style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            color: stealth ? "#1e293b" : isHighContrast ? "#6b7280" : "#475569",
          }}>
            <span style={{ color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#1e3a5f" }}>
              {t("paper_trading.entry_price")}:{" "}
            </span>
            YES {trade.entry_yes_price.toFixed(4)}
          </span>
          <span style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            color: stealth ? "#1e293b" : isHighContrast ? "#6b7280" : "#475569",
          }}>
            <span style={{ color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#1e3a5f" }}>
              {t("paper_trading.btc_price")}:{" "}
            </span>
            {fmtBtc(trade.entry_binance_price)}
          </span>
          <span style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            color: stealth ? "#1e293b" : isHighContrast ? "#6b7280" : "#475569",
          }}>
            <span style={{ color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#1e3a5f" }}>
              {t("paper_trading.amount")}:{" "}
            </span>
            {fmtUsd(trade.virtual_amount_usd)}
          </span>
        </div>
      </div>

      {/* Potential profit */}
      <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "2px" }}>
        <span style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.58rem",
          fontWeight: 700,
          color: stealth
            ? "#1e293b"
            : isHighContrast
            ? "#15803d"
            : "#22c55e",
          textShadow: stealth || isHighContrast ? "none" : "0 0 8px #22c55e55",
        }}>
          +{fmtUsd(trade.potential_profit_usd)}
        </span>
        <span style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.5rem",
          letterSpacing: "0.08em",
          color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#0c2a3a",
          textTransform: "uppercase",
        }}>
          {t("paper_trading.open")}
        </span>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function VirtualTradeLog() {
  const { t, isRTL } = useI18n();
  const { tokens, isHighContrast } = useTheme();
  const { stealth } = useStealth();

  const { data, error, isLoading } = useSWR<PaperTradesResponse>(
    "/api/prediction/paper-trades",
    swrFetcher<PaperTradesResponse>,
    { refreshInterval: 10_000 },
  );

  const scrollRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  // Auto-scroll to top (newest trade) when data changes
  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = 0;
    }
  }, [data?.total, autoScroll]);

  const cardBg = isHighContrast
    ? "rgba(255,255,255,0.97)"
    : stealth
    ? "rgba(8,12,24,0.6)"
    : "linear-gradient(160deg, #080d18 0%, #050912 100%)";

  const cardBorder = stealth
    ? "#141824"
    : isHighContrast
    ? "#374151"
    : "rgba(245,158,11,0.18)";

  const glowColor = "#f59e0b";

  const totalPnl = data?.total_virtual_pnl ?? 0;

  return (
    <div
      style={{
        background: cardBg,
        border: `1px solid ${cardBorder}`,
        borderRadius: "14px",
        overflow: "hidden",
        boxShadow: stealth || isHighContrast
          ? "none"
          : `0 0 30px ${glowColor}11, 0 8px 32px rgba(0,0,0,0.4)`,
      }}
    >
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexDirection: isRTL ? "row-reverse" : "row",
          padding: "0.85rem 1.25rem",
          borderBottom: `1px solid ${isHighContrast ? "#e5e7eb" : "#0f172a"}`,
          background: isHighContrast ? "rgba(248,249,250,0.98)" : "rgba(255,255,255,0.025)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
          {/* Traffic lights */}
          {(["#ef4444", "#f59e0b", "#22c55e"] as const).map((c, i) => (
            <span
              key={i}
              style={{
                width: 9, height: 9, borderRadius: "50%",
                background: stealth ? "#0f172a" : isHighContrast ? "#e5e7eb" : (i === 1 ? c : "#1e293b"),
                display: "inline-block",
                boxShadow: !stealth && !isHighContrast && i === 1 ? `0 0 6px ${c}` : "none",
                animation: !stealth && !isHighContrast && i === 1 ? "sim-pulse 2s infinite" : "none",
              }}
            />
          ))}

          {/* Title */}
          <span style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: stealth ? "#1e293b" : isHighContrast ? "#374151" : "#475569",
          }}>
            {t("paper_trading.trade_log_title")}
          </span>

          {/* SIM badge */}
          {!stealth && (
            <span style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.55rem",
              fontWeight: 700,
              padding: "2px 7px",
              borderRadius: "999px",
              background: isHighContrast ? "#fef3c7" : "rgba(245,158,11,0.15)",
              color: isHighContrast ? "#92400e" : "#f59e0b",
              border: isHighContrast ? "1px solid #f59e0b" : "1px solid rgba(245,158,11,0.3)",
              letterSpacing: "0.1em",
              animation: isHighContrast ? "none" : "sim-pulse 2.5s infinite",
            }}>
              {t("paper_trading.badge")} — {t("paper_trading.paper_mode_label")}
            </span>
          )}
        </div>

        {/* PnL counter */}
        {data && (
          <PnlCounter
            value={totalPnl}
            label={t("paper_trading.total_pnl")}
            stealth={stealth}
            isHighContrast={isHighContrast}
          />
        )}
      </div>

      {/* ── Subtitle row ──────────────────────────────────────────────────── */}
      <div style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        flexDirection: isRTL ? "row-reverse" : "row",
        padding: "0.4rem 1.25rem",
        borderBottom: `1px solid ${isHighContrast ? "#e5e7eb" : "#0a1020"}`,
        background: isHighContrast ? "rgba(248,249,250,0.95)" : "rgba(0,0,0,0.2)",
      }}>
        <span style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.58rem",
          color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#0c2a3a",
          letterSpacing: "0.06em",
        }}>
          {t("paper_trading.trade_log_sub")}
        </span>
        <span style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.58rem",
          color: stealth ? "#1e293b" : isHighContrast ? "#6b7280" : "#1e3a5f",
        }}>
          {data ? `${data.total} ${t("paper_trading.virtual_trade").toLowerCase()}s` : "—"}
        </span>
      </div>

      {/* ── Trade list ───────────────────────────────────────────────────── */}
      <div
        ref={scrollRef}
        onScroll={() => {
          if (!scrollRef.current) return;
          const { scrollTop } = scrollRef.current;
          setAutoScroll(scrollTop < 8);
        }}
        style={{
          minHeight: "220px",
          maxHeight: "380px",
          overflowY: "auto",
          padding: "0 1.25rem",
          background: stealth ? "transparent" : isHighContrast ? "#fff" : "#020617",
        }}
      >
        {isLoading && (
          <div style={{
            padding: "2rem 0",
            textAlign: "center",
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#1e3a5f",
          }}>
            {t("status.loading")}
          </div>
        )}

        {error && (
          <div style={{
            padding: "2rem 0",
            textAlign: "center",
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "#ef4444",
          }}>
            {t("status.error")}
          </div>
        )}

        {!isLoading && !error && (!data || data.trades.length === 0) && (
          <div style={{
            padding: "2.5rem 0",
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            gap: "0.5rem",
          }}>
            <span style={{ fontSize: "1.5rem" }}>📊</span>
            <span style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              color: stealth ? "#1e293b" : isHighContrast ? "#9ca3af" : "#1e3a5f",
              textAlign: "center",
            }}>
              {t("paper_trading.no_trades")}
            </span>
          </div>
        )}

        {data && data.trades.map((trade) => (
          <TradeRow
            key={trade.id}
            trade={trade}
            stealth={stealth}
            isHighContrast={isHighContrast}
            t={t}
          />
        ))}

        {/* Cursor blink at bottom when live */}
        {data && data.trades.length > 0 && !stealth && (
          <div style={{
            display: "flex",
            alignItems: "center",
            gap: "0.5rem",
            padding: "0.5rem 0",
          }}>
            <span style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              color: isHighContrast ? "#f59e0b" : "#f59e0b",
              animation: "terminal-blink 1s step-end infinite",
            }}>▋</span>
          </div>
        )}
      </div>

      <style>{`
        @keyframes sim-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.45; }
        }
        @keyframes terminal-blink {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0; }
        }
      `}</style>
    </div>
  );
}
