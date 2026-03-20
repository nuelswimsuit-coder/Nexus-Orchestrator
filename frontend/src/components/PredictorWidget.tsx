"use client";

/**
 * PredictorWidget — Cross-Exchange Predictor
 *
 * Polls /api/prediction/cross-exchange every 30 s and displays:
 *   Left panel  — "Real World Pulse (Binance)" : live BTC price + order-book bias
 *   Right panel — "Market Odds (Polymarket)"   : Yes/No probability prices
 *   Bottom banner — "ARBITRAGE OPPORTUNITY FOUND" when HIGH_CONFIDENCE_BUY fires
 *   Trade log   — last automated trade actions (newest first)
 *
 * Signal logic (mirrors backend):
 *   HIGH_CONFIDENCE_BUY   buy > 70 %  AND  Yes < $0.52  →  gold alert
 *   BUY_BIAS              buy > 70 %  (market caught up) →  cyan
 *   POLYMARKET_LAGGING    Yes < $0.52 (no OB support)    →  amber
 *   NEUTRAL               no edge                         →  slate
 *
 * Automated trading fires only when buy > 80 % AND gap > 3 %.
 */

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

// ── Types ─────────────────────────────────────────────────────────────────────

interface BinanceSnapshot {
  price: number;
  total_bids: number;
  total_asks: number;
  buy_pct: number;
  sell_pct: number;
  imbalance_direction: string;
  imbalance_strength: number;
}

interface PolymarketSnapshot {
  market_found: boolean;
  market_question: string | null;
  yes_price: number | null;
  no_price: number | null;
  volume: number | null;
}

interface CrossExchangeData {
  status: string;
  signal: string;
  signal_label: string;
  high_confidence: boolean;
  arbitrage_gap: number | null;
  binance: BinanceSnapshot | null;
  polymarket: PolymarketSnapshot | null;
  thresholds: {
    imbalance_threshold: number;
    polymarket_yes_ceiling: number;
  };
  errors: string[];
  duration_s: number;
  fetched_at: string;
}

interface TradeLogEntry {
  timestamp: string;
  side: string;
  price: number;
  shares: number;
  spent_usd: number;
  market_question: string;
  status: string;
  log_text: string;
  paper: boolean;
  order_id: string | null;
}

interface TradeLogResponse {
  entries: TradeLogEntry[];
  total: number;
  paper_trading: boolean;
  kill_switch_balance_usd: number;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function pct(n: number) {
  return `${(n * 100).toFixed(1)}%`;
}

function fmtPrice(n: number) {
  return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });
}

function signalColors(signal: string, stealth: boolean) {
  if (stealth) return { primary: "#475569", glow: "transparent", bg: "#0f172a33" };
  switch (signal) {
    case "HIGH_CONFIDENCE_BUY":
      return { primary: "#f59e0b", glow: "#f59e0b44", bg: "#f59e0b0d" };
    case "BUY_BIAS":
      return { primary: "#06b6d4", glow: "#06b6d444", bg: "#06b6d40d" };
    case "POLYMARKET_LAGGING":
      return { primary: "#f97316", glow: "#f9731644", bg: "#f973160d" };
    default:
      return { primary: "#475569", glow: "transparent", bg: "#0f172a33" };
  }
}

// ── Sub-components ────────────────────────────────────────────────────────────

function DataRow({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        padding: "0.35rem 0",
        borderBottom: "1px solid #1e293b",
      }}
    >
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.56rem",
          color: "#64748b",
          letterSpacing: "0.06em",
          textTransform: "uppercase",
        }}
      >
        {label}
      </span>
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.7rem",
          fontWeight: 700,
          color,
        }}
      >
        {value}
      </span>
    </div>
  );
}

function ImbalanceBar({ buyPct, stealth }: { buyPct: number; stealth: boolean }) {
  const sellPct = 1 - buyPct;
  const buyColor  = stealth ? "#334155" : buyPct  > 0.7 ? "#22c55e" : "#06b6d4";
  const sellColor = stealth ? "#1e293b" : "#ef4444";

  return (
    <div style={{ marginTop: "0.5rem" }}>
      <div
        style={{
          display: "flex",
          height: "6px",
          borderRadius: "3px",
          overflow: "hidden",
          background: "#1e293b",
        }}
      >
        <div
          style={{
            width: pct(buyPct),
            background: buyColor,
            transition: "width 0.6s ease",
            boxShadow: stealth ? "none" : `0 0 6px ${buyColor}`,
          }}
        />
        <div
          style={{
            flex: 1,
            background: sellColor,
            opacity: 0.6,
          }}
        />
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          marginTop: "3px",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: stealth ? "#334155" : buyColor,
          }}
        >
          BUY {pct(buyPct)}
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: stealth ? "#334155" : sellColor,
          }}
        >
          SELL {pct(sellPct)}
        </span>
      </div>
    </div>
  );
}

function OddsGauge({ yes, no, stealth }: { yes: number; no: number; stealth: boolean }) {
  const yesColor = stealth ? "#334155" : yes < 0.52 ? "#f59e0b" : "#22c55e";
  const noColor  = stealth ? "#1e293b" : "#ef4444";

  return (
    <div style={{ marginTop: "0.5rem" }}>
      <div
        style={{
          display: "flex",
          height: "6px",
          borderRadius: "3px",
          overflow: "hidden",
          background: "#1e293b",
        }}
      >
        <div
          style={{
            width: pct(yes),
            background: yesColor,
            transition: "width 0.6s ease",
            boxShadow: stealth ? "none" : `0 0 6px ${yesColor}`,
          }}
        />
        <div style={{ flex: 1, background: noColor, opacity: 0.6 }} />
      </div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          marginTop: "3px",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: stealth ? "#334155" : yesColor,
          }}
        >
          YES {pct(yes)}
        </span>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: stealth ? "#334155" : noColor,
          }}
        >
          NO {pct(no)}
        </span>
      </div>
    </div>
  );
}

// ── Main widget ───────────────────────────────────────────────────────────────

// ── Trade log helpers ─────────────────────────────────────────────────────────

function tradeStatusColor(status: string, stealth: boolean): string {
  if (stealth) return "#475569";
  switch (status) {
    case "success": return "#22c55e";
    case "halted":  return "#ef4444";
    case "timeout": return "#f97316";
    case "failed":  return "#ef4444";
    default:        return "#64748b";
  }
}

function relativeTime(iso: string): string {
  try {
    const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
    if (diff < 60)  return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ago`;
  } catch {
    return "—";
  }
}

export default function PredictorWidget() {
  const { stealth } = useStealth();

  const { data, error, isLoading } = useSWR<CrossExchangeData>(
    "/api/prediction/cross-exchange",
    swrFetcher<CrossExchangeData>,
    { refreshInterval: 30_000 }
  );

  const { data: tradeLog } = useSWR<TradeLogResponse>(
    "/api/prediction/trade-log",
    swrFetcher<TradeLogResponse>,
    { refreshInterval: 15_000 }
  );

  const signal  = data?.signal  ?? "NEUTRAL";
  const colors  = signalColors(signal, stealth);
  const isArb   = signal === "HIGH_CONFIDENCE_BUY";
  const accentColor = stealth ? "#1e5a7a" : "#0ea5e9";

  // ── Loading / error states ─────────────────────────────────────────────────
  if (isLoading || (!data && !error)) {
    return (
      <div
        style={{
          background: "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)",
          border: "1px solid #1e293b",
          borderRadius: "12px",
          padding: "1.25rem",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "220px",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "#334155",
            letterSpacing: "0.1em",
            animation: "pw-blink 1.4s ease-in-out infinite",
          }}
        >
          FETCHING CROSS-EXCHANGE DATA…
        </span>
        <style>{`@keyframes pw-blink { 0%,100%{opacity:1} 50%{opacity:0.3} }`}</style>
      </div>
    );
  }

  if (error && !data) {
    return (
      <div
        style={{
          background: "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)",
          border: "1px solid #ef444433",
          borderRadius: "12px",
          padding: "1.25rem",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "220px",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "#ef4444",
            letterSpacing: "0.08em",
          }}
        >
          ⚠ DATA UNAVAILABLE — RETRYING…
        </span>
      </div>
    );
  }

  const b  = data!.binance;
  const pm = data!.polymarket;

  return (
    <div
      style={{
        background: "linear-gradient(160deg, #0a0f1e 0%, #050912 100%)",
        border: `1px solid ${stealth ? "#1e293b" : colors.primary}33`,
        borderRadius: "12px",
        padding: "1.25rem",
        boxShadow: stealth ? "none" : `0 0 24px ${colors.glow}`,
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
          🔮 Cross-Exchange Predictor
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: stealth ? "#334155" : colors.primary,
              display: "inline-block",
              boxShadow: stealth ? "none" : `0 0 8px ${colors.primary}`,
              animation: stealth || signal === "NEUTRAL" ? "none" : "pw-pulse 1.8s ease-in-out infinite",
            }}
          />
        </h3>
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            color: stealth ? "#334155" : "#64748b",
          }}
        >
          30s refresh
        </span>
      </div>

      {/* ── Two-panel body ── */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "0.75rem",
          marginBottom: "0.75rem",
        }}
      >
        {/* ── LEFT: Binance ── */}
        <div
          style={{
            background: "#0f172a",
            border: "1px solid #1e293b",
            borderRadius: "10px",
            padding: "0.75rem",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: stealth ? "#334155" : "#06b6d4",
              marginBottom: "0.6rem",
              display: "flex",
              alignItems: "center",
              gap: "0.3rem",
            }}
          >
            <span style={{ fontSize: "0.7rem" }}>⚡</span>
            Real World Pulse
            <span style={{ color: stealth ? "#1e293b" : "#1e5a7a", fontWeight: 400 }}>(Binance)</span>
          </div>

          {b ? (
            <>
              {/* BTC Price */}
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "1.35rem",
                  fontWeight: 700,
                  color: stealth ? "#94a3b8" : "#f8fafc",
                  letterSpacing: "0.02em",
                  marginBottom: "0.5rem",
                  textShadow: stealth ? "none" : "0 0 12px #ffffff1a",
                }}
              >
                {fmtPrice(b.price)}
              </div>
              <DataRow
                label="OB Direction"
                value={b.imbalance_direction}
                color={stealth ? "#64748b" : b.imbalance_direction === "BUY" ? "#22c55e" : "#ef4444"}
              />
              <DataRow
                label="Strength"
                value={pct(b.imbalance_strength)}
                color={stealth ? "#64748b" : colors.primary}
              />
              <DataRow
                label="Bid Volume"
                value={b.total_bids.toFixed(2)}
                color={stealth ? "#64748b" : "#22c55e"}
              />
              <DataRow
                label="Ask Volume"
                value={b.total_asks.toFixed(2)}
                color={stealth ? "#64748b" : "#ef4444"}
              />
              <ImbalanceBar buyPct={b.buy_pct} stealth={stealth} />
            </>
          ) : (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.6rem",
                color: "#ef4444",
              }}
            >
              Binance unavailable
            </span>
          )}
        </div>

        {/* ── RIGHT: Polymarket ── */}
        <div
          style={{
            background: "#0f172a",
            border: "1px solid #1e293b",
            borderRadius: "10px",
            padding: "0.75rem",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: stealth ? "#334155" : "#a78bfa",
              marginBottom: "0.6rem",
              display: "flex",
              alignItems: "center",
              gap: "0.3rem",
            }}
          >
            <span style={{ fontSize: "0.7rem" }}>🎲</span>
            Market Odds
            <span style={{ color: stealth ? "#1e293b" : "#1e3a5a", fontWeight: 400 }}>(Polymarket)</span>
          </div>

          {pm && pm.market_found ? (
            <>
              {/* Market question */}
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.52rem",
                  color: stealth ? "#334155" : "#64748b",
                  marginBottom: "0.5rem",
                  lineHeight: 1.4,
                  overflow: "hidden",
                  display: "-webkit-box",
                  WebkitLineClamp: 2,
                  WebkitBoxOrient: "vertical" as const,
                }}
              >
                {pm.market_question ?? "—"}
              </div>

              {/* Yes price — big display */}
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "1.35rem",
                  fontWeight: 700,
                  color: stealth
                    ? "#94a3b8"
                    : (pm.yes_price ?? 0) < 0.52
                    ? "#f59e0b"
                    : "#22c55e",
                  letterSpacing: "0.02em",
                  marginBottom: "0.5rem",
                }}
              >
                ${pm.yes_price?.toFixed(3) ?? "—"}
                <span
                  style={{
                    fontSize: "0.55rem",
                    color: stealth ? "#475569" : "#64748b",
                    marginLeft: "0.3rem",
                    fontWeight: 400,
                  }}
                >
                  YES
                </span>
              </div>

              <DataRow
                label="No Price"
                value={`$${pm.no_price?.toFixed(3) ?? "—"}`}
                color={stealth ? "#64748b" : "#ef4444"}
              />
              <DataRow
                label="Threshold"
                value={`$${data!.thresholds.polymarket_yes_ceiling.toFixed(2)}`}
                color={stealth ? "#64748b" : "#64748b"}
              />
              {pm.volume != null && (
                <DataRow
                  label="Volume"
                  value={`$${Number(pm.volume).toLocaleString("en-US", { maximumFractionDigits: 0 })}`}
                  color={stealth ? "#64748b" : "#94a3b8"}
                />
              )}
              <OddsGauge
                yes={pm.yes_price ?? 0.5}
                no={pm.no_price ?? 0.5}
                stealth={stealth}
              />
            </>
          ) : (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.6rem",
                color: "#ef4444",
              }}
            >
              Polymarket unavailable
            </span>
          )}
        </div>
      </div>

      {/* ── Signal banner ── */}
      <div
        style={{
          background: stealth ? "rgba(15,23,42,0.5)" : colors.bg,
          border: `1px solid ${stealth ? "#1e293b" : colors.primary}44`,
          borderRadius: "8px",
          padding: "0.55rem 0.75rem",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: "0.5rem",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          {isArb && !stealth && (
            <span
              style={{
                display: "inline-block",
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: colors.primary,
                boxShadow: `0 0 10px ${colors.primary}`,
                animation: "pw-pulse 1.2s ease-in-out infinite",
                flexShrink: 0,
              }}
            />
          )}
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              fontWeight: 700,
              color: stealth ? "#475569" : colors.primary,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
            }}
          >
            {isArb && !stealth
              ? "⚡ ARBITRAGE OPPORTUNITY FOUND"
              : data?.signal_label ?? "Neutral"}
          </span>
        </div>

        {data?.arbitrage_gap != null && !stealth && (
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.6rem",
              fontWeight: 700,
              color: colors.primary,
              background: `${colors.primary}1a`,
              padding: "0.15rem 0.5rem",
              borderRadius: "4px",
              whiteSpace: "nowrap",
            }}
          >
            GAP +${data.arbitrage_gap.toFixed(3)}
          </span>
        )}
      </div>

      {/* ── Live Trade Log ── */}
      <div
        style={{
          marginTop: "0.6rem",
          background: "#0a0f1e",
          border: `1px solid ${stealth ? "#1e293b" : "#1e293b"}`,
          borderRadius: "8px",
          padding: "0.6rem 0.75rem",
        }}
      >
        {/* Trade log header */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: "0.4rem",
          }}
        >
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.56rem",
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              color: stealth ? "#334155" : "#64748b",
            }}
          >
            ⚡ Live Trade Log
          </span>
          {tradeLog && !stealth && (
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.5rem",
                color: tradeLog.paper_trading ? "#f97316" : "#22c55e",
                background: tradeLog.paper_trading ? "#f973160d" : "#22c55e0d",
                border: `1px solid ${tradeLog.paper_trading ? "#f9731633" : "#22c55e33"}`,
                padding: "0.1rem 0.4rem",
                borderRadius: "3px",
              }}
            >
              {tradeLog.paper_trading ? "PAPER" : "LIVE"}
            </span>
          )}
        </div>

        {/* Trade entries */}
        {(!tradeLog || tradeLog.entries.length === 0) ? (
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.56rem",
              color: stealth ? "#1e293b" : "#334155",
              letterSpacing: "0.04em",
            }}
          >
            No automated trades yet — waiting for signal…
          </span>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
            {tradeLog.entries.slice(0, 5).map((entry, i) => (
              <div
                key={i}
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  gap: "0.5rem",
                  padding: "0.2rem 0",
                  borderBottom: i < Math.min(tradeLog.entries.length, 5) - 1
                    ? "1px solid #1e293b"
                    : "none",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: "0.35rem", flex: 1, minWidth: 0 }}>
                  <span
                    style={{
                      display: "inline-block",
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      background: stealth ? "#334155" : tradeStatusColor(entry.status, stealth),
                      flexShrink: 0,
                    }}
                  />
                  <span
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.58rem",
                      color: stealth ? "#475569" : tradeStatusColor(entry.status, stealth),
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                    }}
                  >
                    {entry.log_text || `${entry.side} ${entry.shares.toFixed(1)} @ $${entry.price.toFixed(3)}`}
                  </span>
                </div>
                <span
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.5rem",
                    color: stealth ? "#1e293b" : "#475569",
                    flexShrink: 0,
                    whiteSpace: "nowrap",
                  }}
                >
                  {relativeTime(entry.timestamp)}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── Footer ── */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          marginTop: "0.5rem",
        }}
      >
        <span
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: "#1e293b",
            letterSpacing: "0.06em",
          }}
        >
          OB THRESHOLD: {pct(data?.thresholds.imbalance_threshold ?? 0.7)}
          &nbsp;|&nbsp;
          YES CEILING: ${data?.thresholds.polymarket_yes_ceiling.toFixed(2) ?? "0.52"}
        </span>
        {data?.errors && data.errors.length > 0 && (
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.5rem",
              color: "#ef4444",
              letterSpacing: "0.06em",
            }}
          >
            {data.errors.length} error{data.errors.length > 1 ? "s" : ""}
          </span>
        )}
      </div>

      <style>{`
        @keyframes pw-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%       { opacity: 0.5; transform: scale(0.85); }
        }
        @keyframes pw-blink {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </div>
  );
}
