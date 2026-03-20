"use client";

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import type { BudgetWidgetResponse } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

export default function BudgetWidget() {
  const { stealth } = useStealth();
  
  const { data: budget } = useSWR<BudgetWidgetResponse>(
    "/api/projects/budget/widget",
    swrFetcher<BudgetWidgetResponse>,
    { refreshInterval: 60_000 }
  );

  if (!budget?.available) {
    return (
      <div style={{
        background: "#0a0e1a", border: "1px solid #1e293b",
        borderRadius: "12px", padding: "1.25rem",
      }}>
        <h3 style={{
          fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700,
          letterSpacing: "0.1em", textTransform: "uppercase", color: "#334155",
          margin: "0 0 0.5rem 0",
        }}>
          💰 Daily P&L
        </h3>
        <div style={{
          fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155",
          textAlign: "center", padding: "1rem 0",
        }}>
          BudgetTracker not available
        </div>
      </div>
    );
  }

  const pnl = budget.daily_pnl || 0;
  const isProfit = pnl > 0;
  const isBreakeven = pnl === 0;
  
  const pnlColor = isBreakeven ? "#94a3b8" : isProfit ? "#22c55e" : "#ef4444";
  const pnlPrefix = isBreakeven ? "±" : isProfit ? "+" : "";

  return (
    <div style={{
      background: "linear-gradient(160deg, #0a0e1a 0%, #080d18 100%)",
      border: `1px solid ${stealth ? "#1e293b" : isProfit ? "#22c55e33" : isBreakeven ? "#1e293b" : "#ef444433"}`,
      borderRadius: "12px", padding: "1.25rem",
      boxShadow: !stealth && isProfit ? "0 0 20px #22c55e11" : "none",
      transition: "border-color 0.3s, box-shadow 0.3s",
    }}>
      
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "1rem" }}>
        <h3 style={{
          fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700,
          letterSpacing: "0.1em", textTransform: "uppercase",
          color: stealth ? "#334155" : "#94a3b8", margin: 0,
          display: "flex", alignItems: "center", gap: "0.5rem",
        }}>
          💰 Daily P&L
          <span style={{
            width: 6, height: 6, borderRadius: "50%", background: pnlColor,
            display: "inline-block",
            boxShadow: stealth ? "none" : `0 0 6px ${pnlColor}`,
            animation: stealth ? "none" : isProfit ? "rgb-pulse 2s infinite" : "none",
          }} />
        </h3>
        <span style={{
          fontFamily: "var(--font-mono)", fontSize: "0.6rem",
          color: "#334155", textTransform: "uppercase",
        }}>
          {budget.status || "Unknown"}
        </span>
      </div>

      {/* P&L Display */}
      <div style={{ textAlign: "center", marginBottom: "1rem" }}>
        <div style={{
          fontFamily: "var(--font-mono)", fontSize: "1.8rem", fontWeight: 700,
          color: pnlColor, letterSpacing: "0.05em",
          textShadow: !stealth && isProfit ? `0 0 12px ${pnlColor}44` : "none",
        }}>
          {pnlPrefix}{Math.abs(pnl).toFixed(2)}
        </div>
        <div style={{
          fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#64748b",
          letterSpacing: "0.08em", textTransform: "uppercase",
        }}>
          {budget.currency || "USD"} Today
        </div>
      </div>

      {/* Status indicator */}
      <div style={{
        background: isProfit ? "rgba(34,197,94,0.08)" 
                  : isBreakeven ? "rgba(148,163,184,0.08)"
                  : "rgba(239,68,68,0.08)",
        border: `1px solid ${pnlColor}33`,
        borderRadius: "8px", padding: "0.5rem",
        textAlign: "center",
      }}>
        <span style={{
          fontFamily: "var(--font-mono)", fontSize: "0.65rem", fontWeight: 700,
          color: pnlColor, letterSpacing: "0.06em",
        }}>
          {isProfit ? "📈 PROFIT" : isBreakeven ? "⚖️ BREAKEVEN" : "📉 LOSS"}
        </span>
      </div>

      {/* Last transaction */}
      {budget.last_transaction && (
        <div style={{
          marginTop: "0.75rem", fontFamily: "var(--font-mono)", fontSize: "0.6rem",
          color: "#334155", textAlign: "center",
        }}>
          Last: {new Date(budget.last_transaction).toLocaleString()}
        </div>
      )}

      <style>{`
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}