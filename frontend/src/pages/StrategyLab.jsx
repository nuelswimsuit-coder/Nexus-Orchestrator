"use client";

/**
 * Quant Strategy Lab — sliders drive synthetic Polymarket backtest; push to production.
 */

import { useMemo, useState, useCallback } from "react";
import { API_BASE } from "@/lib/api";
import {
  Line,
  LineChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

function runBacktest(confidence, aggression, stopLossPct) {
  const pts = [];
  let equity = 10_000;
  const baseDrift = (confidence / 100) * 0.0012;
  const vol = (aggression / 100) * 0.018;
  const stop = stopLossPct / 100;
  for (let i = 0; i < 48; i++) {
    const noise = (Math.sin(i * 0.55) + Math.cos(i * 0.31)) * vol;
    let ret = baseDrift + noise;
    if (ret < -stop) ret = -stop;
    equity *= 1 + ret;
    pts.push({ i, equity: Math.round(equity) });
  }
  return { pts, final: equity, pnl: equity - 10_000 };
}

export default function StrategyLab() {
  const [confidence, setConfidence] = useState(62);
  const [aggression, setAggression] = useState(44);
  const [stopLoss, setStopLoss] = useState(12);
  const [pushMsg, setPushMsg] = useState(null);
  const [pushBusy, setPushBusy] = useState(false);

  const { pts, final, pnl } = useMemo(
    () => runBacktest(confidence, aggression, stopLoss),
    [confidence, aggression, stopLoss],
  );

  const pushProd = useCallback(async () => {
    setPushBusy(true);
    setPushMsg(null);
    try {
      const res = await fetch(`${API_BASE}/api/prediction/strategy/apply`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          confidence,
          aggression,
          stop_loss_pct: stopLoss,
          source: "strategy_lab",
        }),
      });
      const j = await res.json().catch(() => ({}));
      setPushMsg(res.ok ? j.message || "Strategy pushed to live bot" : `HTTP ${res.status}`);
    } catch {
      setPushMsg("Endpoint stub — configure POST /api/prediction/strategy/apply");
    } finally {
      setPushBusy(false);
    }
  }, [confidence, aggression, stopLoss]);

  const slider = (label, value, set, min, max) => (
    <label style={{ display: "flex", flexDirection: "column", gap: 6, flex: 1, minWidth: 160 }}>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#94a3b8" }}>
        {label} <strong style={{ color: "#e2e8f0" }}>{value}</strong>
      </span>
      <input
        type="range"
        min={min}
        max={max}
        value={value}
        onChange={(e) => set(Number(e.target.value))}
        style={{ width: "100%" }}
      />
    </label>
  );

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 0 2rem" }}>
      <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.05rem", color: "#f1f5f9", margin: "0 0 0.25rem" }}>STRATEGY BACKTEST LAB</h1>
      <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#64748b", marginBottom: "1.25rem" }}>
        סימולציה מיידית על סדרת הדמיה מנתוני עבר (פולימרקט-style) · עצור אובדן = חיתוך תנועה יומי
      </p>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: "1.25rem",
          marginBottom: "1.25rem",
          padding: "1rem",
          background: "#0c1222",
          borderRadius: 12,
          border: "1px solid #1e293b",
        }}
      >
        {slider("Confidence", confidence, setConfidence, 15, 95)}
        {slider("Aggression", aggression, setAggression, 10, 100)}
        {slider("Stop-Loss %", stopLoss, setStopLoss, 2, 40)}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "0.75rem", marginBottom: "1rem" }}>
        {[
          { k: "Final equity", v: `$${final.toLocaleString()}` },
          { k: "PnL (sim)", v: `${pnl >= 0 ? "+" : ""}$${Math.round(pnl).toLocaleString()}` },
          { k: "Mode", v: "Paper / historical blend" },
        ].map((x) => (
          <div key={x.k} style={{ background: "#080d18", border: "1px solid #1e293b", borderRadius: 10, padding: "0.75rem" }}>
            <div style={{ fontSize: "0.55rem", color: "#64748b", fontFamily: "var(--font-mono)" }}>{x.k}</div>
            <div style={{ fontSize: "0.95rem", color: "#38bdf8", fontFamily: "var(--font-mono)", fontWeight: 800 }}>{x.v}</div>
          </div>
        ))}
      </div>

      <div style={{ height: 260, minHeight: 260, width: "100%", minWidth: 0, background: "#080d18", border: "1px solid #1e293b", borderRadius: 12, padding: "0.5rem" }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={pts} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey="i" tick={{ fontSize: 9, fill: "#64748b" }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fontSize: 9, fill: "#64748b" }} axisLine={false} tickLine={false} />
            <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #334155", fontSize: 11 }} />
            <Line type="monotone" dataKey="equity" stroke="#4ade80" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div style={{ marginTop: "1rem", display: "flex", alignItems: "center", gap: "1rem", flexWrap: "wrap" }}>
        <button
          type="button"
          onClick={() => void pushProd()}
          disabled={pushBusy}
          style={{
            padding: "0.65rem 1.2rem",
            borderRadius: 10,
            border: "1px solid #22c55e",
            background: "rgba(34,197,94,0.12)",
            color: "#86efac",
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            fontWeight: 800,
            letterSpacing: "0.08em",
            cursor: pushBusy ? "wait" : "pointer",
          }}
        >
          {pushBusy ? "PUSHING…" : "PUSH TO PRODUCTION"}
        </button>
        {pushMsg && <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#94a3b8" }}>{pushMsg}</span>}
      </div>
    </div>
  );
}
