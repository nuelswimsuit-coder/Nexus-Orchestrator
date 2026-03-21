"use client";

import useSWR from "swr";
import {
  API_BASE,
  type WarRoomIntelResponse,
  swrFetcher,
} from "@/lib/api";
import { useStealth } from "@/lib/stealth";

function heatColor(v: number, stealth: boolean): string {
  if (stealth) return "#1e293b";
  const t = Math.max(0, Math.min(100, v)) / 100;
  const r = Math.round(20 + t * 180);
  const g = Math.round(40 + t * 200);
  const b = Math.round(80 + (1 - t) * 120);
  return `rgb(${r},${g},${b})`;
}

export default function WarRoomIntel() {
  const { stealth } = useStealth();
  const { data, error, isLoading } = useSWR<WarRoomIntelResponse>(
    `${API_BASE}/api/business/war-room`,
    swrFetcher<WarRoomIntelResponse>,
    { refreshInterval: 5_000 },
  );

  if (isLoading && !data) {
    return (
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          color: stealth ? "#1e293b" : "#64748b",
          letterSpacing: "0.06em",
        }}
      >
        Loading war-room intel…
      </div>
    );
  }

  if (error || !data) {
    return (
      <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#f87171" }}>
        War-room unavailable — is the API up?
      </div>
    );
  }

  const conf = data.master_confidence_pct;
  const race = data.race_to_1000_pct;
  const grid = data.sentiment_heatmap ?? [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1.1rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: "1rem",
        }}
      >
        <div
          style={{
            padding: "1rem",
            borderRadius: "12px",
            border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.25)"}`,
            background: stealth ? "transparent" : "rgba(0,20,40,0.35)",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              letterSpacing: "0.14em",
              color: stealth ? "#1e293b" : "#6b8fab",
              marginBottom: "0.35rem",
            }}
          >
            MASTER CONFIDENCE
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.85rem",
              fontWeight: 800,
              color: stealth ? "#1e293b" : "#e8f2ff",
            }}
          >
            {conf.toFixed(1)}%
          </div>
          <div
            style={{
              marginTop: "0.5rem",
              height: 6,
              borderRadius: 3,
              background: stealth ? "#0f172a" : "#0f172a",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                width: `${conf}%`,
                height: "100%",
                background: stealth ? "#1e293b" : "linear-gradient(90deg,#00b4ff,#46ff8b)",
                transition: "width 0.6s ease",
              }}
            />
          </div>
        </div>

        <div
          style={{
            padding: "1rem",
            borderRadius: "12px",
            border: `1px solid ${stealth ? "#1e293b" : "rgba(255,215,80,0.28)"}`,
            background: stealth ? "transparent" : "rgba(40,30,0,0.25)",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              letterSpacing: "0.14em",
              color: stealth ? "#1e293b" : "#b89a3c",
              marginBottom: "0.35rem",
            }}
          >
            RACE TO 1000% · SIM WALLET
          </div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.85rem",
              fontWeight: 800,
              color: stealth ? "#1e293b" : "#ffe066",
            }}
          >
            {race.toFixed(1)}%
          </div>
          <div style={{ fontSize: "0.62rem", color: stealth ? "#1e293b" : "#94a3b8", marginTop: "0.35rem" }}>
            Target +${data.race_target_profit_usd.toFixed(0)} sim profit · Kelly {data.kelly_fraction.toFixed(3)}
          </div>
          <div
            style={{
              marginTop: "0.5rem",
              height: 6,
              borderRadius: 3,
              background: stealth ? "#0f172a" : "#0f172a",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                width: `${Math.min(100, race)}%`,
                height: "100%",
                background: stealth ? "#1e293b" : "linear-gradient(90deg,#ffd84d,#ff4fd8)",
                transition: "width 0.6s ease",
              }}
            />
          </div>
        </div>

        <div
          style={{
            padding: "1rem",
            borderRadius: "12px",
            border: `1px solid ${stealth ? "#1e293b" : "rgba(255,79,216,0.22)"}`,
            background: stealth ? "transparent" : "rgba(30,0,40,0.28)",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              letterSpacing: "0.14em",
              color: stealth ? "#1e293b" : "#c084fc",
              marginBottom: "0.35rem",
            }}
          >
            SWARM · OPENCLAW
          </div>
          <div style={{ fontSize: "0.72rem", color: stealth ? "#1e293b" : "#cbd5e1", lineHeight: 1.5 }}>
            Sentiment <strong style={{ color: stealth ? "#1e293b" : "#fff" }}>{data.openclaw_sentiment.toFixed(1)}</strong>
            /10 · Workers {data.swarm_workers_seen} · Whale hits {data.swarm_whale_hits}
          </div>
          <div style={{ fontSize: "0.65rem", color: stealth ? "#1e293b" : "#94a3b8", marginTop: "0.4rem" }}>
            Alpha: {data.top_alpha_channel}
          </div>
          {data.aggressive_strike && (
            <div
              style={{
                marginTop: "0.5rem",
                fontSize: "0.62rem",
                fontWeight: 700,
                color: stealth ? "#1e293b" : "#f472b6",
                letterSpacing: "0.08em",
              }}
            >
              ⚡ STRIKE MODE · Reinvest {data.strike_reinvest_pct}%
            </div>
          )}
        </div>
      </div>

      <div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            letterSpacing: "0.14em",
            color: stealth ? "#1e293b" : "#6b8fab",
            marginBottom: "0.5rem",
          }}
        >
          FLEET SENTIMENT HEATMAP
        </div>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: `repeat(${grid[0]?.length ?? 8}, minmax(0, 1fr))`,
            gap: 3,
            maxWidth: 480,
          }}
        >
          {grid.flatMap((row, ri) =>
            row.map((cell, ci) => (
              <div
                key={`${ri}-${ci}`}
                title={`mood ${cell}`}
                style={{
                  aspectRatio: "1",
                  borderRadius: 2,
                  background: heatColor(cell, stealth),
                  opacity: stealth ? 0.25 : 1,
                  minHeight: 14,
                }}
              />
            )),
          )}
        </div>
      </div>
    </div>
  );
}
