"use client";

/**
 * SessionHealthGauge — OTP Sessions Fuel Gauge Widget
 * 
 * Real-time monitoring of OTP_Sessions_Creator with a fuel gauge visualization.
 * Shows session count and recent activity as "fuel level" percentage.
 * Connected to the TeleFix module system.
 */

import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useStealth } from "@/lib/stealth";

interface FuelGaugeData {
  available: boolean;
  session_count: number;
  recent_activity: number;
  fuel_level: number;
}

export default function SessionHealthGauge() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  
  const { data: fuelData } = useSWR<FuelGaugeData>(
    "/api/modules/widgets/fuel-gauge",
    swrFetcher<FuelGaugeData>,
    { refreshInterval: 30_000 }
  );

  if (!fuelData?.available) {
    return (
      <div style={{
        background: "linear-gradient(160deg, #0a0e1a 0%, #080d18 100%)",
        border: "1px solid #1e293b",
        borderRadius: "12px", padding: "1.25rem",
      }}>
        <h3 style={{
          fontFamily: "var(--font-sans)", fontSize: "0.7rem", fontWeight: 600,
          letterSpacing: "0.08em", textTransform: "uppercase",
          color: "#2a3450", margin: "0 0 0.5rem 0",
        }}>
          {t("session_fuel")}
        </h3>
        <div style={{
          fontFamily: "var(--font-sans)", fontSize: "0.65rem", color: "#2a3450",
          textAlign: "center", padding: "1rem 0",
        }}>
          {t("otp_unavailable")}
        </div>
      </div>
    );
  }

  const fuelLevel = Math.max(0, Math.min(100, fuelData.fuel_level));
  const sessionCount = fuelData.session_count;
  const recentActivity = fuelData.recent_activity;

  // Fuel level color coding
  const getFuelColor = (level: number) => {
    if (level >= 70) return "#22c55e";      // Green (high)
    if (level >= 40) return "#f59e0b";      // Orange (medium)
    return "#ef4444";                       // Red (low)
  };

  const fuelColor = getFuelColor(fuelLevel);
  const isHealthy = fuelLevel >= 70;
  const isLow = fuelLevel < 40;

  return (
    <div style={{
      background: "linear-gradient(160deg, #0a0e1a 0%, #080d18 100%)",
      border: `1px solid ${stealth ? "#1e293b" : isLow ? "#ef444433" : isHealthy ? "#22c55e33" : "#f59e0b33"}`,
      borderRadius: "12px", padding: "1.25rem",
      boxShadow: stealth ? "none" 
               : isHealthy ? "0 0 20px #22c55e11" 
               : isLow ? "0 0 20px #ef444411"
               : "0 0 20px #f59e0b11",
      transition: "border-color 0.3s, box-shadow 0.3s",
    }}>
      
      {/* Header */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        marginBottom: "1.25rem",
        flexDirection: isRTL ? "row-reverse" : "row",
      }}>
        <h3 style={{
          fontFamily: "var(--font-sans)", fontSize: "0.7rem", fontWeight: 600,
          letterSpacing: "0.08em", textTransform: "uppercase",
          color: stealth ? "#2a3450" : "#6b8fab", margin: 0,
          display: "flex", alignItems: "center", gap: "0.5rem",
        }}>
          {t("session_fuel")}
          <div style={{
            width: 6, height: 6, borderRadius: "50%", background: fuelColor,
            display: "inline-block",
            boxShadow: stealth ? "none" : `0 0 8px ${fuelColor}`,
            animation: stealth ? "none" : isHealthy ? "fuel-pulse 2s infinite" : "none",
          }} />
        </h3>
        <span style={{
          fontFamily: "var(--font-sans)", fontSize: "0.6rem",
          color: fuelColor, textTransform: "uppercase", fontWeight: 600,
        }}>
          {isHealthy ? t("healthy") : isLow ? t("low") : t("medium")}
        </span>
      </div>

      {/* Fuel Gauge Visualization */}
      <div style={{ position: "relative", marginBottom: "1.25rem" }}>
        {/* Gauge background */}
        <svg viewBox="0 0 200 120" style={{ width: "100%", height: "80px" }}>
          {/* Background arc */}
          <path
            d="M 20 100 A 80 80 0 0 1 180 100"
            fill="none"
            stroke={stealth ? "#1e293b" : "#0f172a"}
            strokeWidth="8"
            strokeLinecap="round"
          />
          
          {/* Fuel level arc */}
          <path
            d="M 20 100 A 80 80 0 0 1 180 100"
            fill="none"
            stroke={stealth ? "#475569" : fuelColor}
            strokeWidth="8"
            strokeLinecap="round"
            strokeDasharray={`${fuelLevel * 2.51} 251`}
            style={{
              transition: "stroke-dasharray 0.8s ease, stroke 0.3s",
              filter: stealth ? "none" : `drop-shadow(0 0 8px ${fuelColor}44)`,
            }}
          />
          
          {/* Center percentage */}
          <text
            x="100" y="85"
            textAnchor="middle"
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "1.5rem",
              fontWeight: 700,
              fill: stealth ? "#94a3b8" : fuelColor,
              filter: stealth ? "none" : `drop-shadow(0 0 10px ${fuelColor}44)`,
            }}
          >
            {fuelLevel.toFixed(0)}%
          </text>
          
          {/* Fuel level text */}
          <text
            x="100" y="105"
            textAnchor="middle"
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.6rem",
              fill: stealth ? "#64748b" : "#94a3b8",
              letterSpacing: "0.08em",
            }}
          >
            {t("fuel_level")}
          </text>
        </svg>

        {/* Gauge markers */}
        {[0, 25, 50, 75, 100].map(value => {
          const angle = (value / 100) * 160 - 80; // -80° to +80°
          const radian = (angle * Math.PI) / 180;
          const x = 100 + Math.cos(radian) * 75;
          const y = 100 + Math.sin(radian) * 75;
          
          return (
            <div key={value} style={{
              position: "absolute",
              left: `${(x / 200) * 100}%`,
              top: `${(y / 120) * 100}%`,
              transform: "translate(-50%, -50%)",
              fontFamily: "var(--font-mono)",
              fontSize: "0.45rem",
              color: stealth ? "#334155" : "#475569",
              fontWeight: 700,
            }}>
              {value}
            </div>
          );
        })}
      </div>

      {/* Session metrics */}
      <div style={{
        display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem",
        fontFamily: "var(--font-sans)", fontSize: "0.65rem",
      }}>
        <div style={{ textAlign: isRTL ? "right" : "left" }}>
          <span style={{ color: "#2a3450", display: "block", marginBottom: "0.25rem" }}>
            {t("total_sessions")}
          </span>
          <span style={{ color: fuelColor, fontWeight: 600, fontSize: "0.85rem" }}>
            {sessionCount}
          </span>
        </div>
        <div style={{ textAlign: isRTL ? "left" : "right" }}>
          <span style={{ color: "#2a3450", display: "block", marginBottom: "0.25rem" }}>
            {t("activity_24h")}
          </span>
          <span style={{ color: fuelColor, fontWeight: 600, fontSize: "0.85rem" }}>
            {recentActivity}
          </span>
        </div>
      </div>

      <style>{`
        @keyframes fuel-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.7; }
        }
      `}</style>
    </div>
  );
}