"use client";

/**
 * AutonomyControl — The AI Pilot Slider (Phase 20)
 * 
 * Dynamic percentage slider (0% to 100%) with glow effects:
 * - 0-30% (Manual): Orange glow — AI suggests, requires manual approval
 * - 31-70% (Assisted): Green glow — AI auto-executes high-confidence actions
 * - 71-100% (AI Pilot): Bright cyan glow — Full autonomy, no approval requests
 * 
 * Connected to the decision engine's auto-threshold system.
 */

import { useCallback, useState } from "react";
import useSWR from "swr";
import { swrFetcher, patchConfig } from "@/lib/api";
import type { ConfigResponse } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useStealth } from "@/lib/stealth";

type AutonomyMode = "manual" | "assisted" | "pilot";

function getAutonomyMode(level: number): AutonomyMode {
  if (level <= 30) return "manual";
  if (level <= 70) return "assisted";
  return "pilot";
}

function getAutonomyColors(mode: AutonomyMode, stealth: boolean) {
  if (stealth) {
    return { primary: "#334155", glow: "none", bg: "rgba(51, 65, 85, 0.1)" };
  }
  
  switch (mode) {
    case "manual":
      return { 
        primary: "#f59e0b", 
        glow: "0 0 20px #f59e0b44, 0 0 40px #f59e0b22",
        bg: "rgba(245, 158, 11, 0.08)"
      };
    case "assisted":
      return { 
        primary: "#22c55e", 
        glow: "0 0 20px #22c55e44, 0 0 40px #22c55e22",
        bg: "rgba(34, 197, 94, 0.08)"
      };
    case "pilot":
      return { 
        primary: "#00ffff", 
        glow: "0 0 25px #00ffff66, 0 0 50px #00ffff33, 0 0 75px #00ffff11",
        bg: "rgba(0, 255, 255, 0.1)"
      };
  }
}

export default function AutonomyControl() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  
  // Read current autonomy level from HITL threshold config
  const { data: config, mutate } = useSWR<ConfigResponse>(
    "/api/config",
    swrFetcher<ConfigResponse>,
    { refreshInterval: 0 }
  );
  
  // Convert HITL threshold (60) to autonomy percentage (0-100)
  // Lower threshold = higher autonomy
  const currentLevel = config ? Math.round((100 - (config as any).hitl_threshold || 60) * 1.67) : 50;
  const [localLevel, setLocalLevel] = useState(currentLevel);
  
  const mode = getAutonomyMode(localLevel);
  const colors = getAutonomyColors(mode, stealth);
  
  const modeLabels: Record<AutonomyMode, string> = {
    manual:   t("autonomy.mode_manual"),
    assisted: t("autonomy.mode_assisted"),
    pilot:    t("autonomy.mode_pilot"),
  };

  const modeDescriptions: Record<AutonomyMode, string> = {
    manual:   t("autonomy.desc_manual"),
    assisted: t("autonomy.desc_assisted"),
    pilot:    t("autonomy.desc_pilot"),
  };

  const handleLevelChange = useCallback(async (newLevel: number) => {
    setLocalLevel(newLevel);
    
    // Convert autonomy percentage back to HITL threshold
    const hitlThreshold = Math.round(100 - (newLevel / 1.67));
    
    try {
      await patchConfig({ hitl_threshold: hitlThreshold } as any);
      await mutate();
    } catch (err) {
      console.error("Failed to update autonomy level:", err);
      setLocalLevel(currentLevel); // Revert on error
    }
  }, [currentLevel, mutate]);

  return (
    <div style={{
      background: stealth ? "rgba(15,23,42,0.6)" : "rgba(2,6,23,0.8)",
      backdropFilter: "blur(12px)",
      border: `1px solid ${stealth ? "#1e293b" : colors.primary}44`,
      borderRadius: "16px",
      padding: "1.5rem",
      boxShadow: stealth ? "none" : colors.glow,
      transition: "all 0.3s ease",
    }}>
      
      {/* Header */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        marginBottom: "1.25rem",
      }}>
        <div>
          <h3 style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.85rem",
            fontWeight: 700,
            color: stealth ? "#94a3b8" : colors.primary,
            letterSpacing: "0.08em",
            textTransform: "uppercase",
            margin: 0,
            textShadow: stealth ? "none" : `0 0 15px ${colors.primary}66`,
          }}>
            {t("autonomy.title")}
          </h3>
          <p style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.6rem",
            color: stealth ? "#64748b" : "#94a3b8",
            margin: "0.25rem 0 0",
            textAlign: isRTL ? "right" : "left",
          }}>
            {modeDescriptions[mode]}
          </p>
        </div>
        
        <div style={{
          fontFamily: "var(--font-mono)",
          fontSize: "1.2rem",
          fontWeight: 700,
          color: stealth ? "#94a3b8" : colors.primary,
          textShadow: stealth ? "none" : `0 0 20px ${colors.primary}`,
        }}>
          {localLevel}%
        </div>
      </div>

      {/* Slider track */}
      <div style={{
        position: "relative",
        height: "8px",
        background: stealth ? "#1e293b" : "#0f172a",
        borderRadius: "4px",
        marginBottom: "1rem",
        border: "1px solid #334155",
      }}>
        {/* Fill */}
        <div style={{
          position: "absolute",
          left: 0, top: 0,
          width: `${localLevel}%`,
          height: "100%",
          background: stealth ? "#475569" 
                    : `linear-gradient(90deg, #f59e0b, #22c55e, #00ffff)`,
          borderRadius: "4px",
          transition: "width 0.3s ease",
          boxShadow: stealth ? "none" : `0 0 10px ${colors.primary}44`,
        }} />
        
        {/* Slider thumb */}
        <div style={{
          position: "absolute",
          left: `calc(${localLevel}% - 12px)`,
          top: "-4px",
          width: "20px",
          height: "16px",
          background: stealth ? "#6b7280" 
                    : `radial-gradient(circle, ${colors.primary}, ${colors.primary}cc)`,
          borderRadius: "8px",
          border: `2px solid ${stealth ? "#94a3b8" : "#fff"}`,
          cursor: "pointer",
          transition: "all 0.3s ease",
          boxShadow: stealth ? "none" : colors.glow,
          animation: stealth ? "none" : "thumb-pulse 2s ease-in-out infinite",
        }} />

        {/* Range input (invisible overlay) */}
        <input
          type="range"
          min={0}
          max={100}
          value={localLevel}
          onChange={(e) => handleLevelChange(parseInt(e.target.value))}
          style={{
            position: "absolute",
            inset: 0,
            opacity: 0,
            cursor: "pointer",
          }}
        />
      </div>

      {/* Mode indicator */}
      <div style={{
        display: "flex", alignItems: "center", justifyContent: "center",
        padding: "0.75rem",
        background: stealth ? "rgba(15,23,42,0.5)" : colors.bg,
        borderRadius: "10px",
        border: `1px solid ${stealth ? "#1e293b" : colors.primary}33`,
      }}>
        <div style={{
          display: "flex", alignItems: "center", gap: "0.5rem",
          flexDirection: isRTL ? "row-reverse" : "row",
        }}>
          <div style={{
            width: "8px", height: "8px", borderRadius: "50%",
            background: stealth ? "#6b7280" : colors.primary,
            boxShadow: stealth ? "none" : `0 0 10px ${colors.primary}`,
            animation: stealth ? "none" : "mode-pulse 1.5s ease-in-out infinite",
          }} />
          <span style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.7rem",
            fontWeight: 600,
            color: stealth ? "#94a3b8" : colors.primary,
            letterSpacing: "0.05em",
            textShadow: stealth ? "none" : `0 0 8px ${colors.primary}44`,
          }}>
            {modeLabels[mode]}
          </span>
        </div>
      </div>

      <style>{`
        @keyframes thumb-pulse {
          0%, 100% { transform: scale(1); }
          50% { transform: scale(1.1); }
        }
        @keyframes mode-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.6; }
        }
      `}</style>
    </div>
  );
}