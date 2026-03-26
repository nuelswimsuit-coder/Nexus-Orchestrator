"use client";

import React from "react";
import PanicButtonPanel from "@/components/PanicButtonPanel";
import EmergencyKillSwitch from "@/components/EmergencyKillSwitch";
import { useStealth } from "@/lib/stealth";

/**
 * Unified emergency control panel.
 *
 * Combines two escalation levels:
 *   1. PanicButtonPanel  — Soft stop (reversible). Sets SYSTEM_STATE:PANIC,
 *      broadcasts TERMINATE to workers. Can be reset immediately.
 *   2. EmergencyKillSwitch — Hard stop (irreversible). Full kill-switch:
 *      halts trading, wipes exposure, requires exact confirmation phrase.
 *
 * Use this component wherever a single "emergency controls" section is needed
 * instead of placing both components separately.
 */
export default function EmergencyPanel() {
  const { stealth } = useStealth();

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "1rem",
      }}
    >
      {/* Tier 1: Soft panic — reversible */}
      <PanicButtonPanel />

      {/* Tier 2: Full kill-switch — irreversible */}
      <EmergencyKillSwitch />

      {/* Escalation legend */}
      {!stealth && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.55rem",
            color: "#1e3a5f",
            letterSpacing: "0.06em",
            textAlign: "center",
            lineHeight: 1.6,
          }}
        >
          TIER 1: PANIC (reversible) → TIER 2: KILL SWITCH (irreversible)
        </div>
      )}
    </div>
  );
}
