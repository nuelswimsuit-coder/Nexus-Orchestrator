"use client";

import React, { useState } from "react";
import { postFullKillSwitch } from "@/lib/api";
import { useStealth } from "@/lib/stealth";

const CONFIRM_PHRASE = "TERMINATE_NEXUS_NOW";

/**
 * Full NEXUS emergency kill-switch (matches `nexus.shared.kill_switch` + `/api/system/kill-switch`).
 * Double confirmation + exact phrase + optional `X-Nexus-Kill-Auth` when server token is set.
 */
export default function EmergencyKillSwitch() {
  const { stealth } = useStealth();
  const [armed, setArmed] = useState(false);
  const [phrase, setPhrase] = useState("");
  const [authToken, setAuthToken] = useState("");
  const [evacuate, setEvacuate] = useState(false);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  const BLOOD = "#b91c1c";
  const BG = "linear-gradient(165deg, #1a0508 0%, #0a0608 100%)";

  async function onPrimaryClick() {
    if (!armed) {
      setArmed(true);
      setMessage(null);
      return;
    }
    if (phrase.trim() !== CONFIRM_PHRASE) {
      setMessage(`Type exactly: ${CONFIRM_PHRASE}`);
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      const res = await postFullKillSwitch({
        confirmPhrase: CONFIRM_PHRASE,
        evacuate,
        authToken: authToken.trim() || undefined,
      });
      setMessage(
        typeof res === "object" && res !== null && "status" in res
          ? `Engaged: ${(res as { status?: string }).status}`
          : "Kill-switch engaged",
      );
      setArmed(false);
      setPhrase("");
    } catch (e) {
      setMessage(e instanceof Error ? e.message : "Request failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      dir="ltr"
      style={{
        background: BG,
        border: `2px solid ${stealth ? "#27272a" : `${BLOOD}88`}`,
        borderRadius: "16px",
        padding: "1.35rem",
        boxShadow: stealth ? "none" : `0 0 48px ${BLOOD}22, inset 0 1px 0 ${BLOOD}33`,
        display: "flex",
        flexDirection: "column",
        gap: "0.85rem",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
        <span style={{ fontSize: "1.4rem" }} aria-hidden>⛔</span>
        <div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.72rem",
              fontWeight: 800,
              letterSpacing: "0.14em",
              color: stealth ? "#52525b" : "#fecaca",
              textTransform: "uppercase",
            }}
          >
            Emergency kill-switch
          </div>
          <div
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.75rem",
              color: stealth ? "#3f3f46" : "#9ca3af",
              marginTop: "0.15rem",
            }}
          >
            Halts scalpers, workers (TERMINATE + FORCE_STOP), flattens Polymarket exposure, wipes API keys in-process.
          </div>
        </div>
      </div>

      {armed && (
        <>
          <label style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#a1a1aa" }}>
            Confirmation phrase
            <input
              value={phrase}
              onChange={(e) => setPhrase(e.target.value)}
              placeholder={CONFIRM_PHRASE}
              autoComplete="off"
              style={{
                display: "block",
                width: "100%",
                marginTop: "0.35rem",
                padding: "0.5rem 0.65rem",
                borderRadius: "8px",
                border: "1px solid #3f3f46",
                background: "#09090b",
                color: "#f4f4f5",
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
              }}
            />
          </label>
          <label style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#a1a1aa" }}>
            Optional: X-Nexus-Kill-Auth (if server requires)
            <input
              type="password"
              value={authToken}
              onChange={(e) => setAuthToken(e.target.value)}
              autoComplete="off"
              style={{
                display: "block",
                width: "100%",
                marginTop: "0.35rem",
                padding: "0.5rem 0.65rem",
                borderRadius: "8px",
                border: "1px solid #3f3f46",
                background: "#09090b",
                color: "#f4f4f5",
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
              }}
            />
          </label>
          <label
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.5rem",
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              color: "#fca5a5",
            }}
          >
            <input
              type="checkbox"
              checked={evacuate}
              onChange={(e) => setEvacuate(e.target.checked)}
            />
            Attempt USDC evacuation (NEXUS_KILL_SWITCH_EVACUATE + web3 + safe wallet)
          </label>
        </>
      )}

      <button
        type="button"
        onClick={onPrimaryClick}
        disabled={loading}
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: armed ? "0.82rem" : "0.78rem",
          fontWeight: 900,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          color: loading ? "#52525b" : "#fff",
          background: armed
            ? `linear-gradient(180deg, ${BLOOD} 0%, #7f1d1d 100%)`
            : `linear-gradient(180deg, #dc2626 0%, ${BLOOD} 100%)`,
          border: "none",
          borderRadius: "12px",
          padding: "1rem 1rem",
          cursor: loading ? "not-allowed" : "pointer",
          boxShadow: stealth || loading ? "none" : `0 0 28px ${BLOOD}55`,
          animation: armed && !loading && !stealth ? "emks-pulse 1s ease-in-out infinite" : "none",
        }}
      >
        {loading ? "EXECUTING…" : armed ? "CONFIRM TERMINATE NEXUS" : "ARM KILL-SWITCH"}
      </button>

      {armed && !loading && (
        <button
          type="button"
          onClick={() => {
            setArmed(false);
            setPhrase("");
            setMessage(null);
          }}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.62rem",
            color: "#71717a",
            background: "transparent",
            border: "none",
            cursor: "pointer",
          }}
        >
          Cancel
        </button>
      )}

      {message && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: message.startsWith("Engaged") ? "#86efac" : "#fca5a5",
            textAlign: "center",
          }}
        >
          {message}
        </div>
      )}

      <style>{`
        @keyframes emks-pulse {
          0%, 100% { opacity: 1; filter: brightness(1); }
          50% { opacity: 0.88; filter: brightness(1.12); }
        }
      `}</style>
    </div>
  );
}
