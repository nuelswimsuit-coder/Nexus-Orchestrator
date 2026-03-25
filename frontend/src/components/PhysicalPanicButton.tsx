"use client";

/**
 * 3D physical-style panic control — glow intensifies near “risk” (engine warning).
 */

import { useStealth } from "@/lib/stealth";

type Props = {
  isPanic: boolean;
  armed: boolean;
  loading: boolean;
  urgentGlow: boolean;
  onPrimary: () => void;
  onReset: () => void;
  onDisarm: () => void;
};

const RED = "#dc2626";
const RED_DEEP = "#7f1d1d";

export default function PhysicalPanicButton({
  isPanic,
  armed,
  loading,
  urgentGlow,
  onPrimary,
  onReset,
  onDisarm,
}: Props) {
  const { stealth } = useStealth();

  const glow =
    stealth
      ? "none"
      : isPanic
        ? `0 0 50px rgba(220,38,38,0.55), 0 0 100px rgba(220,38,38,0.2)`
        : urgentGlow || armed
          ? `0 0 36px rgba(248,113,113,0.65), 0 0 72px rgba(239,68,68,0.25)`
          : `0 0 24px rgba(220,38,38,0.25)`;

  return (
    <div
      style={{
        perspective: 520,
        width: "100%",
        padding: "0.35rem 0",
      }}
    >
      <div
        style={{
          transform: "rotateX(8deg)",
          transformStyle: "preserve-3d",
          transition: "transform 0.25s ease",
        }}
      >
        {!isPanic ? (
          <button
            type="button"
            onClick={onPrimary}
            disabled={loading}
            aria-pressed={armed}
            style={{
              position: "relative",
              width: "100%",
              minHeight: 76,
              borderRadius: "50% / 42%",
              border: `3px solid ${stealth ? "#334155" : armed ? "#fbbf24" : `${RED}aa`}`,
              background: stealth
                ? "linear-gradient(165deg, #1e293b, #0f172a)"
                : `radial-gradient(ellipse at 30% 25%, #fca5a5 0%, ${RED} 38%, ${RED_DEEP} 100%)`,
              boxShadow: stealth ? "none" : `${glow}, inset 0 -8px 18px rgba(0,0,0,0.45), inset 0 6px 14px rgba(255,255,255,0.18)`,
              cursor: loading ? "not-allowed" : "pointer",
              transform: armed ? "translateY(4px) scale(0.98)" : "translateY(0) scale(1)",
              transition: "box-shadow 0.35s, transform 0.12s ease-out, border-color 0.2s",
              animation:
                !stealth && (urgentGlow || armed) && !loading
                  ? "panic-phys-pulse 1.1s ease-in-out infinite"
                  : "none",
            }}
          >
            <span
              style={{
                position: "relative",
                zIndex: 1,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 4,
                fontFamily: "var(--font-sans)",
                fontWeight: 900,
                fontSize: "0.95rem",
                letterSpacing: "0.04em",
                color: stealth ? "#475569" : "#fff",
                textShadow: stealth ? "none" : "0 2px 8px rgba(0,0,0,0.5)",
              }}
            >
              עצירת חירום (Panic)
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.58rem",
                  fontWeight: 600,
                  opacity: 0.88,
                }}
              >
                {loading ? "מפעיל…" : armed ? "לחץ שוב לאישור" : "לחץ להפעלה"}
              </span>
            </span>
            {!stealth && (
              <span
                style={{
                  position: "absolute",
                  inset: "12% 18%",
                  borderRadius: "inherit",
                  background:
                    "linear-gradient(180deg, rgba(255,255,255,0.35) 0%, transparent 55%)",
                  pointerEvents: "none",
                  opacity: 0.55,
                }}
              />
            )}
          </button>
        ) : (
          <button
            type="button"
            onClick={onReset}
            disabled={loading}
            style={{
              width: "100%",
              minHeight: 64,
              borderRadius: 16,
              border: "2px solid #22c55e88",
              background: "linear-gradient(180deg, #166534, #14532d)",
              color: "#ecfccb",
              fontFamily: "var(--font-mono)",
              fontWeight: 800,
              fontSize: "0.72rem",
              letterSpacing: "0.08em",
              cursor: loading ? "not-allowed" : "pointer",
              boxShadow: stealth ? "none" : "0 0 28px rgba(34,197,94,0.35)",
            }}
          >
            {loading ? "מאפס…" : "✓ אפס מערכת / RESUME"}
          </button>
        )}
      </div>

      {armed && !loading && !isPanic && (
        <button
          type="button"
          onClick={onDisarm}
          style={{
            marginTop: "0.65rem",
            width: "100%",
            fontFamily: "var(--font-mono)",
            fontSize: "0.58rem",
            fontWeight: 600,
            color: "#64748b",
            background: "transparent",
            border: "none",
            cursor: "pointer",
          }}
        >
          ✕ ביטול
        </button>
      )}

      <style>{`
        @keyframes panic-phys-pulse {
          0%, 100% { filter: brightness(1); }
          50% { filter: brightness(1.18); }
        }
      `}</style>
    </div>
  );
}
