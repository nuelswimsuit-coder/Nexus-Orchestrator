"use client";

import { useState } from "react";
import { useSWRConfig } from "swr";
import {
  forceRunTask,
  forceScrape,
  swrFetcher,
  type StuckStateResponse,
} from "@/lib/api";
import { useNexus } from "@/lib/nexus-context";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { useStealth } from "@/lib/stealth";

const LIME = "#b8ff3d";

export default function SidebarCommandStrip({ expanded }: { expanded: boolean }) {
  const { t, isRTL } = useI18n();
  const { tokens, isHighContrast } = useTheme();
  const { stealth } = useStealth();
  const { mutate: globalMutate } = useSWRConfig();
  const { smartSleep, setSmartSleep, autoApproveHitl, setAutoApproveHitl } = useNexus();
  const [busy, setBusy] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);

  function flash(msg: string) {
    setToast(msg);
    setTimeout(() => setToast(null), 4200);
  }

  async function onAggressive() {
    setBusy("agg");
    try {
      const stuck = await swrFetcher<StuckStateResponse>("/api/business/stuck-state");
      if (stuck.stuck && stuck.task_type) {
        await forceRunTask(stuck.task_type, stuck.task_params ?? {});
        flash("אופטימיזציה: נשלח force-run למשימה תקועה");
      } else {
        await forceScrape([], true);
        flash("אופטימיזציה: נשלחה גרידה אגרסיבית");
      }
      await globalMutate("/api/business/stuck-state");
    } catch (e) {
      flash(e instanceof Error ? e.message : "בקשה נכשלה");
    } finally {
      setBusy(null);
    }
  }

  async function onSelfRepair() {
    setBusy("fix");
    try {
      await swrFetcher<unknown>("/api/cluster/health");
      await globalMutate("/api/cluster/status");
      await globalMutate("/api/cluster/health");
      flash("תיקון עצמי: סריקת בריאות + רענון צי");
    } catch (e) {
      flash(e instanceof Error ? e.message : "סריקה נכשלה");
    } finally {
      setBusy(null);
    }
  }

  function onSmartSleep() {
    const next = !smartSleep;
    setSmartSleep(next);
    if (next) {
      setAutoApproveHitl(true);
      flash("מצב שינה חכם: מעמעם UI + אישור אוטומטי ל־HITL");
    } else {
      setAutoApproveHitl(false);
      flash("מצב שינה כבוי — אישור ידני חזר");
    }
  }

  const actions = [
    {
      id: "agg",
      emoji: "🚀",
      label: "אופטימיזציה אגרסיבית",
      sub: "Force-run / גרידה",
      onClick: onAggressive,
    },
    {
      id: "fix",
      emoji: "🛠️",
      label: "תיקון עצמי יזום",
      sub: "Health + רענון",
      onClick: onSelfRepair,
    },
    {
      id: "sleep",
      emoji: "🌙",
      label: "מצב שינה חכם",
      sub: smartSleep ? "פעיל" : "כבוי",
      onClick: onSmartSleep,
    },
  ] as const;

  if (!expanded) {
    return toast ? (
      <div
        style={{
          padding: "4px 8px",
          fontSize: "0.5rem",
          color: tokens.warning,
          fontFamily: "var(--font-mono)",
          textAlign: "center",
          borderTop: `1px solid ${tokens.borderFaint}`,
        }}
      >
        …
      </div>
    ) : null;
  }

  return (
    <div
      style={{
        borderTop: `1px solid ${tokens.borderFaint}`,
        borderBottom: `1px solid ${tokens.borderFaint}`,
        padding: "0.5rem 8px 0.65rem",
        display: "flex",
        flexDirection: "column",
        gap: 6,
        background: isHighContrast ? tokens.surface1 : "rgba(0,20,40,0.35)",
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.52rem",
          fontWeight: 700,
          letterSpacing: "0.1em",
          color: tokens.textMuted,
          textTransform: "uppercase",
          textAlign: isRTL ? "right" : "left",
        }}
      >
        {t("sidebar_command_bar")}
      </div>
      {actions.map((a) => (
        <button
          key={a.id}
          type="button"
          disabled={busy !== null}
          onClick={() => void a.onClick()}
          title={a.sub}
          style={{
            display: "flex",
            flexDirection: isRTL ? "row-reverse" : "row",
            alignItems: "center",
            gap: 8,
            width: "100%",
            padding: "6px 8px",
            borderRadius: 8,
            border: `1px solid ${stealth ? tokens.borderSubtle : "rgba(0,229,255,0.2)"}`,
            background:
              a.id === "sleep" && smartSleep
                ? "rgba(184,255,61,0.12)"
                : isHighContrast
                  ? tokens.surface2
                  : "rgba(15,23,42,0.75)",
            cursor: busy ? "wait" : "pointer",
            textAlign: isRTL ? "right" : "left",
            opacity: busy && busy !== a.id ? 0.45 : 1,
          }}
        >
          <span style={{ fontSize: "1rem", flexShrink: 0 }}>{a.emoji}</span>
          <span
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.72rem",
              fontWeight: 600,
              color: tokens.textPrimary,
              lineHeight: 1.25,
            }}
          >
            {a.label}
          </span>
        </button>
      ))}
      {autoApproveHitl && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.5rem",
            color: LIME,
            textAlign: "center",
          }}
        >
          HITL auto ✓
        </div>
      )}
      {toast && (
        <div
          dir="rtl"
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.55rem",
            color: tokens.warning,
            lineHeight: 1.35,
            textAlign: "center",
          }}
        >
          {toast}
        </div>
      )}
    </div>
  );
}
