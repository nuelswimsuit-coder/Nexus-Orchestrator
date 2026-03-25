"use client";

/**
 * AI Development Timeline + autonomy rate chart (recharts).
 */

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

const TIMELINE: { ts: string; file: string; change: string }[] = [
  { ts: "2025-03-18T14:00:00Z", file: "deployer.py", change: "Local SSH bypass + os.replace sync" },
  { ts: "2025-03-17T09:30:00Z", file: "ssh_handler.py", change: "Loopback known_hosts skip" },
  { ts: "2025-03-15T22:10:00Z", file: "ProfitDashboard.tsx", change: "PnL table + ROI tiles" },
  { ts: "2025-03-14T11:00:00Z", file: "ai_terminal.py", change: "Gemini command bridge" },
  { ts: "2025-03-12T08:45:00Z", file: "sentinel.py", change: "Crash triage JSON contract" },
];

const AUTONOMY_WEEKS = [
  { week: "W1", pct: 62 },
  { week: "W2", pct: 68 },
  { week: "W3", pct: 74 },
  { week: "W4", pct: 79 },
  { week: "W5", pct: 84 },
  { week: "W6", pct: 88 },
];

export default function AIEvolution() {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";
  const border = `1px solid ${tokens.borderSubtle}`;

  return (
    <div dir={he ? "rtl" : "ltr"}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 320px), 1fr))",
          gap: "1.25rem",
          alignItems: "stretch",
        }}
      >
        <div
          style={{
            background: isHighContrast ? tokens.surface1 : tokens.cardBg,
            border,
            borderRadius: 12,
            padding: "1rem",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              fontWeight: 700,
              letterSpacing: "0.1em",
              color: tokens.textMuted,
              marginBottom: "0.75rem",
            }}
          >
            {he ? "ציר זמן פיתוח" : "Development timeline"}
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
            {TIMELINE.map((row) => (
              <div
                key={row.ts + row.file}
                style={{
                  display: "grid",
                  gridTemplateColumns: he ? "1fr 100px 1fr" : "100px 1fr 1fr",
                  gap: "0.5rem",
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.68rem",
                  padding: "0.5rem 0",
                  borderBottom: border,
                  color: tokens.textPrimary,
                }}
              >
                <span style={{ color: tokens.accentBright }}>
                  {new Date(row.ts).toLocaleString(he ? "he-IL" : undefined)}
                </span>
                <span style={{ color: tokens.accent }}>{row.file}</span>
                <span style={{ color: tokens.textSecondary }}>{row.change}</span>
              </div>
            ))}
          </div>
        </div>

        <div
          style={{
            background: isHighContrast ? tokens.surface1 : tokens.cardBg,
            border,
            borderRadius: 12,
            padding: "1rem",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              fontWeight: 700,
              letterSpacing: "0.1em",
              color: tokens.textMuted,
              marginBottom: "0.5rem",
            }}
          >
            {he ? "שיעור אוטונומיית AI (% תיקונים ללא אדם)" : "AI autonomy rate (% fixes w/o human)"}
          </div>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={AUTONOMY_WEEKS}>
              <CartesianGrid stroke={tokens.borderFaint} strokeDasharray="3 3" />
              <XAxis dataKey="week" tick={{ fill: tokens.textMuted, fontSize: 10 }} />
              <YAxis domain={[0, 100]} tick={{ fill: tokens.textMuted, fontSize: 10 }} />
              <Tooltip
                contentStyle={{
                  background: tokens.surface2,
                  border: border,
                  fontFamily: "var(--font-mono)",
                  fontSize: 11,
                }}
              />
              <Line
                type="monotone"
                dataKey="pct"
                stroke={tokens.accentBright}
                strokeWidth={2}
                dot={{ fill: tokens.accent }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
