"use client";

import { useCallback, useState } from "react";
import { postAiTerminalChat } from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

export default function AITerminal({ embedded }: { embedded?: boolean }) {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";
  const [input, setInput] = useState("");
  const [lines, setLines] = useState<{ role: "you" | "ai"; text: string }[]>([]);
  const [thinking, setThinking] = useState<string[] | null>(null);
  const [loading, setLoading] = useState(false);

  const run = useCallback(async () => {
    const msg = input.trim();
    if (!msg) return;
    setInput("");
    setLines((L) => [...L, { role: "you", text: msg }]);
    setLoading(true);
    setThinking(null);
    try {
      const r = await postAiTerminalChat(msg);
      setThinking(r.thinking_steps);
      setLines((L) => [...L, { role: "ai", text: r.reply }]);
    } catch {
      setLines((L) => [
        ...L,
        { role: "ai", text: he ? "שגיאת רשת." : "Network error." },
      ]);
    } finally {
      setLoading(false);
      setTimeout(() => setThinking(null), 4500);
    }
  }, [he, input]);

  const border = `1px solid ${tokens.borderSubtle}`;
  const bg = isHighContrast ? tokens.surface1 : tokens.terminalBg;

  return (
    <div dir={he ? "rtl" : "ltr"}>
      {thinking && thinking.length > 0 && (
        <div
          style={{
            position: embedded ? "relative" : "fixed",
            ...(embedded ? {} : { bottom: 24, [he ? "left" : "right"]: 24, zIndex: 60, maxWidth: 320 }),
            marginBottom: embedded ? "0.75rem" : 0,
            background: `${tokens.surface2}ee`,
            border: `1px solid ${tokens.accent}55`,
            borderRadius: 10,
            padding: "0.75rem",
            boxShadow: `0 0 24px ${tokens.accentDim}`,
          }}
        >
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.58rem", color: tokens.accentBright, marginBottom: 6 }}>
            {he ? "מעבד חשיבה…" : "Thinking…"}
          </div>
          <ol style={{ margin: 0, paddingInlineStart: "1.1rem", fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: tokens.textSecondary }}>
            {thinking.map((s, i) => (
              <li key={i}>{s}</li>
            ))}
          </ol>
        </div>
      )}

      <div
        style={{
          background: bg,
          border,
          borderRadius: 10,
          padding: embedded ? "0.65rem" : "1rem",
          fontFamily: "var(--font-mono)",
          minHeight: embedded ? 140 : 200,
          maxHeight: embedded ? 220 : 360,
          overflowY: "auto",
          fontSize: "0.72rem",
          color: tokens.textPrimary,
        }}
      >
        {lines.length === 0 && (
          <div style={{ color: tokens.textMuted }}>
            {he
              ? 'דוגמאות: "יעקב חתן: הגדל חשיפה ב־10%", "תקן קובץ deployer", "הצג דוח שבועי"'
              : 'Examples: "Increase exposure 10%", "Fix deployer file", "Show weekly report"'}
          </div>
        )}
        {lines.map((ln, i) => (
          <div key={i} style={{ marginBottom: "0.5rem", whiteSpace: "pre-wrap" }}>
            <span style={{ color: ln.role === "you" ? tokens.accent : tokens.success }}>
              {ln.role === "you" ? (he ? "אתה" : "you") : "AI"} ›{" "}
            </span>
            {ln.text}
          </div>
        ))}
        {loading && <div style={{ color: tokens.warning }}>…</div>}
      </div>

      <div style={{ display: "flex", gap: 8, marginTop: 8, flexDirection: he ? "row-reverse" : "row" }}>
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && void run()}
          placeholder={he ? "פקודה…" : "Command…"}
          style={{
            flex: 1,
            background: tokens.surface2,
            border,
            borderRadius: 8,
            padding: "8px 10px",
            color: tokens.textPrimary,
            fontFamily: "var(--font-mono)",
            fontSize: "0.75rem",
          }}
        />
        <button
          type="button"
          disabled={loading}
          onClick={() => void run()}
          style={{
            padding: "8px 14px",
            borderRadius: 8,
            border: `1px solid ${tokens.accent}`,
            background: `${tokens.accent}22`,
            color: tokens.accentBright,
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            cursor: loading ? "wait" : "pointer",
          }}
        >
          RUN
        </button>
      </div>
    </div>
  );
}
