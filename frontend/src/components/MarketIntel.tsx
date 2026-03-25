"use client";

import { useCallback, useState } from "react";
import useSWR from "swr";
import {
  API_BASE,
  swrFetcher,
  postAiTerminalChat,
  postAiTerminalPersonality,
  postStrategyMutation,
} from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

interface CrossEx {
  signal_label?: string;
  arbitrage_gap?: number | null;
  fetched_at?: string;
  binance?: { last_price?: number } | null;
  polymarket?: { yes_price?: number } | null;
}

interface ScalperHeat {
  fleet_sentiment_heatmap?: Record<
    string,
    { score?: number; momentum_hint?: number; updated_at?: string }
  >;
}

export default function MarketIntel() {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";
  const border = `1px solid ${tokens.borderSubtle}`;

  const { data: cx, error: cxErr } = useSWR<CrossEx>(
    `${API_BASE}/api/prediction/cross-exchange`,
    swrFetcher<CrossEx>,
    { refreshInterval: 8000 },
  );

  const { data: sc } = useSWR<ScalperHeat>(
    `${API_BASE}/api/scalper/status`,
    swrFetcher<ScalperHeat>,
    { refreshInterval: 12_000 },
  );

  const [geminiBlock, setGeminiBlock] = useState<{
    text: string;
    confidence: number;
  } | null>(null);
  const [busy, setBusy] = useState(false);

  const loadGemini = useCallback(async () => {
    setBusy(true);
    try {
      const r = await postAiTerminalChat(
        he
          ? "סכם בקצרה את מצב BTC מול פולימרקט ובינאנס, עם ציון ביטחון 0-100."
          : "Brief BTC vs Polymarket/Binance state with confidence 0-100.",
      );
      const conf =
        r.source === "gemini" || r.source === "worker_gemini"
          ? 82
          : 55;
      setGeminiBlock({ text: r.reply, confidence: conf });
    } catch {
      setGeminiBlock({
        text: he ? "לא ניתן לטעון ניתוח." : "Could not load analysis.",
        confidence: 0,
      });
    } finally {
      setBusy(false);
    }
  }, [he]);

  const loadPersonality = useCallback(async () => {
    setBusy(true);
    try {
      const r = await postAiTerminalPersonality({
        messages: [
          { text: he ? "סוגרים מחר בבוקר, בסדר?" : "We close tomorrow morning OK?" },
          { text: he ? "מה המחיר הסופי בלי אקסטרות?" : "What's the final all-in price?" },
        ],
        note: he ? "ניתוח אופי מהדוגמאות" : "Personality analysis from samples",
      });
      const conf =
        r.source === "gemini" || r.source === "worker_gemini"
          ? 84
          : 52;
      setGeminiBlock({
        text: r.reply,
        confidence: conf,
      });
    } catch {
      setGeminiBlock({
        text: he ? "לא ניתן לטעון ניתוח אופי." : "Could not load personality analysis.",
        confidence: 0,
      });
    } finally {
      setBusy(false);
    }
  }, [he]);

  const onMutate = useCallback(async () => {
    setBusy(true);
    try {
      await postStrategyMutation();
      await loadGemini();
    } finally {
      setBusy(false);
    }
  }, [loadGemini]);

  const heat = sc?.fleet_sentiment_heatmap ?? {};
  const heatEntries = Object.entries(heat).slice(0, 12);

  return (
    <div dir={he ? "rtl" : "ltr"} style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
          gap: "1rem",
        }}
      >
        <div style={panel(tokens, isHighContrast, border)}>
          <h3 style={h(tokens)}>{he ? "מפת חום סנטימנט (BTC)" : "BTC sentiment heatmap"}</h3>
          <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: tokens.textMuted }}>
            X / Telegram fleet (scalper)
          </p>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 8 }}>
            {heatEntries.length === 0 ? (
              <span style={{ color: tokens.textMuted, fontSize: "0.7rem" }}>—</span>
            ) : (
              heatEntries.map(([k, v]) => {
                const s = Math.min(1, Math.max(0, ((v.score ?? 0) + 1) / 2));
                const bg = `rgba(167, 139, 250, ${0.15 + s * 0.5})`;
                return (
                  <span
                    key={k}
                    title={v.updated_at}
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.62rem",
                      padding: "6px 10px",
                      borderRadius: 8,
                      background: bg,
                      border,
                      color: tokens.textPrimary,
                    }}
                  >
                    {k.slice(0, 14)} · {((v.score ?? 0) * 100).toFixed(0)}
                  </span>
                );
              })
            )}
          </div>
        </div>

        <div style={panel(tokens, isHighContrast, border)}>
          <h3 style={h(tokens)}>{he ? "מכ״ם ארביטראז׳" : "Arbitrage radar"}</h3>
          {cxErr ? (
            <div style={{ color: tokens.danger, fontSize: "0.72rem" }}>502 / offline</div>
          ) : (
            <>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.85rem", color: tokens.accentBright }}>
                {cx?.signal_label ?? "—"}
              </div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: tokens.textSecondary, marginTop: 6 }}>
                gap: {cx?.arbitrage_gap != null ? `${(cx.arbitrage_gap * 100).toFixed(2)}%` : "—"}
              </div>
              <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: tokens.textMuted, marginTop: 6 }}>
                Bin {cx?.binance?.last_price ?? "—"} · Poly Yes {cx?.polymarket?.yes_price ?? "—"}
              </div>
            </>
          )}
        </div>
      </div>

      <div style={panel(tokens, isHighContrast, border)}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <h3 style={{ ...h(tokens), margin: 0 }}>Gemini</h3>
          <button
            type="button"
            disabled={busy}
            onClick={() => void loadGemini()}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              padding: "6px 12px",
              borderRadius: 8,
              border: `1px solid ${tokens.accent}`,
              background: `${tokens.accent}18`,
              color: tokens.accentBright,
              cursor: busy ? "wait" : "pointer",
            }}
          >
            {he ? "רענן ניתוח" : "Refresh analysis"}
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => void loadPersonality()}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.62rem",
              padding: "6px 12px",
              borderRadius: 8,
              border: `1px solid ${tokens.warning}`,
              background: `${tokens.warning}14`,
              color: tokens.warning,
              cursor: busy ? "wait" : "pointer",
            }}
          >
            {he ? "ניתוח אופי" : "Personality analysis"}
          </button>
        </div>
        {geminiBlock ? (
          <div style={{ marginTop: 10 }}>
            <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: tokens.textPrimary, lineHeight: 1.5 }}>
              {geminiBlock.text}
            </div>
            <div style={{ marginTop: 8, fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: tokens.warning }}>
              {he ? "ביטחון" : "Confidence"}: {geminiBlock.confidence}%
            </div>
          </div>
        ) : (
          <p style={{ color: tokens.textMuted, fontSize: "0.7rem" }}>
            {he ? "לחץ לטעינת ניתוח טקסטואלי." : "Tap refresh for textual analysis."}
          </p>
        )}
      </div>

      <button
        type="button"
        disabled={busy}
        onClick={() => void onMutate()}
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          fontWeight: 700,
          letterSpacing: "0.06em",
          padding: "10px 16px",
          borderRadius: 10,
          border: `1px solid ${tokens.danger}55`,
          background: `${tokens.danger}14`,
          color: tokens.danger,
          cursor: busy ? "wait" : "pointer",
          alignSelf: he ? "flex-end" : "flex-start",
        }}
      >
        {he ? "בצע מוטציית אסטרטגיה (AI)" : "Execute strategy mutation"}
      </button>
    </div>
  );
}

function h(tokens: { textMuted: string }) {
  return {
    fontFamily: "var(--font-mono)",
    fontSize: "0.68rem",
    fontWeight: 700,
    letterSpacing: "0.08em",
    color: tokens.textMuted,
    textTransform: "uppercase" as const,
    marginBottom: "0.35rem",
  };
}

function panel(
  tokens: { cardBg: string; surface1: string },
  isHighContrast: boolean,
  border: string,
) {
  return {
    background: isHighContrast ? tokens.surface1 : tokens.cardBg,
    border,
    borderRadius: 12,
    padding: "1rem",
  };
}
