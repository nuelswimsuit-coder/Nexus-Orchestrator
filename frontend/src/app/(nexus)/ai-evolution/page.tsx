"use client";

import PageTransition from "@/components/PageTransition";
import AIEvolution from "@/components/AIEvolution";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

export default function AiEvolutionPage() {
  const { t } = useI18n();
  const { tokens } = useTheme();

  return (
    <PageTransition>
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "2rem 1.5rem" }}>
        <h1
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "1.05rem",
            fontWeight: 700,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            color: tokens.textPrimary,
            marginBottom: "0.35rem",
          }}
        >
          🧠 {t("ai_dev_timeline")}
        </h1>
        <p
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: tokens.textMuted,
            marginBottom: "1.25rem",
          }}
        >
          {t("nav_desc_ai_dev_timeline")}
        </p>
        <AIEvolution />
      </div>
    </PageTransition>
  );
}
