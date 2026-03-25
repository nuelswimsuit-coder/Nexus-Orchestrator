"use client";

import PageTransition from "@/components/PageTransition";
import Vault from "@/components/Vault";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

export default function VaultPage() {
  const { t } = useI18n();
  const { tokens } = useTheme();

  return (
    <PageTransition>
      <div style={{ maxWidth: 1000, margin: "0 auto", padding: "2rem 1.5rem" }}>
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
          🔐 {t("vault")}
        </h1>
        <p
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: tokens.textMuted,
            marginBottom: "1.25rem",
          }}
        >
          {t("nav_desc_vault")}
        </p>
        <Vault />
      </div>
    </PageTransition>
  );
}
