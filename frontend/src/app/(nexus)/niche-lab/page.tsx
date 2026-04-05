"use client";

import dynamic from "next/dynamic";
import PageTransition from "@/components/PageTransition";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

const ProjectIncubatorHub = dynamic(() => import("@/pages/Incubator"), {
  ssr: false,
  loading: () => <p style={{ padding: "1rem", opacity: 0.7 }}>Loading niche lab…</p>,
});

export default function NicheLabPage() {
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
          🧪 {t("niche_lab")}
        </h1>
        <p
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: tokens.textMuted,
            marginBottom: "1.25rem",
          }}
        >
          {t("nav_desc_niche_lab")}
        </p>
        <ProjectIncubatorHub />
      </div>
    </PageTransition>
  );
}
