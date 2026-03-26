"use client";

import useSWR from "swr";
import { motion } from "framer-motion";
import { API_BASE, swrFetcher, type PowerProfileResponse } from "@/lib/api";

function formatEta(seconds: number | null | undefined): string {
  if (seconds == null || Number.isNaN(Number(seconds))) return "—";
  const s = Math.max(0, Math.floor(Number(seconds)));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m ${sec}s`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

const itemVariants = {
  hidden: { opacity: 0, y: 8 },
  visible: { opacity: 1, y: 0, transition: { duration: 0.4, ease: "easeOut" as const } },
};

export default function PowerProfileBar() {
  const { data, error } = useSWR<PowerProfileResponse>(
    `${API_BASE}/api/system/power-profile`,
    swrFetcher<PowerProfileResponse>,
    { refreshInterval: 15_000 },
  );

  if (error) {
    return null;
  }

  const label = data?.display_label ?? "MASTER: […]";
  const ov = (data?.override ?? "auto").toUpperCase().replace(/_/g, "-");
  const eta = formatEta(data?.seconds_until_shift);
  const poly = data?.poly5m_cycle_seconds;

  return (
    <motion.div
      variants={itemVariants}
      style={{
        background: "linear-gradient(100deg, rgba(8, 14, 28, 0.92) 0%, rgba(12, 20, 40, 0.88) 100%)",
        border: "1px solid rgba(0, 180, 255, 0.22)",
        borderRadius: "14px",
        padding: "0.85rem 1.25rem",
        display: "flex",
        flexWrap: "wrap",
        alignItems: "center",
        gap: "0.75rem 1.5rem",
        boxShadow: "0 0 24px rgba(0, 180, 255, 0.06)",
      }}
    >
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.78rem",
          fontWeight: 800,
          letterSpacing: "0.12em",
          color: "#7dd3fc",
          textTransform: "uppercase",
        }}
      >
        Power profile
      </span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.82rem", color: "#e2e8f0", fontWeight: 700 }}>
        {label}
      </span>
      <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#64748b" }}>
        override: <span style={{ color: "#94a3b8" }}>{ov}</span>
        {poly != null ? (
          <>
            {" "}
            · Poly5M cycle: <span style={{ color: "#94a3b8" }}>{poly}s</span>
          </>
        ) : null}
      </span>
      <span
        style={{
          marginLeft: "auto",
          fontFamily: "var(--font-mono)",
          fontSize: "0.68rem",
          color: "#38bdf8",
          letterSpacing: "0.06em",
        }}
      >
        Next shift ≈ {eta}
      </span>
    </motion.div>
  );
}
