"use client";

/**
 * Dynamic dashboard strip for the active project (NUEL vs Management Ahu).
 * Data comes from GET /api/v1/projects/active (dashboard_context) and cluster summary.
 */

import { motion } from "framer-motion";
import { useNexus } from "@/lib/nexus-context";
import { useI18n } from "@/lib/i18n";

function MetricCard({
  title,
  value,
  sub,
}: {
  title: string;
  value: string;
  sub?: string;
}) {
  return (
    <div
      style={{
        background: "rgba(5, 12, 28, 0.75)",
        border: "1px solid rgba(0, 180, 255, 0.2)",
        borderRadius: "12px",
        padding: "1rem 1.1rem",
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.62rem",
          fontWeight: 600,
          letterSpacing: "0.08em",
          color: "#6b8fab",
          textTransform: "uppercase",
          marginBottom: "0.35rem",
        }}
      >
        {title}
      </div>
      <div
        style={{
          fontFamily: "var(--font-sans)",
          fontSize: "1.25rem",
          fontWeight: 700,
          color: "#e8f2ff",
          lineHeight: 1.2,
        }}
      >
        {value}
      </div>
      {sub && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: "#475569",
            marginTop: "0.35rem",
          }}
        >
          {sub}
        </div>
      )}
    </div>
  );
}

export default function ProjectOverview() {
  const { activeProject, activeProjectLoading, cluster } = useNexus();
  const { t } = useI18n();
  const ptype = activeProject?.project_type ?? "generic";
  const ctx = activeProject?.dashboard_context ?? null;
  const workerCpu = cluster?.worker_cpu_avg_percent ?? 0;
  const telefix = cluster?.telefix_context_active ?? true;

  if (activeProjectLoading && !activeProject) {
    return (
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          color: "#64748b",
          padding: "0.5rem 0",
        }}
      >
        {t("loading")}
      </div>
    );
  }

  const shopify = ctx && typeof ctx === "object" && "shopify_sync" in ctx
    ? (ctx.shopify_sync as { status?: string; last_sync_at?: string | null })
    : null;
  const adSpend =
    ctx && typeof ctx === "object" && "ad_spend_usd" in ctx
      ? Number((ctx as { ad_spend_usd?: number }).ad_spend_usd ?? 0)
      : 0;
  const imgQ =
    ctx && typeof ctx === "object" && "image_gen_queue" in ctx
      ? Number((ctx as { image_gen_queue?: number }).image_gen_queue ?? 0)
      : 0;

  const doc =
    ctx && typeof ctx === "object" && "doc_analysis" in ctx
      ? (ctx.doc_analysis as { pending?: number; completed?: number; percent?: number })
      : null;
  const leads =
    ctx && typeof ctx === "object" && "lead_extraction" in ctx
      ? (ctx.lead_extraction as { last_run_at?: string | null; batch_size?: number })
      : null;
  const autoLog =
    ctx && typeof ctx === "object" && "automation_logs" in ctx
      ? (ctx.automation_logs as { tail_ref?: string })
      : null;

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35 }}
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "1rem",
      }}
    >
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "baseline",
          justifyContent: "space-between",
          gap: "0.75rem",
        }}
      >
        <div>
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              color: "#38bdf8",
              letterSpacing: "0.14em",
              textTransform: "uppercase",
            }}
          >
            {activeProject?.display_name ?? "—"}
          </div>
          <div
            style={{
              fontFamily: "var(--font-sans)",
              fontSize: "0.85rem",
              color: "#94a3b8",
              marginTop: "0.25rem",
            }}
          >
            {ptype === "ecommerce_swimwear"
              ? t("overview_nuel_sub")
              : ptype === "operations_legal"
                ? t("overview_ops_sub")
                : t("project_scope_hint")}
          </div>
        </div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            color: "#475569",
          }}
        >
          workers CPU ∅ {workerCpu.toFixed(1)}%
          {!telefix && (
            <span style={{ marginLeft: "0.75rem", color: "#f59e0b" }}>
              Telefix metrics hidden
            </span>
          )}
        </div>
      </div>

      {ptype === "ecommerce_swimwear" && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
            gap: "0.85rem",
          }}
        >
          <MetricCard
            title="Shopify sync"
            value={shopify?.status ?? "—"}
            sub={
              shopify?.last_sync_at
                ? `Last: ${shopify.last_sync_at}`
                : "Awaiting sync signal"
            }
          />
          <MetricCard
            title="Ad spend (USD)"
            value={adSpend.toLocaleString(undefined, { maximumFractionDigits: 0 })}
            sub="Context from dashboard DB"
          />
          <MetricCard
            title="Image generation queue"
            value={String(imgQ)}
            sub="Jobs waiting"
          />
        </div>
      )}

      {ptype === "operations_legal" && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
            gap: "0.85rem",
          }}
        >
          <MetricCard
            title="Document analysis"
            value={
              doc
                ? `${doc.percent ?? 0}%`
                : "—"
            }
            sub={
              doc
                ? `${doc.pending ?? 0} pending · ${doc.completed ?? 0} done`
                : "Progress from dashboard DB"
            }
          />
          <MetricCard
            title="Lead extraction"
            value={leads?.batch_size != null ? `${leads.batch_size} / batch` : "—"}
            sub={
              leads?.last_run_at
                ? `Last: ${leads.last_run_at}`
                : "OpenClaw / scraper pipelines"
            }
          />
          <MetricCard
            title="Automation logs"
            value="Live"
            sub={autoLog?.tail_ref ?? "nexus:agent:log"}
          />
        </div>
      )}

      {ptype !== "ecommerce_swimwear" && ptype !== "operations_legal" && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: "#64748b",
            border: "1px dashed #1e293b",
            borderRadius: "10px",
            padding: "1rem",
          }}
        >
          No dedicated template for project type &quot;{ptype}&quot;. Use the sidebar
          switcher to select NUEL or Management Ahu, or dispatch via nexus_core to
          initialize dashboard context.
        </div>
      )}
    </motion.div>
  );
}
