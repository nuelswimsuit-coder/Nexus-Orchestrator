"use client";

/**
 * TeleFix Modules Page — Phase 5: Multi-Project Integration Hub
 *
 * Displays all external desktop projects as live TeleFix modules with
 * real-time status, metrics, and one-click deployment controls.
 */

import { useState } from "react";
import useSWR from "swr";
import { swrFetcher, triggerProjectScan } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";

// ── Types ─────────────────────────────────────────────────────────────────────

interface ModuleInfo {
  name: string;
  path: string;
  exists: boolean;
  status: string;
  category: string;
  priority: number;
  icon: string;
  description: string;
  live_stats: Record<string, unknown>;
}

interface ModulesResponse {
  modules: Record<string, ModuleInfo>;
  total_count: number;
  running_count: number;
  available_count: number;
  last_scan: string;
}

// ── Category colours ──────────────────────────────────────────────────────────

const CATEGORY_COLORS: Record<string, string> = {
  communication: "#06b6d4",
  infrastructure: "#8b5cf6",
  financial:      "#22c55e",
  trading:        "#f59e0b",
  business:       "#f472b6",
};

// ── Module Card ───────────────────────────────────────────────────────────────

function ModuleCard({
  moduleId,
  module,
}: {
  moduleId: string;
  module: ModuleInfo;
}) {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<string | null>(null);

  const isRunning = module.status === "running";
  const isNotFound = !module.exists;
  const categoryColor = CATEGORY_COLORS[module.category] ?? "#64748b";
  const statusColor = isNotFound
    ? "#ef4444"
    : isRunning
    ? "#22c55e"
    : "#64748b";

  const handleAction = async (action: string) => {
    if (actionLoading) return;
    setActionLoading(action);
    setLastResult(null);
    try {
      const res = await fetch(`/api/modules/${moduleId}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action }),
      });
      const data = await res.json();
      setLastResult(data.message ?? data.status);
    } catch {
      setLastResult("Error");
    } finally {
      setActionLoading(null);
    }
  };

  const actionButtons = [
    { action: "start",   label: `\u25B6\uFE0F ${t("start")}`,   show: !isRunning && module.exists, color: "#22c55e" },
    { action: "stop",    label: `\u23F9\uFE0F ${t("stop")}`,    show: isRunning,                   color: "#ef4444" },
    { action: "restart", label: `\u{1F504} ${t("restart")}`,    show: isRunning,                   color: "#f59e0b" },
    { action: "sync",    label: `\u{1F680} ${t("sync")}`,       show: module.exists,               color: "#a855f7" },
    { action: "scan",    label: `\u{1F50D} ${t("modules.scan")}`, show: true,                      color: "#06b6d4" },
  ].filter((b) => b.show);

  return (
    <div
      style={{
        background: "linear-gradient(160deg, #0a0f1e, #080d18)",
        border: `1px solid ${stealth ? "#1e293b" : statusColor}33`,
        borderRadius: "12px",
        padding: "1.25rem",
        display: "flex",
        flexDirection: "column",
        gap: "0.75rem",
        transition: "box-shadow 0.3s",
        boxShadow: !stealth && isRunning ? `0 0 20px ${statusColor}11` : "none",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          flexDirection: isRTL ? "row-reverse" : "row",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ fontSize: "1.3rem" }}>{module.icon}</span>
          <div>
            <h3
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.82rem",
                fontWeight: 700,
                color: stealth ? "#94a3b8" : "#f1f5f9",
                margin: 0,
                letterSpacing: "0.04em",
              }}
            >
              {module.name}
            </h3>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.58rem",
                color: stealth ? "#334155" : categoryColor,
                textTransform: "uppercase",
                letterSpacing: "0.06em",
              }}
            >
              {module.category}
            </span>
          </div>
        </div>
        <span
          style={{
            display: "flex",
            alignItems: "center",
            gap: "0.3rem",
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            fontWeight: 700,
            color: stealth ? "#475569" : statusColor,
            letterSpacing: "0.08em",
          }}
        >
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: stealth ? "#475569" : statusColor,
              display: "inline-block",
              boxShadow: stealth ? "none" : `0 0 6px ${statusColor}`,
              animation:
                !stealth && isRunning
                  ? "mc-pulse 2s infinite"
                  : "none",
            }}
          />
          {isNotFound
            ? t("modules.not_found")
            : isRunning
            ? t("running")
            : t("stopped")}
        </span>
      </div>

      {/* Description */}
      <p
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.65rem",
          color: stealth ? "#475569" : "#94a3b8",
          margin: 0,
        }}
      >
        {module.description}
      </p>

      {/* Live stats */}
      {Object.keys(module.live_stats).length > 0 && (
        <div
          style={{
            background: "#0f172a",
            border: "1px solid #1e293b",
            borderRadius: "8px",
            padding: "0.65rem",
          }}
        >
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.55rem",
              color: stealth ? "#334155" : categoryColor,
              letterSpacing: "0.08em",
              marginBottom: "0.4rem",
            }}
          >
            {t("modules.live_stats")}
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: "0.3rem",
            }}
          >
            {Object.entries(module.live_stats)
              .filter(([k]) => k !== "available" && k !== "path")
              .slice(0, 6)
              .map(([key, value]) => (
                <div key={key}>
                  <span
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.52rem",
                      color: "#475569",
                    }}
                  >
                    {key.replace(/_/g, " ")}
                  </span>
                  <span
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.68rem",
                      fontWeight: 700,
                      color: stealth ? "#94a3b8" : categoryColor,
                      display: "block",
                    }}
                  >
                    {typeof value === "object"
                      ? JSON.stringify(value)
                      : String(value)}
                  </span>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Action buttons */}
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
        {actionButtons.map(({ action, label, color }) => (
          <button
            key={action}
            onClick={() => handleAction(action)}
            disabled={!!actionLoading}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.65rem",
              fontWeight: 700,
              padding: "4px 11px",
              borderRadius: "6px",
              border: `1px solid ${color}66`,
              background:
                actionLoading === action ? `${color}22` : `${color}11`,
              color: actionLoading === action ? "#f1f5f9" : color,
              cursor: actionLoading ? "not-allowed" : "pointer",
              opacity: actionLoading && actionLoading !== action ? 0.4 : 1,
              transition: "all 0.2s",
            }}
          >
            {actionLoading === action ? "⏳" : label}
          </button>
        ))}
      </div>

      {/* Last result */}
      {lastResult && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.6rem",
            color: lastResult.toLowerCase().includes("error") ? "#ef4444" : "#22c55e",
            background: "#0f172a",
            borderRadius: "6px",
            padding: "0.4rem 0.6rem",
            border: "1px solid #1e293b",
          }}
        >
          {lastResult}
        </div>
      )}
    </div>
  );
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function ModulesPage() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const [scanLoading, setScanLoading] = useState(false);

  const { data, error, isLoading, mutate } = useSWR<ModulesResponse>(
    "/api/modules",
    swrFetcher<ModulesResponse>,
    { refreshInterval: 30_000 }
  );

  const handleRefresh = async () => {
    if (scanLoading) return;
    setScanLoading(true);
    try {
      await fetch("/api/modules/scan" , { method: "POST" });
      await mutate();
    } catch {
      // ignore
    } finally {
      setScanLoading(false);
    }
  };

  // Sort by priority then name
  const sortedModules = data
    ? Object.entries(data.modules).sort(
        ([, a], [, b]) => a.priority - b.priority || a.name.localeCompare(b.name)
      )
    : [];

  return (
    <PageTransition>
      <div
        style={{
          maxWidth: "1400px",
          margin: "0 auto",
          padding: "2rem 1.5rem",
        }}
      >
        {/* Page header */}
        <div
          style={{
            marginBottom: "2rem",
            display: "flex",
          alignItems: "flex-end",
          justifyContent: "space-between",
          flexDirection: isRTL ? "row-reverse" : "row",
        }}
        >
          <div>
            <h1
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "1.2rem",
                fontWeight: 700,
                letterSpacing: "0.1em",
                textTransform: "uppercase",
                color: stealth ? "#334155" : "#f1f5f9",
                margin: "0 0 0.25rem",
              }}
            >
              {t("modules.title")}
            </h1>
            <p
              style={{
                fontFamily: "var(--font-sans)",
                fontSize: "0.7rem",
                color: stealth ? "#334155" : "#475569",
                margin: 0,
              }}
            >
              External project catalog — monitoring, control &amp; deployment
            </p>
          </div>

          <button
            onClick={handleRefresh}
            disabled={scanLoading}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.7rem",
              fontWeight: 700,
              padding: "6px 14px",
              borderRadius: "7px",
              border: "1px solid #6366f166",
              background: "rgba(99,102,241,0.1)",
              color: "#a5b4fc",
              cursor: scanLoading ? "not-allowed" : "pointer",
              opacity: scanLoading ? 0.6 : 1,
            }}
          >
            {scanLoading
              ? `\u{1F504} ${t("modules.scanning")}`
              : `\u{1F504} ${t("modules.refresh")}`}
          </button>
        </div>

        {/* Stats bar */}
        {data && (
          <div
            style={{
              display: "flex",
              gap: "2rem",
              marginBottom: "2rem",
              padding: "1rem",
              background: "#0f172a",
              border: "1px solid #1e293b",
              borderRadius: "10px",
              flexWrap: "wrap",
            }}
          >
            {[
              { label: t("modules.modules_label"), value: data.total_count },
              { label: t("modules.available"),     value: data.available_count },
              { label: t("modules.running_label"), value: data.running_count },
              {
                label: t("modules.last_scan"),
                value: new Date(data.last_scan).toLocaleTimeString(),
              },
            ].map(({ label, value }) => (
              <div key={label} style={{ textAlign: "center" }}>
                <div
                  style={{
                    fontFamily: "var(--font-sans)",
                    fontSize: "0.58rem",
                    color: "#334155",
                    marginBottom: "0.25rem",
                  }}
                >
                  {label}
                </div>
                <div
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.82rem",
                    fontWeight: 700,
                    color: stealth ? "#94a3b8" : "#f1f5f9",
                  }}
                >
                  {value}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Loading */}
        {isLoading && !data && (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))",
              gap: "1.5rem",
            }}
          >
            {Array.from({ length: 5 }, (_, i) => (
              <div
                key={i}
                style={{
                  height: "260px",
                  background: "#0a0e1a",
                  border: "1px solid #1e293b",
                  borderRadius: "12px",
                  animation: "skeleton 1.5s ease-in-out infinite",
                }}
              />
            ))}
          </div>
        )}

        {/* Error */}
        {error && (
          <div
            style={{
              background: "rgba(239,68,68,0.08)",
              border: "1px solid #ef4444",
              borderRadius: "8px",
              padding: "1rem",
              color: "#ef4444",
              fontFamily: "var(--font-mono)",
              fontSize: "0.75rem",
            }}
          >
            {"\u26A0"} {t("modules.load_error")}{" "}
            {error.message}
          </div>
        )}

        {/* Module grid */}
        {data && (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(340px, 1fr))",
              gap: "1.5rem",
            }}
          >
            {sortedModules.map(([moduleId, module]) => (
              <ModuleCard key={moduleId} moduleId={moduleId} module={module} />
            ))}
          </div>
        )}
      </div>

      <style>{`
        @keyframes mc-pulse {
          0%, 100% { opacity: 1; }
          50%       { opacity: 0.4; }
        }
        @keyframes skeleton {
          0%, 100% { opacity: 0.6; }
          50%       { opacity: 0.3; }
        }
      `}</style>
    </PageTransition>
  );
}
