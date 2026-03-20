"use client";

import { useState } from "react";
import useSWR from "swr";
import { swrFetcher, projectAction, triggerProjectScan } from "@/lib/api";
import type { ProjectsResponse, ProjectInfo } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import PageTransition from "@/components/PageTransition";

// ── Project Card ──────────────────────────────────────────────────────────────

function ProjectCard({ project }: { project: ProjectInfo }) {
  const { stealth } = useStealth();
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  const isRunning = project.status === "Running";
  const statusColor = isRunning ? "#22c55e" : project.status === "Stopped" ? "#94a3b8" : "#ef4444";

  const handleAction = async (action: string) => {
    if (actionLoading) return;
    setActionLoading(action);
    try {
      await projectAction(project.name, action);
      // Refresh the data after action
      setTimeout(() => window.location.reload(), 1000);
    } catch (err) {
      console.error(`Project ${action} failed:`, err);
    } finally {
      setActionLoading(null);
    }
  };

  const actionButtons = [
    { action: "start",   label: "▶️ Start",   show: !isRunning, color: "#22c55e" },
    { action: "stop",    label: "⏹️ Stop",    show: isRunning,  color: "#ef4444" },
    { action: "restart", label: "🔄 Restart", show: isRunning,  color: "#f59e0b" },
    { action: "sync",    label: "🚀 Sync",    show: true,       color: "#a855f7" },
  ];

  return (
    <div style={{
      background: "linear-gradient(160deg, #0a0e1a 0%, #080d18 100%)",
      border: "1px solid #1e293b",
      borderRadius: "12px",
      padding: "1.25rem",
      transition: "border-color 0.3s, box-shadow 0.3s",
      ...(isRunning && !stealth ? {
        borderColor: "#22c55e33",
        boxShadow: "0 0 20px #22c55e11"
      } : {})
    }}>
      
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "1rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{
            width: 8, height: 8, borderRadius: "50%", background: statusColor,
            display: "inline-block", flexShrink: 0,
            boxShadow: stealth ? "none" : `0 0 6px ${statusColor}`,
            animation: stealth ? "none" : isRunning ? "rgb-pulse 2s infinite" : "none",
          }} />
          <h3 style={{
            fontFamily: "var(--font-mono)", fontSize: "0.9rem", fontWeight: 700,
            color: stealth ? "#94a3b8" : "#f1f5f9", margin: 0,
            letterSpacing: "0.05em",
          }}>
            {project.name}
          </h3>
        </div>
        <span style={{
          fontFamily: "var(--font-mono)", fontSize: "0.65rem", 
          color: statusColor, fontWeight: 700, letterSpacing: "0.08em",
        }}>
          {project.status.toUpperCase()}
        </span>
      </div>

      {/* Metadata */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.75rem", marginBottom: "1rem" }}>
        <div>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155" }}>Language</span>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#94a3b8" }}>{project.language}</div>
        </div>
        <div>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155" }}>Size</span>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#94a3b8" }}>{project.size_mb} MB</div>
        </div>
        <div>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155" }}>Processes</span>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#94a3b8" }}>{project.running_processes.length || "None"}</div>
        </div>
        <div>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155" }}>Config</span>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#94a3b8" }}>{project.config_keys.length} keys</div>
        </div>
      </div>

      {/* Live Stats */}
      {Object.keys(project.live_stats).length > 0 && (
        <div style={{
          background: "#0f172a", border: "1px solid #1e293b", borderRadius: "8px",
          padding: "0.75rem", marginBottom: "1rem",
        }}>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#334155", marginBottom: "0.5rem" }}>
            Live Stats
          </div>
          {Object.entries(project.live_stats).map(([key, value]) => (
            <div key={key} style={{ display: "flex", justifyContent: "space-between", marginBottom: "0.25rem" }}>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", color: "#64748b" }}>{key}</span>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", color: "#94a3b8" }}>
                {typeof value === "object" ? JSON.stringify(value) : String(value)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Actions */}
      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
        {actionButtons.filter(btn => btn.show).map(({ action, label, color }) => (
          <button
            key={action}
            onClick={() => handleAction(action)}
            disabled={actionLoading !== null}
            style={{
              fontFamily: "var(--font-mono)", fontSize: "0.68rem", fontWeight: 700,
              padding: "4px 12px", borderRadius: "6px",
              border: `1px solid ${color}66`,
              background: actionLoading === action ? `${color}22` : `${color}11`,
              color: actionLoading === action ? "#f1f5f9" : color,
              cursor: actionLoading ? "not-allowed" : "pointer",
              transition: "all 0.2s",
              opacity: actionLoading && actionLoading !== action ? 0.5 : 1,
            }}
          >
            {actionLoading === action ? "⏳" : label}
          </button>
        ))}
      </div>

      {/* Stack details */}
      {project.stack.length > 0 && (
        <div style={{ marginTop: "0.75rem" }}>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155", marginBottom: "0.25rem" }}>
            Tech Stack
          </div>
          <div style={{ display: "flex", gap: "0.25rem", flexWrap: "wrap" }}>
            {project.stack.map((tech, i) => (
              <span key={i} style={{
                fontFamily: "var(--font-mono)", fontSize: "0.58rem",
                background: "#0f172a", color: "#64748b",
                padding: "2px 6px", borderRadius: "4px", border: "1px solid #1e293b",
              }}>
                {tech}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Project Hub Client ────────────────────────────────────────────────────────

export default function ProjectsClient() {
  const { stealth } = useStealth();
  const [scanLoading, setScanLoading] = useState(false);

  const { data, error, isLoading, mutate } = useSWR<ProjectsResponse>(
    "/api/projects", 
    swrFetcher<ProjectsResponse>,
    { refreshInterval: 30_000 }
  );

  const handleRefresh = async () => {
    if (scanLoading) return;
    setScanLoading(true);
    try {
      await triggerProjectScan();
      await mutate();
    } catch (err) {
      console.error("Project scan failed:", err);
    } finally {
      setScanLoading(false);
    }
  };

  return (
    <PageTransition>
      <div style={{ maxWidth: "1400px", margin: "0 auto", padding: "2rem 1.5rem" }}>
        
        {/* Header */}
        <div style={{ marginBottom: "2rem" }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "0.5rem" }}>
            <h1 style={{
              fontFamily: "var(--font-mono)", fontSize: "1.2rem", fontWeight: 700,
              letterSpacing: "0.1em", textTransform: "uppercase",
              color: stealth ? "#334155" : "#f1f5f9", margin: 0,
            }}>
              🏗️ Project Hub
            </h1>
            <button
              onClick={handleRefresh}
              disabled={scanLoading}
              style={{
                fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700,
                padding: "6px 14px", borderRadius: "7px",
                border: "1px solid #6366f166", background: "rgba(99,102,241,0.1)",
                color: "#a5b4fc", cursor: scanLoading ? "not-allowed" : "pointer",
                opacity: scanLoading ? 0.6 : 1, transition: "all 0.2s",
              }}
            >
              {scanLoading ? "🔄 Scanning..." : "🔄 Refresh"}
            </button>
          </div>
          <p style={{
            fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569", margin: 0,
          }}>
            Desktop project catalog — live process monitoring & deployment control
          </p>
        </div>

        {/* Stats bar */}
        {data && (
          <div style={{
            display: "flex", gap: "1.5rem", marginBottom: "2rem",
            padding: "1rem", background: "#0f172a", border: "1px solid #1e293b",
            borderRadius: "10px",
          }}>
            {[
              ["Projects", data.total_count],
              ["Running", data.running_count],
              ["Total Size", `${data.total_size_mb} MB`],
              ["Last Scan", new Date(data.last_scan).toLocaleTimeString()],
            ].map(([label, value]) => (
              <div key={label as string} style={{ textAlign: "center" }}>
                <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155", marginBottom: "0.25rem" }}>
                  {label}
                </div>
                <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.8rem", fontWeight: 700, color: "#94a3b8" }}>
                  {value}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Loading state */}
        {isLoading && !data && (
          <div style={{
            display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
            gap: "1.5rem",
          }}>
            {[...Array(5)].map((_, i) => (
              <div key={i} style={{
                height: "280px", background: "#0a0e1a", border: "1px solid #1e293b",
                borderRadius: "12px", animation: "pulse 2s infinite",
              }} />
            ))}
          </div>
        )}

        {/* Error state */}
        {error && (
          <div style={{
            background: "rgba(239,68,68,0.1)", border: "1px solid #ef4444",
            borderRadius: "8px", padding: "1rem", color: "#ef4444",
            fontFamily: "var(--font-mono)", fontSize: "0.75rem",
          }}>
            ⚠ Failed to load projects: {error.message}
          </div>
        )}

        {/* Project grid */}
        {data && (
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(360px, 1fr))",
            gap: "1.5rem",
          }}>
            {Object.values(data.projects).map(project => (
              <ProjectCard key={project.name} project={project} />
            ))}
          </div>
        )}

      </div>

      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }
        @keyframes rgb-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </PageTransition>
  );
}
