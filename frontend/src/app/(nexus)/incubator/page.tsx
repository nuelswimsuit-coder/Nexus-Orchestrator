"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  activateKillSwitch,
  approveProject,
  generateProject,
  getIncubatorProjects,
  getIncubatorState,
  getNiches,
  killProject,
  refreshNiches,
  setGodMode,
  type IncubatorProject,
  type IncubatorStateResponse,
  type NicheItem,
} from "@/lib/api";

// ── DNA Helix Animation ───────────────────────────────────────────────────────

function DnaHelix({ active }: { active: boolean }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animRef   = useRef<number>(0);
  const tRef      = useRef(0);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const W = canvas.width  = canvas.offsetWidth;
    const H = canvas.height = canvas.offsetHeight;
    const NODES = 18;
    const SPEED = active ? 0.025 : 0.008;

    const draw = () => {
      ctx.clearRect(0, 0, W, H);
      tRef.current += SPEED;
      const t = tRef.current;

      for (let i = 0; i < NODES; i++) {
        const y     = (i / (NODES - 1)) * H;
        const phase = (i / NODES) * Math.PI * 2;
        const x1    = W / 2 + Math.sin(t + phase) * (W * 0.28);
        const x2    = W / 2 - Math.sin(t + phase) * (W * 0.28);

        const hue1 = active ? (200 + i * 8) % 360 : 220;
        ctx.beginPath();
        ctx.arc(x1, y, 5, 0, Math.PI * 2);
        ctx.fillStyle = `hsla(${hue1}, 90%, 65%, 0.9)`;
        ctx.fill();

        const hue2 = active ? (280 + i * 8) % 360 : 260;
        ctx.beginPath();
        ctx.arc(x2, y, 5, 0, Math.PI * 2);
        ctx.fillStyle = `hsla(${hue2}, 90%, 65%, 0.9)`;
        ctx.fill();

        if (i < NODES - 1) {
          const y2  = ((i + 1) / (NODES - 1)) * H;
          const nx1 = W / 2 + Math.sin(t + ((i + 1) / NODES) * Math.PI * 2) * (W * 0.28);
          const nx2 = W / 2 - Math.sin(t + ((i + 1) / NODES) * Math.PI * 2) * (W * 0.28);
          ctx.beginPath(); ctx.moveTo(x1, y); ctx.lineTo(nx1, y2);
          ctx.strokeStyle = `hsla(${hue1}, 80%, 55%, 0.3)`; ctx.lineWidth = 1.5; ctx.stroke();
          ctx.beginPath(); ctx.moveTo(x2, y); ctx.lineTo(nx2, y2);
          ctx.strokeStyle = `hsla(${hue2}, 80%, 55%, 0.3)`; ctx.lineWidth = 1.5; ctx.stroke();
        }

        if (i % 2 === 0) {
          ctx.beginPath(); ctx.moveTo(x1, y); ctx.lineTo(x2, y);
          ctx.strokeStyle = `rgba(139, 92, 246, ${active ? 0.4 : 0.15})`;
          ctx.lineWidth = 1; ctx.stroke();
        }
      }
      animRef.current = requestAnimationFrame(draw);
    };

    animRef.current = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(animRef.current);
  }, [active]);

  return <canvas ref={canvasRef} style={{ width: "100%", height: "100%", display: "block" }} />;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ConfBar({ value, color = "#6366f1" }: { value: number; color?: string }) {
  return (
    <div style={{ background: "#0f172a", borderRadius: 4, height: 6, overflow: "hidden" }}>
      <motion.div
        initial={{ width: 0 }} animate={{ width: `${value}%` }}
        transition={{ duration: 0.8, ease: "easeOut" }}
        style={{ height: "100%", background: color, borderRadius: 4 }}
      />
    </div>
  );
}

const STATUS_COLORS: Record<string, string> = {
  live: "#22c55e", pending_review: "#f59e0b", paused: "#64748b", killed: "#ef4444",
};

function StatusBadge({ status }: { status: string }) {
  const color = STATUS_COLORS[status] ?? "#64748b";
  return (
    <span style={{
      fontSize: "0.65rem", fontFamily: "var(--font-mono)", fontWeight: 700,
      color, background: `${color}22`, border: `1px solid ${color}44`,
      borderRadius: 4, padding: "2px 7px", letterSpacing: "0.05em", textTransform: "uppercase",
    }}>
      {status.replace("_", " ")}
    </span>
  );
}

function NicheCard({ niche, rank, onGenerate, generating }: {
  niche: NicheItem; rank: number;
  onGenerate: (n: NicheItem) => void; generating: boolean;
}) {
  const rankColors = ["#f59e0b", "#94a3b8", "#b45309"];
  const rc = rankColors[rank] ?? "#6366f1";
  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }} animate={{ opacity: 1, y: 0 }}
      transition={{ delay: rank * 0.1 }}
      style={{
        background: "linear-gradient(135deg, #0f172a 0%, #0a0e1a 100%)",
        border: `1px solid ${rc}44`, borderRadius: 12, padding: "1rem 1.25rem", position: "relative",
      }}
    >
      <div style={{
        position: "absolute", top: 10, right: 12, fontSize: "1.4rem",
        fontWeight: 900, color: rc, opacity: 0.25, fontFamily: "var(--font-mono)",
      }}>#{rank + 1}</div>

      <div style={{ display: "flex", alignItems: "flex-start", gap: "0.75rem", marginBottom: "0.75rem" }}>
        <div style={{
          width: 36, height: 36, borderRadius: 8, background: `${rc}22`,
          border: `1px solid ${rc}44`, display: "flex", alignItems: "center",
          justifyContent: "center", fontSize: "1.1rem", flexShrink: 0,
        }}>
          {niche.source === "crypto" ? "₿" : niche.source === "trends" ? "📈" : "📰"}
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.82rem", fontWeight: 700, color: "#f1f5f9", marginBottom: 2 }}>
            {niche.name}
          </div>
          <div style={{ fontSize: "0.65rem", color: "#475569", fontFamily: "var(--font-mono)" }}>
            {niche.source} · {niche.confidence}% conf
          </div>
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: "0.4rem", marginBottom: "0.75rem" }}>
        {[
          { label: "Volume", value: niche.volume_score, color: "#3b82f6" },
          { label: "Velocity", value: niche.velocity_score, color: "#10b981" },
          { label: "Monetisation", value: niche.monetisation_score, color: "#f59e0b" },
        ].map(({ label, value, color }) => (
          <div key={label}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 2 }}>
              <span style={{ fontSize: "0.6rem", color: "#475569", fontFamily: "var(--font-mono)" }}>{label}</span>
              <span style={{ fontSize: "0.6rem", color: "#64748b", fontFamily: "var(--font-mono)" }}>{value.toFixed(0)}</span>
            </div>
            <ConfBar value={value} color={color} />
          </div>
        ))}
      </div>

      <div style={{ fontSize: "0.65rem", color: "#6366f1", fontFamily: "var(--font-mono)", marginBottom: "0.75rem", fontStyle: "italic" }}>
        {niche.roi_estimate}
      </div>

      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: "0.75rem" }}>
        {niche.keywords.slice(0, 5).map(kw => (
          <span key={kw} style={{ fontSize: "0.58rem", fontFamily: "var(--font-mono)", background: "#1e293b", color: "#64748b", borderRadius: 4, padding: "1px 6px" }}>
            {kw}
          </span>
        ))}
      </div>

      <button
        onClick={() => onGenerate(niche)}
        disabled={generating}
        style={{
          width: "100%", padding: "0.45rem",
          background: generating ? "#1e293b" : `${rc}22`,
          border: `1px solid ${generating ? "#1e293b" : rc}`,
          borderRadius: 6, color: generating ? "#475569" : rc,
          fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700,
          cursor: generating ? "not-allowed" : "pointer", transition: "all 0.15s",
        }}
      >
        {generating ? "⏳ GENERATING..." : "🧬 GENERATE PROJECT"}
      </button>
    </motion.div>
  );
}

function ProjectRow({ project, onApprove, onKill }: {
  project: IncubatorProject;
  onApprove: (id: string) => void;
  onKill: (id: string) => void;
}) {
  const ageStr = project.age_hours < 24
    ? `${project.age_hours.toFixed(1)}h`
    : `${(project.age_hours / 24).toFixed(1)}d`;

  return (
    <motion.div
      initial={{ opacity: 0, x: -12 }} animate={{ opacity: 1, x: 0 }}
      style={{
        display: "grid", gridTemplateColumns: "1fr 130px 70px 55px 55px 140px",
        gap: "0.75rem", alignItems: "center",
        padding: "0.7rem 1rem", background: "#080d18",
        borderRadius: 8, border: "1px solid #0f172a", marginBottom: 6,
      }}
    >
      <div>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", fontWeight: 700, color: "#f1f5f9" }}>
          {project.name}
        </div>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155" }}>
          {project.niche} · Gen {project.generation}
          {project.god_mode_deployed && <span style={{ color: "#ef4444", marginLeft: 6 }}>⚡ GOD</span>}
        </div>
      </div>
      <StatusBadge status={project.status} />
      <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#64748b", textAlign: "center" }}>{ageStr}</div>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#6366f1", textAlign: "center" }}>{project.confidence_at_birth}%</div>
      <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#475569", textAlign: "center" }}>Gen {project.generation}</div>
      <div style={{ display: "flex", gap: 4 }}>
        {project.status === "pending_review" && (
          <button onClick={() => onApprove(project.project_id)} style={{
            padding: "3px 8px", fontSize: "0.6rem", fontFamily: "var(--font-mono)",
            background: "#22c55e22", border: "1px solid #22c55e44", borderRadius: 4,
            color: "#22c55e", cursor: "pointer", fontWeight: 700,
          }}>✓ OK</button>
        )}
        {project.status !== "killed" && (
          <button onClick={() => onKill(project.project_id)} style={{
            padding: "3px 8px", fontSize: "0.6rem", fontFamily: "var(--font-mono)",
            background: "#ef444422", border: "1px solid #ef444444", borderRadius: 4,
            color: "#ef4444", cursor: "pointer", fontWeight: 700,
          }}>✕ KILL</button>
        )}
      </div>
    </motion.div>
  );
}

function GodModeToggle({ enabled, onToggle, loading }: {
  enabled: boolean; onToggle: (v: boolean) => void; loading: boolean;
}) {
  return (
    <motion.div
      animate={{ boxShadow: enabled ? "0 0 32px #ef444466, 0 0 64px #ef444422" : "0 0 8px #00000044" }}
      style={{
        background: enabled ? "linear-gradient(135deg, #1a0505, #0f0000)" : "linear-gradient(135deg, #0f172a, #080d18)",
        border: `2px solid ${enabled ? "#ef4444" : "#1e293b"}`,
        borderRadius: 16, padding: "1.25rem 1.5rem",
        display: "flex", alignItems: "center", justifyContent: "space-between", gap: "1rem",
      }}
    >
      <div>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "1rem", fontWeight: 900, color: enabled ? "#ef4444" : "#64748b", letterSpacing: "0.1em", marginBottom: 4 }}>
          ⚡ GOD MODE
        </div>
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: enabled ? "#fca5a5" : "#334155", maxWidth: 300 }}>
          {enabled
            ? "ACTIVE — Projects deploy without ANY human approval. Confidence threshold = 0."
            : "OFF — All new projects require human approval before going live."}
        </div>
      </div>
      <button
        onClick={() => onToggle(!enabled)} disabled={loading}
        style={{
          width: 64, height: 32, borderRadius: 16,
          background: enabled ? "#ef4444" : "#1e293b",
          border: `2px solid ${enabled ? "#ef4444" : "#334155"}`,
          cursor: loading ? "not-allowed" : "pointer", position: "relative", transition: "all 0.2s", flexShrink: 0,
        }}
      >
        <motion.div
          animate={{ x: enabled ? 32 : 2 }}
          transition={{ type: "spring", stiffness: 400, damping: 25 }}
          style={{ position: "absolute", top: 2, width: 24, height: 24, borderRadius: "50%", background: enabled ? "#fff" : "#475569" }}
        />
      </button>
    </motion.div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function IncubatorPage() {
  const [niches,     setNiches]     = useState<NicheItem[]>([]);
  const [projects,   setProjects]   = useState<IncubatorProject[]>([]);
  const [state,      setState]      = useState<IncubatorStateResponse | null>(null);
  const [godMode,    setGodModeState] = useState(false);
  const [godLoading, setGodLoading] = useState(false);
  const [scoutState, setScoutState] = useState("idle");
  const [generating, setGenerating] = useState<string | null>(null);
  const [toast,      setToast]      = useState<string | null>(null);
  const [loading,    setLoading]    = useState(true);
  const [killLoading, setKillLoading] = useState(false);

  const showToast = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 3500);
  };

  const load = useCallback(async () => {
    try {
      const [nichesRes, projectsRes, stateRes] = await Promise.all([
        getNiches(),
        getIncubatorProjects(),
        getIncubatorState(),
      ]);
      setNiches(nichesRes.niches);
      setScoutState(nichesRes.state);
      setProjects(projectsRes.projects);
      setState(stateRes);
      setGodModeState(stateRes.god_mode);
    } catch { /* API may not be running */ }
    finally { setLoading(false); }
  }, []);

  useEffect(() => {
    load();
    const interval = setInterval(load, 8000);
    return () => clearInterval(interval);
  }, [load]);

  const handleRefreshNiches = async () => {
    try {
      await refreshNiches();
      showToast("Scout scan started — results in ~30s");
      setScoutState("scanning");
    } catch { showToast("Failed to start Scout scan"); }
  };

  const handleGenerate = async (niche: NicheItem) => {
    setGenerating(niche.name);
    try {
      const res = await generateProject({
        niche_name: niche.name, keywords: niche.keywords,
        roi_estimate: niche.roi_estimate, confidence: niche.confidence, source: niche.source,
      });
      showToast(`✅ Project born: ${res.name} (${res.status})`);
      await load();
    } catch (e: unknown) {
      showToast(`❌ ${e instanceof Error ? e.message : String(e)}`);
    } finally { setGenerating(null); }
  };

  const handleApprove = async (id: string) => {
    try { await approveProject(id); showToast("✅ Project approved"); await load(); }
    catch { showToast("❌ Approval failed"); }
  };

  const handleKill = async (id: string) => {
    try { await killProject(id); showToast("💀 Project killed"); await load(); }
    catch { showToast("❌ Kill failed"); }
  };

  const handleGodMode = async (enabled: boolean) => {
    setGodLoading(true);
    try {
      await setGodMode(enabled);
      setGodModeState(enabled);
      showToast(enabled ? "⚡ GOD MODE ACTIVATED" : "GOD MODE deactivated");
    } catch { showToast("❌ Failed to toggle GOD MODE"); }
    finally { setGodLoading(false); }
  };

  const handleKillSwitch = async () => {
    if (!confirm("⚠️ KILL SWITCH: This will stop ALL autonomous projects and disable GOD MODE. Continue?")) return;
    setKillLoading(true);
    try {
      const res = await activateKillSwitch();
      showToast(`🚨 Kill Switch: ${res.projects_killed} projects stopped`);
      await load();
    } catch { showToast("❌ Kill switch failed"); }
    finally { setKillLoading(false); }
  };

  const liveProjects    = projects.filter(p => p.status === "live");
  const pendingProjects = projects.filter(p => p.status === "pending_review");
  const killedProjects  = projects.filter(p => p.status === "killed");

  return (
    <div style={{ padding: "1.5rem", maxWidth: 1200, margin: "0 auto" }}>

      {/* Toast */}
      <AnimatePresence>
        {toast && (
          <motion.div
            initial={{ opacity: 0, y: -20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -20 }}
            style={{
              position: "fixed", top: 70, right: 20, zIndex: 9999,
              background: "#0f172a", border: "1px solid #6366f1", borderRadius: 8,
              padding: "0.6rem 1rem", fontFamily: "var(--font-mono)", fontSize: "0.75rem",
              color: "#f1f5f9", boxShadow: "0 4px 24px #00000088",
            }}
          >{toast}</motion.div>
        )}
      </AnimatePresence>

      {/* Header */}
      <div style={{ marginBottom: "1.5rem" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "1rem" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "1rem" }}>
            <div style={{
              width: 48, height: 48, borderRadius: 12,
              background: "linear-gradient(135deg, #6366f122, #8b5cf622)",
              border: "1px solid #6366f144",
              display: "flex", alignItems: "center", justifyContent: "center", fontSize: "1.5rem",
            }}>🧬</div>
            <div>
              <h1 style={{ fontFamily: "var(--font-mono)", fontSize: "1.3rem", fontWeight: 900, color: "#f1f5f9", margin: 0, letterSpacing: "0.05em" }}>
                EVOLUTION INCUBATOR
              </h1>
              <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#475569", margin: 0 }}>
                Self-evolving AI project generation engine · Phase 13
              </p>
            </div>
          </div>

          {/* Kill Switch */}
          <button
            onClick={handleKillSwitch}
            disabled={killLoading}
            style={{
              padding: "0.6rem 1.2rem",
              background: killLoading ? "#1e293b" : "#ef444422",
              border: "2px solid #ef4444",
              borderRadius: 8, color: "#ef4444",
              fontFamily: "var(--font-mono)", fontSize: "0.75rem", fontWeight: 900,
              cursor: killLoading ? "not-allowed" : "pointer",
              letterSpacing: "0.08em",
              boxShadow: "0 0 16px #ef444433",
            }}
          >
            {killLoading ? "⏳ KILLING..." : "🚨 KILL SWITCH"}
          </button>
        </div>

        {state && (
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: "0.75rem" }}>
            {[
              { label: `Scout: ${scoutState}`, color: scoutState === "scanning" ? "#f59e0b" : "#22c55e" },
              { label: `Architect: ${state.architect_state}`, color: state.architect_state === "generating" ? "#6366f1" : "#22c55e" },
              { label: `${state.total_projects} projects`, color: "#64748b" },
              { label: `${state.live_projects} live`, color: "#22c55e" },
            ].map(({ label, color }) => (
              <span key={label} style={{
                fontFamily: "var(--font-mono)", fontSize: "0.62rem", fontWeight: 700,
                color, background: `${color}18`, border: `1px solid ${color}33`,
                borderRadius: 4, padding: "2px 8px",
              }}>{label}</span>
            ))}
          </div>
        )}
      </div>

      {/* GOD MODE */}
      <div style={{ marginBottom: "1.5rem" }}>
        <GodModeToggle enabled={godMode} onToggle={handleGodMode} loading={godLoading} />
      </div>

      {/* Main grid */}
      <div style={{ display: "grid", gridTemplateColumns: "300px 1fr", gap: "1.5rem", alignItems: "start" }}>

        {/* Left: DNA + Scout */}
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          <div style={{ background: "#080d18", border: "1px solid #1e293b", borderRadius: 12, overflow: "hidden", height: 220, position: "relative" }}>
            <DnaHelix active={state?.architect_state === "generating" || scoutState === "scanning"} />
            <div style={{ position: "absolute", bottom: 8, left: 0, right: 0, textAlign: "center", fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155" }}>
              {state?.architect_state === "generating" ? "⚡ ARCHITECT GENERATING..." : scoutState === "scanning" ? "🔍 SCOUT SCANNING..." : "DNA EVOLUTION ENGINE"}
            </div>
          </div>

          <div style={{ background: "#0a0e1a", border: "1px solid #1e293b", borderRadius: 12, padding: "1rem" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.75rem" }}>
              <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.7rem", fontWeight: 700, color: "#94a3b8", letterSpacing: "0.08em" }}>
                🔍 TOP NICHES
              </span>
              <button onClick={handleRefreshNiches} style={{
                fontSize: "0.6rem", fontFamily: "var(--font-mono)",
                background: "#1e293b", border: "1px solid #334155",
                borderRadius: 4, color: "#64748b", cursor: "pointer", padding: "2px 8px",
              }}>↺ RESCAN</button>
            </div>

            {loading ? (
              <div style={{ color: "#334155", fontFamily: "var(--font-mono)", fontSize: "0.65rem" }}>Loading...</div>
            ) : niches.length === 0 ? (
              <div style={{ color: "#334155", fontFamily: "var(--font-mono)", fontSize: "0.65rem" }}>No niches yet — click RESCAN</div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                {niches.map((niche, i) => (
                  <NicheCard key={niche.name} niche={niche} rank={i} onGenerate={handleGenerate} generating={generating === niche.name} />
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Right: Projects */}
        <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "0.75rem" }}>
            {[
              { label: "Total Born", value: projects.length, color: "#6366f1" },
              { label: "Live", value: liveProjects.length, color: "#22c55e" },
              { label: "Pending", value: pendingProjects.length, color: "#f59e0b" },
              { label: "Killed", value: killedProjects.length, color: "#ef4444" },
            ].map(({ label, value, color }) => (
              <div key={label} style={{ background: "#080d18", border: `1px solid ${color}22`, borderRadius: 10, padding: "0.75rem", textAlign: "center" }}>
                <div style={{ fontFamily: "var(--font-mono)", fontSize: "1.4rem", fontWeight: 900, color, marginBottom: 2 }}>{value}</div>
                <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#334155", letterSpacing: "0.06em" }}>{label}</div>
              </div>
            ))}
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 130px 70px 55px 55px 140px", gap: "0.75rem", padding: "0.4rem 1rem", borderBottom: "1px solid #0f172a" }}>
            {["PROJECT", "STATUS", "AGE", "CONF", "GEN", "ACTIONS"].map(h => (
              <span key={h} style={{ fontFamily: "var(--font-mono)", fontSize: "0.58rem", fontWeight: 700, color: "#334155", letterSpacing: "0.08em" }}>{h}</span>
            ))}
          </div>

          <div>
            {loading ? (
              <div style={{ color: "#334155", fontFamily: "var(--font-mono)", fontSize: "0.7rem", padding: "1rem" }}>Loading projects...</div>
            ) : projects.length === 0 ? (
              <div style={{ color: "#1e293b", fontFamily: "var(--font-mono)", fontSize: "0.7rem", padding: "2rem", textAlign: "center" }}>
                No projects yet. Generate one from a niche above.
              </div>
            ) : (
              <AnimatePresence>
                {projects.map(p => (
                  <ProjectRow key={p.project_id} project={p} onApprove={handleApprove} onKill={handleKill} />
                ))}
              </AnimatePresence>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
