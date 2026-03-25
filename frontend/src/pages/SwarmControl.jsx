"use client";

/**
 * Swarm Neural Map — force-style layout: Master center, workers in orbit.
 * SVG edges with glow + data pulse; failed workers blink red + self-repair label.
 * Overclock: client-side flag + optional POST to cluster overclock stub.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import useSWR from "swr";
import { API_BASE, swrFetcher } from "@/lib/api";

const W = 920;
const H = 540;
const CX = W / 2;
const CY = H / 2;
const R_ORBIT = 200;

function clamp(n, a, b) {
  return Math.max(a, Math.min(b, n));
}

export default function SwarmControl() {
  const { data } = useSWR("/api/cluster/status", swrFetcher, { refreshInterval: 5_000 });
  const nodes = data?.nodes ?? [];

  const master = useMemo(
    () => nodes.find((n) => n.role === "master") ?? { node_id: "master", role: "master", online: true },
    [nodes],
  );
  const workers = useMemo(() => nodes.filter((n) => n.role === "worker"), [nodes]);

  const [layout, setLayout] = useState(() => ({}));
  const [pulseEdge, setPulseEdge] = useState(null);
  const [overclock, setOverclock] = useState({});

  const tickRef = useRef(0);

  useEffect(() => {
    let rafId = 0;
    let cancelled = false;
    const n = Math.max(workers.length, 1);
    const next = {};
    workers.forEach((w, i) => {
      const angle = (i / n) * Math.PI * 2 + tickRef.current * 0.0004;
      const jitter = (w.node_id || "").split("").reduce((a, c) => a + c.charCodeAt(0), 0) % 37;
      const r = R_ORBIT + (jitter % 12) - 6;
      next[w.node_id] = {
        x: CX + Math.cos(angle) * r,
        y: CY + Math.sin(angle) * r,
      };
    });
    setLayout(next);

    function loop() {
      if (cancelled) return;
      tickRef.current += 1;
      if (tickRef.current % 2 === 0) {
        const n2 = Math.max(workers.length, 1);
        setLayout((prev) => {
          const o = { ...prev };
          workers.forEach((w, i) => {
            const angle = (i / n2) * Math.PI * 2 + tickRef.current * 0.00035;
            const jitter = (w.node_id || "").split("").reduce((a, c) => a + c.charCodeAt(0), 0) % 37;
            const r = R_ORBIT + (jitter % 12) - 6;
            const tx = CX + Math.cos(angle) * r;
            const ty = CY + Math.sin(angle) * r;
            const px = o[w.node_id]?.x ?? tx;
            const py = o[w.node_id]?.y ?? ty;
            o[w.node_id] = {
              x: px + (tx - px) * 0.06,
              y: py + (ty - py) * 0.06,
            };
          });
          return o;
        });
      }
      rafId = requestAnimationFrame(loop);
    }
    rafId = requestAnimationFrame(loop);
    return () => {
      cancelled = true;
      cancelAnimationFrame(rafId);
    };
  }, [workers]);

  useEffect(() => {
    if (workers.length === 0) return;
    const id = setInterval(() => {
      const pick = workers[Math.floor(Math.random() * workers.length)];
      if (pick) setPulseEdge(pick.node_id);
      setTimeout(() => setPulseEdge(null), 420);
    }, 900);
    return () => clearInterval(id);
  }, [workers]);

  const toggleOverclock = useCallback(async (nodeId) => {
    setOverclock((o) => ({ ...o, [nodeId]: !o[nodeId] }));
    try {
      await fetch(`${API_BASE}/api/cluster/node/overclock`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_id: nodeId, scan_speed_multiplier: 2.0 }),
      });
    } catch {
      /* optional backend */
    }
  }, []);

  const masterOnline = master.online !== false;

  return (
    <div style={{ position: "relative", width: "100%", maxWidth: 960, margin: "0 auto" }}>
      <svg
        width="100%"
        viewBox={`0 0 ${W} ${H}`}
        style={{ display: "block", background: "radial-gradient(ellipse at center, #0c1528 0%, #050810 70%)", borderRadius: 16 }}
      >
        <defs>
          <filter id="swarmGlow" x="-40%" y="-40%" width="180%" height="180%">
            <feGaussianBlur stdDeviation="3" result="b" />
            <feMerge>
              <feMergeNode in="b" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <linearGradient id="edgeNeon" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#00e5ff" stopOpacity="0.15" />
            <stop offset="50%" stopColor="#22d3ee" stopOpacity="0.95" />
            <stop offset="100%" stopColor="#00e5ff" stopOpacity="0.15" />
          </linearGradient>
        </defs>

        <circle cx={CX} cy={CY} r={R_ORBIT + 28} fill="none" stroke="#1e3a5f" strokeWidth="1" strokeDasharray="6 10" opacity={0.35} />

        {workers.map((w) => {
          const p = layout[w.node_id];
          if (!p) return null;
          const active = pulseEdge === w.node_id;
          const failed = w.online === false;
          return (
            <g key={w.node_id}>
              <line
                x1={CX}
                y1={CY}
                x2={p.x}
                y2={p.y}
                stroke="url(#edgeNeon)"
                strokeWidth={active ? 3.2 : 1.4}
                filter={active ? "url(#swarmGlow)" : undefined}
                opacity={failed ? 0.35 : 0.85}
              >
                {active && (
                  <animate attributeName="stroke-opacity" values="0.3;1;0.3" dur="0.45s" repeatCount="1" />
                )}
              </line>
              {active && (
                <circle r="5" fill="#67e8f9" filter="url(#swarmGlow)">
                  <animateMotion dur="0.45s" fill="freeze" path={`M${CX},${CY} L${p.x},${p.y}`} />
                </circle>
              )}
            </g>
          );
        })}

        <g transform={`translate(${CX},${CY})`}>
          <circle
            r={28}
            fill={masterOnline ? "#0e7490" : "#7f1d1d"}
            stroke={masterOnline ? "#22d3ee" : "#f87171"}
            strokeWidth={2}
            filter="url(#swarmGlow)"
            style={{
              animation: masterOnline ? "swarm-master-pulse 3s ease-in-out infinite" : "swarm-fail-blink 0.7s infinite",
            }}
          />
          <text textAnchor="middle" dy="5" fill="#ecfeff" fontSize="11" fontFamily="var(--font-mono)" fontWeight={700}>
            M
          </text>
        </g>

        {workers.map((w) => {
          const p = layout[w.node_id];
          if (!p) return null;
          const failed = w.online === false;
          const oc = overclock[w.node_id];
          const fill = failed ? "#dc2626" : oc ? "#f59e0b" : "#0369a1";
          const stroke = failed ? "#fecaca" : oc ? "#fcd34d" : "#38bdf8";
          return (
            <g key={`n-${w.node_id}`} transform={`translate(${p.x},${p.y})`}>
              <circle
                r={22}
                fill={fill}
                stroke={stroke}
                strokeWidth={2}
                filter={failed || oc ? "url(#swarmGlow)" : undefined}
                style={{
                  animation: failed ? "swarm-fail-blink 0.85s infinite" : undefined,
                  cursor: "pointer",
                }}
              />
              <title>{`${w.node_id} · ${w.os_info || "worker"} · jobs ${w.active_jobs ?? 0}`}</title>
              <text textAnchor="middle" dy={4} fill="#f0f9ff" fontSize="9" fontFamily="var(--font-mono)">
                {(w.node_id || "w").slice(0, 6)}
              </text>
              {failed && (
                <text textAnchor="middle" y={36} fill="#fecaca" fontSize="7" fontFamily="var(--font-sans)">
                  תיקון עצמי בתהליך
                </text>
              )}
            </g>
          );
        })}
      </svg>

      {workers.map((w) => {
        const p = layout[w.node_id];
        if (!p) return null;
        const leftPct = clamp((p.x / W) * 100, 8, 92);
        const topPct = clamp((p.y / H) * 100, 10, 90);
        return (
          <button
            key={`oc-${w.node_id}`}
            type="button"
            onClick={() => toggleOverclock(w.node_id)}
            style={{
              position: "absolute",
              left: `${leftPct}%`,
              top: `${topPct}%`,
              transform: "translate(-50%, 42px)",
              fontSize: "0.58rem",
              fontFamily: "var(--font-mono)",
              fontWeight: 800,
              letterSpacing: "0.06em",
              padding: "3px 8px",
              borderRadius: 6,
              border: `1px solid ${overclock[w.node_id] ? "#f59e0b" : "#334155"}`,
              background: overclock[w.node_id] ? "rgba(245,158,11,0.2)" : "rgba(15,23,42,0.85)",
              color: overclock[w.node_id] ? "#fcd34d" : "#94a3b8",
              cursor: "pointer",
            }}
          >
            Overclock {overclock[w.node_id] ? "ON" : ""}
          </button>
        );
      })}

      <p
        style={{
          marginTop: 10,
          fontFamily: "var(--font-mono)",
          fontSize: "0.62rem",
          color: "#64748b",
          textAlign: "center",
        }}
      >
        Data pulse = synthetic edge highlight · Overclock = 200% scan cadence (CPU cost) · optional POST /api/cluster/node/overclock
      </p>

      <style>{`
        @keyframes swarm-master-pulse {
          0%, 100% { filter: drop-shadow(0 0 6px #22d3ee); }
          50% { filter: drop-shadow(0 0 14px #67e8f9); }
        }
        @keyframes swarm-fail-blink {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.35; }
        }
      `}</style>
    </div>
  );
}
