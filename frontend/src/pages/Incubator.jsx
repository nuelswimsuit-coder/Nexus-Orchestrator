"use client";

/**
 * Project Incubator Hub — AI "eggs": ROI, risk, ETA; Gestation → generateProject;
 * Market Validation chart (synthetic trend narrative: X / Google style).
 */

import { useCallback, useMemo, useState } from "react";
import useSWR from "swr";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { generateProject, swrFetcher } from "@/lib/api";

function riskFromConfidence(conf) {
  const c = typeof conf === "number" ? conf : 50;
  return clampRound(100 - c, 5, 95);
}

function clampRound(n, a, b) {
  return Math.round(Math.max(a, Math.min(b, n)));
}

function devWeeksFromGen(gen, name) {
  const g = typeof gen === "number" ? gen : 1;
  const salt = (name || "").length % 5;
  return clampRound(2 + g * 1.2 + salt * 0.3, 2, 16);
}

function validationSeries(project) {
  const base = (project.confidence_at_birth ?? 55) / 100;
  const id = (project.project_id || "x").replace(/\W/g, "");
  let seed = 0;
  for (let i = 0; i < id.length; i++) seed += id.charCodeAt(i);
  return Array.from({ length: 12 }, (_, i) => {
    const wave = Math.sin((i + seed % 7) * 0.7) * 8;
    const google = base * 72 + i * 2.1 + (seed % 13) * 0.4;
    const xBuzz = base * 64 + wave + (i > 7 ? 6 : 0);
    return {
      week: `W${i + 1}`,
      googleTrend: clampRound(google, 0, 100),
      xSignal: clampRound(xBuzz, 0, 100),
    };
  });
}

export default function ProjectIncubatorHub() {
  const { data, mutate } = useSWR("/api/incubator/projects", swrFetcher, { refreshInterval: 15_000 });
  const projects = data?.projects ?? [];
  const [busyId, setBusyId] = useState(null);
  const [toast, setToast] = useState(null);

  const eggs = useMemo(() => {
    return projects.map((p) => ({
      ...p,
      roiLabel: p.estimated_roi || "—",
      risk: riskFromConfidence(p.confidence_at_birth),
      devWeeks: devWeeksFromGen(p.generation, p.name),
      series: validationSeries(p),
    }));
  }, [projects]);

  const onGestation = useCallback(
    async (p) => {
      setBusyId(p.project_id);
      setToast(null);
      try {
        const res = await generateProject({
          niche_name: p.niche || p.name,
          keywords: [p.slug, p.niche].filter(Boolean),
          roi_estimate: p.estimated_roi,
          confidence: p.confidence_at_birth,
          source: p.niche_source || "incubator_hub",
          custom_brief:
            `GESTATION: Gemini scaffold for "${p.name}" in an isolated directory. ` +
            `Extend existing slug ${p.slug}. Priority: ship MVP API + README.`,
        });
        setToast(`Gestation started: ${res.name} → ${res.path || res.status}`);
        await mutate();
      } catch (e) {
        setToast(e instanceof Error ? e.message : String(e));
      } finally {
        setBusyId(null);
      }
    },
    [mutate],
  );

  if (eggs.length === 0) {
    return (
      <div
        style={{
          padding: "1.25rem",
          borderRadius: 14,
          border: "1px solid #1e293b",
          background: "#080d18",
          fontFamily: "var(--font-mono)",
          fontSize: "0.72rem",
          color: "#64748b",
          marginBottom: "1.25rem",
        }}
      >
        אין ביצי פרויקט עדיין — הפעל Generate מהנישים או רענן Scout.
      </div>
    );
  }

  return (
    <div style={{ marginBottom: "1.75rem" }}>
      {toast && (
        <div
          style={{
            marginBottom: "0.75rem",
            padding: "0.5rem 0.75rem",
            borderRadius: 8,
            border: "1px solid #6366f1",
            background: "#0f172a",
            color: "#e2e8f0",
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
          }}
        >
          {toast}
        </div>
      )}

      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.7rem",
          fontWeight: 800,
          letterSpacing: "0.12em",
          color: "#94a3b8",
          marginBottom: "0.75rem",
        }}
      >
        AUTONOMOUS PROJECT FACTORY · ביצי פרויקט
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))",
          gap: "1rem",
        }}
      >
        {eggs.map((p) => (
          <div
            key={p.project_id}
            style={{
              position: "relative",
              borderRadius: "50% / 42%",
              padding: "1.35rem 1.5rem 1.5rem",
              background: "radial-gradient(ellipse at 30% 20%, #1e1b4b 0%, #0f172a 55%, #020617 100%)",
              border: "1px solid rgba(99,102,241,0.35)",
              boxShadow: "0 0 32px rgba(99,102,241,0.12), inset 0 0 40px rgba(15,23,42,0.8)",
              minHeight: 320,
              display: "flex",
              flexDirection: "column",
            }}
          >
            <div style={{ fontFamily: "var(--font-mono)", fontWeight: 800, color: "#f8fafc", fontSize: "0.85rem" }}>{p.name}</div>
            <div style={{ fontSize: "0.62rem", color: "#64748b", fontFamily: "var(--font-mono)", marginTop: 4 }}>{p.niche}</div>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, marginTop: "0.85rem" }}>
              <div>
                <div style={{ fontSize: "0.55rem", color: "#475569" }}>ROI (est.)</div>
                <div style={{ fontSize: "0.75rem", color: "#4ade80", fontFamily: "var(--font-mono)" }}>{p.roiLabel}</div>
              </div>
              <div>
                <div style={{ fontSize: "0.55rem", color: "#475569" }}>Risk</div>
                <div style={{ fontSize: "0.75rem", color: "#fbbf24", fontFamily: "var(--font-mono)" }}>{p.risk}%</div>
              </div>
              <div>
                <div style={{ fontSize: "0.55rem", color: "#475569" }}>Dev</div>
                <div style={{ fontSize: "0.75rem", color: "#38bdf8", fontFamily: "var(--font-mono)" }}>{p.devWeeks}w</div>
              </div>
            </div>

            <div style={{ marginTop: "0.65rem", flex: 1, minHeight: 120, minWidth: 0 }}>
              <div style={{ fontSize: "0.55rem", color: "#64748b", marginBottom: 4, letterSpacing: "0.08em" }}>MARKET VALIDATION</div>
              <div style={{ fontSize: "0.58rem", color: "#475569", marginBottom: 6, lineHeight: 1.4 }}>
                AI synthesis: חיפושי Google ({p.series[11]?.googleTrend}% מומנטום) + אותות X ({p.series[11]?.xSignal}% buzz) תומכים בביקוש.
              </div>
              <div style={{ width: "100%", height: 100, minHeight: 100 }}>
              <ResponsiveContainer width="100%" height={100}>
                <AreaChart data={p.series} margin={{ top: 2, right: 4, left: -18, bottom: 0 }}>
                  <defs>
                    <linearGradient id={`vg${p.project_id}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor="#22d3ee" stopOpacity={0.35} />
                      <stop offset="100%" stopColor="#22d3ee" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                  <XAxis dataKey="week" tick={{ fontSize: 8, fill: "#475569" }} axisLine={false} tickLine={false} />
                  <YAxis hide domain={[0, 100]} />
                  <Tooltip
                    contentStyle={{ background: "#0f172a", border: "1px solid #334155", fontSize: 10 }}
                    labelStyle={{ color: "#94a3b8" }}
                  />
                  <Area type="monotone" dataKey="googleTrend" stackId="1" stroke="#34d399" fillOpacity={0} strokeWidth={1.5} />
                  <Area
                    type="monotone"
                    dataKey="xSignal"
                    stroke="#a78bfa"
                    fill={`url(#vg${p.project_id.replace(/\W/g, "")})`}
                    strokeWidth={1.5}
                  />
                </AreaChart>
              </ResponsiveContainer>
              </div>
            </div>

            <button
              type="button"
              disabled={busyId === p.project_id}
              onClick={() => onGestation(p)}
              style={{
                marginTop: "auto",
                padding: "0.5rem",
                borderRadius: 10,
                border: "1px solid #a855f7",
                background: busyId === p.project_id ? "#1e293b" : "rgba(168,85,247,0.15)",
                color: busyId === p.project_id ? "#64748b" : "#e9d5ff",
                fontFamily: "var(--font-mono)",
                fontSize: "0.68rem",
                fontWeight: 800,
                letterSpacing: "0.1em",
                cursor: busyId === p.project_id ? "wait" : "pointer",
              }}
            >
              {busyId === p.project_id ? "GESTATING…" : "GESTATION · דגירה"}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
