"use client";

/**
 * Matrix-style falling glyphs + live agent log lines with Hebrew gloss.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import type { AgentLogEntry, AgentLogResponse } from "@/lib/api";

const GLYPHS = "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン01";

function glossHebrew(msg: string): string {
  let s = msg;
  const pairs: [RegExp, string][] = [
    [/panic/gi, "חירום"],
    [/deploy/gi, "פריסה"],
    [/worker/gi, "מעבד"],
    [/task/gi, "משימה"],
    [/error/gi, "שגיאה"],
    [/warning/gi, "אזהרה"],
    [/approve/gi, "אישור"],
    [/redis/gi, "Redis"],
    [/queue/gi, "תור"],
    [/scrape/gi, "גרידה"],
    [/confidence/gi, "ביטחון"],
    [/threshold/gi, "סף"],
  ];
  for (const [re, he] of pairs) {
    s = s.replace(re, (m) => `${m}⟨${he}⟩`);
  }
  return s;
}

export default function MatrixRawLogs() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const { data } = useSWR<AgentLogResponse>(
    "/api/business/agent-log",
    swrFetcher<AgentLogResponse>,
    { refreshInterval: 2000 },
  );

  const lines = useMemo(() => {
    const entries = data?.entries ?? [];
    return entries.slice(-40).reverse();
  }, [data?.entries]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf = 0;
    const dpr = Math.min(window.devicePixelRatio ?? 1, 2);
    const resize = () => {
      const parent = canvas.parentElement;
      if (!parent) return;
      const w = parent.clientWidth;
      const h = parent.clientHeight;
      canvas.width = w * dpr;
      canvas.height = h * dpr;
      canvas.style.width = `${w}px`;
      canvas.style.height = `${h}px`;
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.scale(dpr, dpr);
    };
    resize();
    window.addEventListener("resize", resize);

    const fontSize = 13;
    const colW = fontSize * 1.1;
    const cols = Math.ceil((canvas.clientWidth || 400) / colW) || 20;
    const drops = Array.from({ length: cols }, () => Math.random() * -50);

    const draw = () => {
      const w = canvas.clientWidth;
      const h = canvas.clientHeight;
      ctx.fillStyle = "rgba(0, 8, 0, 0.12)";
      ctx.fillRect(0, 0, w, h);
      ctx.font = `${fontSize}px var(--font-mono), monospace`;

      for (let i = 0; i < cols; i++) {
        const ch = GLYPHS[Math.floor(Math.random() * GLYPHS.length)] ?? "0";
        const x = i * colW;
        const y = drops[i]! * fontSize;
        const flicker = Math.random() > 0.96 ? "#b8ff3d" : "#00ff41";
        ctx.fillStyle = flicker;
        ctx.fillText(ch, x, y);
        if (y > h && Math.random() > 0.975) drops[i] = 0;
        drops[i]! += 0.55 + Math.random() * 0.35;
      }
      raf = requestAnimationFrame(draw);
    };
    raf = requestAnimationFrame(draw);
    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", resize);
    };
  }, []);

  return (
    <div
      style={{
        position: "relative",
        borderRadius: 14,
        overflow: "hidden",
        border: "1px solid rgba(0,255,65,0.25)",
        minHeight: 420,
        background: "#000800",
      }}
    >
      <canvas
        ref={canvasRef}
        style={{
          position: "absolute",
          inset: 0,
          width: "100%",
          height: "100%",
          opacity: 0.35,
          pointerEvents: "none",
        }}
      />
      <div
        dir="ltr"
        style={{
          position: "relative",
          zIndex: 1,
          fontFamily: "var(--font-mono)",
          fontSize: "0.62rem",
          lineHeight: 1.55,
          padding: "1rem 1.1rem",
          maxHeight: 420,
          overflowY: "auto",
          color: "#86efac",
          textShadow: "0 0 6px rgba(34,197,94,0.35)",
        }}
      >
        {lines.length === 0 && (
          <div style={{ color: "#166534", padding: "2rem", textAlign: "center" }}>
            ממתין לזרם לוגים…
          </div>
        )}
        {lines.map((row: AgentLogEntry) => (
          <LogRow key={`${row.ts}-${row.message.slice(0, 24)}`} row={row} />
        ))}
      </div>
    </div>
  );
}

function LogRow({ row }: { row: AgentLogEntry }) {
  const [open, setOpen] = useState(false);
  const he = glossHebrew(row.message);
  return (
    <div
      style={{
        borderBottom: "1px solid rgba(22,101,52,0.35)",
        padding: "0.35rem 0",
        cursor: "pointer",
      }}
      onClick={() => setOpen((o) => !o)}
    >
      <span style={{ color: "#4ade80", marginRight: 8 }}>{row.ts.slice(11, 19)}</span>
      <span style={{ color: "#bbf7d0" }}>[{row.level}]</span>{" "}
      <span style={{ color: "#ecfccb" }}>{row.message}</span>
      {open && (
        <div
          dir="rtl"
          style={{
            marginTop: 6,
            padding: "0.45rem 0.5rem",
            borderRadius: 6,
            background: "rgba(0,40,20,0.55)",
            color: "#a7f3d0",
            fontSize: "0.58rem",
          }}
        >
          מילון: {he}
        </div>
      )}
    </div>
  );
}
