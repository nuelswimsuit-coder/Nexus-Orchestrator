"use client";

import { useMemo } from "react";
import { motion } from "framer-motion";
import CyberGrid from "@/components/CyberGrid";
import PageTransition from "@/components/PageTransition";

type VersionNode = {
  id: string;
  label: string;
  parent?: string;
  fitness: number;
  mutations: string[];
  at: string;
};

/** Demo lineage — replace with GET /api/... when backend exposes strategy versions. */
const DEMO_TREE: VersionNode[] = [
  {
    id: "v0",
    label: "BASELINE · v0",
    fitness: 0.42,
    mutations: ["initial spread model"],
    at: "2025-03-10",
  },
  {
    id: "v1",
    label: "STRIKE-A · v1",
    parent: "v0",
    fitness: 0.58,
    mutations: ["widen Poly band", "tighten Binance slippage"],
    at: "2025-03-14",
  },
  {
    id: "v2",
    label: "STRIKE-B · v2",
    parent: "v1",
    fitness: 0.71,
    mutations: ["Kelly cap 0.12", "panic hook on 3 losses"],
    at: "2025-03-17",
  },
  {
    id: "v3",
    label: "NIGHT-FORK · v3",
    parent: "v1",
    fitness: 0.66,
    mutations: ["OpenClaw sentiment blend"],
    at: "2025-03-18",
  },
  {
    id: "v4",
    label: "CANARY · v4",
    parent: "v2",
    fitness: 0.79,
    mutations: ["self-patch path resolver", "SSH health backoff"],
    at: "2025-03-20",
  },
];

export default function EvolutionPage() {
  const byParent = useMemo(() => {
    const m = new Map<string | undefined, VersionNode[]>();
    for (const n of DEMO_TREE) {
      const k = n.parent;
      const arr = m.get(k) ?? [];
      arr.push(n);
      m.set(k, arr);
    }
    return m;
  }, []);

  const roots = byParent.get(undefined) ?? [];

  return (
    <>
      <CyberGrid opacity={0.4} speed={0.75} />
      <PageTransition>
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          style={{
            position: "relative",
            zIndex: 1,
            maxWidth: 960,
            margin: "0 auto",
            padding: "2rem 1.5rem 3rem",
            display: "flex",
            flexDirection: "column",
            gap: "1.5rem",
          }}
        >
          <header dir="rtl" style={{ textAlign: "right" }}>
            <h1
              style={{
                fontSize: "1.65rem",
                fontWeight: 800,
                letterSpacing: "0.04em",
                color: "#e0f2fe",
                margin: 0,
              }}
            >
              עץ אסטרטגיות
            </h1>
            <p
              style={{
                margin: "0.35rem 0 0",
                color: "#7dd3fc",
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
              }}
            >
              גרסאות ליליות · מוטציות שתועדו אוטומטית (דמו — מחובר ל־API בעתיד)
            </p>
          </header>

          <div style={{ display: "flex", flexDirection: "column", gap: "1.25rem" }}>
            {roots.map((r) => (
              <TreeBranch key={r.id} node={r} byParent={byParent} depth={0} />
            ))}
          </div>
        </motion.div>
      </PageTransition>
    </>
  );
}

function TreeBranch({
  node,
  byParent,
  depth,
}: {
  node: VersionNode;
  byParent: Map<string | undefined, VersionNode[]>;
  depth: number;
}) {
  const kids = byParent.get(node.id) ?? [];
  const cyan = "var(--neon-binance, #00e5ff)";
  const lime = "var(--neon-poly, #b8ff3d)";

  return (
    <motion.div
      initial={{ opacity: 0, x: depth % 2 === 0 ? -12 : 12 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: depth * 0.06 }}
      style={{
        marginRight: depth * 18,
        borderRight: `2px solid ${depth === 0 ? cyan : lime}`,
        paddingRight: "1rem",
      }}
    >
      <div
        dir="rtl"
        style={{
          background: "var(--glass-command, rgba(6,12,28,0.65))",
          backdropFilter: "blur(18px)",
          border: "1px solid var(--glass-command-border, rgba(0,229,255,0.2))",
          borderRadius: 14,
          padding: "1rem 1.15rem",
          marginBottom: kids.length ? "0.75rem" : 0,
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: "0.75rem",
            flexWrap: "wrap",
            alignItems: "baseline",
          }}
        >
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontWeight: 800,
              fontSize: "0.82rem",
              color: "#f0f9ff",
            }}
          >
            {node.label}
          </span>
          <span style={{ fontFamily: "var(--font-mono)", fontSize: "0.62rem", color: "#94a3b8" }}>
            כושר: {(node.fitness * 100).toFixed(0)}% · {node.at}
          </span>
        </div>
        <ul
          style={{
            margin: "0.65rem 0 0",
            padding: "0 1rem 0 0",
            color: "#a5f3fc",
            fontSize: "0.72rem",
            lineHeight: 1.6,
          }}
        >
          {node.mutations.map((m) => (
            <li key={m}>{m}</li>
          ))}
        </ul>
      </div>
      {kids.map((c) => (
        <TreeBranch key={c.id} node={c} byParent={byParent} depth={depth + 1} />
      ))}
    </motion.div>
  );
}
