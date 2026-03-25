"use client";

import { motion, AnimatePresence } from "framer-motion";
import useSWR from "swr";
import { useCallback, useMemo, useState } from "react";
import { API_BASE, swrFetcher } from "@/lib/api";
import { useNexus } from "@/lib/nexus-context";
import { useCcWebSocket } from "@/lib/use-cc-ws";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import ClusterResourceGlobe3D, { type HeatNode } from "@/components/ClusterResourceGlobe3D";
import GeminiTerminal from "@/pages/GeminiTerminal.jsx";

type CcProject = {
  slug: string;
  name: string;
  vision_summary: string;
  metadata: Record<string, unknown>;
  updated_at: string;
};

type CcProjectsResponse = { projects: CcProject[]; count: number };

type HeatmapResponse = { nodes: HeatNode[]; ts: string };

type RoiResponse = { projects: Record<string, unknown>; ts: string };

type CryptoResponse = {
  polymarket_bot: { pnl: unknown; session: unknown };
  wallet: unknown;
  ts: string;
};

const tabs = ["vision", "intel", "infra", "finance"] as const;
type TabId = (typeof tabs)[number];

export default function CommandControlSuite() {
  const { activeCcProject, setActiveCcProject } = useNexus();
  const [tab, setTab] = useState<TabId>("vision");
  const [wsOn, setWsOn] = useState(true);
  const { connected, lastMessages } = useCcWebSocket(wsOn);

  const { data: projectsData } = useSWR<CcProjectsResponse>(
    "/api/cc/projects",
    swrFetcher<CcProjectsResponse>,
    { refreshInterval: 60_000 },
  );

  const { data: heatData } = useSWR<HeatmapResponse>(
    "/api/cc/infra/cluster-heatmap",
    swrFetcher<HeatmapResponse>,
    { refreshInterval: 5_000 },
  );

  const { data: roiData } = useSWR<RoiResponse>(
    "/api/cc/finance/roi",
    swrFetcher<RoiResponse>,
    { refreshInterval: 15_000 },
  );

  const { data: cryptoData } = useSWR<CryptoResponse>(
    "/api/cc/finance/crypto-snapshot",
    swrFetcher<CryptoResponse>,
    { refreshInterval: 8_000 },
  );

  const { data: nuelData } = useSWR(
    activeCcProject === "nuel" ? "/api/cc/ecom/nuel" : null,
    swrFetcher<Record<string, unknown>>,
    { refreshInterval: 20_000 },
  );

  const activeVision = useMemo(() => {
    const list = projectsData?.projects ?? [];
    return list.find((p) => p.slug === activeCcProject) ?? list[0];
  }, [projectsData, activeCcProject]);

  const [leadForm, setLeadForm] = useState({
    source: "telegram_scrape",
    message_count: 3,
    days_since_contact: 2,
    company: "Acme",
    tags: "warm",
  });
  const [leadResult, setLeadResult] = useState<Record<string, unknown> | null>(null);

  const scoreLead = useCallback(async () => {
    const tags = leadForm.tags.split(",").map((t) => t.trim()).filter(Boolean);
    const res = await fetch(`${API_BASE}/api/cc/intelligence/lead-score`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: leadForm.source,
        message_count: leadForm.message_count,
        days_since_contact: leadForm.days_since_contact,
        company: leadForm.company,
        tags,
      }),
    });
    setLeadResult(await res.json());
  }, [leadForm]);

  const [recoverNode, setRecoverNode] = useState("*");
  const [recoverMsg, setRecoverMsg] = useState<string | null>(null);

  const recoverWorker = useCallback(async () => {
    setRecoverMsg(null);
    const res = await fetch(`${API_BASE}/api/sentinel/recover-worker`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ node_id: recoverNode || "*", mode: "restart_process" }),
    });
    setRecoverMsg(JSON.stringify(await res.json(), null, 2));
  }, [recoverNode]);

  const [flightArmed, setFlightArmed] = useState(false);
  const [flightMsg, setFlightMsg] = useState<string | null>(null);
  const [intelPreview, setIntelPreview] = useState<string | null>(null);

  const flightAdvanced = useCallback(async () => {
    if (!flightArmed) {
      setFlightArmed(true);
      return;
    }
    setFlightMsg(null);
    const res = await fetch(`${API_BASE}/api/flight-mode/panic-advanced`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirm: "FLIGHT_ADVANCED_CONFIRM", operator: "dashboard" }),
    });
    const j = await res.json().catch(() => ({}));
    setFlightMsg(res.ok ? JSON.stringify(j, null, 2) : `Error ${res.status}: ${JSON.stringify(j)}`);
    setFlightArmed(false);
  }, [flightArmed]);

  const [heatMsg, setHeatMsg] = useState<string | null>(null);
  const loadSentiment = useCallback(async () => {
    const res = await fetch(`${API_BASE}/api/cc/heshbonator/sentiment-heatmap`);
    const j = await res.json();
    setHeatMsg(`groups=${j.telegram_groups?.length ?? 0} keywords=${Object.keys(j.keyword_counts ?? {}).length}`);
  }, []);

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] as const }}
      className="flex flex-col gap-4"
    >
      <div className="flex flex-wrap items-center gap-2">
        {tabs.map((id) => (
          <Button
            key={id}
            variant={tab === id ? "default" : "outline"}
            size="sm"
            type="button"
            onClick={() => setTab(id)}
          >
            {id}
          </Button>
        ))}
        <span className="ml-auto font-mono text-[0.55rem] uppercase tracking-widest text-[var(--color-text-muted)]">
          ws {connected ? "live" : "off"}
        </span>
        <Button variant="ghost" size="sm" type="button" onClick={() => setWsOn((v) => !v)}>
          toggle stream
        </Button>
      </div>

      <AnimatePresence mode="wait">
        <motion.div
          key={tab}
          initial={{ opacity: 0, x: 8 }}
          animate={{ opacity: 1, x: 0 }}
          exit={{ opacity: 0, x: -8 }}
          transition={{ duration: 0.25 }}
        >
          {tab === "vision" && (
            <div className="flex flex-col gap-4">
              <GeminiTerminal />
              <div className="grid gap-4 lg:grid-cols-[1fr_320px]">
              <Card>
                <CardHeader>
                  <CardTitle>Project vision</CardTitle>
                  <CardDescription>
                    Isolated context switch — each slug maps to SQLite metadata in{" "}
                    <code className="text-[var(--color-accent)]">nexus/data/cc_hub.db</code>
                  </CardDescription>
                </CardHeader>
                <CardContent className="flex flex-col gap-4">
                  <div className="flex flex-wrap gap-2">
                    {(projectsData?.projects ?? []).map((p) => (
                      <Button
                        key={p.slug}
                        size="sm"
                        variant={activeCcProject === p.slug ? "default" : "outline"}
                        type="button"
                        onClick={() => setActiveCcProject(p.slug)}
                      >
                        {p.name}
                      </Button>
                    ))}
                  </div>
                  {activeVision && (
                    <div className="rounded-lg border border-[var(--color-surface-4)] bg-[var(--color-surface-0)]/50 p-4">
                      <p className="font-mono text-[0.6rem] uppercase tracking-[0.18em] text-[var(--color-accent)]">
                        {activeVision.name}
                      </p>
                      <p className="mt-2 text-[0.78rem] leading-relaxed text-[var(--color-text-secondary)]">
                        {activeVision.vision_summary}
                      </p>
                    </div>
                  )}
                  {activeCcProject === "nuel" && nuelData && (
                    <pre className="max-h-48 overflow-auto rounded-lg border border-[var(--color-surface-4)] bg-black/30 p-3 font-mono text-[0.62rem] text-[var(--color-text-muted)]">
                      {JSON.stringify(nuelData, null, 2)}
                    </pre>
                  )}
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle>Live task bus</CardTitle>
                  <CardDescription>Redis channel · nexus:cc:events</CardDescription>
                </CardHeader>
                <CardContent>
                  <ul className="max-h-64 space-y-1 overflow-auto font-mono text-[0.58rem] text-[var(--color-text-muted)]">
                    {lastMessages.map((m, i) => (
                      <li key={i} className="truncate border-b border-[var(--color-surface-4)]/40 py-1">
                        {String(m.parsed?.type ?? m.raw.slice(0, 120))}
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
              </div>
            </div>
          )}

          {tab === "intel" && (
            <div className="grid gap-4 lg:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Lead scoring</CardTitle>
                  <CardDescription>Management Ahu — heuristic 1–100 (swap for LLM later)</CardDescription>
                </CardHeader>
                <CardContent className="flex flex-col gap-2">
                  <label className="font-mono text-[0.58rem] text-[var(--color-text-muted)]">source</label>
                  <input
                    className="rounded border border-[var(--color-surface-4)] bg-[var(--color-surface-0)] px-2 py-1 font-mono text-xs"
                    value={leadForm.source}
                    onChange={(e) => setLeadForm((f) => ({ ...f, source: e.target.value }))}
                  />
                  <label className="font-mono text-[0.58rem] text-[var(--color-text-muted)]">company</label>
                  <input
                    className="rounded border border-[var(--color-surface-4)] bg-[var(--color-surface-0)] px-2 py-1 font-mono text-xs"
                    value={leadForm.company}
                    onChange={(e) => setLeadForm((f) => ({ ...f, company: e.target.value }))}
                  />
                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <label className="font-mono text-[0.58rem] text-[var(--color-text-muted)]">msgs</label>
                      <input
                        type="number"
                        className="w-full rounded border border-[var(--color-surface-4)] bg-[var(--color-surface-0)] px-2 py-1 font-mono text-xs"
                        value={leadForm.message_count}
                        onChange={(e) => setLeadForm((f) => ({ ...f, message_count: Number(e.target.value) }))}
                      />
                    </div>
                    <div>
                      <label className="font-mono text-[0.58rem] text-[var(--color-text-muted)]">days</label>
                      <input
                        type="number"
                        className="w-full rounded border border-[var(--color-surface-4)] bg-[var(--color-surface-0)] px-2 py-1 font-mono text-xs"
                        value={leadForm.days_since_contact}
                        onChange={(e) => setLeadForm((f) => ({ ...f, days_since_contact: Number(e.target.value) }))}
                      />
                    </div>
                  </div>
                  <label className="font-mono text-[0.58rem] text-[var(--color-text-muted)]">tags (comma)</label>
                  <input
                    className="rounded border border-[var(--color-surface-4)] bg-[var(--color-surface-0)] px-2 py-1 font-mono text-xs"
                    value={leadForm.tags}
                    onChange={(e) => setLeadForm((f) => ({ ...f, tags: e.target.value }))}
                  />
                  <Button type="button" onClick={() => void scoreLead()}>
                    score lead
                  </Button>
                  {leadResult && (
                    <pre className="mt-2 rounded-lg bg-black/35 p-2 font-mono text-[0.62rem] text-[var(--color-success)]">
                      {JSON.stringify(leadResult, null, 2)}
                    </pre>
                  )}
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle>Heshbonator core</CardTitle>
                  <CardDescription>Swarm signals + reply stubs</CardDescription>
                </CardHeader>
                <CardContent className="flex flex-col gap-3">
                  <Button type="button" variant="outline" onClick={() => void loadSentiment()}>
                    sentiment heatmap
                  </Button>
                  {heatMsg && (
                    <p className="font-mono text-[0.62rem] text-[var(--color-text-secondary)]">{heatMsg}</p>
                  )}
                  <Button
                    type="button"
                    variant="outline"
                    onClick={async () => {
                      const res = await fetch(`${API_BASE}/api/cc/heshbonator/predict-reply`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ goal: "close_deal", last_messages: ["מחיר סופי?"] }),
                      });
                      const j = await res.json();
                      setIntelPreview((j.suggested_replies as string[]).join("\n"));
                    }}
                  >
                    response predictor
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={async () => {
                      const res = await fetch(`${API_BASE}/api/cc/heshbonator/digital-shadow`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                          target_id: "demo",
                          messages: [{ text: "Thanks!! 😊 deal closed" }, { text: "Send invoice tomorrow" }],
                        }),
                      });
                      const j = await res.json();
                      setIntelPreview(JSON.stringify(j.digital_shadow, null, 2));
                    }}
                  >
                    digital shadow (demo)
                  </Button>
                  {intelPreview && (
                    <pre className="max-h-36 overflow-auto rounded-lg border border-[var(--color-surface-4)] bg-black/35 p-2 font-mono text-[0.6rem] text-[var(--color-text-secondary)]">
                      {intelPreview}
                    </pre>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {tab === "infra" && (
            <div className="grid gap-4 lg:grid-cols-[1fr_360px]">
              <Card>
                <CardHeader>
                  <CardTitle>Global resource heatmap</CardTitle>
                  <CardDescription>3D CPU/RAM pillars · live heartbeats</CardDescription>
                </CardHeader>
                <CardContent>
                  <ClusterResourceGlobe3D nodes={heatData?.nodes ?? []} />
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle>Sentinel recovery</CardTitle>
                  <CardDescription>Publish RESTART_WORKER to workers via Redis</CardDescription>
                </CardHeader>
                <CardContent className="flex flex-col gap-2">
                  <input
                    className="rounded border border-[var(--color-surface-4)] bg-[var(--color-surface-0)] px-2 py-1 font-mono text-xs"
                    placeholder="NODE_ID or *"
                    value={recoverNode}
                    onChange={(e) => setRecoverNode(e.target.value)}
                  />
                  <Button type="button" onClick={() => void recoverWorker()}>
                    recover worker
                  </Button>
                  {recoverMsg && (
                    <pre className="max-h-40 overflow-auto rounded bg-black/35 p-2 font-mono text-[0.58rem]">{recoverMsg}</pre>
                  )}
                  <Button
                    type="button"
                    variant="destructive"
                    onClick={() => void flightAdvanced()}
                  >
                    {flightArmed ? "confirm FLIGHT ADVANCED" : "flight mode advanced"}
                  </Button>
                  <p className="text-[0.58rem] text-[var(--color-danger)]">
                    Seals blob (optional Fernet), kill-switch phase-1, strips integration env from API process.
                  </p>
                  {flightMsg && (
                    <pre className="max-h-40 overflow-auto rounded bg-black/35 p-2 font-mono text-[0.58rem]">{flightMsg}</pre>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {tab === "finance" && (
            <div className="grid gap-4 lg:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>ROI tracker</CardTitle>
                  <CardDescription>Redis hash nexus:cc:roi</CardDescription>
                </CardHeader>
                <CardContent>
                  <pre className="max-h-72 overflow-auto rounded-lg border border-[var(--color-surface-4)] bg-black/30 p-3 font-mono text-[0.62rem] text-[var(--color-text-muted)]">
                    {JSON.stringify(roiData?.projects ?? {}, null, 2)}
                  </pre>
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle>Crypto / Polymarket HUD</CardTitle>
                  <CardDescription>Bot PnL + wallet stub</CardDescription>
                </CardHeader>
                <CardContent>
                  <pre className="max-h-72 overflow-auto rounded-lg border border-[var(--color-surface-4)] bg-black/30 p-3 font-mono text-[0.62rem] text-[var(--color-text-muted)]">
                    {JSON.stringify(cryptoData ?? {}, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            </div>
          )}
        </motion.div>
      </AnimatePresence>
    </motion.div>
  );
}
