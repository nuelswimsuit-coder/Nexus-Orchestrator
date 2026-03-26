"use client";

/**
 * NexusContext — global shared data store.
 *
 * Fetches cluster status, business stats, and HITL pending once at the
 * top level so every page gets consistent data without duplicate requests.
 * Stealth mode and stealthOverride persist across all pages via this context.
 *
 * Also exposes `deployingNodes` — the set of node IDs currently receiving a
 * code update — so ClusterStatus can colour those nodes purple.
 */

import {
  createContext,
  useCallback,
  useContext,
  useState,
  type ReactNode,
} from "react";
import useSWR from "swr";
import { swrFetcher } from "@/lib/api";
import type {
  ClusterStatusResponse,
  BusinessStatsResponse,
  HitlPendingResponse,
  EngineStateResponse,
} from "@/lib/api";

export type DeployPhase = "idle" | "running" | "done" | "error";

interface NexusContextValue {
  cluster:     ClusterStatusResponse | undefined;
  bizStats:    BusinessStatsResponse | undefined;
  hitl:        HitlPendingResponse   | undefined;
  engineState: EngineStateResponse   | undefined;
  clusterLoading: boolean;
  bizLoading:     boolean;
  mutateHitl: () => void;
  // Deploy state — shared between DeployTerminal and Header button
  deployingNodes: Set<string>;
  setDeployingNode: (node_id: string, active: boolean) => void;
  deployPhase: DeployPhase;
  setDeployPhase: (phase: DeployPhase) => void;
  deployLiveStep?: string;
  setDeployLiveStep?: (step: string) => void;
  // Command Control Suite active project
  activeCcProject: string;
  setActiveCcProject: (project: string) => void;
}

const NexusContext = createContext<NexusContextValue>({
  cluster:        undefined,
  bizStats:       undefined,
  hitl:           undefined,
  engineState:    undefined,
  clusterLoading: true,
  bizLoading:     true,
  mutateHitl:     () => {},
  deployingNodes: new Set(),
  setDeployingNode: () => {},
  deployPhase: "idle",
  setDeployPhase: () => {},
  activeCcProject: "nuel",
  setActiveCcProject: () => {},
});

export function NexusProvider({ children }: { children: ReactNode }) {
  const { data: cluster, isLoading: clusterLoading } =
    useSWR<ClusterStatusResponse>(
      "/api/cluster/status",
      swrFetcher<ClusterStatusResponse>,
      { refreshInterval: 10_000 }
    );

  const { data: bizStats, isLoading: bizLoading } =
    useSWR<BusinessStatsResponse>(
      "/api/business/stats",
      swrFetcher<BusinessStatsResponse>,
      { refreshInterval: 60_000 }
    );

  const { data: hitl, mutate: mutateHitl } =
    useSWR<HitlPendingResponse>(
      "/api/hitl/pending",
      swrFetcher<HitlPendingResponse>,
      { refreshInterval: 2_000 }
    );

  const { data: engineState } =
    useSWR<EngineStateResponse>(
      "/api/business/engine-state",
      swrFetcher<EngineStateResponse>,
      { refreshInterval: 3_000 }
    );

  const [deployingNodes, setDeployingNodes] = useState<Set<string>>(new Set());
  const [deployPhase, setDeployPhase]       = useState<DeployPhase>("idle");
  const [activeCcProject, setActiveCcProject] = useState<string>("nuel");

  const setDeployingNode = useCallback((node_id: string, active: boolean) => {
    setDeployingNodes((prev) => {
      const next = new Set(prev);
      if (active) next.add(node_id);
      else next.delete(node_id);
      return next;
    });
  }, []);

  return (
    <NexusContext.Provider
      value={{
        cluster,
        bizStats,
        hitl,
        engineState,
        clusterLoading,
        bizLoading,
        mutateHitl,
        deployingNodes,
        setDeployingNode,
        deployPhase,
        setDeployPhase,
        activeCcProject,
        setActiveCcProject,
      }}
    >
      {children}
    </NexusContext.Provider>
  );
}

export function useNexus() {
  return useContext(NexusContext);
}
