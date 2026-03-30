/**
 * Nexus API client — typed wrappers around the FastAPI backend.
 *
 * All fetch calls go through the `apiFetch` helper so the base URL
 * is configured in one place.  Change API_BASE to point at a remote
 * host when deploying.
 */

const _SERVER_API_BASE = (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8001").trim();

/** Browser: same-origin `/api` (Next rewrites → backend). Server: absolute URL. */
export const API_BASE = typeof window !== "undefined" ? "" : _SERVER_API_BASE;

/** WebSocket base for the active API target. */
export function apiWsBase(): string {
  if (typeof window !== "undefined" && API_BASE === "") {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${window.location.host}`;
  }
  return _SERVER_API_BASE.replace(/^https/, "wss").replace(/^http/, "ws");
}

// ── Generic fetch helper ──────────────────────────────────────────────────────

function _resolveApiUrl(path: string): string {
  const p = (path || "").trim();
  if (/^https?:\/\//i.test(p)) {
    return p;
  }
  const base = typeof window !== "undefined" ? API_BASE : _SERVER_API_BASE;
  return `${base}${p.startsWith("/") ? p : `/${p}`}`;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(_resolveApiUrl(path), {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json() as Promise<T>;
}

// SWR-compatible fetcher typed for a specific response shape.
// Usage: useSWR<MyType>(key, swrFetcher<MyType>)
export function swrFetcher<T>(path: string): Promise<T> {
  return apiFetch<T>(path);
}

// ── Types (mirror nexus/api/schemas.py) ──────────────────────────────────────

export type NodeRole = "master" | "worker";

export interface NodeStatus {
  node_id: string;
  role: NodeRole;
  cpu_percent: number;
  ram_used_mb: number;
  active_jobs: number;
  last_seen: string;
  online: boolean;
  // Phase 3 hardware identity fields
  local_ip?: string;
  cpu_model?: string;
  gpu_model?: string;
  ram_total_mb?: number;
  active_tasks_count?: number;
  os_info?: string;
  // Phase 4 extended hardware
  motherboard?: string;
  cpu_temp_c?: number;
  display_name?: string;
}

export interface ResourceCaps {
  cpu_cap_percent: number;
  ram_cap_mb: number;
}

export interface QueueStats {
  queue_name: string;
  pending_jobs: number;
}

export interface ClusterStatusResponse {
  nodes: NodeStatus[];
  master_resource_caps: ResourceCaps;
  queues: QueueStats[];
  timestamp: string;
  worker_cpu_avg_percent?: number;
  telefix_context_active?: boolean;
}

/** GET /api/cluster/health — fleet grid + swarm tail + target heatmap */
export interface ClusterHealthNode {
  node_id: string;
  role: NodeRole;
  online: boolean;
  status: string;
  probe_latency_ms: number;
  cpu_percent: number;
  ram_used_mb: number;
  active_jobs: number;
  last_seen: string;
  local_ip?: string;
  cpu_model?: string;
  gpu_model?: string;
  ram_total_mb?: number;
  os_info?: string;
  display_label: string;
  // Phase 4 extended hardware
  motherboard?: string;
  cpu_temp_c?: number;
  display_name?: string;
}

export interface TargetHeatCell {
  id: string;
  label: string;
  intensity: number;
}

export interface ClusterHealthResponse {
  redis_ok: boolean;
  redis_ping_ms: number | null;
  nodes: ClusterHealthNode[];
  workers_online: number;
  swarm_activity: string[];
  targets: TargetHeatCell[];
  timestamp: string;
}

export interface HitlPendingItem {
  request_id: string;
  task_id: string;
  task_type: string;
  context: string;
  requested_at: string;
  expires_at: string | null;
}

export interface HitlPendingResponse {
  items: HitlPendingItem[];
  total: number;
}

export interface HitlResolveRequest {
  request_id: string;
  approved: boolean;
  reviewer_id?: string;
  reason?: string;
}

export interface HitlResolveResponse {
  request_id: string;
  task_id: string;
  approved: boolean;
  reviewer_id: string;
  responded_at: string;
  message: string;
}

// ── Stuck state & Force Run ───────────────────────────────────────────────────

export interface StuckStateResponse {
  stuck: boolean;
  action_type: string;
  confidence: number;
  threshold: number;
  gap: number;
  task_type: string;
  task_params: Record<string, unknown>;
  detected_at: string;
}

export interface ThresholdInfoResponse {
  action_type: string;
  effective_threshold: number;
  default_threshold: number;
  approval_streak: number;
  streak_needed: number;
}

export interface ForceRunResponse {
  task_id: string;
  message: string;
}

export function forceRunTask(
  task_type: string,
  task_params: Record<string, unknown> = {},
): Promise<ForceRunResponse> {
  return apiFetch<ForceRunResponse>("/api/business/force-run", {
    method: "POST",
    body: JSON.stringify({ task_type, task_params, reviewer_id: "dashboard" }),
  });
}

/** Full NEXUS kill-switch — see `nexus.shared.kill_switch` */
export async function postFullKillSwitch(opts: {
  confirmPhrase: string;
  evacuate?: boolean;
  authToken?: string;
}): Promise<Record<string, unknown>> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.authToken) headers["X-Nexus-Kill-Auth"] = opts.authToken;
  const res = await fetch(`${API_BASE}/api/system/kill-switch`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      confirm: opts.confirmPhrase,
      evacuate: !!opts.evacuate,
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json() as Promise<Record<string, unknown>>;
}

// ── Business / Operational Intelligence ──────────────────────────────────────

export interface FleetGroupAssetRow {
  group_id: string;
  group_name: string;
  member_count: number;
  premium_count: number;
  owner_session: string | null;
  status: string;
  last_automation: string | null;
}

/** Staged-session Telethon mapper — reach & premium density per account */
export interface MapperFleetSessionRow {
  session_id: string;
  session_label: string;
  phone: string | null;
  total_groups: number;
  total_reach: number;
  premium_density: number | null;
  mapper_status: string;
}

export interface FleetAssetsResponse {
  groups: FleetGroupAssetRow[];
  db_available: boolean;
  queried_at: string;
  mapper_fleet?: MapperFleetSessionRow[];
  mapper_available?: boolean;
  mapper_generated_at?: string | null;
}

export interface WarRoomIntelResponse {
  updated_at: string;
  master_confidence_pct: number;
  openclaw_sentiment: number;
  top_alpha_channel: string;
  paper: {
    virtual_pnl: number;
    wins: number;
    losses: number;
    total_trades: number;
    win_rate: number;
  };
  real_pnl_usd: number;
  sim_pnl_usd: number;
  race_to_1000_pct: number;
  race_target_profit_usd: number;
  kelly_fraction: number;
  swarm_workers_seen: number;
  swarm_whale_hits: number;
  aggressive_strike: boolean;
  strike_reinvest_pct: number;
  sentiment_heatmap: number[][];
}

export function getWarRoomIntel(): Promise<WarRoomIntelResponse> {
  return apiFetch<WarRoomIntelResponse>("/api/business/war-room");
}

export interface BusinessStatsResponse {
  // Groups & targets
  total_managed_groups: number;
  total_targets: number;
  source_groups: number;
  target_groups: number;
  // Users
  total_scraped_users: number;
  total_users_pipeline: number;
  // Sessions (Telethon .json files on disk)
  active_sessions: number;
  frozen_sessions: number;
  manager_sessions: number;
  // Last run timestamps (human-readable UTC strings or null)
  last_scraper_run: string | null;
  last_adder_run: string | null;
  last_forecast_run: string | null;
  // Forecast history
  forecast_history: string[];
  // Meta
  db_available: boolean;
  queried_at: string;
}

// ── Scrape status ─────────────────────────────────────────────────────────────

export type ScrapeStatusValue =
  | "idle"
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "low_resources";

export interface ScrapeStatusResponse {
  status: ScrapeStatusValue;
  detail: string;
  updated_at: string;
}

export interface ForceScrapeResponse {
  task_id: string;
  message: string;
}

export function forceScrape(sources: string[] = [], force = true): Promise<ForceScrapeResponse> {
  return apiFetch<ForceScrapeResponse>("/api/business/force-scrape", {
    method: "POST",
    body: JSON.stringify({ sources, force }),
  });
}

// ── Engine State (RGB sync) ───────────────────────────────────────────────────

export type EngineStateValue = "idle" | "calculating" | "dispatching" | "warning";

export interface EngineStateResponse {
  state: EngineStateValue;
  updated_at: string;
}

// ── Decision Engine ───────────────────────────────────────────────────────────

export interface DecisionItem {
  decision_type: string;
  title: string;
  reasoning: string;
  confidence: number;
  roi_impact: string;
  action_task_type: string;
  requires_approval: boolean;
  created_at: string;
}

export interface DecisionsResponse {
  decisions: DecisionItem[];
  total: number;
  queried_at: string;
}

// ── Agent Log ─────────────────────────────────────────────────────────────────

export interface AgentLogEntry {
  ts: string;
  level: string;
  message: string;
  metadata: Record<string, unknown>;
}

export interface AgentLogResponse {
  entries: AgentLogEntry[];
  total: number;
}

// ── API calls ─────────────────────────────────────────────────────────────────

export function resolveHitl(body: HitlResolveRequest): Promise<HitlResolveResponse> {
  return apiFetch<HitlResolveResponse>("/api/hitl/resolve", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export interface ScaleWorkerResponse {
  message:  string;
  command:  string;
  success?: boolean;
}

export function scaleWorker(): Promise<ScaleWorkerResponse> {
  return apiFetch<ScaleWorkerResponse>("/api/business/scale-worker", { method: "POST" });
}

// ── Content Factory ───────────────────────────────────────────────────────────

export interface ContentPreviewItem {
  preview_id: string;
  project_id: string;
  target_group_id: string;
  niche: string;
  post_text: string;
  image_path: string | null;
  requires_hitl: boolean;
  hitl_reason: string;
  status: string;
  created_at: string;
}

export interface ContentPreviewsResponse {
  previews: ContentPreviewItem[];
  total: number;
}

export interface FactoryActiveResponse {
  active: boolean;
  status: string;
  detail: string;
}

export interface ContentResolveResponse {
  preview_id: string;
  action: string;
  message: string;
}

export function resolveContent(
  preview_id: string,
  action: "approve" | "reject" | "regenerate",
  reviewer_id = "dashboard",
): Promise<ContentResolveResponse> {
  return apiFetch<ContentResolveResponse>("/api/content/resolve", {
    method: "POST",
    body: JSON.stringify({ preview_id, action, reviewer_id }),
  });
}

// ── Profit Report ─────────────────────────────────────────────────────────────

export interface ProfitReportResponse {
  db_available: boolean;
  window_hours: number;
  new_scraped_users: number;
  total_scraped_users: number;
  total_pipeline: number;
  target_groups: number;
  source_groups: number;
  estimated_roi: number;
  active_sessions: number;
  frozen_sessions: number;
  manager_sessions: number;
  health_ratio: number;
  last_scraper_run: string | null;
  last_adder_run: string | null;
  forecast_history: string[];
  generated_at: string;
}

export interface ReportStatusResponse {
  sending: boolean;
  started_at: string;
}

export interface WindowedStatsResponse {
  window_minutes: number;
  new_scraped_users_window: number;
  new_pipeline_users_window: number;
  total_managed_groups: number;
  total_scraped_users: number;
  total_users_pipeline: number;
  active_sessions: number;
  frozen_sessions: number;
  manager_sessions: number;
  total_targets: number;
  source_groups: number;
  target_groups: number;
  last_scraper_run: string | null;
  last_adder_run: string | null;
  last_forecast_run: string | null;
  forecast_history: string[];
  db_available: boolean;
  queried_at: string;
}

// ── Notifications / ChatOps status ───────────────────────────────────────────

export interface ChatOpsProviderStatus {
  name: string;
  connected: boolean;
  mode: string;
  detail: string;
}

export interface ChatOpsStatusResponse {
  providers: ChatOpsProviderStatus[];
  any_connected: boolean;
}

// ── Super-Scraper ─────────────────────────────────────────────────────────────

export interface SuperScraperStatusResponse {
  status: string;
  detail: string;
  updated_at: string;
  candidates_pending: number;
}

export function triggerSuperScrape(
  stealthOverride = false,
): Promise<{ task_id: string; message: string }> {
  return apiFetch("/api/business/force-scrape", {
    method: "POST",
    body: JSON.stringify({ sources: [], force: true, stealth_override: stealthOverride }),
  });
}

export function triggerContentFactory(
  project_id: string,
  target_group_id: string,
  custom_text = "",
): Promise<{ task_id: string; message: string }> {
  return apiFetch("/api/content/generate", {
    method: "POST",
    body: JSON.stringify({ project_id, target_group_id, custom_text }),
  });
}

// ── Evolution / First-Birth Protocol ─────────────────────────────────────────

export type ProjectBirthStatus =
  | "scouting"
  | "architecting"
  | "pending_birth"
  | "deploying"
  | "live"
  | "rejected"
  | "failed";

export interface IncubatorProjectItem {
  project_id: string;
  name: string;
  niche_id: string;
  niche_description: string;
  ai_logic: string;
  file_path: string;
  estimated_roi_pct: number;
  confidence: number;
  status: ProjectBirthStatus;
  created_at: string;
  updated_at: string;
  hitl_request_id: string;
  deployed_worker_id: string;
  rejection_reason: string;
}

export interface IncubatorResponse {
  projects: IncubatorProjectItem[];
  total: number;
  first_birth_approved: boolean;
  queried_at: string;
}

export interface EvolutionStateResponse {
  state: string;
  updated_at: string;
  first_birth_approved: boolean;
}

export interface BirthResolveRequest {
  request_id: string;
  approved: boolean;
  reviewer_id?: string;
  reason?: string;
}

export interface BirthResolveResponse {
  request_id: string;
  project_id: string;
  approved: boolean;
  reviewer_id: string;
  responded_at: string;
  message: string;
}

export function getEvolutionIncubatorProjects(): Promise<IncubatorResponse> {
  return apiFetch<IncubatorResponse>("/api/evolution/incubator");
}

export function getEvolutionState(): Promise<EvolutionStateResponse> {
  return apiFetch<EvolutionStateResponse>("/api/evolution/state");
}

export function resolveBirth(body: BirthResolveRequest): Promise<BirthResolveResponse> {
  return apiFetch<BirthResolveResponse>("/api/evolution/birth-resolve", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function triggerScout(): Promise<{ message: string }> {
  return apiFetch("/api/evolution/scout", { method: "POST" });
}

// ── Incubator / Evolution Engine ──────────────────────────────────────────────

export interface NicheItem {
  name: string;
  source: string;
  keywords: string[];
  volume_score: number;
  velocity_score: number;
  monetisation_score: number;
  composite: number;
  confidence: number;
  roi_estimate: string;
  discovered_at: string;
  raw_data: Record<string, unknown>;
}

export interface NichesResponse {
  niches: NicheItem[];
  total: number;
  last_run: string | null;
  state: string;
}

export interface IncubatorProject {
  project_id: string;
  name: string;
  slug: string;
  niche: string;
  niche_source: string;
  generation: number;
  status: "pending_review" | "live" | "paused" | "killed";
  path: string;
  born_at: string;
  last_updated: string;
  confidence_at_birth: number;
  estimated_roi: string;
  files_generated: string[];
  stats: Record<string, unknown>;
  god_mode_deployed: boolean;
  age_hours: number;
}

export interface IncubatorProjectsResponse {
  projects: IncubatorProject[];
  total: number;
}

export interface IncubatorStateResponse {
  architect_state: string;
  scout_state: string;
  god_mode: boolean;
  total_projects: number;
  live_projects: number;
}

export interface GodModeResponse {
  enabled: boolean;
  message: string;
}

export function getNiches(): Promise<NichesResponse> {
  return apiFetch<NichesResponse>("/api/incubator/niches");
}

export function refreshNiches(): Promise<{ message: string }> {
  return apiFetch("/api/incubator/niches/refresh", { method: "POST" });
}

export function getIncubatorProjects(): Promise<IncubatorProjectsResponse> {
  return apiFetch<IncubatorProjectsResponse>("/api/incubator/projects");
}

export function getIncubatorState(): Promise<IncubatorStateResponse> {
  return apiFetch<IncubatorStateResponse>("/api/incubator/state");
}

export function getGodMode(): Promise<GodModeResponse> {
  return apiFetch<GodModeResponse>("/api/incubator/god-mode");
}

export function setGodMode(enabled: boolean): Promise<GodModeResponse> {
  return apiFetch<GodModeResponse>("/api/incubator/god-mode", {
    method: "POST",
    body: JSON.stringify({ enabled }),
  });
}

export function generateProject(body: {
  niche_name: string;
  keywords?: string[];
  roi_estimate?: string;
  confidence?: number;
  source?: string;
  custom_brief?: string;
}): Promise<{ project_id: string; name: string; slug: string; status: string; path: string; message: string }> {
  return apiFetch("/api/incubator/generate", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function approveProject(project_id: string): Promise<{ project_id: string; status: string; message: string }> {
  return apiFetch(`/api/incubator/approve/${project_id}`, { method: "POST" });
}

export function killProject(project_id: string): Promise<{ project_id: string; status: string; message: string }> {
  return apiFetch(`/api/incubator/kill/${project_id}`, { method: "POST" });
}

export function activateKillSwitch(): Promise<{ status: string; projects_killed: number; god_mode_disabled: boolean; message: string }> {
  return apiFetch("/api/incubator/kill-switch", { method: "POST" });
}

export function clearKillSwitch(): Promise<{ status: string; message: string }> {
  return apiFetch("/api/incubator/kill-switch/clear", { method: "POST" });
}

// ── Supervisor Watchdog ───────────────────────────────────────────────────────

export type SupervisorWorkerStatusValue = "healthy" | "recovering" | "critical";

export interface SupervisorWorkerStatus {
  name:            string;
  node_id:         string;
  /** "healthy" | "recovering" | "critical" */
  status:          SupervisorWorkerStatusValue;
  strike_count:    number;
  pid:             number | null;
  last_restart_ts: number;
  first_strike_ts: number;
}

export interface SupervisorStatusResponse {
  workers:      SupervisorWorkerStatus[];
  updated_at:   string;
  any_critical: boolean;
}

export interface SupervisorResetResponse {
  worker:  string;
  success: boolean;
  message: string;
}

export function getSupervisorStatus(): Promise<SupervisorStatusResponse> {
  return apiFetch<SupervisorStatusResponse>("/api/business/supervisor-status");
}

export function resetSupervisorWorker(workerName: string): Promise<SupervisorResetResponse> {
  return apiFetch<SupervisorResetResponse>(
    `/api/business/supervisor-reset/${encodeURIComponent(workerName)}`,
    { method: "POST" },
  );
}

// ── Auto-Deployer / Phase 15 ──────────────────────────────────────────────────

export type DeployStepName =
  | "connecting"
  | "stopping_worker"
  | "uploading"
  | "installing_deps"
  | "bootstrapping"      // legacy alias
  | "restarting"
  | "done"
  | "error";

export type DeployStepStatus = "running" | "done" | "error";

export interface DeployProgressEvent {
  node_id: string;
  step: DeployStepName;
  status: DeployStepStatus;
  detail: string;
  label?: string;   // human-readable ticker text (e.g. "Installing deps…")
  ts: string;
}

export interface DeployResponse {
  job_id: string;
  targets: string[] | null;
  message: string;
  started_at: string;
}

export interface DeployStatusResponse {
  nodes: Record<string, DeployProgressEvent | null>;
  queried_at: string;
}

export function triggerClusterDeploy(
  node_ids?: string[],
): Promise<DeployResponse> {
  return apiFetch<DeployResponse>("/api/deploy/cluster", {
    method: "POST",
    body: JSON.stringify({ node_ids: node_ids ?? null }),
  });
}

/** Phase 18 — Nexus-Push: sync directly to WORKER_IP laptop. */
export function triggerSync(): Promise<DeployResponse> {
  return apiFetch<DeployResponse>("/api/deploy/sync", { method: "POST" });
}

export function getDeployStatus(): Promise<DeployStatusResponse> {
  return apiFetch<DeployStatusResponse>("/api/deploy/status");
}

/**
 * Open a Server-Sent Events stream for a single node's deploy progress.
 * Returns an EventSource — caller is responsible for closing it.
 */
export function openDeployProgressStream(node_id: string): EventSource {
  return new EventSource(`${API_BASE}/api/deploy/progress/${node_id}`);
}

// ── Paper Trading ─────────────────────────────────────────────────────────────

export interface VirtualTradeEntry {
  id:                   string;
  timestamp:            string;
  signal:               string;
  direction:            string;
  entry_yes_price:      number;
  entry_binance_price:  number;
  virtual_amount_usd:   number;
  potential_profit_usd: number;
  market_question:      string;
  market_id:            string | null;
  status:               string;
}

export interface PaperTradesResponse {
  trades:                VirtualTradeEntry[];
  total:                 number;
  total_virtual_pnl:     number;
  paper_trading_enabled: boolean;
}

export interface TradingModeResponse {
  paper_trading:       boolean;
  virtual_trade_count: number;
}

export function getPaperTrades(): Promise<PaperTradesResponse> {
  return apiFetch<PaperTradesResponse>("/api/prediction/paper-trades");
}

export function getTradingMode(): Promise<TradingModeResponse> {
  return apiFetch<TradingModeResponse>("/api/prediction/trading-mode");
}

// ── Config / Settings ─────────────────────────────────────────────────────────

export interface ConfigResponse {
  power_limit:               number;
  max_workers:               number;
  master_cpu_cap_percent:   number;
  master_ram_cap_mb:        number;
  worker_max_jobs:          number;
  task_default_timeout:     number;
  worker_max_tries:         number;
  worker_ip:                string;
  worker_ssh_user:          string;
  worker_deploy_root_linux: string;
  log_level:                string;
}

export type ConfigPatch = Partial<ConfigResponse>;

export function getConfig(): Promise<ConfigResponse> {
  return apiFetch<ConfigResponse>("/api/config");
}

export function patchConfig(patch: ConfigPatch): Promise<ConfigResponse> {
  return apiFetch<ConfigResponse>("/api/config", {
    method: "PATCH",
    body: JSON.stringify(patch),
  });
}

// ── Project Hub ───────────────────────────────────────────────────────────────

export interface EnvKey {
  key:   string;
  value: string;
}

export interface BudgetStats {
  available:          boolean;
  today_income:       number;
  today_expense:      number;
  today_pnl:          number;
  wallet_count:       number;
  wallets:            Array<{ name: string; currency: string; icon: string }>;
  total_transactions: number;
  currency:           string;
  queried_at:         string;
  reason?:            string;
}

export interface BudgetWidgetResponse {
  available:        boolean;
  daily_pnl?:       number;
  currency?:        string;
  status?:          string;
  project_path?:    string;
  last_transaction?: string;
}

export interface ProjectInfo {
  id:                string;
  name:              string;
  icon:              string;
  description:       string;
  path:              string;
  exists:            boolean;
  language:          string;
  framework:         string;
  status:            "running" | "stopped" | "unknown";
  pid:               number | null;
  env_keys:          EnvKey[];
  entry_point:       string;
  last_modified:     string;
  start_cmd:         string;
  budget_stats:      Partial<BudgetStats>;
  scanned_at:        string;
  size_mb?:          number;
  running_processes: string[];
  config_keys:       string[];
  live_stats:        Record<string, unknown>;
  stack:             string[];
}

export interface ProjectHubResponse {
  projects:       ProjectInfo[];
  total:          number;
  total_count?:   number;
  running_count?: number;
  total_size_mb?: number;
  last_scan:      string;
}

/** @deprecated Use ProjectHubResponse (project hub) or IncubatorProjectsResponse (incubator) */
export type ProjectsResponse = ProjectHubResponse;

export interface ProjectActionResponse {
  project_id: string;
  action:     string;
  success:    boolean;
  message:    string;
}

export function getProjects(): Promise<ProjectHubResponse> {
  return apiFetch<ProjectHubResponse>("/api/projects");
}

export function getProject(name: string): Promise<ProjectInfo> {
  return apiFetch<ProjectInfo>(`/api/projects/${name}`);
}

export function projectAction(name: string, action: string): Promise<{ project: string; action: string; status: string; message: string }> {
  return apiFetch(`/api/projects/${name}/action`, {
    method: "POST",
    body: JSON.stringify({ action }),
  });
}

export function refreshProjects(): Promise<ProjectHubResponse> {
  return apiFetch<ProjectHubResponse>("/api/projects/refresh");
}

export function getBudgetStats(): Promise<BudgetStats> {
  return apiFetch<BudgetStats>("/api/projects/budget");
}

export function getBudgetWidget(): Promise<BudgetWidgetResponse> {
  return apiFetch<BudgetWidgetResponse>("/api/projects/budget/widget");
}

export function triggerProjectScan(): Promise<{ status: string; projects_scanned: number; running_projects: number; scanned_at: string }> {
  return apiFetch("/api/projects/scan", { method: "POST" });
}

export function startProject(project_id: string): Promise<ProjectActionResponse> {
  return apiFetch<ProjectActionResponse>(`/api/projects/${project_id}/start`, { method: "POST" });
}

export function stopProject(project_id: string): Promise<ProjectActionResponse> {
  return apiFetch<ProjectActionResponse>(`/api/projects/${project_id}/stop`, { method: "POST" });
}

// ── Arbitrage Visualizer ──────────────────────────────────────────────────────

export interface ArbitrageDataPoint {
  timestamp:     string;
  binance_price: number | null;
  poly_price:    number | null;
}

export interface ArbitrageChartDataResponse {
  data:  ArbitrageDataPoint[];
  total: number;
}

export function getArbitrageChartData(): Promise<ArbitrageChartDataResponse> {
  return apiFetch<ArbitrageChartDataResponse>("/api/prediction/chart-data");
}

// ── System Panic / Kill-Switch ────────────────────────────────────────────────

export interface PanicStateResponse {
  panic:        boolean;
  activated_at?: string;
  /** Wallet brake / kill-switch metadata may use ``ts`` instead of ``activated_at``. */
  ts?:          string;
  reason?:       string;
  activated_by?: string;
}

/** GET /api/system/power-profile */
export interface PowerProfileResponse {
  ok: boolean;
  source?: string;
  effective_mode?: string;
  display_label?: string;
  cpu_cap_percent?: number;
  affinity_cores?: number[];
  affinity_applied?: boolean;
  logical_cores?: number | null;
  override?: string;
  scheduled_night?: boolean;
  idle_dropped_to_active?: boolean;
  seconds_since_input?: number | null;
  poly5m_cycle_seconds?: number;
  master_pid?: number | null;
  updated_at?: string | null;
  next_shift_local?: string;
  seconds_until_shift?: number;
  message?: string;
}

export interface PanicEngageResponse {
  status:            string;
  activated_at:      string;
  workers_terminated: string[];
  elapsed_ms:        number;
  cpu_percent:       number;
  ram_used_mb:       number;
  last_trade_price:  string;
}

export function getPanicState(): Promise<PanicStateResponse> {
  return apiFetch<PanicStateResponse>("/api/system/panic/state");
}

export function triggerPanic(): Promise<PanicEngageResponse> {
  return apiFetch<PanicEngageResponse>("/api/system/panic", { method: "POST" });
}

export function resetPanic(): Promise<{ status: string; message: string }> {
  return apiFetch<{ status: string; message: string }>("/api/system/panic/reset", { method: "POST" });
}

// ── Sentinel AI ───────────────────────────────────────────────────────────────

export interface SentinelStatusResponse {
  state: "active" | "stopped" | "offline" | "unknown";
  node_id: string;
  latency_ms: number | null;
  ram_pct: number | null;
  latency_bad_cycles: number;
  ram_bad_cycles: number;
  windows_worker_online: boolean | null;
  rpc_url: string;
  rpc_switched: boolean;
  updated_at: string;
}

export interface SentinelEvent {
  ts: string;
  event_type: string;
  trigger: string;
  metric_value: number;
  action_taken: string;
  reason_he: string;
  ai_reason_en: string;
}

export interface SentinelEventsResponse {
  events: SentinelEvent[];
  total: number;
}

export interface SentinelMetric {
  ts: string;
  latency_ms: number;
  ram_pct: number;
}

export interface SentinelMetricsResponse {
  metrics: SentinelMetric[];
  latency_threshold_ms: number;
  memory_threshold_pct: number;
}

export function getSentinelStatus(): Promise<SentinelStatusResponse> {
  return apiFetch<SentinelStatusResponse>("/api/sentinel/status");
}

export function getSentinelEvents(limit = 20): Promise<SentinelEventsResponse> {
  return apiFetch<SentinelEventsResponse>(`/api/sentinel/events?limit=${limit}`);
}

export function getSentinelMetrics(limit = 30): Promise<SentinelMetricsResponse> {
  return apiFetch<SentinelMetricsResponse>(`/api/sentinel/metrics?limit=${limit}`);
}

// ── Session Commander ──────────────────────────────────────────────────────────

export interface SessionCommanderAccount {
  session_stem:     string;
  phone:            string | null;
  proxy_ip:         string | null;
  status:           string | null;
  health:           "green" | "yellow" | "red" | string | null | undefined;
  lease_worker_id:  string | null;
  lease_task_id:    string | null;
  lease_ttl_seconds: number | null;
}

export interface SessionCommanderResponse {
  accounts: SessionCommanderAccount[];
  total:    number;
  updated_at: string;
}

// ── Proxy ─────────────────────────────────────────────────────────────────────

export interface ProxyEntry {
  index:    number;
  label:    string;
  raw_line: string;
}

export interface ProxyStatusResponse {
  active_index:              number;
  active_label:              string | null;
  active_public_ip:          string | null;
  active_ip_country:         string | null;
  active_ip_city:            string | null;
  active_ip_isp:             string | null;
  last_rotation_at:          string | null;
  last_rotation_ago_seconds: number | null;
  total_rotations:           number;
  pool_size:                 number;
  provider:                  string;
  provider_plan:             string;
  proxies:                   ProxyEntry[];
}

export interface RotationEvent {
  ts:          string;
  to_label:    string;
  resolved_ip: string | null;
  trigger:     string;
}

export interface RotationHistoryResponse {
  events: RotationEvent[];
}

export interface RotateProxyResponse {
  new_label:   string;
  resolved_ip: string | null;
}

export function rotateProxy(): Promise<RotateProxyResponse> {
  return apiFetch<RotateProxyResponse>("/api/proxy/rotate", { method: "POST" });
}

// ── AI Terminal ───────────────────────────────────────────────────────────────

export interface AiTerminalReply {
  reply:           string;
  source:          string;
  thinking_steps?: string[] | null;
}

export function postAiTerminalChat(prompt: string): Promise<AiTerminalReply> {
  return apiFetch<AiTerminalReply>("/api/ai/chat", {
    method: "POST",
    body: JSON.stringify({ prompt }),
  });
}

export interface PersonalityMessage {
  text: string;
}

export interface PersonalityRequest {
  messages: PersonalityMessage[];
  note?: string;
}

export function postAiTerminalPersonality(body: PersonalityRequest): Promise<AiTerminalReply> {
  return apiFetch<AiTerminalReply>("/api/ai/personality", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function postStrategyMutation(): Promise<{ status: string; message: string }> {
  return apiFetch<{ status: string; message: string }>("/api/ai/strategy-mutation", {
    method: "POST",
  });
}

// ── Scan ─────────────────────────────────────────────────────────────────────

export interface ScanLogLine {
  ts:      string;
  level:   string;
  message?: string;
  msg?:    string;
  detail?: string;
  source?: string;
}

export interface ScanStatusResponse {
  running:       boolean;
  phase?:        string;
  progress_pct?: number;
  started_at?:   string;
  finished_at?:  string;
  error?:        string | null;
  nodes_online?:   number;
  nodes_found?:    number;
  tasks_queued?:   number;
  tasks_enqueued?: number;
  tasks_done?:     number;
  tasks_failed?:   number;
  queue_depth?:    number;
  elapsed_s?:      number;
  errors?:         string[];
}

export interface ScanHistoryEntry {
  run_id:          string;
  started_at:      string;
  finished_at?:    string;
  duration_s?:     number;
  tasks_count?:    number;
  status?:         string;
  phase?:          string;
  nodes_found?:    number;
  tasks_done?:     number;
  tasks_enqueued?: number;
  tasks_failed?:   number;
  error?:          string;
}

export interface ScanHistoryResponse {
  history: ScanHistoryEntry[];
  runs?:   ScanHistoryEntry[];
}

export function triggerScan(opts?: { force?: boolean }): Promise<{ status: string; message: string }> {
  return apiFetch<{ status: string; message: string }>("/api/scan/run", {
    method: "POST",
    body: opts ? JSON.stringify(opts) : undefined,
  });
}

// ── Prediction manual override ────────────────────────────────────────────────

export function postPredictionHalt(): Promise<{ status: string; message: string }> {
  return apiFetch<{ status: string; message: string }>("/api/prediction/manual-override", {
    method: "POST",
  });
}

export function postPredictionResume(): Promise<{ status: string; message: string }> {
  return apiFetch<{ status: string; message: string }>("/api/prediction/manual-override/clear", {
    method: "POST",
  });
}
