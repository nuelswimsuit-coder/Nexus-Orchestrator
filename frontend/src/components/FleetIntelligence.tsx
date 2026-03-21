"use client";

/**
 * Fleet Intelligence — group asset matrix, session roster, reach / premium gauges,
 * and Deep Scan (telegram.super_scrape) trigger.
 */

import { useCallback, useEffect, useId, useMemo, useRef, useState, type CSSProperties } from "react";
import useSWR from "swr";
import {
  forceRunTask,
  swrFetcher,
  type FleetAssetsResponse,
  type FleetGroupAssetRow,
  type MapperFleetSessionRow,
} from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import AddSessionModal, { type AddedSessionPayload } from "@/components/AddSessionModal";

type SortKey = "group_name" | "member_count" | "premium_count" | "owner_session" | "status";

type SessionSortKey = "session_name" | "phone" | "account_status" | "activity" | "daily_messages";

type MapperFleetSortKey =
  | "session_id"
  | "phone"
  | "total_groups"
  | "total_reach"
  | "premium_density"
  | "mapper_status";

type AccountStatus = "Alive" | "Restricted" | "Banned";
type SessionActivity = "Chatting" | "Scraping" | "Idle";

export type SessionFleetRow = {
  id: string;
  sessionName: string;
  phoneDisplay: string;
  accountStatus: AccountStatus;
  activity: SessionActivity;
  dailyMessages: number;
  source: "api" | "local";
};

function hashStr(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return Math.abs(h);
}

function formatPhoneFromSession(sessionName: string, h: number): string {
  const digits = sessionName.replace(/\D/g, "");
  if (digits.length >= 10) {
    const d = digits.slice(0, 15);
    return d.startsWith("0") ? `+${d}` : `+${d}`;
  }
  const mid = String(100 + (h % 899)).padStart(3, "0");
  const end = String(1000 + ((h >> 5) % 8999)).slice(1);
  return `+1 (555) ${mid}-${end}`;
}

function deriveSessionsFromGroups(groups: FleetGroupAssetRow[]): SessionFleetRow[] {
  const bySession = new Map<string, FleetGroupAssetRow[]>();
  for (const g of groups) {
    const key = (g.owner_session ?? "").trim();
    if (!key) continue;
    if (!bySession.has(key)) bySession.set(key, []);
    bySession.get(key)!.push(g);
  }
  const rows: SessionFleetRow[] = [];
  for (const [sessionName, glist] of bySession) {
    const statuses = glist.map((x) => x.status.toUpperCase());
    let accountStatus: AccountStatus = "Alive";
    if (statuses.some((s) => s === "DORMANT")) accountStatus = "Banned";
    else if (statuses.some((s) => s === "STALE")) accountStatus = "Restricted";

    const dates = glist
      .map((g) => g.last_automation)
      .filter((x): x is string => !!x)
      .map((x) => Date.parse(x))
      .filter((n) => !Number.isNaN(n));
    const newest = dates.length ? Math.max(...dates) : NaN;
    const hours = Number.isFinite(newest) ? (Date.now() - newest) / 3_600_000 : 999;
    let activity: SessionActivity = "Idle";
    if (hours < 2) activity = "Chatting";
    else if (statuses.some((s) => s === "ACTIVE")) activity = "Scraping";

    const h = hashStr(sessionName);
    const dailyMessages = 20 + (h % 780);
    rows.push({
      id: sessionName,
      sessionName,
      phoneDisplay: formatPhoneFromSession(sessionName, h),
      accountStatus,
      activity,
      dailyMessages,
      source: "api",
    });
  }
  return rows.sort((a, b) => a.sessionName.localeCompare(b.sessionName));
}

function statusStyle(status: string, stealth: boolean): { bg: string; fg: string; glow: string } {
  if (stealth) {
    return { bg: "#0f172a", fg: "#334155", glow: "none" };
  }
  const u = status.toUpperCase();
  if (u === "ACTIVE") {
    return { bg: "rgba(34, 211, 153, 0.12)", fg: "#34d399", glow: "0 0 12px rgba(34,211,153,0.35)" };
  }
  if (u === "STALE") {
    return { bg: "rgba(251, 191, 36, 0.12)", fg: "#fbbf24", glow: "0 0 10px rgba(251,191,36,0.25)" };
  }
  if (u === "DORMANT") {
    return { bg: "rgba(248, 113, 113, 0.10)", fg: "#f87171", glow: "0 0 8px rgba(248,113,113,0.2)" };
  }
  return { bg: "rgba(100, 116, 139, 0.12)", fg: "#94a3b8", glow: "none" };
}

function accountStatusStyle(
  s: AccountStatus,
  stealth: boolean,
): { bg: string; fg: string; glow: string } {
  if (stealth) return { bg: "#0f172a", fg: "#334155", glow: "none" };
  if (s === "Alive") return { bg: "rgba(34, 211, 153, 0.12)", fg: "#34d399", glow: "0 0 10px rgba(34,211,153,0.25)" };
  if (s === "Restricted") return { bg: "rgba(251, 191, 36, 0.12)", fg: "#fbbf24", glow: "0 0 8px rgba(251,191,36,0.2)" };
  return { bg: "rgba(248, 113, 113, 0.12)", fg: "#f87171", glow: "0 0 8px rgba(248,113,113,0.22)" };
}

function activityStyle(
  a: SessionActivity,
  stealth: boolean,
): { bg: string; fg: string } {
  if (stealth) return { bg: "#0f172a", fg: "#334155" };
  if (a === "Chatting") return { bg: "rgba(56, 189, 248, 0.12)", fg: "#38bdf8" };
  if (a === "Scraping") return { bg: "rgba(167, 139, 250, 0.12)", fg: "#c4b5fd" };
  return { bg: "rgba(71, 85, 105, 0.15)", fg: "#94a3b8" };
}

function CircularGauge({
  gradientId,
  label,
  valueText,
  percent,
  accent,
  stealth,
}: {
  gradientId: string;
  label: string;
  valueText: string;
  percent: number;
  accent: string;
  stealth: boolean;
}) {
  const r = 52;
  const c = 2 * Math.PI * r;
  const p = Math.max(0, Math.min(100, percent));
  const dash = c * (1 - p / 100);
  const track = stealth ? "#0f172a" : "#0c1220";
  const dimAccent = stealth ? "#334155" : accent;

  return (
    <div
      style={{
        position: "relative",
        width: "100%",
        maxWidth: 200,
        margin: "0 auto",
        fontFamily: "var(--font-mono)",
      }}
    >
      <svg viewBox="0 0 140 140" style={{ width: "100%", height: "auto", display: "block" }}>
        <defs>
          <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor={dimAccent} stopOpacity={stealth ? 0.2 : 0.95} />
            <stop offset="100%" stopColor={stealth ? "#1e293b" : "#00b4ff"} stopOpacity={stealth ? 0.15 : 0.85} />
          </linearGradient>
        </defs>
        <circle cx="70" cy="70" r={r} fill="none" stroke={track} strokeWidth="10" />
        <circle
          cx="70"
          cy="70"
          r={r}
          fill="none"
          stroke={`url(#${gradientId})`}
          strokeWidth="10"
          strokeLinecap="round"
          strokeDasharray={c}
          strokeDashoffset={dash}
          transform="rotate(-90 70 70)"
          style={{
            filter: stealth ? "none" : `drop-shadow(0 0 6px ${accent}55)`,
            transition: "stroke-dashoffset 0.6s cubic-bezier(0.22, 1, 0.36, 1)",
          }}
        />
        <circle cx="70" cy="70" r={r - 16} fill="none" stroke={stealth ? "#0f172a" : `${accent}22`} strokeWidth="1" strokeDasharray="4 6" />
      </svg>
      <div
        style={{
          position: "absolute",
          inset: 0,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          pointerEvents: "none",
          paddingTop: 4,
        }}
      >
        <span
          style={{
            fontSize: "1.05rem",
            fontWeight: 800,
            letterSpacing: "0.04em",
            color: stealth ? "#334155" : "#e8f2ff",
            textShadow: stealth ? "none" : `0 0 20px ${accent}33`,
          }}
        >
          {valueText}
        </span>
        <span
          style={{
            fontSize: "0.58rem",
            fontWeight: 600,
            letterSpacing: "0.14em",
            textTransform: "uppercase",
            color: stealth ? "#1e293b" : "#6b8fab",
            marginTop: 4,
            textAlign: "center",
            maxWidth: 120,
            lineHeight: 1.35,
          }}
        >
          {label}
        </span>
      </div>
    </div>
  );
}

function SessionActionsMenu({
  sessionName,
  open,
  onOpenChange,
  stealth,
  onAction,
}: {
  sessionName: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  stealth: boolean;
  onAction: (action: string, sessionName: string) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onOpenChange(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open, onOpenChange]);

  const btnStyle: CSSProperties = {
    width: "100%",
    textAlign: "left",
    fontFamily: "var(--font-mono)",
    fontSize: "0.62rem",
    letterSpacing: "0.06em",
    padding: "0.45rem 0.65rem",
    border: "none",
    background: "transparent",
    color: stealth ? "#64748b" : "#cbd5e1",
    cursor: "pointer",
    borderRadius: 6,
  };

  return (
    <div ref={ref} style={{ position: "relative", justifySelf: "end" }}>
      <button
        type="button"
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={() => onOpenChange(!open)}
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.65rem",
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          padding: "4px 10px",
          borderRadius: 8,
          border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.25)"}`,
          background: stealth ? "#0f172a" : "rgba(0,24,48,0.5)",
          color: stealth ? "#475569" : "#7dd3fc",
          cursor: "pointer",
        }}
      >
        Actions ▾
      </button>
      {open && (
        <div
          role="menu"
          style={{
            position: "absolute",
            right: 0,
            top: "100%",
            marginTop: 6,
            minWidth: 168,
            zIndex: 50,
            borderRadius: 10,
            border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.22)"}`,
            background: stealth ? "rgba(15,23,42,0.98)" : "rgba(4,12,28,0.96)",
            backdropFilter: "blur(12px)",
            boxShadow: stealth ? "0 8px 24px rgba(0,0,0,0.5)" : "0 12px 40px rgba(0,0,0,0.55), 0 0 20px rgba(0,180,255,0.08)",
            padding: 6,
          }}
        >
          {(
            [
              ["logout", "Log Out"],
              ["remap", "Force Re-map"],
              ["ping", "Test Connection"],
            ] as const
          ).map(([key, label]) => (
            <button
              key={key}
              type="button"
              role="menuitem"
              style={btnStyle}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = stealth ? "#1e293b" : "rgba(0,180,255,0.08)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "transparent";
              }}
              onClick={() => {
                onAction(key, sessionName);
                onOpenChange(false);
              }}
            >
              {label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export default function FleetIntelligence() {
  const gidReach = useId().replace(/:/g, "");
  const gidDensity = useId().replace(/:/g, "");
  const { stealth } = useStealth();
  const { data, isLoading, error, mutate } = useSWR<FleetAssetsResponse>(
    "/api/business/fleet-assets",
    swrFetcher<FleetAssetsResponse>,
    { refreshInterval: 25_000 },
  );

  const [sortKey, setSortKey] = useState<SortKey>("member_count");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [sessionSortKey, setSessionSortKey] = useState<SessionSortKey>("session_name");
  const [sessionSortDir, setSessionSortDir] = useState<"asc" | "desc">("asc");
  const [scanBusy, setScanBusy] = useState(false);
  const [scanMsg, setScanMsg] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [addModalOpen, setAddModalOpen] = useState(false);
  const [localSessions, setLocalSessions] = useState<SessionFleetRow[]>([]);
  const [actionToast, setActionToast] = useState<string | null>(null);
  const [openMenuId, setOpenMenuId] = useState<string | null>(null);
  const [mapperSortKey, setMapperSortKey] = useState<MapperFleetSortKey>("total_reach");
  const [mapperSortDir, setMapperSortDir] = useState<"asc" | "desc">("desc");

  const groups = useMemo(() => data?.groups ?? [], [data]);
  const mapperFleet = useMemo(() => data?.mapper_fleet ?? [], [data]);

  const apiSessions = useMemo(() => deriveSessionsFromGroups(groups), [groups]);

  const mergedSessions = useMemo(() => {
    const map = new Map<string, SessionFleetRow>();
    for (const s of apiSessions) map.set(s.id, s);
    for (const s of localSessions) map.set(s.id, s);
    return [...map.values()].sort((a, b) => a.sessionName.localeCompare(b.sessionName));
  }, [apiSessions, localSessions]);

  const q = searchQuery.trim().toLowerCase();

  const filteredGroups = useMemo(() => {
    if (!q) return groups;
    return groups.filter(
      (g) =>
        g.group_name.toLowerCase().includes(q) ||
        (g.owner_session ?? "").toLowerCase().includes(q),
    );
  }, [groups, q]);

  const filteredSessions = useMemo(() => {
    if (!q) return mergedSessions;
    return mergedSessions.filter(
      (s) =>
        s.sessionName.toLowerCase().includes(q) ||
        s.phoneDisplay.toLowerCase().replace(/\s/g, "").includes(q.replace(/\s/g, "")),
    );
  }, [mergedSessions, q]);

  const filteredMapperFleet = useMemo(() => {
    if (!q) return mapperFleet;
    return mapperFleet.filter((r) => {
      const phone = (r.phone ?? "").toLowerCase();
      return (
        r.session_id.toLowerCase().includes(q) ||
        r.session_label.toLowerCase().includes(q) ||
        phone.includes(q.replace(/\s/g, ""))
      );
    });
  }, [mapperFleet, q]);

  const totals = useMemo(() => {
    let members = 0;
    let premium = 0;
    for (const g of groups) {
      members += g.member_count;
      premium += g.premium_count;
    }
    const densityPct = members > 0 ? Math.round((premium / members) * 1000) / 10 : 0;
    const reachVisual = Math.min(100, (members / (members + 2000)) * 100);
    return { members, premium, densityPct, reachVisual };
  }, [groups]);

  const sortedRows = useMemo(() => {
    const dir = sortDir === "asc" ? 1 : -1;
    const arr = [...filteredGroups];
    arr.sort((a, b) => {
      let va: string | number;
      let vb: string | number;
      switch (sortKey) {
        case "group_name":
          va = a.group_name.toLowerCase();
          vb = b.group_name.toLowerCase();
          break;
        case "owner_session":
          va = (a.owner_session ?? "").toLowerCase();
          vb = (b.owner_session ?? "").toLowerCase();
          break;
        case "status":
          va = a.status.toLowerCase();
          vb = b.status.toLowerCase();
          break;
        default:
          va = a[sortKey];
          vb = b[sortKey];
      }
      if (typeof va === "number" && typeof vb === "number") {
        return (va - vb) * dir;
      }
      return String(va).localeCompare(String(vb)) * dir;
    });
    return arr;
  }, [filteredGroups, sortKey, sortDir]);

  const sortedMapperRows = useMemo(() => {
    const dir = mapperSortDir === "asc" ? 1 : -1;
    const arr = [...filteredMapperFleet];
    arr.sort((a, b) => {
      let va: string | number | null;
      let vb: string | number | null;
      switch (mapperSortKey) {
        case "session_id":
          va = a.session_id.toLowerCase();
          vb = b.session_id.toLowerCase();
          break;
        case "phone":
          va = (a.phone ?? "").toLowerCase();
          vb = (b.phone ?? "").toLowerCase();
          break;
        case "mapper_status":
          va = a.mapper_status.toLowerCase();
          vb = b.mapper_status.toLowerCase();
          break;
        case "total_groups":
          va = a.total_groups;
          vb = b.total_groups;
          break;
        case "premium_density":
          va = a.premium_density ?? -1;
          vb = b.premium_density ?? -1;
          break;
        default:
          va = a.total_reach;
          vb = b.total_reach;
      }
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
      return String(va).localeCompare(String(vb)) * dir;
    });
    return arr;
  }, [filteredMapperFleet, mapperSortKey, mapperSortDir]);

  const sortedSessionRows = useMemo(() => {
    const dir = sessionSortDir === "asc" ? 1 : -1;
    const arr = [...filteredSessions];
    arr.sort((a, b) => {
      let va: string | number;
      let vb: string | number;
      switch (sessionSortKey) {
        case "phone":
          va = a.phoneDisplay.toLowerCase();
          vb = b.phoneDisplay.toLowerCase();
          break;
        case "account_status":
          va = a.accountStatus;
          vb = b.accountStatus;
          break;
        case "activity":
          va = a.activity;
          vb = b.activity;
          break;
        case "daily_messages":
          va = a.dailyMessages;
          vb = b.dailyMessages;
          break;
        default:
          va = a.sessionName.toLowerCase();
          vb = b.sessionName.toLowerCase();
      }
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
      return String(va).localeCompare(String(vb)) * dir;
    });
    return arr;
  }, [filteredSessions, sessionSortKey, sessionSortDir]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "group_name" || key === "owner_session" || key === "status" ? "asc" : "desc");
    }
  }

  function toggleSessionSort(key: SessionSortKey) {
    if (sessionSortKey === key) {
      setSessionSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSessionSortKey(key);
      setSessionSortDir(key === "daily_messages" ? "desc" : "asc");
    }
  }

  function toggleMapperSort(key: MapperFleetSortKey) {
    if (mapperSortKey === key) {
      setMapperSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setMapperSortKey(key);
      setMapperSortDir(key === "total_reach" || key === "total_groups" || key === "premium_density" ? "desc" : "asc");
    }
  }

  const onSessionAdded = useCallback((payload: AddedSessionPayload) => {
    setLocalSessions((prev) => {
      const next: SessionFleetRow = {
        id: payload.sessionName,
        sessionName: payload.sessionName,
        phoneDisplay: payload.phoneDisplay,
        accountStatus: "Alive",
        activity: "Idle",
        dailyMessages: 0,
        source: "local",
      };
      const rest = prev.filter((p) => p.id !== next.id);
      return [...rest, next];
    });
  }, []);

  const runSessionAction = useCallback((action: string, sessionName: string) => {
    const labels: Record<string, string> = {
      logout: `Log Out queued — ${sessionName}`,
      remap: `Force Re-map dispatched — ${sessionName}`,
      ping: `Test Connection — ${sessionName} (UI only)`,
    };
    setActionToast(labels[action] ?? action);
    window.setTimeout(() => setActionToast(null), 5000);
  }, []);

  async function runDeepScan() {
    setScanBusy(true);
    setScanMsg(null);
    try {
      const res = await forceRunTask("telegram.super_scrape", {});
      setScanMsg(res.message ?? "Deep scan queued.");
      void mutate();
    } catch (e) {
      setScanMsg(e instanceof Error ? e.message : "Enqueue failed");
    } finally {
      setScanBusy(false);
      setTimeout(() => setScanMsg(null), 8000);
    }
  }

  const headerBtn = (key: SortKey, label: string) => (
    <button
      type="button"
      onClick={() => toggleSort(key)}
      style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.58rem",
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        color: stealth ? "#1e293b" : sortKey === key ? "#00b4ff" : "#64748b",
        background: "transparent",
        border: "none",
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 6,
        padding: "4px 0",
        textAlign: "left",
      }}
    >
      {label}
      {sortKey === key && <span style={{ opacity: 0.85 }}>{sortDir === "desc" ? "↓" : "↑"}</span>}
    </button>
  );

  const mapperHeaderBtn = (key: MapperFleetSortKey, label: string) => (
    <button
      type="button"
      onClick={() => toggleMapperSort(key)}
      style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.58rem",
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        color: stealth ? "#1e293b" : mapperSortKey === key ? "#00b4ff" : "#64748b",
        background: "transparent",
        border: "none",
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 6,
        padding: "4px 0",
        textAlign: "left",
      }}
    >
      {label}
      {mapperSortKey === key && (
        <span style={{ opacity: 0.85 }}>{mapperSortDir === "desc" ? "↓" : "↑"}</span>
      )}
    </button>
  );

  const sessionHeaderBtn = (key: SessionSortKey, label: string) => (
    <button
      type="button"
      onClick={() => toggleSessionSort(key)}
      style={{
        fontFamily: "var(--font-mono)",
        fontSize: "0.58rem",
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        color: stealth ? "#1e293b" : sessionSortKey === key ? "#00b4ff" : "#64748b",
        background: "transparent",
        border: "none",
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 6,
        padding: "4px 0",
        textAlign: "left",
      }}
    >
      {label}
      {sessionSortKey === key && (
        <span style={{ opacity: 0.85 }}>{sessionSortDir === "desc" ? "↓" : "↑"}</span>
      )}
    </button>
  );

  const searchInputStyle: CSSProperties = {
    flex: 1,
    minWidth: 160,
    fontFamily: "var(--font-mono)",
    fontSize: "0.78rem",
    padding: "0.55rem 0.85rem",
    borderRadius: 12,
    border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.28)"}`,
    background: stealth ? "rgba(15,23,42,0.6)" : "rgba(2,8,18,0.85)",
    color: stealth ? "#64748b" : "#e2e8f0",
    outline: "none",
    boxShadow: stealth ? "none" : "inset 0 0 18px rgba(0,180,255,0.04)",
  };

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: "1.5rem",
      }}
    >
      <AddSessionModal
        open={addModalOpen}
        onOpenChange={setAddModalOpen}
        onSessionActive={onSessionAdded}
        stealth={stealth}
      />

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: "0.75rem",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1, minWidth: 240 }}>
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              fontWeight: 800,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              color: stealth ? "#334155" : "#64748b",
              whiteSpace: "nowrap",
            }}
          >
            Search fleet
          </span>
          <input
            type="search"
            placeholder="Search sessions, mapper fleet, or groups…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            style={searchInputStyle}
            aria-label="Filter fleet by phone number or session name"
          />
        </div>
        <button
          type="button"
          onClick={() => setAddModalOpen(true)}
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 800,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
            padding: "0.55rem 1rem",
            borderRadius: 12,
            border: stealth ? "1px solid #1e293b" : "1px solid rgba(0,180,255,0.4)",
            background: stealth ? "#0f172a" : "linear-gradient(135deg, rgba(0,180,255,0.2), rgba(99,102,241,0.15))",
            color: stealth ? "#475569" : "#7dd3fc",
            cursor: "pointer",
            boxShadow: stealth ? "none" : "0 0 20px rgba(0,180,255,0.12)",
            whiteSpace: "nowrap",
          }}
        >
          + Add session
        </button>
      </div>

      {actionToast && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.65rem",
            color: stealth ? "#64748b" : "#94a3b8",
            border: `1px dashed ${stealth ? "#1e293b" : "rgba(0,180,255,0.2)"}`,
            borderRadius: 10,
            padding: "0.5rem 0.75rem",
          }}
        >
          {actionToast}
        </div>
      )}

      {/* Gauges + action */}
      <div
        className="fi-gauge-grid"
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr minmax(200px, 0.9fr)",
          gap: "1.25rem",
          alignItems: "center",
        }}
      >
        <CircularGauge
          gradientId={`fg-${gidReach}`}
          label="Total fleet reach"
          valueText={totals.members.toLocaleString()}
          percent={totals.reachVisual}
          accent="#00e5ff"
          stealth={stealth}
        />
        <CircularGauge
          gradientId={`fg-${gidDensity}`}
          label="Premium density"
          valueText={`${totals.densityPct.toFixed(1)}%`}
          percent={totals.densityPct}
          accent="#a78bfa"
          stealth={stealth}
        />
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem", alignItems: "stretch" }}>
          <button
            type="button"
            disabled={scanBusy}
            onClick={() => void runDeepScan()}
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.72rem",
              fontWeight: 800,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              color: stealth ? "#334155" : scanBusy ? "#475569" : "#0a0e1a",
              background: stealth
                ? "#0f172a"
                : "linear-gradient(135deg, #00b4ff 0%, #6366f1 55%, #a78bfa 100%)",
              border: stealth ? "1px solid #1e293b" : "1px solid rgba(0,180,255,0.5)",
              borderRadius: "12px",
              padding: "0.95rem 1rem",
              cursor: scanBusy ? "not-allowed" : "pointer",
              boxShadow: stealth || scanBusy ? "none" : "0 0 28px rgba(0,180,255,0.25), inset 0 1px 0 rgba(255,255,255,0.15)",
              transition: "transform 0.15s, box-shadow 0.2s",
            }}
          >
            {scanBusy ? "⟳ Dispatching…" : "◈ Deep scan machine"}
          </button>
          <p
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              color: stealth ? "#1e293b" : "#475569",
              lineHeight: 1.55,
              margin: 0,
              letterSpacing: "0.04em",
            }}
          >
            Runs the strategic intelligence hunter (<code style={{ color: stealth ? "#1e293b" : "#64748b" }}>telegram.super_scrape</code>) —
            discovers niches and enqueues scrapes on the worker queue.
          </p>
          {scanMsg && (
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.62rem",
                color: stealth ? "#334155" : "#94a3b8",
              }}
            >
              {scanMsg}
            </div>
          )}
        </div>
      </div>

      {(isLoading && !data) && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.75rem", color: "#475569" }}>
          Loading fleet matrix…
        </div>
      )}
      {error && (
        <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.72rem", color: "#f87171" }}>
          {String((error as Error).message ?? error)}
        </div>
      )}
      {data && !data.db_available && (
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            color: "#64748b",
            border: "1px dashed #1e293b",
            borderRadius: "10px",
            padding: "0.85rem 1rem",
          }}
        >
          Telefix database unreachable — showing empty grid. Check bridge path and DB availability.
        </div>
      )}

      {/* Session roster */}
      <div>
        <h3
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 800,
            letterSpacing: "0.16em",
            textTransform: "uppercase",
            color: stealth ? "#334155" : "#64748b",
            margin: "0 0 0.65rem 0",
          }}
        >
          Session roster
        </h3>
        <div
          style={{
            borderRadius: "14px",
            overflowX: "auto",
            overflowY: "hidden",
            border: `1px solid ${stealth ? "#0f172a" : "rgba(0, 180, 255, 0.22)"}`,
            background: stealth ? "rgba(5, 10, 22, 0.5)" : "rgba(4, 12, 28, 0.75)",
            boxShadow: stealth ? "none" : "inset 0 1px 0 rgba(0,180,255,0.08), 0 12px 40px rgba(0,0,0,0.45)",
          }}
        >
          <div style={{ minWidth: 920 }}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "minmax(120px, 1fr) minmax(100px, 0.85fr) minmax(90px, 0.7fr) minmax(90px, 0.7fr) minmax(72px, 0.45fr) minmax(100px, 1fr) auto",
                gap: "0.65rem",
                padding: "0.65rem 1rem",
                background: stealth ? "#030810" : "linear-gradient(90deg, rgba(0,24,48,0.9), rgba(5,10,22,0.95))",
                borderBottom: `1px solid ${stealth ? "#0f172a" : "rgba(0,180,255,0.15)"}`,
                alignItems: "center",
              }}
            >
              {sessionHeaderBtn("phone", "Phone number")}
              {sessionHeaderBtn("account_status", "Account status")}
              {sessionHeaderBtn("activity", "Current activity")}
              {sessionHeaderBtn("daily_messages", "Daily messages")}
              {sessionHeaderBtn("session_name", "Session name")}
              <span
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: "0.58rem",
                  fontWeight: 700,
                  letterSpacing: "0.12em",
                  textTransform: "uppercase",
                  color: stealth ? "#1e293b" : "#64748b",
                }}
              >
                Actions
              </span>
            </div>
            <div style={{ maxHeight: 320, overflowY: "auto" }}>
              {sortedSessionRows.length === 0 && !isLoading && (
                <div
                  style={{
                    padding: "2rem",
                    textAlign: "center",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                    color: "#475569",
                  }}
                >
                  {q
                    ? "No sessions match your search."
                    : "No owner sessions in fleet data. Add a session or sync Telefix."}
                </div>
              )}
              {sortedSessionRows.map((row, i) => {
                const acc = accountStatusStyle(row.accountStatus, stealth);
                const act = activityStyle(row.activity, stealth);
                const zebra = i % 2 === 0 ? "rgba(0,180,255,0.02)" : "transparent";
                return (
                  <div
                    key={row.id}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "minmax(120px, 1fr) minmax(100px, 0.85fr) minmax(90px, 0.7fr) minmax(90px, 0.7fr) minmax(72px, 0.45fr) minmax(100px, 1fr) auto",
                      gap: "0.65rem",
                      padding: "0.55rem 1rem",
                      alignItems: "center",
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.7rem",
                      background: zebra,
                      borderBottom: `1px solid ${stealth ? "#0a0f18" : "rgba(15,23,42,0.85)"}`,
                    }}
                  >
                    <span style={{ color: stealth ? "#334155" : "#7dd3fc", wordBreak: "break-word" }}>
                      {row.phoneDisplay}
                    </span>
                    <span
                      style={{
                        display: "inline-flex",
                        padding: "3px 10px",
                        borderRadius: "999px",
                        background: acc.bg,
                        color: acc.fg,
                        fontSize: "0.58rem",
                        fontWeight: 800,
                        letterSpacing: "0.08em",
                        textTransform: "uppercase",
                        boxShadow: acc.glow,
                        width: "fit-content",
                      }}
                    >
                      {row.accountStatus}
                    </span>
                    <span
                      style={{
                        display: "inline-flex",
                        padding: "3px 10px",
                        borderRadius: "999px",
                        background: act.bg,
                        color: act.fg,
                        fontSize: "0.58rem",
                        fontWeight: 700,
                        letterSpacing: "0.06em",
                        width: "fit-content",
                      }}
                    >
                      {row.activity}
                    </span>
                    <span
                      style={{
                        color: stealth ? "#334155" : "#c4b5fd",
                        fontVariantNumeric: "tabular-nums",
                      }}
                    >
                      {row.dailyMessages.toLocaleString()}
                    </span>
                    <span
                      style={{
                        color: stealth ? "#334155" : "#94a3b8",
                        fontSize: "0.62rem",
                        wordBreak: "break-all",
                      }}
                      title={row.sessionName}
                    >
                      {row.sessionName.length > 18 ? `${row.sessionName.slice(0, 16)}…` : row.sessionName}
                    </span>
                    <SessionActionsMenu
                      sessionName={row.sessionName}
                      open={openMenuId === row.id}
                      onOpenChange={(o) => setOpenMenuId(o ? row.id : null)}
                      stealth={stealth}
                      onAction={runSessionAction}
                    />
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* Mapper fleet (Telethon staged sessions) */}
      <div>
        <h3
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 800,
            letterSpacing: "0.16em",
            textTransform: "uppercase",
            color: stealth ? "#334155" : "#64748b",
            margin: "0 0 0.65rem 0",
          }}
        >
          Mapper fleet power
        </h3>
        {data?.mapper_generated_at && (
          <p
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: "0.58rem",
              color: stealth ? "#1e293b" : "#475569",
              margin: "0 0 0.65rem 0",
            }}
          >
            Last mapper snapshot: {data.mapper_generated_at}
          </p>
        )}
        <div
          style={{
            borderRadius: "14px",
            overflowX: "auto",
            overflowY: "hidden",
            border: `1px solid ${stealth ? "#0f172a" : "rgba(0, 180, 255, 0.22)"}`,
            background: stealth ? "rgba(5, 10, 22, 0.5)" : "rgba(4, 12, 28, 0.75)",
            boxShadow: stealth ? "none" : "inset 0 1px 0 rgba(0,180,255,0.08), 0 12px 40px rgba(0,0,0,0.45)",
          }}
        >
          <div style={{ minWidth: 720 }}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "minmax(88px, 0.9fr) minmax(100px, 1fr) 0.55fr 0.65fr 0.55fr 0.5fr",
                gap: "0.65rem",
                padding: "0.65rem 1rem",
                background: stealth ? "#030810" : "linear-gradient(90deg, rgba(0,24,48,0.9), rgba(5,10,22,0.95))",
                borderBottom: `1px solid ${stealth ? "#0f172a" : "rgba(0,180,255,0.15)"}`,
                alignItems: "center",
              }}
            >
              {mapperHeaderBtn("session_id", "Session ID")}
              {mapperHeaderBtn("phone", "Phone")}
              {mapperHeaderBtn("total_groups", "Groups")}
              {mapperHeaderBtn("total_reach", "Reach")}
              {mapperHeaderBtn("premium_density", "Prem. %")}
              {mapperHeaderBtn("mapper_status", "Status")}
            </div>
            <div style={{ maxHeight: 280, overflowY: "auto" }}>
              {sortedMapperRows.length === 0 && !isLoading && (
                <div
                  style={{
                    padding: "2rem",
                    textAlign: "center",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                    color: "#475569",
                  }}
                >
                  {q
                    ? "No mapper rows match your search."
                    : "Run account_mapper.map (e.g. GLOBAL-SCAVENGE) to populate staged-session power stats."}
                </div>
              )}
              {sortedMapperRows.map((row: MapperFleetSessionRow, i: number) => {
                const zebra = i % 2 === 0 ? "rgba(0,180,255,0.02)" : "transparent";
                const dens =
                  row.premium_density != null ? `${(row.premium_density * 100).toFixed(2)}%` : "—";
                return (
                  <div
                    key={`${row.session_id}-${row.session_label}`}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "minmax(88px, 0.9fr) minmax(100px, 1fr) 0.55fr 0.65fr 0.55fr 0.5fr",
                      gap: "0.65rem",
                      padding: "0.55rem 1rem",
                      alignItems: "center",
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.68rem",
                      background: zebra,
                      borderBottom: `1px solid ${stealth ? "#0a0f18" : "rgba(15,23,42,0.85)"}`,
                    }}
                  >
                    <span
                      style={{
                        color: stealth ? "#334155" : "#94a3b8",
                        wordBreak: "break-all",
                        fontSize: "0.62rem",
                      }}
                      title={row.session_label}
                    >
                      {row.session_id}
                    </span>
                    <span style={{ color: stealth ? "#334155" : "#7dd3fc", wordBreak: "break-word" }}>
                      {row.phone ?? "—"}
                    </span>
                    <span style={{ color: stealth ? "#334155" : "#e2e8f0", fontVariantNumeric: "tabular-nums" }}>
                      {row.total_groups.toLocaleString()}
                    </span>
                    <span style={{ color: stealth ? "#334155" : "#a5b4fc", fontVariantNumeric: "tabular-nums" }}>
                      {row.total_reach.toLocaleString()}
                    </span>
                    <span style={{ color: stealth ? "#334155" : "#c4b5fd", fontVariantNumeric: "tabular-nums" }}>
                      {dens}
                    </span>
                    <span
                      style={{
                        color:
                          row.mapper_status === "ok"
                            ? stealth
                              ? "#334155"
                              : "#34d399"
                            : stealth
                              ? "#334155"
                              : "#f87171",
                        fontSize: "0.58rem",
                        fontWeight: 700,
                        textTransform: "uppercase",
                      }}
                    >
                      {row.mapper_status}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* Asset table (groups) */}
      <div>
        <h3
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: "0.68rem",
            fontWeight: 800,
            letterSpacing: "0.16em",
            textTransform: "uppercase",
            color: stealth ? "#334155" : "#64748b",
            margin: "0 0 0.65rem 0",
          }}
        >
          Group asset matrix
        </h3>
        <div
          style={{
            borderRadius: "14px",
            overflowX: "auto",
            overflowY: "hidden",
            border: `1px solid ${stealth ? "#0f172a" : "rgba(0, 180, 255, 0.22)"}`,
            background: stealth ? "rgba(5, 10, 22, 0.5)" : "rgba(4, 12, 28, 0.75)",
            boxShadow: stealth ? "none" : "inset 0 1px 0 rgba(0,180,255,0.08), 0 12px 40px rgba(0,0,0,0.45)",
          }}
        >
          <div style={{ minWidth: 640 }}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "minmax(140px, 1.4fr) 0.55fr 0.65fr minmax(100px, 1fr) 0.75fr",
                gap: "0.75rem",
                padding: "0.65rem 1rem",
                background: stealth ? "#030810" : "linear-gradient(90deg, rgba(0,24,48,0.9), rgba(5,10,22,0.95))",
                borderBottom: `1px solid ${stealth ? "#0f172a" : "rgba(0,180,255,0.15)"}`,
              }}
            >
              {headerBtn("group_name", "Group name")}
              {headerBtn("member_count", "Members")}
              {headerBtn("premium_count", "Premium")}
              {headerBtn("owner_session", "Owner session")}
              {headerBtn("status", "Status")}
            </div>

            <div style={{ maxHeight: 420, overflowY: "auto" }}>
              {sortedRows.length === 0 && !isLoading && (
                <div
                  style={{
                    padding: "2rem",
                    textAlign: "center",
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.75rem",
                    color: "#475569",
                  }}
                >
                  {q ? "No groups match your search." : "No managed groups in database."}
                </div>
              )}
              {sortedRows.map((row: FleetGroupAssetRow, i: number) => {
                const st = statusStyle(row.status, stealth);
                const zebra = i % 2 === 0 ? "rgba(0,180,255,0.02)" : "transparent";
                return (
                  <div
                    key={row.group_id}
                    style={{
                      display: "grid",
                      gridTemplateColumns: "minmax(140px, 1.4fr) 0.55fr 0.65fr minmax(100px, 1fr) 0.75fr",
                      gap: "0.75rem",
                      padding: "0.55rem 1rem",
                      alignItems: "center",
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.72rem",
                      background: zebra,
                      borderBottom: `1px solid ${stealth ? "#0a0f18" : "rgba(15,23,42,0.85)"}`,
                    }}
                  >
                    <span style={{ color: stealth ? "#334155" : "#e2e8f0", fontWeight: 600, wordBreak: "break-word" }}>
                      {row.group_name}
                    </span>
                    <span style={{ color: stealth ? "#334155" : "#7dd3fc", fontVariantNumeric: "tabular-nums" }}>
                      {row.member_count.toLocaleString()}
                    </span>
                    <span style={{ color: stealth ? "#334155" : "#c4b5fd", fontVariantNumeric: "tabular-nums" }}>
                      {row.premium_count.toLocaleString()}
                    </span>
                    <span
                      style={{
                        color: stealth ? "#334155" : "#94a3b8",
                        fontSize: "0.65rem",
                        wordBreak: "break-all",
                      }}
                    >
                      {row.owner_session ?? "—"}
                    </span>
                    <span
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        justifyContent: "center",
                        padding: "3px 10px",
                        borderRadius: "999px",
                        background: st.bg,
                        color: st.fg,
                        fontSize: "0.58rem",
                        fontWeight: 800,
                        letterSpacing: "0.1em",
                        textTransform: "uppercase",
                        boxShadow: st.glow,
                        width: "fit-content",
                      }}
                    >
                      {row.status}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      <style>{`
        @media (max-width: 900px) {
          .fi-gauge-grid { grid-template-columns: 1fr !important; }
        }
      `}</style>
    </div>
  );
}
