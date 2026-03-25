"use client";

import { useState } from "react";
import useSWR from "swr";
import {
  swrFetcher,
  rotateProxy,
  type ProxyStatusResponse,
  type RotationHistoryResponse,
} from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";

function formatAgo(seconds: number | null): string {
  if (seconds === null) return "—";
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

function formatTs(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleTimeString("he-IL", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}

export default function ProxyStatusPanel() {
  const { tokens, isHighContrast } = useTheme();
  const { language } = useI18n();
  const he = language === "he";
  const [rotating, setRotating] = useState(false);
  const [rotateMsg, setRotateMsg] = useState<string | null>(null);
  const [showHistory, setShowHistory] = useState(false);

  const { data, error, isLoading, mutate } = useSWR<ProxyStatusResponse>(
    "/api/proxy/status",
    swrFetcher<ProxyStatusResponse>,
    { refreshInterval: 30_000 },
  );

  const { data: histData } = useSWR<RotationHistoryResponse>(
    showHistory ? "/api/proxy/rotations?limit=10" : null,
    swrFetcher<RotationHistoryResponse>,
    { refreshInterval: 15_000 },
  );

  const handleRotate = async () => {
    setRotating(true);
    setRotateMsg(null);
    try {
      const r = await rotateProxy();
      setRotateMsg(
        he
          ? `✓ הוחלף ל-${r.new_label} · IP: ${r.resolved_ip ?? "?"}`
          : `✓ Rotated to ${r.new_label} · IP: ${r.resolved_ip ?? "?"}`,
      );
      await mutate();
    } catch {
      setRotateMsg(he ? "שגיאה בהחלפת פרוקסי" : "Rotation failed");
    } finally {
      setRotating(false);
      setTimeout(() => setRotateMsg(null), 6000);
    }
  };

  const border = isHighContrast ? tokens.borderDefault : "#1e293b";
  const bg = isHighContrast ? tokens.surface1 : "linear-gradient(145deg, #060d1a, #080f1c)";
  const mono = "var(--font-mono)";
  const cyan = "#22d3ee";
  const green = "#4ade80";
  const amber = "#fbbf24";
  const muted = isHighContrast ? tokens.textMuted : "#475569";
  const text = isHighContrast ? tokens.textPrimary : "#e2e8f0";

  return (
    <div
      style={{
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 14,
        overflow: "hidden",
        boxShadow: "0 8px 32px #00000066",
        fontFamily: mono,
      }}
    >
      {/* Header bar */}
      <div
        style={{
          padding: "0.55rem 1rem",
          borderBottom: `1px solid ${border}`,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          background: isHighContrast ? tokens.surface2 : "#0a1628",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          <span style={{ fontSize: "0.6rem", color: cyan, textTransform: "uppercase", letterSpacing: "0.18em", fontWeight: 700 }}>
            {he ? "מצב פרוקסי" : "PROXY STATUS"}
          </span>
          {data && (
            <span
              style={{
                fontSize: "0.58rem",
                padding: "1px 6px",
                borderRadius: 4,
                background: "#0e4429",
                color: green,
                border: `1px solid #166534`,
              }}
            >
              {data.pool_size} {he ? "פרוקסים" : "proxies"}
            </span>
          )}
        </div>
        <div style={{ display: "flex", gap: "0.4rem" }}>
          <button
            type="button"
            onClick={() => setShowHistory((v) => !v)}
            style={{
              fontFamily: mono,
              fontSize: "0.6rem",
              padding: "3px 8px",
              borderRadius: 5,
              border: `1px solid ${border}`,
              background: showHistory ? "#1e293b" : "transparent",
              color: muted,
              cursor: "pointer",
            }}
          >
            {he ? "היסטוריה" : "HISTORY"}
          </button>
          <button
            type="button"
            onClick={() => void mutate()}
            style={{
              fontFamily: mono,
              fontSize: "0.6rem",
              padding: "3px 8px",
              borderRadius: 5,
              border: `1px solid ${border}`,
              background: "transparent",
              color: muted,
              cursor: "pointer",
            }}
          >
            ↻
          </button>
        </div>
      </div>

      {/* Body */}
      <div style={{ padding: "0.85rem 1rem" }}>
        {isLoading && !data && (
          <div style={{ color: muted, fontSize: "0.72rem" }}>
            {he ? "טוען…" : "Loading…"}
          </div>
        )}

        {error && (
          <div style={{ color: "#f87171", fontSize: "0.72rem" }}>
            {he ? "שגיאת חיבור לשרת" : "Failed to reach API"}
          </div>
        )}

        {data && (
          <>
            {/* Active IP block */}
            <div
              style={{
                background: isHighContrast ? tokens.accentSubtle : "#0d1f3c",
                border: `1px solid #1e3a5f`,
                borderRadius: 10,
                padding: "0.75rem 0.9rem",
                marginBottom: "0.85rem",
              }}
            >
              <div style={{ fontSize: "0.58rem", color: muted, textTransform: "uppercase", letterSpacing: "0.14em", marginBottom: "0.45rem" }}>
                {he ? "IP פעיל כרגע" : "ACTIVE PUBLIC IP"}
              </div>

              <div style={{ display: "flex", alignItems: "baseline", gap: "0.6rem", flexWrap: "wrap" }}>
                <span
                  style={{
                    fontSize: "1.15rem",
                    fontWeight: 700,
                    color: data.active_public_ip ? cyan : amber,
                    letterSpacing: "0.04em",
                  }}
                >
                  {data.active_public_ip ?? (he ? "לא נפתר" : "unresolved")}
                </span>
                {data.active_ip_country && (
                  <span style={{ fontSize: "0.7rem", color: green }}>
                    {data.active_ip_country}
                    {data.active_ip_city ? ` · ${data.active_ip_city}` : ""}
                  </span>
                )}
              </div>

              {data.active_ip_isp && (
                <div style={{ fontSize: "0.65rem", color: muted, marginTop: "0.2rem" }}>
                  ISP: {data.active_ip_isp}
                </div>
              )}

              <div style={{ marginTop: "0.55rem", display: "flex", gap: "1.2rem", flexWrap: "wrap" }}>
                <Stat label={he ? "פרוקסי פעיל" : "Active proxy"} value={data.active_label ?? "—"} color={text} />
                <Stat
                  label={he ? "החלפה אחרונה" : "Last rotation"}
                  value={
                    data.last_rotation_at
                      ? `${formatTs(data.last_rotation_at)} (${formatAgo(data.last_rotation_ago_seconds)})`
                      : he ? "לא בוצע" : "never"
                  }
                  color={data.last_rotation_at ? text : muted}
                />
                <Stat label={he ? "סה״כ החלפות" : "Total rotations"} value={String(data.total_rotations)} color={text} />
              </div>
            </div>

            {/* Provider info */}
            <div style={{ marginBottom: "0.85rem", display: "flex", gap: "1.2rem", flexWrap: "wrap" }}>
              <Stat label={he ? "ספק" : "Provider"} value={data.provider} color={cyan} />
              <Stat label={he ? "תוכנית" : "Plan"} value={data.provider_plan} color={text} />
            </div>

            {/* Proxy pool list */}
            <div style={{ marginBottom: "0.85rem" }}>
              <div style={{ fontSize: "0.58rem", color: muted, textTransform: "uppercase", letterSpacing: "0.14em", marginBottom: "0.4rem" }}>
                {he ? "מאגר פרוקסים" : "PROXY POOL"}
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
                {data.proxies.map((p) => {
                  const isActive = p.index === data.active_index;
                  return (
                    <div
                      key={p.index}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "0.5rem",
                        padding: "0.3rem 0.5rem",
                        borderRadius: 6,
                        background: isActive ? "#0e2d1a" : "transparent",
                        border: isActive ? `1px solid #166534` : `1px solid transparent`,
                      }}
                    >
                      <span
                        style={{
                          width: 7,
                          height: 7,
                          borderRadius: "50%",
                          background: isActive ? green : "#334155",
                          boxShadow: isActive ? `0 0 6px ${green}` : "none",
                          flexShrink: 0,
                        }}
                      />
                      <span style={{ fontSize: "0.68rem", color: isActive ? green : muted, minWidth: 120 }}>
                        {p.label}
                      </span>
                      <span style={{ fontSize: "0.6rem", color: "#334155", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {p.raw_line}
                      </span>
                      {isActive && (
                        <span style={{ fontSize: "0.55rem", color: green, textTransform: "uppercase", letterSpacing: "0.1em" }}>
                          {he ? "פעיל" : "ACTIVE"}
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Rotate button */}
            <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", flexWrap: "wrap" }}>
              <button
                type="button"
                disabled={rotating}
                onClick={() => void handleRotate()}
                style={{
                  fontFamily: mono,
                  fontSize: "0.68rem",
                  fontWeight: 700,
                  padding: "7px 16px",
                  borderRadius: 8,
                  border: `1px solid ${cyan}55`,
                  background: rotating ? "#0a1628" : `${cyan}18`,
                  color: rotating ? muted : cyan,
                  cursor: rotating ? "wait" : "pointer",
                  letterSpacing: "0.08em",
                  transition: "all 0.15s",
                }}
              >
                {rotating
                  ? he ? "מחליף…" : "ROTATING…"
                  : he ? "החלף פרוקסי" : "ROTATE PROXY"}
              </button>
              {rotateMsg && (
                <span style={{ fontSize: "0.68rem", color: rotateMsg.startsWith("✓") ? green : "#f87171" }}>
                  {rotateMsg}
                </span>
              )}
            </div>
          </>
        )}

        {/* Rotation history */}
        {showHistory && histData && histData.events.length > 0 && (
          <div style={{ marginTop: "0.85rem" }}>
            <div style={{ fontSize: "0.58rem", color: muted, textTransform: "uppercase", letterSpacing: "0.14em", marginBottom: "0.4rem" }}>
              {he ? "היסטוריית החלפות" : "ROTATION HISTORY"}
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
              {histData.events.map((ev, i) => (
                <div
                  key={i}
                  style={{
                    display: "flex",
                    gap: "0.6rem",
                    alignItems: "baseline",
                    fontSize: "0.65rem",
                    padding: "0.25rem 0",
                    borderBottom: `1px solid ${border}`,
                  }}
                >
                  <span style={{ color: muted, minWidth: 60 }}>{formatTs(ev.ts)}</span>
                  <span style={{ color: text }}>{ev.to_label}</span>
                  {ev.resolved_ip && (
                    <span style={{ color: cyan }}>{ev.resolved_ip}</span>
                  )}
                  <span
                    style={{
                      fontSize: "0.55rem",
                      color: ev.trigger === "manual" ? amber : muted,
                      textTransform: "uppercase",
                      marginLeft: "auto",
                    }}
                  >
                    {ev.trigger}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {showHistory && histData && histData.events.length === 0 && (
          <div style={{ marginTop: "0.75rem", fontSize: "0.68rem", color: muted }}>
            {he ? "אין היסטוריית החלפות עדיין" : "No rotation history yet"}
          </div>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div>
      <div style={{ fontSize: "0.55rem", color: "#475569", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 2 }}>
        {label}
      </div>
      <div style={{ fontSize: "0.72rem", color, fontWeight: 600 }}>{value}</div>
    </div>
  );
}
