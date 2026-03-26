"use client";

import { useState } from "react";
import useSWR, { useSWRConfig } from "swr";
import PageTransition from "@/components/PageTransition";
import CyberGrid from "@/components/CyberGrid";
import { useStealth } from "@/lib/stealth";
import { useI18n } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";
import { API_BASE, swrFetcher } from "@/lib/api";

const KEY = "/api/telefix/group-infiltration";
const SCHEDULE_KEY = "/api/telefix/group-factory/schedule";

const PHASE_LABELS = {
  warmup: "חימום",
  public_trial: "ניסיון ציבורי",
  in_search: "בחיפוש",
  search_indexed: "בחיפוש",
  private_cooldown: "קולדאון",
};

const PHASE_COLORS = {
  warmup: "#f59e0b",
  public_trial: "#3b82f6",
  in_search: "#39ff14",
  search_indexed: "#39ff14",
  private_cooldown: "#ef4444",
};

function StatCard({ label, value, color, tokens }) {
  return (
    <div
      style={{
        flex: "1 1 120px",
        padding: "0.85rem 1rem",
        borderRadius: 12,
        background: "rgba(37, 47, 61, 0.72)",
        border: `1px solid rgba(255,255,255,0.07)`,
        textAlign: "center",
      }}
    >
      <div
        style={{
          fontSize: "1.6rem",
          fontWeight: 800,
          color: color || tokens.textPrimary,
          fontFamily: "var(--font-mono)",
          lineHeight: 1,
        }}
      >
        {value}
      </div>
      <div
        style={{
          fontSize: "0.72rem",
          color: tokens.textMuted,
          marginTop: "0.35rem",
          fontWeight: 500,
        }}
      >
        {label}
      </div>
    </div>
  );
}

function WarmupBar({ days, maxDays = 14, tokens }) {
  const pct = Math.min(100, Math.round((days / maxDays) * 100));
  const color =
    pct >= 100 ? "#39ff14" : pct >= 60 ? "#3b82f6" : "#f59e0b";
  return (
    <div style={{ width: "100%", minWidth: 80 }}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: "0.7rem",
          color: tokens.textMuted,
          marginBottom: 3,
          fontFamily: "var(--font-mono)",
        }}
      >
        <span>חימום</span>
        <span style={{ color }}>
          {days}/{maxDays}
        </span>
      </div>
      <div
        style={{
          height: 5,
          borderRadius: 4,
          background: "rgba(255,255,255,0.08)",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            background: color,
            borderRadius: 4,
            transition: "width 0.4s ease",
          }}
        />
      </div>
    </div>
  );
}

export default function GroupInfiltration() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const { isHighContrast, tokens } = useTheme();
  const { mutate } = useSWRConfig();

  const [busyId, setBusyId] = useState(null);
  const [toast, setToast] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [createForm, setCreateForm] = useState({
    name_he: "",
    is_private: true,
    warmup_days: 7,
    invite_link: "",
  });
  const [settingsForm, setSettingsForm] = useState(null);
  const [savingSettings, setSavingSettings] = useState(false);
  const [creatingGroup, setCreatingGroup] = useState(false);

  const { data, error, isLoading } = useSWR(KEY, swrFetcher, {
    refreshInterval: 30_000,
  });

  const { data: scheduleData } = useSWR(SCHEDULE_KEY, swrFetcher, {
    refreshInterval: 60_000,
    onSuccess: (d) => {
      if (!settingsForm && d?.settings) {
        setSettingsForm({ ...d.settings });
      }
    },
  });

  const cardBg = isHighContrast ? tokens.surface2 : "rgba(37, 47, 61, 0.72)";
  const border = `1px solid ${tokens.borderSubtle}`;

  function showToast(type, text) {
    setToast({ type, text });
    setTimeout(() => setToast(null), 4000);
  }

  async function forceSearch(groupId) {
    setBusyId(groupId);
    try {
      const res = await fetch(
        `${API_BASE}/api/telefix/group-infiltration/${encodeURIComponent(groupId)}/force-search`,
        { method: "POST", headers: { "Content-Type": "application/json" } },
      );
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      showToast("ok", body.detail || "הקבוצה דחויה לשלב הבא");
      await mutate(KEY);
    } catch (e) {
      showToast("err", String(e.message || e));
    } finally {
      setBusyId(null);
    }
  }

  async function deleteGroup(groupId, name) {
    if (!confirm(`למחוק את הקבוצה "${name}"?`)) return;
    setBusyId(`del-${groupId}`);
    try {
      const res = await fetch(
        `${API_BASE}/api/telefix/group-infiltration/${encodeURIComponent(groupId)}`,
        { method: "DELETE" },
      );
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      showToast("ok", "הקבוצה נמחקה");
      await mutate(KEY);
    } catch (e) {
      showToast("err", String(e.message || e));
    } finally {
      setBusyId(null);
    }
  }

  async function createGroup(e) {
    e.preventDefault();
    if (!createForm.name_he.trim()) return;
    setCreatingGroup(true);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name_he: createForm.name_he.trim(),
          is_private: createForm.is_private,
          warmup_days: Number(createForm.warmup_days),
          invite_link: createForm.invite_link || null,
        }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      showToast("ok", `קבוצה "${createForm.name_he}" נוצרה`);
      setCreateForm({ name_he: "", is_private: true, warmup_days: 7, invite_link: "" });
      setShowCreate(false);
      await mutate(KEY);
    } catch (e) {
      showToast("err", String(e.message || e));
    } finally {
      setCreatingGroup(false);
    }
  }

  async function saveSettings(e) {
    e.preventDefault();
    if (!settingsForm) return;
    setSavingSettings(true);
    try {
      const res = await fetch(`${API_BASE}/api/telefix/group-factory/schedule`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          warmup_days: Number(settingsForm.warmup_days),
          cooldown_hours: Number(settingsForm.cooldown_hours),
          groups_per_day: Number(settingsForm.groups_per_day),
        }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      showToast("ok", "הגדרות נשמרו");
      setShowSettings(false);
      await mutate(SCHEDULE_KEY);
    } catch (e) {
      showToast("err", String(e.message || e));
    } finally {
      setSavingSettings(false);
    }
  }

  const groups = data?.groups || [];
  const totalGroups = groups.length;
  const inWarmup = groups.filter((g) => !g.in_search && g.warmup_days < 14).length;
  const readyForSearch = groups.filter((g) => !g.in_search && g.warmup_days >= 14).length;
  const inSearch = groups.filter((g) => g.in_search).length;

  const inputStyle = {
    width: "100%",
    padding: "0.55rem 0.75rem",
    borderRadius: 8,
    border: `1px solid ${tokens.borderSubtle}`,
    background: "rgba(255,255,255,0.05)",
    color: tokens.textPrimary,
    fontSize: "0.85rem",
    fontFamily: "var(--font-sans)",
    outline: "none",
  };

  const labelStyle = {
    fontSize: "0.78rem",
    color: tokens.textMuted,
    marginBottom: "0.3rem",
    display: "block",
    fontWeight: 600,
  };

  const btnPrimary = {
    padding: "0.55rem 1.1rem",
    borderRadius: 8,
    border: "none",
    background: "linear-gradient(135deg, #3b82f6, #6366f1)",
    color: "#fff",
    fontSize: "0.82rem",
    fontWeight: 700,
    cursor: "pointer",
    whiteSpace: "nowrap",
  };

  const btnSecondary = {
    padding: "0.55rem 1rem",
    borderRadius: 8,
    border: `1px solid ${tokens.borderSubtle}`,
    background: "transparent",
    color: tokens.textMuted,
    fontSize: "0.82rem",
    fontWeight: 600,
    cursor: "pointer",
    whiteSpace: "nowrap",
  };

  return (
    <PageTransition>
      <div style={{ position: "relative", minHeight: "calc(100vh - 56px)", padding: "1.25rem" }}>
        <CyberGrid />
        <div
          style={{
            position: "relative",
            zIndex: 1,
            maxWidth: 1100,
            margin: "0 auto",
            direction: isRTL ? "rtl" : "ltr",
          }}
        >
          {/* Header */}
          <header
            style={{
              marginBottom: "1.25rem",
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "space-between",
              flexWrap: "wrap",
              gap: "0.75rem",
            }}
          >
            <div>
              <h1
                style={{
                  fontFamily: "var(--font-sans)",
                  fontSize: "1.35rem",
                  fontWeight: 700,
                  color: tokens.textPrimary,
                  margin: 0,
                }}
              >
                מפעל קבוצות
              </h1>
              <p
                style={{
                  margin: "0.35rem 0 0",
                  fontSize: "0.88rem",
                  color: tokens.textMuted,
                  maxWidth: 540,
                }}
              >
                ניהול חימום ואינדוקס קבוצות Telegram — יצירה, מעקב שלבים, ודחיפה ידנית לחיפוש
              </p>
            </div>
            <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap" }}>
              <button
                type="button"
                style={btnSecondary}
                onClick={() => {
                  setShowSettings((v) => !v);
                  setShowCreate(false);
                }}
              >
                ⚙ הגדרות
              </button>
              <button
                type="button"
                style={btnPrimary}
                onClick={() => {
                  setShowCreate((v) => !v);
                  setShowSettings(false);
                }}
              >
                + צור קבוצה חדשה
              </button>
            </div>
          </header>

          {/* Toast */}
          {toast && (
            <div
              role="status"
              style={{
                marginBottom: "1rem",
                padding: "0.65rem 0.9rem",
                borderRadius: 10,
                fontSize: "0.82rem",
                background:
                  toast.type === "ok"
                    ? "rgba(57, 255, 20, 0.08)"
                    : "rgba(239,68,68,0.1)",
                border: `1px solid ${toast.type === "ok" ? "rgba(57,255,20,0.3)" : "rgba(239,68,68,0.3)"}`,
                color: tokens.textPrimary,
              }}
            >
              {toast.text}
            </div>
          )}

          {/* Stats Bar */}
          <div
            style={{
              display: "flex",
              gap: "0.75rem",
              flexWrap: "wrap",
              marginBottom: "1.25rem",
            }}
          >
            <StatCard label="סה״כ קבוצות" value={totalGroups} tokens={tokens} />
            <StatCard label="בחימום" value={inWarmup} color="#f59e0b" tokens={tokens} />
            <StatCard label="מוכנות לחיפוש" value={readyForSearch} color="#3b82f6" tokens={tokens} />
            <StatCard label="בחיפוש" value={inSearch} color="#39ff14" tokens={tokens} />
            {scheduleData && (
              <StatCard
                label="קבוצות ליום"
                value={scheduleData.settings?.groups_per_day ?? "—"}
                color="#8b5cf6"
                tokens={tokens}
              />
            )}
          </div>

          {/* Settings Panel */}
          {showSettings && settingsForm && (
            <form
              onSubmit={saveSettings}
              style={{
                marginBottom: "1.25rem",
                padding: "1.1rem 1.25rem",
                borderRadius: 14,
                background: cardBg,
                border: `1px solid rgba(99,102,241,0.35)`,
              }}
            >
              <div
                style={{
                  fontWeight: 700,
                  fontSize: "0.95rem",
                  color: tokens.textPrimary,
                  marginBottom: "1rem",
                }}
              >
                ⚙ הגדרות מפעל קבוצות
              </div>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
                  gap: "0.85rem",
                  marginBottom: "1rem",
                }}
              >
                <div>
                  <label style={labelStyle}>ימי Warmup (1–30)</label>
                  <input
                    type="number"
                    min={1}
                    max={30}
                    value={settingsForm.warmup_days}
                    onChange={(e) =>
                      setSettingsForm((f) => ({ ...f, warmup_days: e.target.value }))
                    }
                    style={inputStyle}
                  />
                </div>
                <div>
                  <label style={labelStyle}>שעות Cooldown (1–168)</label>
                  <input
                    type="number"
                    min={1}
                    max={168}
                    value={settingsForm.cooldown_hours}
                    onChange={(e) =>
                      setSettingsForm((f) => ({ ...f, cooldown_hours: e.target.value }))
                    }
                    style={inputStyle}
                  />
                </div>
                <div>
                  <label style={labelStyle}>קבוצות ליצור ביום</label>
                  <input
                    type="number"
                    min={1}
                    max={50}
                    value={settingsForm.groups_per_day}
                    onChange={(e) =>
                      setSettingsForm((f) => ({ ...f, groups_per_day: e.target.value }))
                    }
                    style={inputStyle}
                  />
                </div>
              </div>
              <div style={{ display: "flex", gap: "0.5rem" }}>
                <button type="submit" style={btnPrimary} disabled={savingSettings}>
                  {savingSettings ? "שומר..." : "שמור הגדרות"}
                </button>
                <button
                  type="button"
                  style={btnSecondary}
                  onClick={() => setShowSettings(false)}
                >
                  ביטול
                </button>
              </div>
            </form>
          )}

          {/* Create Group Panel */}
          {showCreate && (
            <form
              onSubmit={createGroup}
              style={{
                marginBottom: "1.25rem",
                padding: "1.1rem 1.25rem",
                borderRadius: 14,
                background: cardBg,
                border: `1px solid rgba(59,130,246,0.35)`,
              }}
            >
              <div
                style={{
                  fontWeight: 700,
                  fontSize: "0.95rem",
                  color: tokens.textPrimary,
                  marginBottom: "1rem",
                }}
              >
                + צור קבוצה חדשה
              </div>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                  gap: "0.85rem",
                  marginBottom: "1rem",
                }}
              >
                <div style={{ gridColumn: "1 / -1" }}>
                  <label style={labelStyle}>שם הקבוצה (עברית) *</label>
                  <input
                    type="text"
                    required
                    placeholder="לדוגמה: קהילת משקיעים תל אביב"
                    value={createForm.name_he}
                    onChange={(e) =>
                      setCreateForm((f) => ({ ...f, name_he: e.target.value }))
                    }
                    style={inputStyle}
                  />
                </div>
                <div>
                  <label style={labelStyle}>ימי Warmup</label>
                  <input
                    type="number"
                    min={1}
                    max={14}
                    value={createForm.warmup_days}
                    onChange={(e) =>
                      setCreateForm((f) => ({ ...f, warmup_days: e.target.value }))
                    }
                    style={inputStyle}
                  />
                </div>
                <div>
                  <label style={labelStyle}>סוג קבוצה</label>
                  <select
                    value={createForm.is_private ? "private" : "public"}
                    onChange={(e) =>
                      setCreateForm((f) => ({
                        ...f,
                        is_private: e.target.value === "private",
                      }))
                    }
                    style={inputStyle}
                  >
                    <option value="private">פרטית</option>
                    <option value="public">ציבורית</option>
                  </select>
                </div>
                <div>
                  <label style={labelStyle}>קישור הזמנה (אופציונלי)</label>
                  <input
                    type="text"
                    placeholder="https://t.me/..."
                    value={createForm.invite_link}
                    onChange={(e) =>
                      setCreateForm((f) => ({ ...f, invite_link: e.target.value }))
                    }
                    style={inputStyle}
                  />
                </div>
              </div>
              <div style={{ display: "flex", gap: "0.5rem" }}>
                <button type="submit" style={btnPrimary} disabled={creatingGroup}>
                  {creatingGroup ? "יוצר..." : "צור קבוצה"}
                </button>
                <button
                  type="button"
                  style={btnSecondary}
                  onClick={() => setShowCreate(false)}
                >
                  ביטול
                </button>
              </div>
            </form>
          )}

          {/* Loading / Error */}
          {isLoading && (
            <div style={{ color: tokens.textMuted, padding: "1rem 0" }}>
              {t("loading")}
            </div>
          )}
          {error && (
            <div style={{ color: tokens.danger, padding: "0.5rem 0" }}>
              שגיאה בטעינה: {String(error.message || error)}
            </div>
          )}

          {/* Groups List */}
          {data?.groups && (
            <>
              <div
                style={{
                  fontSize: "0.72rem",
                  fontFamily: "var(--font-mono)",
                  color: tokens.textMuted,
                  marginBottom: "0.75rem",
                  display: "flex",
                  justifyContent: "space-between",
                  flexWrap: "wrap",
                  gap: "0.25rem",
                }}
              >
                <span>עודכן: {data.updated_at || "—"}</span>
                <span>{groups.length} קבוצות</span>
              </div>

              {groups.length === 0 ? (
                <div
                  style={{
                    textAlign: "center",
                    padding: "3rem 1rem",
                    color: tokens.textMuted,
                    fontSize: "0.9rem",
                    background: cardBg,
                    borderRadius: 14,
                    border,
                  }}
                >
                  <div style={{ fontSize: "2rem", marginBottom: "0.5rem" }}>🏭</div>
                  <div>אין קבוצות עדיין</div>
                  <div style={{ fontSize: "0.8rem", marginTop: "0.35rem" }}>
                    לחץ &quot;צור קבוצה חדשה&quot; כדי להתחיל
                  </div>
                </div>
              ) : (
                <ul
                  style={{
                    listStyle: "none",
                    padding: 0,
                    margin: 0,
                    display: "flex",
                    flexDirection: "column",
                    gap: "0.75rem",
                  }}
                >
                  {groups.map((g) => {
                    const neon = g.in_search && !stealth;
                    const phase = g.in_search ? "in_search" : g.warmup_days >= 14 ? "public_trial" : "warmup";
                    const phaseColor = PHASE_COLORS[phase] || tokens.textMuted;
                    const phaseLabel = PHASE_LABELS[phase] || phase;
                    const isBusy = busyId === g.id || busyId === `del-${g.id}`;

                    return (
                      <li
                        key={g.id}
                        className={neon ? "telefix-row-in-search" : undefined}
                        style={{
                          padding: "1rem 1.1rem",
                          borderRadius: 12,
                          background: cardBg,
                          border: neon
                            ? `1px solid rgba(57, 255, 20, 0.55)`
                            : border,
                          display: "flex",
                          flexDirection: "column",
                          gap: "0.65rem",
                        }}
                      >
                        {/* Row top */}
                        <div
                          style={{
                            display: "flex",
                            flexWrap: "wrap",
                            alignItems: "center",
                            gap: "0.65rem 1rem",
                          }}
                        >
                          {/* Name + ID */}
                          <div style={{ minWidth: 0, flex: "1 1 180px" }}>
                            <div
                              style={{
                                fontWeight: 700,
                                fontSize: "1rem",
                                color: tokens.textPrimary,
                                wordBreak: "break-word",
                              }}
                            >
                              {g.name_he || "—"}
                            </div>
                            <div
                              style={{
                                fontSize: "0.72rem",
                                color: tokens.textMuted,
                                marginTop: 3,
                                fontFamily: "var(--font-mono)",
                              }}
                            >
                              {g.id}
                            </div>
                          </div>

                          {/* Phase badge */}
                          <div
                            style={{
                              padding: "4px 10px",
                              borderRadius: 20,
                              fontSize: "0.75rem",
                              fontWeight: 700,
                              color: phaseColor,
                              background: `${phaseColor}18`,
                              border: `1px solid ${phaseColor}44`,
                              whiteSpace: "nowrap",
                            }}
                          >
                            {phaseLabel}
                          </div>

                          {/* Private/Public */}
                          <div
                            style={{
                              fontSize: "0.8rem",
                              fontWeight: 600,
                              color: g.is_private ? tokens.warning : tokens.success,
                              whiteSpace: "nowrap",
                            }}
                          >
                            {g.is_private ? "פרטית" : "ציבורית"}
                          </div>

                          {/* Actions */}
                          <div
                            style={{
                              display: "flex",
                              gap: "0.4rem",
                              marginInlineStart: isRTL ? 0 : "auto",
                              marginInlineEnd: isRTL ? "auto" : 0,
                              flexWrap: "wrap",
                            }}
                          >
                            {!g.in_search && (
                              <button
                                type="button"
                                disabled={isBusy}
                                onClick={() => forceSearch(g.id)}
                                style={{
                                  padding: "6px 11px",
                                  borderRadius: 7,
                                  border: `1px solid rgba(59,130,246,0.4)`,
                                  background: "rgba(59,130,246,0.1)",
                                  color: "#93c5fd",
                                  fontSize: "0.76rem",
                                  fontWeight: 600,
                                  cursor: isBusy ? "wait" : "pointer",
                                  whiteSpace: "nowrap",
                                }}
                              >
                                {busyId === g.id ? "..." : "⬆ דחוף לחיפוש"}
                              </button>
                            )}
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => deleteGroup(g.id, g.name_he)}
                              style={{
                                padding: "6px 11px",
                                borderRadius: 7,
                                border: `1px solid rgba(239,68,68,0.3)`,
                                background: "rgba(239,68,68,0.07)",
                                color: "#fca5a5",
                                fontSize: "0.76rem",
                                fontWeight: 600,
                                cursor: isBusy ? "wait" : "pointer",
                                whiteSpace: "nowrap",
                              }}
                            >
                              {busyId === `del-${g.id}` ? "..." : "✕ מחק"}
                            </button>
                          </div>
                        </div>

                        {/* Warmup progress bar */}
                        <WarmupBar days={g.warmup_days} maxDays={14} tokens={tokens} />
                      </li>
                    );
                  })}
                </ul>
              )}
            </>
          )}
        </div>
      </div>
    </PageTransition>
  );
}
