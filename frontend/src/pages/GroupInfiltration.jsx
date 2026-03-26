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

const WARMUP_TARGET = 14;

// ── Small stat badge ──────────────────────────────────────────────────────────
function StatBadge({ label, value, color, tokens }) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        padding: "0.55rem 1rem",
        borderRadius: 10,
        background: "rgba(255,255,255,0.04)",
        border: `1px solid rgba(255,255,255,0.07)`,
        minWidth: 80,
      }}
    >
      <span style={{ fontSize: "1.3rem", fontWeight: 700, color }}>{value}</span>
      <span style={{ fontSize: "0.68rem", color: tokens.textMuted, marginTop: 2, whiteSpace: "nowrap" }}>{label}</span>
    </div>
  );
}

// ── Warmup progress bar ───────────────────────────────────────────────────────
function WarmupBar({ days, tokens }) {
  const pct = Math.min((days / WARMUP_TARGET) * 100, 100);
  const color = pct >= 100 ? "#39ff14" : pct >= 50 ? "#f59e0b" : "#3b82f6";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", minWidth: 120 }}>
      <div
        style={{
          flex: 1,
          height: 6,
          borderRadius: 3,
          background: "rgba(255,255,255,0.08)",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: "100%",
            background: color,
            borderRadius: 3,
            transition: "width 0.4s ease",
          }}
        />
      </div>
      <span
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: "0.75rem",
          fontWeight: 600,
          color,
          whiteSpace: "nowrap",
        }}
      >
        {days} / {WARMUP_TARGET}
      </span>
    </div>
  );
}

// ── Create Group Modal ────────────────────────────────────────────────────────
function CreateGroupModal({ onClose, onCreated, tokens, isRTL, t }) {
  const [tab, setTab] = useState("manual"); // "manual" | "telegram"
  const [form, setForm] = useState({
    name_he: "",
    group_id: "",
    is_private: false,
    telegram_link: "",
    notes: "",
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);

  const isManual = tab === "manual";

  function handleChange(e) {
    const { name, value, type, checked } = e.target;
    setForm((f) => ({ ...f, [name]: type === "checkbox" ? checked : value }));
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      const payload = {
        name_he: form.name_he.trim(),
        group_id: form.group_id.trim() || `grp_${Date.now()}`,
        is_private: form.is_private,
        telegram_link: form.telegram_link.trim() || null,
        notes: form.notes.trim() || null,
        create_on_telegram: !isManual,
      };
      const res = await fetch(`${API_BASE}/api/telefix/group-infiltration`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      onCreated(body);
    } catch (err) {
      setError(String(err.message || err));
    } finally {
      setBusy(false);
    }
  }

  const inputStyle = {
    width: "100%",
    padding: "0.55rem 0.75rem",
    borderRadius: 8,
    border: `1px solid ${tokens.borderSubtle}`,
    background: "rgba(255,255,255,0.04)",
    color: tokens.textPrimary,
    fontSize: "0.85rem",
    fontFamily: "var(--font-sans)",
    outline: "none",
    boxSizing: "border-box",
  };

  const labelStyle = {
    fontSize: "0.75rem",
    fontWeight: 600,
    color: tokens.textMuted,
    marginBottom: "0.3rem",
    display: "block",
  };

  return (
    <div
      role="dialog"
      aria-modal="true"
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 1000,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "rgba(0,0,0,0.65)",
        backdropFilter: "blur(4px)",
        padding: "1rem",
      }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div
        style={{
          width: "100%",
          maxWidth: 480,
          borderRadius: 16,
          background: "rgba(15,22,36,0.98)",
          border: `1px solid ${tokens.borderSubtle}`,
          boxShadow: "0 24px 64px rgba(0,0,0,0.6)",
          overflow: "hidden",
          direction: isRTL ? "rtl" : "ltr",
        }}
      >
        {/* Header */}
        <div
          style={{
            padding: "1.1rem 1.25rem 0.9rem",
            borderBottom: `1px solid ${tokens.borderSubtle}`,
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <h2 style={{ margin: 0, fontSize: "1rem", fontWeight: 700, color: tokens.textPrimary }}>
            {isManual ? t("telefix_modal_title_manual") : t("telefix_modal_title_tg")}
          </h2>
          <button
            onClick={onClose}
            style={{
              background: "none",
              border: "none",
              color: tokens.textMuted,
              fontSize: "1.2rem",
              cursor: "pointer",
              lineHeight: 1,
              padding: "2px 6px",
            }}
          >
            ×
          </button>
        </div>

        {/* Tab switcher */}
        <div
          style={{
            display: "flex",
            gap: 0,
            padding: "0.75rem 1.25rem 0",
          }}
        >
          {["manual", "telegram"].map((tabKey) => (
            <button
              key={tabKey}
              onClick={() => setTab(tabKey)}
              style={{
                flex: 1,
                padding: "0.5rem",
                border: "none",
                borderBottom: tab === tabKey ? `2px solid #3b82f6` : `2px solid transparent`,
                background: "none",
                color: tab === tabKey ? "#3b82f6" : tokens.textMuted,
                fontWeight: tab === tabKey ? 700 : 400,
                fontSize: "0.82rem",
                cursor: "pointer",
                transition: "all 0.15s",
              }}
            >
              {tabKey === "manual" ? t("telefix_tab_manual") : t("telefix_tab_telegram")}
            </button>
          ))}
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} style={{ padding: "1rem 1.25rem 1.25rem", display: "flex", flexDirection: "column", gap: "0.85rem" }}>
          <div>
            <label style={labelStyle}>{t("telefix_field_name_he")} *</label>
            <input
              name="name_he"
              value={form.name_he}
              onChange={handleChange}
              required
              placeholder="למשל: קבוצת נדל״ן תל אביב"
              style={inputStyle}
            />
          </div>

          {isManual && (
            <div>
              <label style={labelStyle}>{t("telefix_field_group_id")} *</label>
              <input
                name="group_id"
                value={form.group_id}
                onChange={handleChange}
                required={isManual}
                placeholder="-1001234567890"
                style={inputStyle}
                dir="ltr"
              />
            </div>
          )}

          <div>
            <label style={labelStyle}>{t("telefix_field_link")}</label>
            <input
              name="telegram_link"
              value={form.telegram_link}
              onChange={handleChange}
              placeholder="https://t.me/..."
              style={inputStyle}
              dir="ltr"
            />
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: "0.6rem" }}>
            <input
              type="checkbox"
              id="is_private"
              name="is_private"
              checked={form.is_private}
              onChange={handleChange}
              style={{ width: 16, height: 16, cursor: "pointer", accentColor: "#3b82f6" }}
            />
            <label htmlFor="is_private" style={{ ...labelStyle, margin: 0, cursor: "pointer" }}>
              {t("telefix_field_is_private")}
            </label>
          </div>

          <div>
            <label style={labelStyle}>{t("telefix_field_notes")}</label>
            <input
              name="notes"
              value={form.notes}
              onChange={handleChange}
              placeholder="..."
              style={inputStyle}
            />
          </div>

          {error && (
            <div
              style={{
                padding: "0.55rem 0.75rem",
                borderRadius: 8,
                background: "rgba(239,68,68,0.1)",
                border: "1px solid rgba(239,68,68,0.3)",
                color: "#f87171",
                fontSize: "0.8rem",
              }}
            >
              {error}
            </div>
          )}

          <div style={{ display: "flex", gap: "0.6rem", justifyContent: "flex-end", marginTop: "0.25rem" }}>
            <button
              type="button"
              onClick={onClose}
              style={{
                padding: "0.55rem 1rem",
                borderRadius: 8,
                border: `1px solid ${tokens.borderSubtle}`,
                background: "transparent",
                color: tokens.textMuted,
                fontSize: "0.82rem",
                cursor: "pointer",
              }}
            >
              {t("cancel")}
            </button>
            <button
              type="submit"
              disabled={busy}
              style={{
                padding: "0.55rem 1.2rem",
                borderRadius: 8,
                border: "none",
                background: busy ? "rgba(59,130,246,0.4)" : "linear-gradient(135deg,#3b82f6,#6366f1)",
                color: "#fff",
                fontSize: "0.82rem",
                fontWeight: 700,
                cursor: busy ? "wait" : "pointer",
                boxShadow: busy ? "none" : "0 2px 10px rgba(99,102,241,0.35)",
              }}
            >
              {busy
                ? t("telefix_submitting")
                : isManual
                ? t("telefix_submit_manual")
                : t("telefix_submit_tg")}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function GroupInfiltration() {
  const { t, isRTL } = useI18n();
  const { stealth } = useStealth();
  const { isHighContrast, tokens } = useTheme();
  const { mutate } = useSWRConfig();

  const [busyId, setBusyId] = useState(null);
  const [deletingId, setDeletingId] = useState(null);
  const [toast, setToast] = useState(null);
  const [showModal, setShowModal] = useState(false);

  const { data, error, isLoading } = useSWR(KEY, swrFetcher, {
    refreshInterval: 30_000,
  });

  const cardBg = isHighContrast ? tokens.surface2 : "rgba(37, 47, 61, 0.72)";
  const border = `1px solid ${tokens.borderSubtle}`;

  function showToast(type, text) {
    setToast({ type, text });
    setTimeout(() => setToast(null), 4000);
  }

  async function forceSearch(groupId, groupName) {
    setBusyId(groupId);
    try {
      const res = await fetch(
        `${API_BASE}/api/telefix/group-infiltration/${encodeURIComponent(groupId)}/force-search`,
        { method: "POST", headers: { "Content-Type": "application/json" } },
      );
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.detail || res.statusText);
      showToast("ok", body.detail || t("telefix_manual_search"));
      await mutate(KEY);
    } catch (e) {
      showToast("err", String(e.message || e));
    } finally {
      setBusyId(null);
    }
  }

  async function deleteGroup(groupId) {
    if (!window.confirm(`הסר את הקבוצה "${groupId}"?`)) return;
    setDeletingId(groupId);
    try {
      const res = await fetch(
        `${API_BASE}/api/telefix/group-infiltration/${encodeURIComponent(groupId)}`,
        { method: "DELETE" },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || res.statusText);
      }
      showToast("ok", `קבוצה "${groupId}" הוסרה`);
      await mutate(KEY);
    } catch (e) {
      showToast("err", String(e.message || e));
    } finally {
      setDeletingId(null);
    }
  }

  async function handleGroupCreated(newGroup) {
    setShowModal(false);
    showToast("ok", `קבוצה "${newGroup.name_he}" נוספה בהצלחה ✓`);
    await mutate(KEY);
  }

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
              flexWrap: "wrap",
              alignItems: "flex-start",
              justifyContent: "space-between",
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
                {t("telefix_infiltration_title")}
              </h1>
              <p
                style={{
                  margin: "0.35rem 0 0",
                  fontSize: "0.88rem",
                  color: tokens.textMuted,
                  maxWidth: 640,
                }}
              >
                {t("telefix_infiltration_sub")}
              </p>
            </div>

            <button
              type="button"
              onClick={() => setShowModal(true)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: "0.4rem",
                padding: "0.6rem 1.1rem",
                borderRadius: 10,
                border: "none",
                background: "linear-gradient(135deg,#0ea5e9,#6366f1)",
                color: "#fff",
                fontWeight: 700,
                fontSize: "0.85rem",
                cursor: "pointer",
                boxShadow: stealth ? "none" : "0 2px 12px rgba(99,102,241,0.4)",
                whiteSpace: "nowrap",
                flexShrink: 0,
              }}
            >
              {t("telefix_add_group")}
            </button>
          </header>

          {/* Stats row */}
          {data && (
            <div
              style={{
                display: "flex",
                gap: "0.65rem",
                marginBottom: "1.1rem",
                flexWrap: "wrap",
              }}
            >
              <StatBadge label={t("telefix_total_groups")} value={data.total ?? 0} color={tokens.textPrimary} tokens={tokens} />
              <StatBadge label={t("telefix_in_search_count")} value={data.in_search_count ?? 0} color="#39ff14" tokens={tokens} />
              <StatBadge label={t("telefix_warming_count")} value={data.warming_count ?? 0} color="#f59e0b" tokens={tokens} />
            </div>
          )}

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
                    ? isHighContrast
                      ? tokens.successSubtle
                      : "rgba(57, 255, 20, 0.08)"
                    : tokens.dangerSubtle,
                border,
                color: tokens.textPrimary,
              }}
            >
              {toast.text}
            </div>
          )}

          {isLoading && (
            <div style={{ color: tokens.textMuted }}>{t("loading")}</div>
          )}
          {error && (
            <div style={{ color: tokens.danger }}>
              {t("telefix_load_error")}: {String(error.message || error)}
            </div>
          )}

          {/* Groups list */}
          {data?.groups && (
            <>
              <div
                style={{
                  fontSize: "0.72rem",
                  fontFamily: "var(--font-mono)",
                  color: tokens.textMuted,
                  marginBottom: "0.75rem",
                }}
              >
                {t("telefix_updated")}: {data.updated_at || "—"}
              </div>

              {data.groups.length === 0 ? (
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
                  {t("telefix_no_groups")}
                </div>
              ) : (
                <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                  {data.groups.map((g) => {
                    const neon = g.in_search && !stealth;
                    const isBusy = busyId === g.id;
                    const isDeleting = deletingId === g.id;
                    return (
                      <li
                        key={g.id}
                        className={neon ? "telefix-row-in-search" : undefined}
                        style={{
                          display: "flex",
                          flexWrap: "wrap",
                          alignItems: "center",
                          gap: "0.65rem 1rem",
                          padding: "1rem 1.1rem",
                          borderRadius: 12,
                          background: cardBg,
                          border: neon
                            ? `1px solid rgba(57, 255, 20, 0.55)`
                            : border,
                          boxShadow: stealth ? "none" : undefined,
                        }}
                      >
                        {/* Name + ID */}
                        <div style={{ minWidth: 0, flex: "1 1 200px" }}>
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
                              display: "flex",
                              gap: "0.5rem",
                              flexWrap: "wrap",
                            }}
                          >
                            <span>id: {g.id}</span>
                            {g.telegram_link && (
                              <a
                                href={g.telegram_link}
                                target="_blank"
                                rel="noopener noreferrer"
                                style={{ color: "#38bdf8", textDecoration: "none" }}
                              >
                                🔗 link
                              </a>
                            )}
                          </div>
                        </div>

                        {/* Warmup bar */}
                        <WarmupBar days={g.warmup_days} tokens={tokens} />

                        {/* Private / Public */}
                        <div
                          style={{
                            fontSize: "0.82rem",
                            fontWeight: 600,
                            color: g.is_private ? tokens.warning : tokens.success,
                            whiteSpace: "nowrap",
                          }}
                        >
                          {g.is_private ? t("telefix_private") : t("telefix_public")}
                        </div>

                        {/* In search badge */}
                        <div
                          style={{
                            fontSize: "0.82rem",
                            fontWeight: 600,
                            color: g.in_search ? "#39ff14" : tokens.textMuted,
                            whiteSpace: "nowrap",
                          }}
                        >
                          {g.in_search
                            ? t("telefix_in_search_yes")
                            : t("telefix_in_search_no")}
                        </div>

                        {/* Actions */}
                        <div
                          style={{
                            display: "flex",
                            gap: "0.4rem",
                            marginInlineStart: isRTL ? 0 : "auto",
                            marginInlineEnd: isRTL ? "auto" : 0,
                          }}
                        >
                          {!g.in_search && (
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => forceSearch(g.id, g.name_he)}
                              style={{
                                padding: "7px 11px",
                                borderRadius: 8,
                                border: `1px solid ${tokens.borderSubtle}`,
                                background: isHighContrast ? tokens.surface3 : "rgba(99,179,237,0.12)",
                                color: tokens.textPrimary,
                                fontSize: "0.78rem",
                                fontWeight: 600,
                                cursor: isBusy ? "wait" : "pointer",
                                whiteSpace: "nowrap",
                              }}
                            >
                              {isBusy ? t("telefix_bulk_running") : t("telefix_manual_search")}
                            </button>
                          )}
                          <button
                            type="button"
                            disabled={isDeleting}
                            onClick={() => deleteGroup(g.id)}
                            style={{
                              padding: "7px 11px",
                              borderRadius: 8,
                              border: `1px solid rgba(239,68,68,0.3)`,
                              background: "rgba(239,68,68,0.07)",
                              color: "#f87171",
                              fontSize: "0.78rem",
                              fontWeight: 600,
                              cursor: isDeleting ? "wait" : "pointer",
                              whiteSpace: "nowrap",
                            }}
                          >
                            {isDeleting ? "…" : t("telefix_delete_group")}
                          </button>
                        </div>
                      </li>
                    );
                  })}
                </ul>
              )}
            </>
          )}
        </div>
      </div>

      {/* Create group modal */}
      {showModal && (
        <CreateGroupModal
          onClose={() => setShowModal(false)}
          onCreated={handleGroupCreated}
          tokens={tokens}
          isRTL={isRTL}
          t={t}
        />
      )}
    </PageTransition>
  );
}
