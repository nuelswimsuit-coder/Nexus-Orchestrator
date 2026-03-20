"use client";

import { useCallback, useEffect, useState } from "react";
import useSWR from "swr";
import { swrFetcher, patchConfig } from "@/lib/api";
import type { ConfigResponse } from "@/lib/api";
import { useStealth } from "@/lib/stealth";
import { useTheme } from "@/lib/theme";
import type { ColorMode, TextScale } from "@/lib/theme";
import { useI18n } from "@/lib/i18n";
import PageTransition from "@/components/PageTransition";

// ── Shared primitives (theme-aware) ───────────────────────────────────────────

function Section({
  title,
  icon,
  children,
}: {
  title: string;
  icon: string;
  children: React.ReactNode;
}) {
  const { tokens, isHighContrast } = useTheme();
  return (
    <div
      style={{
        background: isHighContrast ? tokens.surface1 : "#0a0e1a",
        border: isHighContrast
          ? `2px solid ${tokens.borderDefault}`
          : "1px solid #1e293b",
        borderRadius: "12px",
        padding: "1.25rem",
        marginBottom: "1.25rem",
      }}
    >
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: isHighContrast ? "0.85rem" : "0.7rem",
          fontWeight: 700,
          letterSpacing: "0.1em",
          textTransform: "uppercase" as const,
          color: isHighContrast ? tokens.textMuted : "#475569",
          marginBottom: "1rem",
          display: "flex",
          alignItems: "center",
          gap: "0.4rem",
        }}
      >
        <span>{icon}</span> {title}
      </div>
      {children}
    </div>
  );
}

function ReadOnlyRow({
  label,
  value,
  hint,
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  const { tokens, isHighContrast } = useTheme();
  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        padding: "0.6rem 0",
        borderBottom: isHighContrast
          ? `1px solid ${tokens.borderFaint}`
          : "1px solid #0f172a",
        gap: "1rem",
      }}
    >
      <div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: isHighContrast ? "0.9rem" : "0.78rem",
            color: isHighContrast ? tokens.textSecondary : "#94a3b8",
            fontWeight: isHighContrast ? 600 : 400,
          }}
        >
          {label}
        </div>
        {hint && (
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: isHighContrast ? "0.75rem" : "0.62rem",
              color: isHighContrast ? tokens.textMuted : "#334155",
              marginTop: "2px",
            }}
          >
            {hint}
          </div>
        )}
      </div>
      <div
        style={{
          fontFamily: "var(--font-mono)",
          fontSize: isHighContrast ? "0.9rem" : "0.78rem",
          color: isHighContrast ? tokens.textMuted : "#475569",
          textAlign: "right" as const,
          wordBreak: "break-all" as const,
          maxWidth: "55%",
        }}
      >
        {value}
      </div>
    </div>
  );
}

// ── Editable field ────────────────────────────────────────────────────────────

function EditableRow({
  label,
  hint,
  value,
  type = "number",
  unit,
  min,
  max,
  step,
  options,
  onChange,
  dirty,
}: {
  label: string;
  hint?: string;
  value: string | number;
  type?: "number" | "text" | "select";
  unit?: string;
  min?: number;
  max?: number;
  step?: number;
  options?: string[];
  onChange: (v: string) => void;
  dirty: boolean;
}) {
  const { tokens, isHighContrast } = useTheme();

  const inputStyle: React.CSSProperties = {
    fontFamily: "var(--font-mono)",
    fontSize: isHighContrast ? "0.9rem" : "0.78rem",
    color: isHighContrast
      ? tokens.textPrimary
      : dirty
      ? "#f1f5f9"
      : "#94a3b8",
    background: isHighContrast
      ? dirty
        ? tokens.accentSubtle
        : tokens.surface2
      : dirty
      ? "rgba(168,85,247,0.08)"
      : "#0f172a",
    border: isHighContrast
      ? `2px solid ${dirty ? tokens.accent : tokens.borderDefault}`
      : `1px solid ${dirty ? "#a855f766" : "#1e293b"}`,
    borderRadius: "6px",
    padding: "3px 8px",
    outline: "none",
    width: type === "number" ? "80px" : "160px",
    textAlign: type === "number" ? ("right" as const) : ("left" as const),
    transition: "border-color 0.2s, background 0.2s",
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "0.6rem 0",
        borderBottom: isHighContrast
          ? `1px solid ${tokens.borderFaint}`
          : "1px solid #0f172a",
        gap: "1rem",
      }}
    >
      <div>
        <div
          style={{
            fontFamily: "var(--font-mono)",
            fontSize: isHighContrast ? "0.9rem" : "0.78rem",
            color: isHighContrast ? tokens.textSecondary : "#94a3b8",
            fontWeight: isHighContrast ? 600 : 400,
          }}
        >
          {label}
        </div>
        {hint && (
          <div
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: isHighContrast ? "0.75rem" : "0.62rem",
              color: isHighContrast ? tokens.textMuted : "#334155",
              marginTop: "2px",
            }}
          >
            {hint}
          </div>
        )}
      </div>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.4rem",
          flexShrink: 0,
        }}
      >
        {type === "select" && options ? (
          <select
            value={String(value)}
            onChange={(e) => onChange(e.target.value)}
            style={{ ...inputStyle, width: "120px", cursor: "pointer" }}
          >
            {options.map((o) => (
              <option key={o} value={o}>
                {o}
              </option>
            ))}
          </select>
        ) : (
          <input
            type={type === "number" ? "number" : "text"}
            value={value}
            min={min}
            max={max}
            step={step ?? 1}
            onChange={(e) => onChange(e.target.value)}
            style={inputStyle}
          />
        )}
        {unit && (
          <span
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: isHighContrast ? "0.78rem" : "0.65rem",
              color: isHighContrast ? tokens.textMuted : "#334155",
            }}
          >
            {unit}
          </span>
        )}
        {dirty && (
          <span
            style={{ fontSize: "0.55rem", color: isHighContrast ? tokens.accent : "#a855f7" }}
            title="Unsaved change"
          >
            ●
          </span>
        )}
      </div>
    </div>
  );
}

// ── Toggle (reusable) ─────────────────────────────────────────────────────────

function Toggle({
  value,
  onChange,
  onColor,
}: {
  value: boolean;
  onChange: () => void;
  onColor?: string;
}) {
  const { tokens, isHighContrast } = useTheme();
  const activeColor = onColor ?? (isHighContrast ? tokens.success : "#22c55e");
  return (
    <button
      onClick={onChange}
      style={{
        width: 44,
        height: 24,
        borderRadius: 12,
        background: value ? activeColor : isHighContrast ? tokens.surface3 : "#1e293b",
        border: isHighContrast ? `2px solid ${tokens.borderDefault}` : "none",
        cursor: "pointer",
        position: "relative",
        boxShadow: value && !isHighContrast ? `0 0 8px ${activeColor}88` : "none",
        transition: "background 0.2s",
        flexShrink: 0,
      }}
    >
      <span
        style={{
          position: "absolute",
          top: isHighContrast ? 2 : 3,
          left: value ? (isHighContrast ? 20 : 22) : 3,
          width: 18,
          height: 18,
          borderRadius: "50%",
          background: value
            ? "#fff"
            : isHighContrast
            ? tokens.textMuted
            : "#475569",
          transition: "left 0.2s",
        }}
      />
    </button>
  );
}

// ── Settings page ─────────────────────────────────────────────────────────────

export default function SettingsPage() {
  const { stealth, toggleStealth, stealthOverride, toggleOverride } = useStealth();
  const { textScale, setTextScale, colorMode, setColorMode, tokens, isHighContrast } = useTheme();
  const { t } = useI18n();

  // Fetch live config from the API
  const { data: cfg, mutate } = useSWR<ConfigResponse>(
    "/api/config",
    swrFetcher<ConfigResponse>,
    { refreshInterval: 0, revalidateOnFocus: false }
  );

  const [draft, setDraft] = useState<Partial<ConfigResponse>>({});
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<"idle" | "ok" | "error">("idle");
  const [errorMsg, setErrorMsg] = useState("");

  useEffect(() => {
    if (cfg && Object.keys(draft).length === 0) {
      setDraft({ ...cfg });
    }
  }, [cfg]); // eslint-disable-line react-hooks/exhaustive-deps

  const field = <K extends keyof ConfigResponse>(key: K): ConfigResponse[K] =>
    (draft[key] ?? cfg?.[key]) as ConfigResponse[K];

  const setField = useCallback(
    <K extends keyof ConfigResponse>(key: K, raw: string) => {
      const schema: Record<string, "int" | "float" | "str"> = {
        master_cpu_cap_percent: "float",
        master_ram_cap_mb: "float",
        worker_max_jobs: "int",
        task_default_timeout: "int",
        worker_max_tries: "int",
        worker_ip: "str",
        worker_ssh_user: "str",
        worker_deploy_root_linux: "str",
        log_level: "str",
      };
      const t2 = schema[key as string];
      const parsed: ConfigResponse[K] =
        t2 === "int"
          ? (parseInt(raw, 10) as ConfigResponse[K])
          : t2 === "float"
          ? (parseFloat(raw) as ConfigResponse[K])
          : (raw as ConfigResponse[K]);
      setDraft((prev) => ({ ...prev, [key]: parsed }));
      setSaveStatus("idle");
    },
    []
  );

  const isDirty = (key: keyof ConfigResponse): boolean =>
    cfg !== undefined && draft[key] !== undefined && draft[key] !== cfg[key];

  const anyDirty =
    cfg !== undefined &&
    (Object.keys(draft) as (keyof ConfigResponse)[]).some(isDirty);

  const handleSave = useCallback(async () => {
    if (!anyDirty || saving) return;
    setSaving(true);
    setSaveStatus("idle");
    const patch: Partial<ConfigResponse> = {};
    (Object.keys(draft) as (keyof ConfigResponse)[]).forEach((k) => {
      if (isDirty(k)) (patch as Record<string, unknown>)[k] = draft[k];
    });
    try {
      const updated = await patchConfig(patch);
      await mutate(updated, false);
      setDraft({ ...updated });
      setSaveStatus("ok");
      setTimeout(() => setSaveStatus("idle"), 3000);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : String(err));
      setSaveStatus("error");
    } finally {
      setSaving(false);
    }
  }, [anyDirty, saving, draft, cfg, mutate]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleReset = useCallback(() => {
    if (cfg) {
      setDraft({ ...cfg });
      setSaveStatus("idle");
    }
  }, [cfg]);

  // ── Appearance section ─────────────────────────────────────────────────────

  const colorModes: { mode: ColorMode; label: string; labelHe: string }[] = [
    { mode: "standard",     label: "Standard",      labelHe: "רגיל" },
    { mode: "high-contrast", label: "High Contrast", labelHe: "ניגודיות גבוהה" },
  ];

  const textScales: { scale: TextScale; label: string; labelHe: string }[] = [
    { scale: "small",  label: "Small",  labelHe: "קטן" },
    { scale: "medium", label: "Medium", labelHe: "בינוני" },
    { scale: "large",  label: "Large",  labelHe: "גדול" },
  ];

  return (
    <PageTransition>
      <div
        style={{
          maxWidth: "900px",
          margin: "0 auto",
          padding: "2rem 1.5rem",
          color: isHighContrast ? tokens.textPrimary : undefined,
        }}
      >
        {/* ── Page header ── */}
        <div style={{ marginBottom: "1.5rem" }}>
          <h1
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: isHighContrast ? "1.875rem" : "1.1rem",
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              color: isHighContrast
                ? tokens.textPrimary
                : stealth
                ? "#334155"
                : "#f1f5f9",
              marginBottom: "0.25rem",
            }}
          >
            ⚙️ {t("settings")}
          </h1>
          <p
            style={{
              fontFamily: "var(--font-mono)",
              fontSize: isHighContrast ? "0.9rem" : "0.75rem",
              color: isHighContrast ? tokens.textMuted : "#475569",
            }}
          >
            {t("nav_desc_settings")} — Live configuration · changes applied immediately
          </p>
        </div>

        {/* ── Appearance ── */}
        <Section title={`${t("color_mode")} & ${t("text_size")}`} icon="🎨">

          {/* Color Mode toggle */}
          <div
            style={{
              padding: "0.75rem 0",
              borderBottom: isHighContrast
                ? `1px solid ${tokens.borderFaint}`
                : "1px solid #0f172a",
            }}
          >
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: isHighContrast ? "0.9rem" : "0.78rem",
                fontWeight: isHighContrast ? 700 : 400,
                color: isHighContrast ? tokens.textSecondary : "#94a3b8",
                marginBottom: "0.6rem",
              }}
            >
              {t("color_mode")} / מצב צבע
            </div>
            <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
              {colorModes.map(({ mode, label, labelHe }) => {
                const isActive = colorMode === mode;
                return (
                  <button
                    key={mode}
                    onClick={() => setColorMode(mode)}
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: isHighContrast ? "0.9rem" : "0.72rem",
                      fontWeight: isActive ? 700 : 400,
                      padding: isHighContrast ? "8px 18px" : "5px 14px",
                      borderRadius: "8px",
                      border: isHighContrast
                        ? `2px solid ${isActive ? tokens.accent : tokens.borderDefault}`
                        : `1px solid ${isActive ? "#00b4ff55" : "#1e293b"}`,
                      background: isHighContrast
                        ? isActive
                          ? tokens.accentSubtle
                          : tokens.surface2
                        : isActive
                        ? "rgba(0,180,255,0.12)"
                        : "transparent",
                      color: isHighContrast
                        ? isActive
                          ? tokens.accent
                          : tokens.textMuted
                        : isActive
                        ? "#38d4ff"
                        : "#475569",
                      cursor: "pointer",
                      transition: "all 0.18s ease",
                      boxShadow:
                        isActive && !isHighContrast
                          ? "0 0 8px rgba(0,180,255,0.18)"
                          : "none",
                    }}
                  >
                    {label} / {labelHe}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Text Size slider buttons */}
          <div
            style={{
              padding: "0.75rem 0",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: "1rem",
              borderBottom: isHighContrast
                ? `1px solid ${tokens.borderFaint}`
                : "1px solid #0f172a",
            }}
          >
            <div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.9rem" : "0.78rem",
                  fontWeight: isHighContrast ? 700 : 400,
                  color: isHighContrast ? tokens.textSecondary : "#94a3b8",
                }}
              >
                {t("text_size")}
              </div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.78rem" : "0.62rem",
                  color: isHighContrast ? tokens.textMuted : "#334155",
                }}
              >
                Scales all UI typography · Hebrew is always 15% larger
              </div>
            </div>
            <div style={{ display: "flex", gap: "4px", flexShrink: 0 }}>
              {textScales.map(({ scale, label, labelHe }) => {
                const isActive = textScale === scale;
                return (
                  <button
                    key={scale}
                    onClick={() => setTextScale(scale)}
                    title={labelHe}
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: isHighContrast ? "0.85rem" : "0.65rem",
                      fontWeight: isActive ? 700 : 400,
                      padding: isHighContrast ? "6px 14px" : "4px 10px",
                      borderRadius: "6px",
                      border: isHighContrast
                        ? `2px solid ${isActive ? tokens.accent : tokens.borderDefault}`
                        : `1px solid ${isActive ? "#00b4ff55" : "#1e293b"}`,
                      background: isHighContrast
                        ? isActive
                          ? tokens.accentSubtle
                          : tokens.surface2
                        : isActive
                        ? "rgba(0,180,255,0.12)"
                        : "transparent",
                      color: isHighContrast
                        ? isActive
                          ? tokens.accent
                          : tokens.textMuted
                        : isActive
                        ? "#38d4ff"
                        : "#475569",
                      cursor: "pointer",
                      transition: "all 0.18s ease",
                      boxShadow:
                        isActive && !isHighContrast
                          ? "0 0 8px rgba(0,180,255,0.18)"
                          : "none",
                    }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Stealth Mode */}
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "0.6rem 0",
              borderBottom: isHighContrast
                ? `1px solid ${tokens.borderFaint}`
                : "1px solid #0f172a",
            }}
          >
            <div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.9rem" : "0.78rem",
                  fontWeight: isHighContrast ? 600 : 400,
                  color: isHighContrast ? tokens.textSecondary : "#94a3b8",
                }}
              >
                {t("stealth_mode")} / מצב סמוי
              </div>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.75rem" : "0.62rem",
                  color: isHighContrast ? tokens.textMuted : "#334155",
                }}
              >
                Suppress all RGB glow effects
              </div>
            </div>
            <Toggle value={stealth} onChange={toggleStealth} />
          </div>

          {stealth && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "0.6rem 0",
                borderBottom: isHighContrast
                  ? `1px solid ${tokens.borderFaint}`
                  : "1px solid #0f172a",
              }}
            >
              <div>
                <div
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: isHighContrast ? "0.9rem" : "0.78rem",
                    fontWeight: isHighContrast ? 600 : 400,
                    color: isHighContrast ? tokens.textSecondary : "#94a3b8",
                  }}
                >
                  {t("override")} / עקיפה
                </div>
                <div
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: isHighContrast ? "0.75rem" : "0.62rem",
                    color: isHighContrast ? tokens.textMuted : "#334155",
                  }}
                >
                  Allow scraper to run at full CPU in stealth
                </div>
              </div>
              <Toggle
                value={stealthOverride}
                onChange={toggleOverride}
                onColor={isHighContrast ? tokens.warning : "#f59e0b"}
              />
            </div>
          )}
        </Section>

        {/* ── Connections (read-only) ── */}
        <Section title="Connections / חיבורים" icon="🔌">
          <ReadOnlyRow
            label="Redis URL"
            value="redis://127.0.0.1:6379"
            hint="Shared broker for all nodes"
          />
          <ReadOnlyRow
            label="API Server"
            value="http://localhost:8001"
            hint="FastAPI Control Center"
          />
          <ReadOnlyRow
            label="Dashboard URL"
            value="http://localhost:3000"
            hint="This dashboard"
          />
          <ReadOnlyRow
            label="Telegram Chat ID"
            value="7849455058"
            hint="Admin notification target"
          />
          <ReadOnlyRow
            label="WhatsApp Provider"
            value="mock"
            hint="Set to evolution or twilio for live delivery"
          />
        </Section>

        {/* ── Performance (editable) ── */}
        <Section title="Performance / ביצועים" icon="⚡">
          {!cfg ? (
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: isHighContrast ? "0.88rem" : "0.7rem",
                color: isHighContrast ? tokens.textMuted : "#334155",
                padding: "0.5rem 0",
              }}
            >
              {t("loading")}
            </div>
          ) : (
            <>
              <div
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.8rem" : "0.62rem",
                  color: isHighContrast ? tokens.textMuted : "#475569",
                  background: isHighContrast ? tokens.surface2 : "#0f172a",
                  border: isHighContrast
                    ? `1px solid ${tokens.borderFaint}`
                    : "1px solid #1e293b",
                  borderRadius: "6px",
                  padding: "0.4rem 0.75rem",
                  marginBottom: "0.5rem",
                }}
              >
                💡 Set CPU or RAM cap to{" "}
                <strong
                  style={{
                    color: isHighContrast ? tokens.textSecondary : "#94a3b8",
                  }}
                >
                  0
                </strong>{" "}
                to remove the limit entirely
              </div>
              <EditableRow
                label="Master CPU Cap"
                hint="0 = no cap  |  1–100 = max % of one core"
                value={field("master_cpu_cap_percent")}
                type="number"
                unit="%"
                min={0}
                max={100}
                step={5}
                onChange={(v) => setField("master_cpu_cap_percent", v)}
                dirty={isDirty("master_cpu_cap_percent")}
              />
              <EditableRow
                label="Master RAM Cap"
                hint="0 = no cap  |  e.g. 4096 = 4 GB, 16384 = 16 GB"
                value={field("master_ram_cap_mb")}
                type="number"
                unit="MB"
                min={0}
                step={512}
                onChange={(v) => setField("master_ram_cap_mb", v)}
                dirty={isDirty("master_ram_cap_mb")}
              />
              <EditableRow
                label="Worker Max Jobs"
                hint="Parallel tasks per worker node (1–64)"
                value={field("worker_max_jobs")}
                type="number"
                min={1}
                max={64}
                onChange={(v) => setField("worker_max_jobs", v)}
                dirty={isDirty("worker_max_jobs")}
              />
              <EditableRow
                label="Task Timeout"
                hint="Seconds before a task is forcibly killed"
                value={field("task_default_timeout")}
                type="number"
                unit="s"
                min={10}
                step={30}
                onChange={(v) => setField("task_default_timeout", v)}
                dirty={isDirty("task_default_timeout")}
              />
              <EditableRow
                label="Max Retries"
                hint="Exponential backoff on failure"
                value={field("worker_max_tries")}
                type="number"
                min={1}
                max={10}
                onChange={(v) => setField("worker_max_tries", v)}
                dirty={isDirty("worker_max_tries")}
              />
              <EditableRow
                label="Log Level"
                hint="Verbosity of structured logs"
                value={field("log_level")}
                type="select"
                options={["DEBUG", "INFO", "WARNING", "ERROR"]}
                onChange={(v) => setField("log_level", v)}
                dirty={isDirty("log_level")}
              />
            </>
          )}
        </Section>

        {/* ── Deployer (editable) ── */}
        <Section title="Deployer — SSH" icon="🚀">
          {!cfg ? (
            <div
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: isHighContrast ? "0.88rem" : "0.7rem",
                color: isHighContrast ? tokens.textMuted : "#334155",
                padding: "0.5rem 0",
              }}
            >
              {t("loading")}
            </div>
          ) : (
            <>
              <EditableRow
                label="Worker IP"
                hint="Direct IP of the Linux worker laptop"
                value={field("worker_ip")}
                type="text"
                onChange={(v) => setField("worker_ip", v)}
                dirty={isDirty("worker_ip")}
              />
              <EditableRow
                label="SSH User"
                hint="Username for SSH login (e.g. yadmin)"
                value={field("worker_ssh_user")}
                type="text"
                onChange={(v) => setField("worker_ssh_user", v)}
                dirty={isDirty("worker_ssh_user")}
              />
              <EditableRow
                label="Deploy Root (Linux)"
                hint="Destination path on the worker"
                value={field("worker_deploy_root_linux")}
                type="text"
                onChange={(v) => setField("worker_deploy_root_linux", v)}
                dirty={isDirty("worker_deploy_root_linux")}
              />
            </>
          )}
        </Section>

        {/* ── Automation (read-only) ── */}
        <Section title="Automation / אוטומציה — פרוייקטים וגירוד מידע" icon="🤖">
          <ReadOnlyRow
            label="גירוד מידע — CPU Threshold / סף מעבד"
            value="30%"
            hint="Abort if CPU exceeds this during scraping"
          />
          <ReadOnlyRow
            label="גירוד מתקדם — CPU Cap / סף גירוד מתקדם"
            value="40%"
            hint="Abort super-scrape if exceeded"
          />
          <ReadOnlyRow
            label="Min Rescrape Interval / מינימום זמן בין גירודים"
            value="6h"
            hint="Skip if scraped within this window"
          />
          <ReadOnlyRow
            label="Min Members Filter / מינימום חברים"
            value="500"
            hint="Only scrape groups with ≥ 500 members"
          />
          <ReadOnlyRow
            label="Daily Reports / דוחות יומיים"
            value="09:00 · 14:00 · 21:00"
            hint="Morning briefing · Afternoon pulse · Evening report"
          />
        </Section>

        {/* ── Save bar ── */}
        {(anyDirty || saveStatus !== "idle") && (
          <div
            style={{
              position: "sticky",
              bottom: "1.5rem",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: "1rem",
              background: isHighContrast
                ? tokens.surface1
                : "linear-gradient(135deg, #0d1525, #080d18)",
              border: isHighContrast
                ? `2px solid ${
                    saveStatus === "ok"
                      ? tokens.success
                      : saveStatus === "error"
                      ? tokens.danger
                      : tokens.accent
                  }`
                : `1px solid ${
                    saveStatus === "ok"
                      ? "#22c55e55"
                      : saveStatus === "error"
                      ? "#ef444455"
                      : "#a855f755"
                  }`,
              borderRadius: "10px",
              padding: "0.75rem 1.25rem",
              boxShadow: isHighContrast
                ? "0 4px 16px rgba(0,0,0,0.15)"
                : "0 8px 32px rgba(0,0,0,0.5)",
            }}
          >
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: isHighContrast ? "0.88rem" : "0.7rem",
                fontWeight: isHighContrast ? 700 : 400,
                color: isHighContrast
                  ? saveStatus === "ok"
                    ? tokens.success
                    : saveStatus === "error"
                    ? tokens.danger
                    : tokens.accent
                  : saveStatus === "ok"
                  ? "#22c55e"
                  : saveStatus === "error"
                  ? "#ef4444"
                  : "#a855f7",
              }}
            >
              {saveStatus === "ok"
                ? "✓ Settings saved and applied"
                : saveStatus === "error"
                ? `✗ ${errorMsg}`
                : `${
                    Object.keys(draft).filter((k) =>
                      isDirty(k as keyof ConfigResponse)
                    ).length
                  } unsaved change(s)`}
            </span>

            <div style={{ display: "flex", gap: "0.5rem" }}>
              <button
                onClick={handleReset}
                disabled={saving}
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.85rem" : "0.68rem",
                  fontWeight: 700,
                  padding: isHighContrast ? "6px 18px" : "5px 14px",
                  borderRadius: "6px",
                  border: isHighContrast
                    ? `2px solid ${tokens.borderDefault}`
                    : "1px solid #1e293b",
                  background: "transparent",
                  color: isHighContrast ? tokens.textMuted : "#475569",
                  cursor: "pointer",
                  opacity: saving ? 0.5 : 1,
                }}
              >
                {t("cancel")}
              </button>

              <button
                onClick={handleSave}
                disabled={saving || !anyDirty}
                style={{
                  fontFamily: "var(--font-mono)",
                  fontSize: isHighContrast ? "0.85rem" : "0.68rem",
                  fontWeight: 700,
                  padding: isHighContrast ? "6px 22px" : "5px 18px",
                  borderRadius: "6px",
                  border: isHighContrast
                    ? `2px solid ${tokens.accent}`
                    : "1px solid #a855f766",
                  background: isHighContrast
                    ? tokens.accentSubtle
                    : saving
                    ? "rgba(168,85,247,0.15)"
                    : "rgba(168,85,247,0.2)",
                  color: isHighContrast ? tokens.accent : "#c084fc",
                  cursor: saving ? "not-allowed" : "pointer",
                  opacity: saving || !anyDirty ? 0.6 : 1,
                  transition: "all 0.2s",
                  boxShadow:
                    !saving && anyDirty && !isHighContrast
                      ? "0 0 12px #a855f733"
                      : "none",
                }}
              >
                {saving ? t("processing") : "Save Changes"}
              </button>
            </div>
          </div>
        )}
      </div>
    </PageTransition>
  );
}
