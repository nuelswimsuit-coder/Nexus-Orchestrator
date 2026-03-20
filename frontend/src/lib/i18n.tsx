"use client";

/**
 * i18n — Bilingual Hebrew/English Support for TeleFix OS
 *
 * Provides dynamic language switching with tech-native Hebrew translations.
 * Language preference is persisted in localStorage.
 */

import { createContext, useContext, useState, useEffect, type ReactNode } from "react";

export type Language = "en" | "he";

// ── Translation dictionary ────────────────────────────────────────────────────

const translations = {
  // ── Navigation labels (flat keys used in Sidebar) ──────────────────────────
  dashboard:   { en: "Dashboard",   he: "לוח בקרה" },
  operations:  { en: "Operations",  he: "פעילות" },
  fleet:       { en: "Fleet",       he: "צי מעבדים" },
  projects:    { en: "Project Hub", he: "מרכז פרויקטים" },
  treasury:    { en: "Treasury",    he: "כספים" },
  automation:  { en: "Automation",  he: "אוטומציה" },
  incubator:   { en: "Incubator",   he: "דגירה" },
  settings:    { en: "Settings",    he: "הגדרות" },
  about:       { en: "About",       he: "אודות" },
  modules:     { en: "Modules",     he: "מודולים" },

  // ── Navigation labels (dot-notation keys used in Header breadcrumb) ─────────
  "nav.dashboard":  { en: "Dashboard",   he: "לוח בקרה" },
  "nav.operations": { en: "Operations",  he: "פעילות" },
  "nav.fleet":      { en: "Fleet",       he: "צי מעבדים" },
  "nav.projects":   { en: "Project Hub", he: "מרכז פרויקטים" },
  "nav.treasury":   { en: "Treasury",    he: "כספים" },
  "nav.automation": { en: "Automation",  he: "אוטומציה" },
  "nav.incubator":  { en: "Incubator",   he: "דגירה" },
  "nav.settings":   { en: "Settings",    he: "הגדרות" },
  "nav.about":      { en: "About",       he: "אודות" },

  // ── Navigation descriptions ────────────────────────────────────────────────
  nav_desc_dashboard:  { en: "Overview",                    he: "סקירה כללית" },
  nav_desc_operations: { en: "פרוייקטים וגירוד מידע",    he: "פרוייקטים וגירוד מידע" },
  nav_desc_fleet:      { en: "Hardware Monitor",    he: "ניטור חומרה" },
  nav_desc_projects:   { en: "Desktop Catalog",     he: "קטלוג פרויקטים" },
  nav_desc_treasury:   { en: "Revenue & ROI",       he: "הכנסות ורווח" },
  nav_desc_automation: { en: "Rules & Tasks",       he: "חוקים ומשימות" },
  nav_desc_incubator:  { en: "Birth Protocol",      he: "פרוטוקול לידה" },
  nav_desc_settings:   { en: "Configuration",       he: "תצורה" },
  nav_desc_about:      { en: "v2.0-Alpha",          he: "v2.0-Alpha" },

  // ── System status ──────────────────────────────────────────────────────────
  online:      { en: "Online",    he: "מחובר" },
  offline:     { en: "Offline",   he: "מנותק" },
  running:     { en: "Running",   he: "פעיל" },
  stopped:     { en: "Stopped",   he: "עצור" },
  active:      { en: "Active",    he: "פעיל" },
  idle:        { en: "Idle",      he: "רגיעה" },
  loading:     { en: "Loading…",  he: "טוען…" },
  error:       { en: "Error",     he: "שגיאה" },
  ready:       { en: "Ready",     he: "מוכן" },

  // ── Technical terms ────────────────────────────────────────────────────────
  cpu_load:        { en: "CPU Load",   he: "עומס מעבד" },
  ram_usage:       { en: "RAM Usage",  he: "שימוש זיכרון" },
  network:         { en: "Network",    he: "רשת" },
  cluster:         { en: "Cluster",    he: "קלאסטר" },
  deployment:      { en: "Deployment", he: "פריסה" },
  synchronization: { en: "Sync",       he: "סנכרון" },
  monitoring:      { en: "Monitoring", he: "ניטור" },

  // ── Professional metric terms (updated from raw transliterations) ──────────
  scout:   { en: "Scout",   he: "סייר נתונים" },
  "yield": { en: "Yield",   he: "תפוקה" },
  latency: { en: "Latency", he: "זמן תגובה" },

  // ── Actions ────────────────────────────────────────────────────────────────
  start:    { en: "Start",    he: "התחל" },
  stop:     { en: "Stop",     he: "עצור" },
  restart:  { en: "Restart",  he: "אתחל" },
  sync:     { en: "Sync",     he: "סנכרן" },
  deploy:   { en: "Deploy",   he: "פרוס" },
  approve:  { en: "Approve",  he: "אשר" },
  reject:   { en: "Reject",   he: "דחה" },
  cancel:   { en: "Cancel",   he: "בטל" },
  details:  { en: "Details",  he: "פרטים" },
  refresh:  { en: "Refresh",  he: "רענן" },
  close:    { en: "Close",    he: "סגור" },
  re_sync:  { en: "Re-sync",  he: "סנכרן מחדש" },

  // ── Header buttons & indicators (dot-notation) ─────────────────────────────
  "header.live":        { en: "LIVE",                   he: "חי" },
  "header.stealth":     { en: "STEALTH",                he: "סמוי" },
  "header.tactical":    { en: "TACTICAL",               he: "טקטי" },
  "header.override":    { en: "OVERRIDE",               he: "עקיפה" },
  "header.override_on": { en: "OVERRIDE ON",            he: "עקיפה פעילה" },
  "header.chatops":     { en: "CHATOPS",                he: "ממשק פקודות (ChatOps)" },

  // ── Header buttons & indicators (flat keys for backward compat) ─────────────
  live:           { en: "LIVE",                   he: "חי" },
  stealth_mode:   { en: "STEALTH",                he: "סמוי" },
  tactical_mode:  { en: "TACTICAL",               he: "טקטי" },
  override:       { en: "OVERRIDE",               he: "עקיפה" },
  override_on:    { en: "OVERRIDE ON",            he: "עקיפה פעילה" },
  chatops:        { en: "CHATOPS",                he: "ממשק פקודות (ChatOps)" },
  sync_cluster:   { en: "SYNC & RESTART CLUSTER", he: "סנכרן ואתחל אשכול" },
  syncing_label:  { en: "SYNCING…",               he: "מסנכרן…" },
  worker_live:    { en: "WORKER LIVE",            he: "מעבד פעיל" },
  sync_failed:    { en: "SYNC FAILED",            he: "סנכרון נכשל" },

  // ── Color / Theme mode ─────────────────────────────────────────────────────
  color_mode:           { en: "Color Mode",      he: "מצב צבע" },
  theme_standard:       { en: "Standard",        he: "רגיל" },
  theme_high_contrast:  { en: "High Contrast",   he: "ניגודיות גבוהה" },

  // ── Text scale ─────────────────────────────────────────────────────────────
  text_size:        { en: "Text Size / גודל טקסט", he: "גודל טקסט" },
  text_size_small:  { en: "Small (Standard)",        he: "קטן (רגיל)" },
  text_size_medium: { en: "Medium",                  he: "בינוני" },
  text_size_large:  { en: "Large",                   he: "גדול" },

  // ── Paper trading badge ────────────────────────────────────────────────────
  "paper_trading.badge":             { en: "SIM",             he: "סימול" },
  "paper_trading.sim_mode":          { en: "Simulation Mode", he: "מצב סימולציה" },
  "paper_trading.virtual_trade":     { en: "Virtual Trade",   he: "עסקה וירטואלית" },
  "paper_trading.paper_mode_label":  { en: "PAPER MODE",      he: "סימולציה (Sandbox)" },

  // ── Sidebar footer ─────────────────────────────────────────────────────────
  master_online:   { en: "Master Online",   he: "מאסטר מחובר" },
  master_offline:  { en: "Master Offline",  he: "מאסטר מנותק" },
  workers_active:  { en: "workers active",  he: "מעבדים פעילים" },

  // ── HITL Manager ───────────────────────────────────────────────────────────
  action_required:       { en: "Action Required",                         he: "נדרשת פעולה" },
  action_required_sub:   { en: "Tasks paused — click APPROVE to unblock", he: "משימות עצורות — לחץ אשר להמשך" },
  approve_action:        { en: "APPROVE ACTION",                          he: "אשר פעולה" },
  reject_action:         { en: "REJECT",                                  he: "דחה" },
  approving:             { en: "Approving…",                              he: "מאשר…" },
  rejecting:             { en: "Rejecting…",                              he: "דוחה…" },
  force_run_label:       { en: "FORCE RUN (bypass confidence check)",     he: "הפעל בכפייה (דלג על ביטחון)" },
  dispatching:           { en: "Dispatching…",                            he: "שולח…" },
  force_run_done:        { en: "Force-run enqueued — task dispatched!",   he: "משימה נשלחה בכפייה!" },
  stuck_loop:            { en: "STUCK LOOP DETECTED",                     he: "לולאה תקועה זוהתה" },
  note_for_audit:        { en: "Note for audit log (optional)",           he: "הערה ליומן ביקורת (רשות)" },
  add_context:           { en: "Add context…",                            he: "הוסף הקשר…" },
  no_tasks_pending:      { en: "No tasks awaiting approval — system running autonomously", he: "אין משימות הממתינות לאישור — המערכת פועלת אוטונומית" },
  checking_approvals:    { en: "Checking for pending approvals…",         he: "בודק אישורים ממתינים…" },
  pending:               { en: "PENDING",                                 he: "ממתין" },
  threshold_label:       { en: "Threshold",                               he: "סף" },
  approval_streak_label: { en: "Approval streak",                         he: "רצף אישורים" },

  // ── Financial ──────────────────────────────────────────────────────────────
  profit:              { en: "Profit",          he: "רווח" },
  loss:                { en: "Loss",            he: "הפסד" },
  revenue:             { en: "Revenue",         he: "הכנסה" },
  roi:                 { en: "ROI",             he: "החזר השקעה" },
  daily_pnl:           { en: "Daily P&L",       he: "רווח/הפסד יומי" },
  budget:              { en: "Budget",          he: "תקציב" },

  // ── System messages ────────────────────────────────────────────────────────
  deployment_complete: { en: "Deployment Complete", he: "פריסה הושלמה" },
  system_optimal:      { en: "System Optimal",      he: "מערכת אופטימלית" },
  high_alert:          { en: "High Alert",           he: "כוננות גבוהה" },

  // ── Telegram / ChatOps ─────────────────────────────────────────────────────
  stats:        { en: "Statistics",         he: "סטטיסטיקות" },
  cluster_mgmt: { en: "Cluster Management", he: "ניהול קלאסטר" },
  wallet:       { en: "Wallet",             he: "ארנק" },
  confirmed:    { en: "Confirmed",          he: "אושר" },
  rejected:     { en: "Rejected",           he: "נדחה" },
  processing:   { en: "Processing",         he: "מעבד" },

  // ── Topology / Cluster HUD ─────────────────────────────────────────────────
  topology_title:            { en: "Cluster Topology",       he: "מפת טופולוגיה" },
  topology_subtitle:         { en: "Physical network map",   he: "מיפוי פיזי של הרשת" },
  "widgets.cluster_hud":     { en: "Cluster HUD",            he: "מצב אשכול" },
  "widgets.master_node":     { en: "Master Node",            he: "צומת מאסטר" },
  "widgets.worker_node":     { en: "Worker Node",            he: "צומת מעבד (Worker)" },
  "widgets.master_log":      { en: "Master Log",             he: "יומן אירועים — מאסטר" },
  "widgets.worker_log":      { en: "Worker Log",             he: "יומן אירועים — מעבד" },
  "widgets.master_status":   { en: "Master Status",          he: "סטטוס מאסטר" },
  "widgets.active_workers":  { en: "Active Workers",         he: "מעבדים פעילים" },
  "widgets.data_stream":     { en: "Data Stream",            he: "זרם נתונים" },
  "widgets.cpu_load":        { en: "CPU Load",               he: "עומס מעבד" },
  "widgets.network_active":  { en: "Network Active",         he: "רשת פעילה" },
  "status.online":           { en: "Online",                 he: "מחובר" },
  "status.offline":          { en: "Offline",                he: "מנותק" },

  // ── Widget section titles ──────────────────────────────────────────────────
  financial_pulse:      { en: "Financial Pulse",            he: "דופק פיננסי" },
  predictor_title:      { en: "Cross-Exchange Predictor",   he: "מנבא ארביטראז'" },
  cluster_topology:     { en: "Cluster Topology",           he: "מפת טופולוגיה" },
  human_in_the_loop:    { en: "Human-in-the-Loop",          he: "בקרה אנושית" },
  financial_engine:     { en: "Financial Engine",           he: "מנוע פיננסי" },
  arbitrage_visualizer: { en: "Arbitrage Visualizer",       he: "ניטור ארביטראז' חי" },
  simulation_mode:      { en: "Simulation Mode",            he: "מצב סימולציה" },
  control_panel:        { en: "Control Panel",              he: "לוח שליטה" },
  intelligence:         { en: "Intelligence",               he: "מודיעין" },
  analytics:            { en: "Analytics",                  he: "אנליטיקה" },
  digital_twin:         { en: "Digital Twin",               he: "תאום דיגיטלי" },

  // ── UI labels (localized professional terms) ──────────────────────────────
  stability_score:    { en: "Stability Score",    he: "מדד חוסן מערכת" },
  real_time_logs:     { en: "Real-time Logs",     he: "יומן אירועים" },
  worker_status:      { en: "Worker Status",      he: "סטטוס מעבדי משימות" },
  panic_button:       { en: "Panic Button",       he: "עצירת חירום (PANIC)" },
  arbitrage_sentinel: { en: "Arbitrage Sentinel", he: "סורק ארביטראז'" },

  // ── System error / self-repair protocol ───────────────────────────────────
  system_error_banner:   { en: "SYSTEM ERROR — Self-Repair Protocol Activated", he: "שגיאת מערכת — פרוטוקול תיקון עצמי הופעל" },
  repair_state_error:    { en: "ERROR",      he: "שגיאה" },
  repair_state_repairing:{ en: "REPAIRING",  he: "מתקן" },
  repair_state_resolved: { en: "RESOLVED",   he: "תוקן" },
} as const;

export type TranslationKey = keyof typeof translations;

// ── i18n Context ──────────────────────────────────────────────────────────────

interface I18nContextValue {
  language: Language;
  setLanguage: (lang: Language) => void;
  t: (key: TranslationKey) => string;
  isRTL: boolean;
}

const I18nContext = createContext<I18nContextValue>({
  language: "en",
  setLanguage: () => {},
  t: (key) => key,
  isRTL: false,
});

export function I18nProvider({ children }: { children: ReactNode }) {
  const [language, setLanguageState] = useState<Language>("en");

  // Restore persisted language preference
  useEffect(() => {
    try {
      const stored = localStorage.getItem("nexus-lang") as Language | null;
      if (stored === "en" || stored === "he") setLanguageState(stored);
    } catch {}
  }, []);

  const setLanguage = (lang: Language) => {
    setLanguageState(lang);
    try { localStorage.setItem("nexus-lang", lang); } catch {}
  };

  const t = (key: TranslationKey): string =>
    translations[key]?.[language] ?? key;

  const isRTL = language === "he";

  // Apply dir and lang globally on the document root for proper RTL layout
  useEffect(() => {
    try {
      document.documentElement.setAttribute("dir", isRTL ? "rtl" : "ltr");
      document.documentElement.setAttribute("lang", isRTL ? "he" : "en");
    } catch {}
  }, [isRTL]);

  return (
    <I18nContext.Provider value={{ language, setLanguage, t, isRTL }}>
      <div dir={isRTL ? "rtl" : "ltr"} style={{ minHeight: "inherit" }}>
        {children}
      </div>
    </I18nContext.Provider>
  );
}

export function useI18n() {
  return useContext(I18nContext);
}

// ── Language selector — glassmorphism segmented control ───────────────────────

export function LanguageToggle({
  stealth,
  isHighContrast = false,
}: {
  stealth: boolean;
  isHighContrast?: boolean;
}) {
  const { language, setLanguage } = useI18n();

  const borderColor = isHighContrast
    ? "#9CA3AF"
    : stealth
    ? "#1e293b"
    : "rgba(14,165,233,0.18)";

  const wrapBg = isHighContrast
    ? "rgba(248,249,250,0.9)"
    : stealth
    ? "transparent"
    : "rgba(14,165,233,0.04)";

  return (
    <div
      title="Toggle Language / החלף שפה"
      style={{
        display: "flex",
        alignItems: "center",
        background: wrapBg,
        backdropFilter: isHighContrast || stealth ? "none" : "blur(8px)",
        border: `1px solid ${borderColor}`,
        borderRadius: "8px",
        padding: "2px",
        gap: "1px",
        flexShrink: 0,
      }}
    >
      {(["en", "he"] as const).map((lang) => {
        const isActive = language === lang;

        const activeBg = isHighContrast
          ? "#E8F0FE"
          : stealth
          ? "#1e293b"
          : "rgba(14,165,233,0.18)";

        const activeColor = isHighContrast
          ? "#0055CC"
          : stealth
          ? "#94a3b8"
          : "#38bdf8";

        const inactiveColor = isHighContrast
          ? "#374151"
          : stealth
          ? "#334155"
          : "#6b8fab";

        return (
          <button
            key={lang}
            onClick={() => setLanguage(lang)}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              padding: "3px 9px",
              borderRadius: "6px",
              border: "none",
              cursor: "pointer",
              background: isActive ? activeBg : "transparent",
              boxShadow:
                isActive && !stealth && !isHighContrast
                  ? "0 0 10px rgba(14,165,233,0.2), inset 0 1px 0 rgba(255,255,255,0.06)"
                  : "none",
              transition: "all 0.18s ease",
            }}
          >
            <span style={{ fontSize: "0.7rem", lineHeight: 1 }}>
              {lang === "en" ? "🇺🇸" : "🇮🇱"}
            </span>
            <span
              style={{
                fontFamily: "var(--font-mono)",
                fontSize: "0.62rem",
                fontWeight: isActive ? 700 : 400,
                color: isActive ? activeColor : inactiveColor,
                letterSpacing: "0.06em",
                transition: "color 0.18s ease",
                userSelect: "none",
              }}
            >
              {lang === "en" ? "EN" : "עב"}
            </span>
          </button>
        );
      })}
    </div>
  );
}
