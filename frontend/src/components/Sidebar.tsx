"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { motion, AnimatePresence } from "framer-motion";
import { useNexus } from "@/lib/nexus-context";
import { useStealth } from "@/lib/stealth";
import { useI18n, type TranslationKey } from "@/lib/i18n";
import { useTheme } from "@/lib/theme";

// ── Nav item definitions ───────────────────────────────────────────────────────

interface NavItem {
  href:     string;
  icon:     string;
  labelKey: TranslationKey;
  descKey:  TranslationKey;
}

interface NavGroup {
  groupLabel: string;
  items: NavItem[];
}

const NAV_GROUPS: NavGroup[] = [
  {
    groupLabel: "CORE",
    items: [
      { href: "/dashboard",  icon: "⚡", labelKey: "dashboard",  descKey: "nav_desc_dashboard" },
      { href: "/operations", icon: "🎯", labelKey: "operations", descKey: "nav_desc_operations" },
      { href: "/fleet",      icon: "🖥️", labelKey: "fleet",      descKey: "nav_desc_fleet" },
      { href: "/treasury",   icon: "💰", labelKey: "treasury",   descKey: "nav_desc_treasury" },
    ],
  },
  {
    groupLabel: "TRADING",
    items: [
      { href: "/wallet-ops",      icon: "📈", labelKey: "wallet_ops",      descKey: "nav_desc_wallet_ops" },
      { href: "/nexus-os?tab=poly-trading", icon: "🎲", labelKey: "polymarket_deck", descKey: "nav_desc_polymarket_deck" },
      { href: "/market-intel",    icon: "🔭", labelKey: "market_intel",    descKey: "nav_desc_market_intel" },
    ],
  },
  {
    groupLabel: "AUTOMATION",
    items: [
      { href: "/incubator",         icon: "🧬", labelKey: "incubator",         descKey: "nav_desc_incubator" },
      { href: "/automation",        icon: "🤖", labelKey: "automation",        descKey: "nav_desc_automation" },
      { href: "/ai-evolution",      icon: "🧠", labelKey: "ai_evolution",      descKey: "nav_desc_ai_evolution" },
      { href: "/swarm-control",     icon: "🐝", labelKey: "swarm_control",     descKey: "nav_desc_swarm_control" },
      { href: "/bot-farm",          icon: "🤖", labelKey: "bot_farm",          descKey: "nav_desc_bot_farm" },
      { href: "/bot-factory",       icon: "🏭", labelKey: "bot_factory",       descKey: "nav_desc_bot_factory" },
      { href: "/evolution",         icon: "🧬", labelKey: "evolution",         descKey: "nav_desc_evolution" },
      { href: "/strategy-lab",      icon: "🔬", labelKey: "strategy_lab",      descKey: "nav_desc_strategy_lab" },
    ],
  },
  {
    groupLabel: "DATA",
    items: [
      { href: "/sessions",          icon: "📱", labelKey: "sessions",          descKey: "nav_desc_sessions" },
      { href: "/vault",             icon: "🔐", labelKey: "vault",             descKey: "nav_desc_vault" },
      { href: "/modules",           icon: "🔧", labelKey: "modules",           descKey: "nav_desc_modules" },
      { href: "/logs-raw",          icon: "📋", labelKey: "logs_raw",          descKey: "nav_desc_logs_raw" },
      { href: "/scrape-browser",    icon: "🕷️", labelKey: "scrape_browser",    descKey: "nav_desc_scrape_browser" },
      { href: "/group-infiltration",icon: "🎯", labelKey: "group_infiltration",descKey: "nav_desc_group_infiltration" },
    ],
  },
  {
    groupLabel: "SYSTEM",
    items: [
      { href: "/projects",  icon: "🏗️", labelKey: "projects",  descKey: "nav_desc_projects" },
      { href: "/nexus-os",  icon: "🌐", labelKey: "nexus_os",  descKey: "nav_desc_nexus_os" },
      { href: "/settings",  icon: "⚙️", labelKey: "settings",  descKey: "nav_desc_settings" },
      { href: "/about",     icon: "ℹ️", labelKey: "about",     descKey: "nav_desc_about" },
    ],
  },
];

// Flat list for backward-compatible active-check logic
const NAV_ITEMS: NavItem[] = NAV_GROUPS.flatMap(g => g.items);

// ── Sidebar ───────────────────────────────────────────────────────────────────

export default function Sidebar() {
  const [expanded, setExpanded] = useState(false);
  const pathname  = usePathname();
  const { cluster, hitl } = useNexus();
  const { stealth } = useStealth();
  const { t, isRTL } = useI18n();
  const { isHighContrast, tokens } = useTheme();

  const masterOnline = cluster?.nodes.some(n => n.role === "master" && n.online) ?? false;
  const workerCount  = cluster?.nodes.filter(n => n.role === "worker" && n.online).length ?? 0;
  const hitlCount    = hitl?.total ?? 0;

  const W_COLLAPSED = 56;
  const W_EXPANDED  = 220;

  // Color choices
  const accentC    = stealth ? "#334155" : tokens.accent;
  const statusDotC = masterOnline ? tokens.success : tokens.danger;
  const sidebarBg  = isHighContrast
    ? "#F8F9FA"
    : "linear-gradient(180deg, #0a0e1a 0%, #080d18 100%)";
  const borderSide = isHighContrast
    ? `1px solid ${tokens.borderDefault}`
    : `1px solid ${stealth ? "#0f172a" : "#1e293b"}`;

  return (
    <motion.aside
      animate={{ width: expanded ? W_EXPANDED : W_COLLAPSED }}
      transition={{ type: "spring", stiffness: 300, damping: 30 }}
      onMouseEnter={() => setExpanded(true)}
      onMouseLeave={() => setExpanded(false)}
      style={{
        position: "fixed",
        top: 56,
        [isRTL ? "right" : "left"]: 0,
        bottom: 0,
        zIndex: 40,
        display: "flex",
        flexDirection: "column",
        background: sidebarBg,
        borderRight: isRTL ? "none" : borderSide,
        borderLeft:  isRTL ? borderSide : "none",
        overflow: "hidden",
        boxShadow: expanded
          ? isRTL
            ? `-4px 0 24px ${isHighContrast ? "rgba(0,0,0,0.1)" : "#00000066"}`
            : `4px 0 24px ${isHighContrast ? "rgba(0,0,0,0.1)" : "#00000066"}`
          : "none",
        transition: "background 0.25s",
      }}
    >
      {/* ── Nav items (grouped) ── */}
      <nav style={{ flex: 1, padding: "0.5rem 0", overflowY: "auto", overflowX: "hidden" }}>
        {NAV_GROUPS.map(({ groupLabel, items }) => (
          <div key={groupLabel}>
            {/* Group label — only visible when expanded */}
            <AnimatePresence>
              {expanded && (
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.12 }}
                  style={{
                    padding: "0.55rem 14px 0.2rem",
                    fontSize: "0.6rem",
                    fontWeight: 700,
                    letterSpacing: "0.1em",
                    color: isHighContrast ? tokens.textMuted : stealth ? "#1e293b" : "#2d4a65",
                    fontFamily: "var(--font-mono)",
                    whiteSpace: "nowrap",
                    textAlign: isRTL ? "right" : "left",
                  }}
                >
                  {groupLabel}
                </motion.div>
              )}
            </AnimatePresence>

            {items.map(({ href, icon, labelKey, descKey }) => {
              const active = pathname === href || (pathname?.startsWith(href + "/") ?? false);
              const activeBg = isHighContrast
                ? tokens.accentSubtle
                : stealth ? "#0f172a" : `${accentC}18`;
              const activeBorder = `2px solid ${stealth ? "#334155" : accentC}`;

              return (
                <Link key={href} href={href} style={{ textDecoration: "none" }}>
                  <div
                    title={expanded ? undefined : t(labelKey)}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      flexDirection: isRTL ? "row-reverse" : "row",
                      gap: "0.75rem",
                      padding: "0.6rem 0",
                      paddingLeft:  isRTL ? 0      : "14px",
                      paddingRight: isRTL ? "14px" : 0,
                      margin: "2px 6px",
                      borderRadius: "8px",
                      background: active ? activeBg : "transparent",
                      borderLeft:  !isRTL && active ? activeBorder : "2px solid transparent",
                      borderRight: isRTL  && active ? activeBorder : isRTL ? "2px solid transparent" : "none",
                      cursor: "pointer",
                      transition: "background 0.15s",
                      position: "relative",
                      overflow: "hidden",
                      minWidth: 0,
                    }}
                  >
                    {/* Icon */}
                    <span style={{ fontSize: "1.1rem", flexShrink: 0, lineHeight: 1 }}>
                      {icon}
                    </span>

                    {/* Label + description */}
                    <AnimatePresence>
                      {expanded && (
                        <motion.div
                          initial={{ opacity: 0, x: isRTL ? 8 : -8 }}
                          animate={{ opacity: 1, x: 0 }}
                          exit={{ opacity: 0, x: isRTL ? 8 : -8 }}
                          transition={{ duration: 0.15 }}
                          style={{
                            display: "flex",
                            flexDirection: "column",
                            minWidth: 0,
                            textAlign: isRTL ? "right" : "left",
                          }}
                        >
                          <span
                            style={{
                              fontFamily: "var(--font-sans)",
                              fontSize: "0.95rem",
                              fontWeight: active ? 700 : 500,
                              color: active
                                ? isHighContrast
                                  ? tokens.textPrimary
                                  : stealth ? "#94a3b8" : "#e8f2ff"
                                : isHighContrast
                                  ? tokens.textSecondary
                                  : stealth ? "#334155" : "#7da8cc",
                              letterSpacing: "0.02em",
                              whiteSpace: "nowrap",
                            }}
                          >
                            {t(labelKey)}
                          </span>
                          <span
                            style={{
                              fontFamily: "var(--font-sans)",
                              fontSize: "0.75rem",
                              fontWeight: 400,
                              color: isHighContrast
                                ? tokens.textMuted
                                : stealth ? "#1e293b" : "#2d4a65",
                              whiteSpace: "nowrap",
                            }}
                          >
                            {t(descKey)}
                          </span>
                        </motion.div>
                      )}
                    </AnimatePresence>

                    {/* HITL badge on Dashboard */}
                    {href === "/dashboard" && hitlCount > 0 && (
                      <motion.span
                        animate={{ scale: [1, 1.15, 1] }}
                        transition={{ repeat: Infinity, duration: 1.5 }}
                        style={{
                          position: expanded ? "static" : "absolute",
                          top: expanded ? undefined : 4,
                          right: expanded ? undefined : 4,
                          marginLeft: expanded && !isRTL ? "auto" : undefined,
                          marginRight: expanded && isRTL ? "auto" : undefined,
                          fontSize: "0.6rem",
                          fontWeight: 700,
                          fontFamily: "var(--font-mono)",
                          background: isHighContrast ? tokens.warning : "#f59e0b",
                          color: "#000",
                          borderRadius: "999px",
                          padding: "1px 5px",
                          flexShrink: 0,
                        }}
                      >
                        {hitlCount}
                      </motion.span>
                    )}
                  </div>
                </Link>
              );
            })}
          </div>
        ))}
      </nav>

      {/* ── System Health footer ── */}
      <div
        style={{
          padding: "0.75rem",
          borderTop: isHighContrast
            ? `1px solid ${tokens.borderFaint}`
            : "1px solid #0f172a",
          display: "flex",
          alignItems: "center",
          flexDirection: isRTL ? "row-reverse" : "row",
          gap: "0.5rem",
          overflow: "hidden",
        }}
      >
        {/* Status dot */}
        <span
          style={{
            width: 7,
            height: 7,
            borderRadius: "50%",
            background: stealth ? "#334155" : statusDotC,
            display: "inline-block",
            boxShadow: (!stealth && !isHighContrast) ? `0 0 6px ${statusDotC}` : "none",
            animation: (!stealth && !isHighContrast && masterOnline) ? "rgb-pulse 2s infinite" : "none",
            flexShrink: 0,
          }}
        />
        <AnimatePresence>
          {expanded && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              style={{
                display: "flex",
                flexDirection: "column",
                minWidth: 0,
                textAlign: isRTL ? "right" : "left",
              }}
            >
              <span
                style={{
                  fontFamily: "var(--font-sans)",
                  fontSize: "0.8rem",
                  fontWeight: 600,
                  color: isHighContrast
                    ? tokens.textSecondary
                    : stealth ? "#334155" : "#7da8cc",
                  whiteSpace: "nowrap",
                }}
              >
                {masterOnline ? t("master_online") : t("master_offline")}
              </span>
              <span
                style={{
                  fontFamily: "var(--font-sans)",
                  fontSize: "0.72rem",
                  color: isHighContrast
                    ? tokens.textMuted
                    : stealth ? "#1e293b" : "#2d4a65",
                  whiteSpace: "nowrap",
                }}
              >
                {workerCount} {t("workers_active")}
              </span>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </motion.aside>
  );
}
