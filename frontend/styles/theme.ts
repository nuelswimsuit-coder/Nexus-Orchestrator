/**
 * Calm Architect — single source of truth for the default (standard) dark theme.
 * High-contrast and panic palettes remain in `src/lib/theme.tsx` / CSS overrides.
 */

export const calmArchitect = {
  /** Primary page / app background */
  background: "#1A202C",
  /** Cards, modules, elevated panels */
  card: "#2D3748",
  /** Body copy */
  text: "#E2E8F0",
  /** Labels, secondary copy (≥4.5:1 on #1A202C) */
  textSecondary: "#A0AEC0",
  /** Tertiary / de-emphasized (≥4.5:1 on #1A202C; use sparingly on #2D3748) */
  textMuted: "#718096",
  accent: "#63B3ED",
  accentBright: "#90CDF4",
  accentHover: "#4299E1",
  success: "#68D391",
  warning: "#F6E05E",
  error: "#FC8181",
} as const;

/** Layered surfaces derived from background + card (depth without noisy neon) */
export const calmSurfaces = {
  surface0: calmArchitect.background,
  surface1: "#1F2733",
  surface2: "#252F3D",
  surface3: calmArchitect.card,
  surface4: "#374151",
} as const;
