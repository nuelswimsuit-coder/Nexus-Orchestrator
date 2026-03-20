"use client";

/**
 * theme — High Contrast / Standard Color Mode for TeleFix OS
 *
 * Provides a global color mode toggle persisted in localStorage.
 * Applies `data-theme` attribute to <html> so CSS overrides activate,
 * and exposes `useTheme()` + `getThemeTokens()` for inline-styled components.
 */

import {
  createContext,
  useContext,
  useState,
  useEffect,
  type ReactNode,
} from "react";

export type ColorMode  = "standard" | "high-contrast" | "panic";
export type TextScale  = "small" | "medium" | "large";

// ── Theme token map ───────────────────────────────────────────────────────────

export interface ThemeTokens {
  // Surface layers
  surface0: string;
  surface1: string;
  surface2: string;
  surface3: string;
  surface4: string;
  // Typography
  textPrimary: string;
  textSecondary: string;
  textMuted: string;
  // Accent (electric blue → accessible navy-blue in HC)
  accent: string;
  accentBright: string;
  accentHover: string;
  accentSubtle: string;
  accentDim: string;
  // Semantic colors
  success: string;
  successSubtle: string;
  warning: string;
  warningSubtle: string;
  danger: string;
  dangerSubtle: string;
  // Borders
  borderStrong: string;
  borderDefault: string;
  borderSubtle: string;
  borderFaint: string;
  // Glass / card backgrounds
  glassBg: string;
  glassBgSubtle: string;
  glassBorder: string;
  cardBg: string;
  // Misc
  terminalBg: string;
  dotGridColor: string;
}

export function getPanicTokens(): ThemeTokens {
  return {
    // Restricted Mode — desaturated gray/red palette, activated by PANIC kill-switch
    surface0: "#080305",
    surface1: "#100508",
    surface2: "#18070a",
    surface3: "#20090c",
    surface4: "#2a0c10",
    textPrimary:   "#cc8899",
    textSecondary: "#884455",
    textMuted:     "#3d1a22",
    accent:        "#cc2233",
    accentBright:  "#ff3344",
    accentHover:   "#aa1122",
    accentSubtle:  "#1a0508",
    accentDim:     "rgba(204,34,51,0.10)",
    success:       "#665544",
    successSubtle: "#1a0f0a",
    warning:       "#cc7700",
    warningSubtle: "#1a1000",
    danger:        "#ff3355",
    dangerSubtle:  "#1c0005",
    borderStrong:  "rgba(204,34,51,0.65)",
    borderDefault: "rgba(204,34,51,0.30)",
    borderSubtle:  "#20090c",
    borderFaint:   "#100508",
    glassBg:       "rgba(12,3,5,0.88)",
    glassBgSubtle: "rgba(12,3,5,0.60)",
    glassBorder:   "2px solid rgba(204,34,51,0.30)",
    cardBg:        "rgba(20,5,8,0.82)",
    terminalBg:    "#040102",
    dotGridColor:  "rgba(204,34,51,0.08)",
  };
}

export function getThemeTokens(isHC: boolean): ThemeTokens {
  if (isHC) {
    return {
      // High Contrast Light Mode — pure white, thick black borders
      surface0: "#FFFFFF",
      surface1: "#F7F8FA",
      surface2: "#EEF0F4",
      surface3: "#E0E4EB",
      surface4: "#CDD3DC",
      textPrimary:   "#000000",
      textSecondary: "#0a0a0a",
      textMuted:     "#2d3748",
      accent:        "#0044BB",
      accentBright:  "#0033AA",
      accentHover:   "#002288",
      accentSubtle:  "#EBF2FF",
      accentDim:     "rgba(0,68,187,0.10)",
      success:       "#0a6640",
      successSubtle: "#DCFCE7",
      warning:       "#7c4e00",
      warningSubtle: "#FFF8DC",
      danger:        "#AA0000",
      dangerSubtle:  "#FFE4E4",
      borderStrong:  "#000000",
      borderDefault: "#000000",
      borderSubtle:  "#374151",
      borderFaint:   "#CDD3DC",
      glassBg:       "#FFFFFF",
      glassBgSubtle: "#F7F8FA",
      glassBorder:   "2px solid #000000",
      cardBg:        "#FFFFFF",
      terminalBg:    "#EEF0F4",
      dotGridColor:  "rgba(0,68,187,0.18)",
    };
  }
  return {
    // Dark Luxury Mode — deep navy/black with neon blue accents
    surface0: "#080b14",
    surface1: "#0d1120",
    surface2: "#131928",
    surface3: "#1a2235",
    surface4: "#222d44",
    textPrimary:   "#e8f2ff",
    textSecondary: "#7da8cc",
    textMuted:     "#2d4a65",
    accent:        "#00b4ff",
    accentBright:  "#38d4ff",
    accentHover:   "#0090d4",
    accentSubtle:  "#061a2e",
    accentDim:     "rgba(0,180,255,0.10)",
    success:       "#00e096",
    successSubtle: "#021f12",
    warning:       "#ffb800",
    warningSubtle: "#1c1200",
    danger:        "#ff3355",
    dangerSubtle:  "#1c0005",
    borderStrong:  "rgba(0,180,255,0.45)",
    borderDefault: "rgba(0,180,255,0.22)",
    borderSubtle:  "#1a2235",
    borderFaint:   "#0d1120",
    glassBg:       "rgba(5,10,22,0.80)",
    glassBgSubtle: "rgba(5,10,22,0.55)",
    glassBorder:   "2px solid rgba(0,180,255,0.22)",
    cardBg:        "rgba(13,17,32,0.70)",
    terminalBg:    "#030610",
    dotGridColor:  "rgba(0,180,255,0.10)",
  };
}

// ── Context ───────────────────────────────────────────────────────────────────

interface ThemeContextValue {
  colorMode: ColorMode;
  setColorMode: (mode: ColorMode) => void;
  isHighContrast: boolean;
  tokens: ThemeTokens;
  textScale: TextScale;
  setTextScale: (scale: TextScale) => void;
}

const ThemeContext = createContext<ThemeContextValue>({
  colorMode: "standard",
  setColorMode: () => {},
  isHighContrast: false,
  tokens: getThemeTokens(false),
  textScale: "medium",
  setTextScale: () => {},
});

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [colorMode, setColorModeState] = useState<ColorMode>("standard");
  const [textScale, setTextScaleState] = useState<TextScale>("medium");

  // Restore persisted preferences
  useEffect(() => {
    try {
      const storedColor = localStorage.getItem("nexus-color-mode") as ColorMode | null;
      if (storedColor === "standard" || storedColor === "high-contrast") {
        setColorModeState(storedColor);
      }
      const storedScale = localStorage.getItem("nexus-text-scale") as TextScale | null;
      if (storedScale === "small" || storedScale === "medium" || storedScale === "large") {
        setTextScaleState(storedScale);
      }
    } catch {}
  }, []);

  // Apply data-theme to <html> and adjust color-scheme for browser chrome
  useEffect(() => {
    const html = document.documentElement;
    html.setAttribute("data-theme", colorMode);
    html.style.colorScheme = colorMode === "high-contrast" ? "light" : "dark";
  }, [colorMode]);

  // Apply data-text-scale to <html> so CSS variable --text-scale-factor updates
  useEffect(() => {
    try {
      document.documentElement.setAttribute("data-text-scale", textScale);
    } catch {}
  }, [textScale]);

  const setColorMode = (mode: ColorMode) => {
    setColorModeState(mode);
    // Never persist panic mode — it is driven by system state, not user preference.
    if (mode !== "panic") {
      try { localStorage.setItem("nexus-color-mode", mode); } catch {}
    }
  };

  const setTextScale = (scale: TextScale) => {
    setTextScaleState(scale);
    try { localStorage.setItem("nexus-text-scale", scale); } catch {}
  };

  const isHighContrast = colorMode === "high-contrast";
  const isPanicMode    = colorMode === "panic";

  const resolvedTokens = isPanicMode
    ? getPanicTokens()
    : getThemeTokens(isHighContrast);

  return (
    <ThemeContext.Provider
      value={{
        colorMode,
        setColorMode,
        isHighContrast,
        tokens: resolvedTokens,
        textScale,
        setTextScale,
      }}
    >
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  return useContext(ThemeContext);
}
