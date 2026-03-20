"use client";

/**
 * Stealth Mode context.
 *
 * Provides:
 *   stealth          — bool: all RGB glows suppressed
 *   toggleStealth    — fn: toggle stealth on/off
 *   stealthOverride  — bool: stealth is ON but the Super-Scraper is allowed
 *                      to run at maximum priority (silent RGB, full CPU)
 *   toggleOverride   — fn: toggle the stealth override
 *
 * When stealthOverride is active:
 *   - Stealth mode remains visually active (no RGB)
 *   - The Super-Scraper task receives stealth_override=true in its parameters
 *   - The header shows a small "OVERRIDE" badge next to the stealth toggle
 *
 * Usage:
 *   const { stealth, toggleStealth, stealthOverride, toggleOverride } = useStealth();
 */

import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";

interface StealthContextValue {
  stealth: boolean;
  toggleStealth: () => void;
  stealthOverride: boolean;
  toggleOverride: () => void;
}

const StealthContext = createContext<StealthContextValue>({
  stealth: false,
  toggleStealth: () => {},
  stealthOverride: false,
  toggleOverride: () => {},
});

export function StealthProvider({ children }: { children: ReactNode }) {
  const [stealth, setStealth] = useState(false);
  const [stealthOverride, setStealthOverride] = useState(false);

  useEffect(() => {
    if (stealth) {
      document.body.classList.add("stealth-mode");
    } else {
      document.body.classList.remove("stealth-mode");
      // Clear override when stealth is disabled
      setStealthOverride(false);
    }
  }, [stealth]);

  function toggleStealth() {
    setStealth((s) => !s);
  }

  function toggleOverride() {
    // Override only makes sense when stealth is active
    if (!stealth) return;
    setStealthOverride((v) => !v);
  }

  return (
    <StealthContext.Provider
      value={{ stealth, toggleStealth, stealthOverride, toggleOverride }}
    >
      {children}
    </StealthContext.Provider>
  );
}

export function useStealth() {
  return useContext(StealthContext);
}
