"use client";

/**
 * Glass-morphism wizard: phone + country → OTP (5 digits) → success animation.
 * Demo flow: verifies OTP client-side after a short delay for a smooth feel.
 */

import { useCallback, useEffect, useId, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";

export type AddedSessionPayload = {
  sessionName: string;
  phoneDisplay: string;
  dialCode: string;
  nationalNumber: string;
};

const COUNTRIES = [
  { code: "US", dial: "+1", flag: "🇺🇸", name: "United States" },
  { code: "IL", dial: "+972", flag: "🇮🇱", name: "Israel" },
  { code: "GB", dial: "+44", flag: "🇬🇧", name: "United Kingdom" },
  { code: "DE", dial: "+49", flag: "🇩🇪", name: "Germany" },
  { code: "FR", dial: "+33", flag: "🇫🇷", name: "France" },
  { code: "IN", dial: "+91", flag: "🇮🇳", name: "India" },
  { code: "BR", dial: "+55", flag: "🇧🇷", name: "Brazil" },
  { code: "RU", dial: "+7", flag: "🇷🇺", name: "Russia" },
  { code: "AE", dial: "+971", flag: "🇦🇪", name: "UAE" },
  { code: "JP", dial: "+81", flag: "🇯🇵", name: "Japan" },
  { code: "KR", dial: "+82", flag: "🇰🇷", name: "South Korea" },
  { code: "CN", dial: "+86", flag: "🇨🇳", name: "China" },
  { code: "AU", dial: "+61", flag: "🇦🇺", name: "Australia" },
  { code: "CA", dial: "+1", flag: "🇨🇦", name: "Canada" },
  { code: "MX", dial: "+52", flag: "🇲🇽", name: "Mexico" },
  { code: "ES", dial: "+34", flag: "🇪🇸", name: "Spain" },
  { code: "IT", dial: "+39", flag: "🇮🇹", name: "Italy" },
  { code: "NL", dial: "+31", flag: "🇳🇱", name: "Netherlands" },
  { code: "TR", dial: "+90", flag: "🇹🇷", name: "Türkiye" },
  { code: "PL", dial: "+48", flag: "🇵🇱", name: "Poland" },
] as const;

function CountryPicker({
  countries,
  countryIdx,
  onSelect,
  stealth,
  baseId,
}: {
  countries: readonly (typeof COUNTRIES)[number][];
  countryIdx: number;
  onSelect: (idx: number) => void;
  stealth: boolean;
  baseId: string;
}) {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState("");
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  const c = countries[countryIdx];
  const q = filter.trim().toLowerCase();
  const list = q
    ? countries.filter(
        (x) =>
          x.name.toLowerCase().includes(q) ||
          x.dial.includes(q) ||
          x.code.toLowerCase().includes(q),
      )
    : countries;

  return (
    <div ref={wrapRef} style={{ position: "relative" }}>
      <button
        type="button"
        id={`${baseId}-country-trigger`}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        style={{
          width: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          fontFamily: "var(--font-mono)",
          fontSize: "0.8rem",
          padding: "0.65rem 0.85rem",
          borderRadius: 12,
          border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.28)"}`,
          background: "rgba(3,8,16,0.88)",
          color: stealth ? "#94a3b8" : "#e2e8f0",
          cursor: "pointer",
          boxShadow: stealth ? "none" : "inset 0 0 20px rgba(0,180,255,0.04)",
        }}
      >
        <span style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: "1.1rem" }}>{c?.flag}</span>
          <span>
            {c?.dial}{" "}
            <span style={{ color: stealth ? "#64748b" : "#64748b" }}>· {c?.name}</span>
          </span>
        </span>
        <span style={{ color: "#64748b", fontSize: "0.65rem" }}>{open ? "▴" : "▾"}</span>
      </button>
      <AnimatePresence>
        {open && (
          <motion.div
            role="listbox"
            aria-labelledby={`${baseId}-country-trigger`}
            initial={{ opacity: 0, y: -6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -4 }}
            transition={{ duration: 0.18 }}
            style={{
              position: "absolute",
              left: 0,
              right: 0,
              top: "calc(100% + 8px)",
              zIndex: 30,
              borderRadius: 14,
              border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.28)"}`,
              background: stealth ? "rgba(15,23,42,0.97)" : "rgba(4,12,28,0.94)",
              backdropFilter: "blur(20px) saturate(1.4)",
              WebkitBackdropFilter: "blur(20px) saturate(1.4)",
              boxShadow: stealth
                ? "0 16px 40px rgba(0,0,0,0.5)"
                : "0 20px 48px rgba(0,0,0,0.55), 0 0 24px rgba(0,180,255,0.1)",
              overflow: "hidden",
              maxHeight: 240,
              display: "flex",
              flexDirection: "column",
            }}
          >
            <input
              type="search"
              placeholder="Search country or code…"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              onClick={(e) => e.stopPropagation()}
              style={{
                margin: 8,
                fontFamily: "var(--font-mono)",
                fontSize: "0.72rem",
                padding: "0.5rem 0.65rem",
                borderRadius: 10,
                border: `1px solid ${stealth ? "#0f172a" : "rgba(0,180,255,0.2)"}`,
                background: "rgba(2,6,14,0.95)",
                color: "#e2e8f0",
                outline: "none",
              }}
            />
            <div style={{ overflowY: "auto", padding: "0 6px 8px" }}>
              {list.map((item) => {
                const i = countries.findIndex((x) => x.code === item.code);
                return (
                  <button
                    key={item.code}
                    type="button"
                    role="option"
                    aria-selected={i === countryIdx}
                    onClick={() => {
                      onSelect(i);
                      setOpen(false);
                      setFilter("");
                    }}
                    style={{
                      width: "100%",
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                      textAlign: "left",
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.75rem",
                      padding: "0.5rem 0.6rem",
                      border: "none",
                      borderRadius: 8,
                      background: i === countryIdx ? (stealth ? "#1e293b" : "rgba(0,180,255,0.12)") : "transparent",
                      color: stealth ? "#94a3b8" : "#e2e8f0",
                      cursor: "pointer",
                    }}
                  >
                    <span>{item.flag}</span>
                    <span style={{ flex: 1 }}>{item.name}</span>
                    <span style={{ color: stealth ? "#64748b" : "#7dd3fc" }}>{item.dial}</span>
                  </button>
                );
              })}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

type Step = "phone" | "otp" | "success";

type Props = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSessionActive?: (payload: AddedSessionPayload) => void;
  stealth?: boolean;
};

function formatPhoneDisplay(dial: string, digits: string): string {
  const d = digits.replace(/\D/g, "");
  if (!d) return dial;
  return `${dial} ${d}`;
}

export default function AddSessionModal({ open, onOpenChange, onSessionActive, stealth = false }: Props) {
  const baseId = useId().replace(/:/g, "");
  const [step, setStep] = useState<Step>("phone");
  const [countryIdx, setCountryIdx] = useState(0);
  const [national, setNational] = useState("");
  const [otp, setOtp] = useState(["", "", "", "", ""]);
  const [busy, setBusy] = useState(false);
  const otpRefs = useRef<(HTMLInputElement | null)[]>([]);
  const otpAutoSent = useRef("");

  const dial = COUNTRIES[countryIdx]?.dial ?? "+1";
  const phoneDisplay = formatPhoneDisplay(dial, national);

  const reset = useCallback(() => {
    setStep("phone");
    setNational("");
    setOtp(["", "", "", "", ""]);
    setBusy(false);
    setCountryIdx(0);
    otpAutoSent.current = "";
  }, []);

  useEffect(() => {
    if (!open) {
      const t = setTimeout(reset, 320);
      return () => clearTimeout(t);
    }
    return undefined;
  }, [open, reset]);

  const goPhoneNext = () => {
    const digits = national.replace(/\D/g, "");
    if (digits.length < 8) return;
    otpAutoSent.current = "";
    setBusy(true);
    window.setTimeout(() => {
      setBusy(false);
      setStep("otp");
      window.setTimeout(() => otpRefs.current[0]?.focus(), 180);
    }, 420);
  };

  const verifyOtp = useCallback(() => {
    const code = otp.join("");
    if (code.length !== 5) return;
    setBusy(true);
    window.setTimeout(() => {
      setBusy(false);
      setStep("success");
      const sessionName = `session_${dial.replace(/\+/g, "")}_${national.replace(/\D/g, "").slice(-4)}`;
      onSessionActive?.({
        sessionName,
        phoneDisplay,
        dialCode: dial,
        nationalNumber: national.replace(/\D/g, ""),
      });
    }, 650);
  }, [dial, national, onSessionActive, otp, phoneDisplay]);

  useEffect(() => {
    if (step !== "otp" || busy) return;
    const code = otp.join("");
    if (code.length < 5) {
      otpAutoSent.current = "";
      return;
    }
    if (otpAutoSent.current === code) return;
    otpAutoSent.current = code;
    const t = window.setTimeout(() => verifyOtp(), 300);
    return () => window.clearTimeout(t);
  }, [step, otp, busy, verifyOtp]);

  const glassBorder = stealth ? "rgba(30, 41, 59, 0.9)" : "rgba(0, 180, 255, 0.28)";
  const glassBg = stealth ? "rgba(15, 23, 42, 0.92)" : "rgba(6, 14, 32, 0.78)";
  const glow = stealth ? "none" : "0 0 60px rgba(0, 180, 255, 0.12)";

  return (
    <AnimatePresence>
      {open && (
        <motion.div
          role="presentation"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.25 }}
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 200,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "1.25rem",
            background: "rgba(2, 6, 15, 0.72)",
            backdropFilter: "blur(10px)",
            WebkitBackdropFilter: "blur(10px)",
          }}
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) onOpenChange(false);
          }}
        >
          <motion.div
            role="dialog"
            aria-modal="true"
            aria-labelledby={`${baseId}-title`}
            initial={{ opacity: 0, scale: 0.94, y: 16 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.96, y: 10 }}
            transition={{ type: "spring", stiffness: 380, damping: 32 }}
            style={{
              width: "100%",
              maxWidth: 420,
              borderRadius: 20,
              border: `1.5px solid ${glassBorder}`,
              background: glassBg,
              backdropFilter: "blur(28px) saturate(1.5)",
              WebkitBackdropFilter: "blur(28px) saturate(1.5)",
              boxShadow: `${glow}, inset 0 1px 0 rgba(255,255,255,0.06)`,
              overflow: "hidden",
              position: "relative",
            }}
          >
            <div
              style={{
                position: "absolute",
                top: 0,
                left: "15%",
                right: "15%",
                height: 1,
                background: stealth
                  ? "linear-gradient(90deg, transparent, #334155, transparent)"
                  : "linear-gradient(90deg, transparent, rgba(0,230,255,0.5), transparent)",
                pointerEvents: "none",
              }}
            />

            <div style={{ padding: "1.5rem 1.5rem 1.25rem" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
                <div>
                  <h2
                    id={`${baseId}-title`}
                    style={{
                      fontFamily: "var(--font-mono)",
                      fontSize: "0.62rem",
                      fontWeight: 800,
                      letterSpacing: "0.2em",
                      textTransform: "uppercase",
                      color: stealth ? "#475569" : "#00b4ff",
                      margin: 0,
                    }}
                  >
                    New session link
                  </h2>
                  <p
                    style={{
                      fontFamily: "var(--font-sans)",
                      fontSize: "1.15rem",
                      fontWeight: 700,
                      color: stealth ? "#64748b" : "#e8f2ff",
                      margin: "0.35rem 0 0 0",
                      letterSpacing: "0.02em",
                    }}
                  >
                    {step === "phone" && "Register number"}
                    {step === "otp" && "Confirm OTP"}
                    {step === "success" && "Linked"}
                  </p>
                </div>
                <button
                  type="button"
                  onClick={() => onOpenChange(false)}
                  style={{
                    fontFamily: "var(--font-mono)",
                    fontSize: "0.65rem",
                    color: "#64748b",
                    background: "rgba(15,23,42,0.6)",
                    border: "1px solid #1e293b",
                    borderRadius: 8,
                    padding: "6px 10px",
                    cursor: "pointer",
                  }}
                >
                  Esc
                </button>
              </div>

              {/* Step indicators */}
              <div style={{ display: "flex", gap: 8, marginTop: "1.25rem" }}>
                {(["phone", "otp", "success"] as const).map((s) => {
                  const active =
                    s === "phone"
                      ? step === "phone"
                      : s === "otp"
                        ? step === "otp" || step === "success"
                        : step === "success";
                  return (
                    <div
                      key={s}
                      style={{
                        flex: 1,
                        height: 3,
                        borderRadius: 99,
                        background: active
                          ? stealth
                            ? "#475569"
                            : "linear-gradient(90deg, #00b4ff, #6366f1)"
                          : "#0f172a",
                        opacity: active ? 1 : 0.45,
                        transition: "background 0.35s ease, opacity 0.35s ease",
                      }}
                    />
                  );
                })}
              </div>
            </div>

            <div style={{ padding: "0 1.5rem 1.5rem" }}>
              <AnimatePresence mode="wait">
                {step === "phone" && (
                  <motion.div
                    key="phone"
                    initial={{ opacity: 0, x: -12 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: 12 }}
                    transition={{ duration: 0.22 }}
                    style={{ display: "flex", flexDirection: "column", gap: "1rem" }}
                  >
                    <label
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.58rem",
                        letterSpacing: "0.12em",
                        textTransform: "uppercase",
                        color: "#64748b",
                      }}
                    >
                      Country
                    </label>
                    <CountryPicker
                      countries={COUNTRIES}
                      countryIdx={countryIdx}
                      onSelect={setCountryIdx}
                      stealth={stealth}
                      baseId={baseId}
                    />

                    <label
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.58rem",
                        letterSpacing: "0.12em",
                        textTransform: "uppercase",
                        color: "#64748b",
                      }}
                    >
                      Phone number
                    </label>
                    <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                      <span
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.85rem",
                          color: stealth ? "#475569" : "#7dd3fc",
                          minWidth: 52,
                        }}
                      >
                        {dial}
                      </span>
                      <input
                        type="tel"
                        inputMode="numeric"
                        autoComplete="tel-national"
                        placeholder="501 234 567"
                        value={national}
                        onChange={(e) => setNational(e.target.value.replace(/[^\d\s-]/g, ""))}
                        style={{
                          flex: 1,
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.95rem",
                          padding: "0.7rem 0.85rem",
                          borderRadius: 12,
                          border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.3)"}`,
                          background: "rgba(2,6,14,0.9)",
                          color: "#f1f5f9",
                          outline: "none",
                          boxShadow: stealth ? "none" : "inset 0 0 20px rgba(0,180,255,0.04)",
                        }}
                      />
                    </div>
                    <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.6rem", color: "#475569", margin: 0, lineHeight: 1.5 }}>
                      One-time code will be sent to this number. After continue, enter 5 digits — verification runs automatically when complete (demo accepts any code).
                    </p>
                    <button
                      type="button"
                      disabled={busy || national.replace(/\D/g, "").length < 8}
                      onClick={goPhoneNext}
                      style={{
                        marginTop: 4,
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.72rem",
                        fontWeight: 800,
                        letterSpacing: "0.14em",
                        textTransform: "uppercase",
                        padding: "0.85rem 1rem",
                        borderRadius: 12,
                        border: stealth ? "1px solid #1e293b" : "1px solid rgba(0,180,255,0.45)",
                        background: busy
                          ? "#0f172a"
                          : stealth
                            ? "#1e293b"
                            : "linear-gradient(135deg, #00b4ff 0%, #6366f1 100%)",
                        color: busy ? "#475569" : stealth ? "#94a3b8" : "#0a0e1a",
                        cursor: busy || national.replace(/\D/g, "").length < 8 ? "not-allowed" : "pointer",
                        boxShadow: stealth || busy ? "none" : "0 0 24px rgba(0,180,255,0.2)",
                      }}
                    >
                      {busy ? "Sending code…" : "Continue →"}
                    </button>
                  </motion.div>
                )}

                {step === "otp" && (
                  <motion.div
                    key="otp"
                    initial={{ opacity: 0, x: -12 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: 12 }}
                    transition={{ duration: 0.22 }}
                    style={{ display: "flex", flexDirection: "column", gap: "1.1rem" }}
                  >
                    <p style={{ fontFamily: "var(--font-mono)", fontSize: "0.68rem", color: "#64748b", margin: 0 }}>
                      Enter the 5-digit code sent to{" "}
                      <span style={{ color: stealth ? "#94a3b8" : "#7dd3fc" }}>{phoneDisplay}</span>
                      . Digits auto-advance; when all five are filled, verification starts automatically.
                    </p>
                    <div style={{ display: "flex", gap: 10, justifyContent: "center" }}>
                      {otp.map((digit, idx) => (
                        <input
                          key={idx}
                          ref={(el) => {
                            otpRefs.current[idx] = el;
                          }}
                          inputMode="numeric"
                          maxLength={1}
                          value={digit}
                          aria-label={`Digit ${idx + 1}`}
                          onChange={(e) => {
                            const v = e.target.value.replace(/\D/g, "").slice(-1);
                            const next = [...otp];
                            next[idx] = v;
                            setOtp(next);
                            if (v && idx < 4) otpRefs.current[idx + 1]?.focus();
                          }}
                          onKeyDown={(e) => {
                            if (e.key === "Backspace" && !otp[idx] && idx > 0) {
                              otpRefs.current[idx - 1]?.focus();
                            }
                          }}
                          onPaste={(e) => {
                            e.preventDefault();
                            const paste = e.clipboardData.getData("text").replace(/\D/g, "").slice(0, 5);
                            if (!paste) return;
                            const next = ["", "", "", "", ""];
                            for (let i = 0; i < paste.length; i++) next[i] = paste[i]!;
                            setOtp(next);
                            otpRefs.current[Math.min(paste.length, 4)]?.focus();
                          }}
                          style={{
                            width: 48,
                            height: 52,
                            textAlign: "center",
                            fontFamily: "var(--font-mono)",
                            fontSize: "1.25rem",
                            fontWeight: 700,
                            borderRadius: 12,
                            border: `1px solid ${stealth ? "#1e293b" : "rgba(0,180,255,0.35)"}`,
                            background: "rgba(2,6,14,0.95)",
                            color: "#f8fafc",
                            outline: "none",
                            boxShadow: stealth ? "none" : "0 0 16px rgba(0,180,255,0.08)",
                          }}
                        />
                      ))}
                    </div>
                    <button
                      type="button"
                      disabled={busy || otp.join("").length !== 5}
                      onClick={verifyOtp}
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.72rem",
                        fontWeight: 800,
                        letterSpacing: "0.14em",
                        textTransform: "uppercase",
                        padding: "0.85rem 1rem",
                        borderRadius: 12,
                        border: stealth ? "1px solid #1e293b" : "1px solid rgba(34,211,153,0.4)",
                        background: busy
                          ? "#0f172a"
                          : stealth
                            ? "#1e293b"
                            : "linear-gradient(135deg, #22c55e 0%, #00b4ff 100%)",
                        color: busy ? "#475569" : stealth ? "#94a3b8" : "#041018",
                        cursor: busy || otp.join("").length !== 5 ? "not-allowed" : "pointer",
                        boxShadow: stealth || busy ? "none" : "0 0 22px rgba(34,211,153,0.2)",
                      }}
                    >
                      {busy ? "Verifying…" : "Verify & link"}
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        otpAutoSent.current = "";
                        setStep("phone");
                        setOtp(["", "", "", "", ""]);
                      }}
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.62rem",
                        color: "#64748b",
                        background: "transparent",
                        border: "none",
                        cursor: "pointer",
                        textDecoration: "underline",
                        textUnderlineOffset: 3,
                      }}
                    >
                      ← Change number
                    </button>
                  </motion.div>
                )}

                {step === "success" && (
                  <motion.div
                    key="success"
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ type: "spring", stiffness: 400, damping: 28 }}
                    style={{
                      display: "flex",
                      flexDirection: "column",
                      alignItems: "center",
                      gap: "1rem",
                      padding: "0.5rem 0 0.25rem",
                    }}
                  >
                    <div style={{ position: "relative", width: 140, height: 140, display: "flex", alignItems: "center", justifyContent: "center" }}>
                      {!stealth && (
                        <>
                          <motion.div
                            aria-hidden
                            style={{
                              position: "absolute",
                              inset: 0,
                              borderRadius: "50%",
                              border: "1px solid rgba(74,222,128,0.35)",
                            }}
                            animate={{ scale: [1, 1.35, 1], opacity: [0.55, 0, 0.55] }}
                            transition={{ duration: 2.2, repeat: Infinity, ease: "easeInOut" }}
                          />
                          <motion.div
                            aria-hidden
                            style={{
                              position: "absolute",
                              inset: 12,
                              borderRadius: "50%",
                              border: "1px solid rgba(34,211,153,0.25)",
                            }}
                            animate={{ scale: [1, 1.2, 1], opacity: [0.4, 0.15, 0.4] }}
                            transition={{ duration: 1.6, repeat: Infinity, ease: "easeInOut", delay: 0.2 }}
                          />
                        </>
                      )}
                      <motion.div
                        animate={{
                          scale: [1, 1.06, 1],
                          boxShadow: stealth
                            ? ["0 0 0 #000", "0 0 0 #000", "0 0 0 #000"]
                            : [
                                "0 0 0 rgba(34,211,153,0)",
                                "0 0 40px rgba(34,211,153,0.45)",
                                "0 0 24px rgba(34,211,153,0.25)",
                              ],
                        }}
                        transition={{ duration: 1.2, repeat: Infinity, ease: "easeInOut" }}
                        style={{
                          width: 88,
                          height: 88,
                          borderRadius: "50%",
                          background: stealth ? "#1e293b" : "radial-gradient(circle at 30% 25%, #4ade80, #16a34a)",
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "center",
                          border: stealth ? "1px solid #334155" : "1px solid rgba(74,222,128,0.5)",
                          position: "relative",
                          zIndex: 1,
                        }}
                      >
                        <motion.span
                          initial={{ scale: 0, rotate: -45 }}
                          animate={{ scale: 1, rotate: 0 }}
                          transition={{ type: "spring", delay: 0.1, stiffness: 500, damping: 22 }}
                          style={{ fontSize: "2.25rem", lineHeight: 1 }}
                        >
                          ✓
                        </motion.span>
                      </motion.div>
                    </div>
                    <motion.div
                      initial={{ opacity: 0, y: 8 }}
                      animate={{ opacity: 1, y: 0 }}
                      transition={{ delay: 0.15 }}
                      style={{ textAlign: "center" }}
                    >
                      <motion.div
                        animate={
                          stealth
                            ? {}
                            : {
                                backgroundPosition: ["0% 50%", "100% 50%", "0% 50%"],
                              }
                        }
                        transition={{ duration: 3.5, repeat: Infinity, ease: "linear" }}
                        style={{
                          fontFamily: "var(--font-mono)",
                          fontSize: "0.72rem",
                          fontWeight: 800,
                          letterSpacing: "0.18em",
                          textTransform: "uppercase",
                          background: stealth
                            ? undefined
                            : "linear-gradient(90deg, #4ade80, #86efac, #22d3ee, #4ade80)",
                          backgroundSize: stealth ? undefined : "220% auto",
                          WebkitBackgroundClip: stealth ? undefined : "text",
                          backgroundClip: stealth ? undefined : "text",
                          color: stealth ? "#64748b" : "transparent",
                          textShadow: stealth ? "none" : "0 0 24px rgba(74,222,128,0.35)",
                        }}
                      >
                        Session active
                      </motion.div>
                      <div style={{ fontFamily: "var(--font-mono)", fontSize: "0.65rem", color: "#64748b", marginTop: 8 }}>
                        {phoneDisplay}
                      </div>
                    </motion.div>
                    <button
                      type="button"
                      onClick={() => onOpenChange(false)}
                      style={{
                        fontFamily: "var(--font-mono)",
                        fontSize: "0.68rem",
                        fontWeight: 700,
                        letterSpacing: "0.1em",
                        textTransform: "uppercase",
                        padding: "0.65rem 1.25rem",
                        borderRadius: 10,
                        border: "1px solid #334155",
                        background: "rgba(15,23,42,0.8)",
                        color: "#94a3b8",
                        cursor: "pointer",
                        marginTop: 4,
                      }}
                    >
                      Done
                    </button>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
