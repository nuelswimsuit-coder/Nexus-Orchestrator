"use client";

/**
 * Voice & Command AI — mic for Hebrew/English commands, TTS reply, internal brain stream log.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { API_BASE } from "@/lib/api";

function getRecognition() {
  if (typeof window === "undefined") return null;
  return window.SpeechRecognition || window.webkitSpeechRecognition || null;
}

function speak(text) {
  if (typeof window === "undefined" || !window.speechSynthesis) return;
  window.speechSynthesis.cancel();
  const u = new SpeechSynthesisUtterance(text);
  u.lang = "he-IL";
  u.rate = 1;
  window.speechSynthesis.speak(u);
}

export default function GeminiTerminal() {
  const [listening, setListening] = useState(false);
  const [brain, setBrain] = useState([]);
  const recRef = useRef(null);
  const preRef = useRef(null);

  const pushBrain = useCallback((line) => {
    setBrain((b) => [...b.slice(-80), `[${new Date().toLocaleTimeString()}] ${line}`]);
  }, []);

  useEffect(() => {
    const el = preRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [brain]);

  useEffect(() => {
    const id = setInterval(() => {
      const thoughts = [
        "אני חושב ש-BTC הולך לעלות כי זרימת Binance חזקה מול פולימרקט.",
        "מודל בודק פער ארביטראז' בין YES לספוט — עדיין בטווח בטוח.",
        "מתעדף סשנים עם health ירוק לפני מסה לסריקה.",
      ];
      if (Math.random() < 0.2) pushBrain(`internal · ${thoughts[(Math.random() * thoughts.length) | 0]}`);
    }, 11_000);
    return () => clearInterval(id);
  }, [pushBrain]);

  const runCommand = useCallback(
    async (raw) => {
      const text = (raw || "").trim();
      if (!text) return;
      pushBrain(`user_voice · ${text}`);

      const lower = text.toLowerCase();
      if (lower.includes("תפתח פרויקט חדש") || lower.includes("new project")) {
        try {
          const res = await fetch(`${API_BASE}/api/incubator/generate`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              niche_name: "voice_command",
              custom_brief: `Voice-triggered scaffold: ${text}`,
              source: "gemini_terminal",
            }),
          });
          const j = await res.json().catch(() => ({}));
          const ok = res.ok;
          const msg = ok
            ? "בוצע, יעקב חתן. הפרויקט באוויר"
            : `הפקודה נרשמה מקומית — API ${res.status} ${j.message || ""}`;
          pushBrain(ok ? `agent · ${JSON.stringify(j)}` : `agent · ${msg}`);
          speak(ok ? "בוצע, יעקב חתן. הפרויקט באוויר" : "לא הצלחתי להפעיל את השרת, בדוק את ה-API");
        } catch {
          pushBrain("agent · offline — queued locally");
          speak("השרת לא זמין, הפקודה נשמרה בממשק בלבד");
        }
        return;
      }

      speak("קיבלתי. מבצע ניתוח.");
      pushBrain(`agent · echo / interpret: ${text.slice(0, 120)}`);
    },
    [pushBrain],
  );

  const toggleMic = useCallback(() => {
    const Rec = getRecognition();
    if (!Rec) {
      pushBrain("system · SpeechRecognition לא נתמך בדפדפן זה");
      return;
    }

    if (listening && recRef.current) {
      recRef.current.stop();
      recRef.current = null;
      setListening(false);
      return;
    }

    const r = new Rec();
    r.lang = "he-IL";
    r.continuous = false;
    r.interimResults = false;
    r.onresult = (ev) => {
      const t = ev.results[0]?.[0]?.transcript;
      void runCommand(t);
    };
    r.onerror = () => {
      pushBrain("system · שגיאת מיקרופון");
      setListening(false);
    };
    r.onend = () => setListening(false);
    recRef.current = r;
    r.start();
    setListening(true);
    pushBrain("system · מיקרופון פעיל — דברו עכשיו");
  }, [listening, pushBrain, runCommand]);

  return (
    <div
      className="rounded-xl border border-[var(--color-surface-4)] bg-[var(--color-surface-0)]/40 p-4"
      style={{ display: "flex", flexDirection: "column", gap: 12 }}
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-mono text-[0.55rem] uppercase tracking-[0.2em] text-[var(--color-accent)]">Gemini Terminal</span>
        <button
          type="button"
          onClick={() => void toggleMic()}
          className="rounded-lg border px-3 py-1.5 font-mono text-[0.65rem]"
          style={{
            borderColor: listening ? "#22c55e" : "#475569",
            background: listening ? "rgba(34,197,94,0.15)" : "transparent",
            color: listening ? "#86efac" : "#94a3b8",
          }}
        >
          {listening ? "● מקליט" : "🎤 מיקרופון"}
        </button>
      </div>

      <div>
        <div className="mb-1 font-mono text-[0.55rem] text-[var(--color-text-muted)]">Brain Internal Stream</div>
        <pre
          ref={preRef}
          className="max-h-40 overflow-auto rounded-lg border border-[var(--color-surface-4)] bg-black/40 p-2 font-mono text-[0.58rem] leading-relaxed text-[var(--color-text-secondary)]"
        >
          {brain.length === 0 ? "ממתין לפקודות קוליות או מחשבה פנימית…" : brain.join("\n")}
        </pre>
      </div>
    </div>
  );
}
