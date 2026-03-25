"use client";

import CyberGrid from "@/components/CyberGrid";
import MatrixRawLogs from "@/components/MatrixRawLogs";
import PageTransition from "@/components/PageTransition";

export default function LogsRawPage() {
  return (
    <>
      <CyberGrid opacity={0.35} speed={1.1} />
      <PageTransition>
        <div
          style={{
            position: "relative",
            zIndex: 1,
            maxWidth: 900,
            margin: "0 auto",
            padding: "2rem 1.5rem 3rem",
          }}
        >
          <header dir="rtl" style={{ marginBottom: "1.25rem", textAlign: "right" }}>
            <h1
              style={{
                fontSize: "1.55rem",
                fontWeight: 800,
                color: "#bbf7d0",
                margin: 0,
                textShadow: "0 0 20px rgba(34,197,94,0.35)",
              }}
            >
              יומן גולמי — מצב מטריקס
            </h1>
            <p
              style={{
                margin: "0.4rem 0 0",
                color: "#86efac",
                fontFamily: "var(--font-mono)",
                fontSize: "0.7rem",
              }}
            >
              זרם בזמן אמת מ־/api/business/agent-log · לחיצה על שורה מציגה תרגום עברי
            </p>
          </header>
          <MatrixRawLogs />
        </div>
      </PageTransition>
    </>
  );
}
