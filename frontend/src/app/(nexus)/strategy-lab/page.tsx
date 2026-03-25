"use client";

import CyberGrid from "@/components/CyberGrid";
import PageTransition from "@/components/PageTransition";
import StrategyLab from "@/pages/StrategyLab.jsx";

export default function StrategyLabPage() {
  return (
    <PageTransition>
      <div style={{ position: "relative", minHeight: "100vh" }}>
        <CyberGrid opacity={0.38} speed={0.5} />
        <div style={{ position: "relative", zIndex: 1, padding: "1.5rem 1.25rem" }}>
          <StrategyLab />
        </div>
      </div>
    </PageTransition>
  );
}
