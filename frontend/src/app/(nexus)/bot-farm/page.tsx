"use client";

import CyberGrid from "@/components/CyberGrid";
import PageTransition from "@/components/PageTransition";
import BotFarm from "@/pages/BotFarm.jsx";

export default function BotFarmPage() {
  return (
    <PageTransition>
      <div style={{ position: "relative", minHeight: "100vh" }}>
        <CyberGrid opacity={0.4} speed={0.55} />
        <div style={{ position: "relative", zIndex: 1, padding: "1.5rem 1.25rem" }}>
          <BotFarm />
        </div>
      </div>
    </PageTransition>
  );
}
