"use client";

import React from "react";
import AgentThinkingLog from "@/components/AgentThinkingLog";
import CyberGrid from "@/components/CyberGrid";
import FlightModeOverlay from "@/components/FlightModeOverlay";
import ArbitrageGraph from "@/components/ArbitrageGraph";
import AutonomyControl from "@/components/AutonomyControl";
import BudgetWidget from "@/components/BudgetWidget";
import CommunityIdentity from "@/components/CommunityIdentity";
import FleetStatus from "@/components/FleetStatus";
import MatrixOpsFooter from "@/components/MatrixOpsFooter";
import FleetScanProgress from "@/components/FleetScanProgress";
import ContentPreview from "@/components/ContentPreview";
import DeployTerminal from "@/components/DeployTerminal";
import EmergencyPanel from "@/components/EmergencyPanel";
import FinancialPulseWidget from "@/components/FinancialPulseWidget";
import HitlManager from "@/components/HitlManager";
import IntelDashboard from "@/components/IntelDashboard";
import GroupHealth from "@/components/GroupHealth";
import ModuleHealth from "@/components/ModuleHealth";
import PageTransition from "@/components/PageTransition";
import PolymarketBotPnL from "@/components/PolymarketBotPnL";
import Poly5mScalperWidget from "@/components/Poly5mScalperWidget";
import PowerProfileBar from "@/components/PowerProfileBar";
import PredictorWidget from "@/components/PredictorWidget";
import PredictionMarketWidget from "@/components/PredictionMarketWidget";
import ProfitHeatmap from "@/components/ProfitHeatmap";
import SessionHealthGauge from "@/components/SessionHealthGauge";
import StabilityGauge from "@/components/StabilityGauge";
import TopologyVisual from "@/components/TopologyVisual";
import UltimateScalperPanel from "@/components/UltimateScalperPanel";
import VirtualTradeLog from "@/components/VirtualTradeLog";
import WarRoomIntel from "@/components/WarRoomIntel";
import WinRateWidget from "@/components/WinRateWidget";
import { motion } from "framer-motion";

// ── Stagger animation helpers ─────────────────────────────────────────────────

const containerVariants = {
  hidden: {},
  visible: {
    transition: {
      staggerChildren: 0.07,
      delayChildren: 0.05,
    },
  },
};

const itemVariants = {
  hidden:  { opacity: 0, y: 16 },
  visible: { opacity: 1, y: 0, transition: { duration: 0.45, ease: [0.22, 1, 0.36, 1] } },
};

// ── Section divider ───────────────────────────────────────────────────────────

function SectionLabel({ label, sub }: { label: string; sub?: string }) {
  return (
    <motion.div
      variants={itemVariants}
      style={{
        display: "flex",
        alignItems: "center",
        gap: "1rem",
        marginBottom: "-0.5rem",
        paddingTop: "0.5rem",
      }}
    >
      {/* Accent bar */}
      <div style={{
        width: "4px",
        height: "32px",
        borderRadius: "2px",
        background: "linear-gradient(180deg, #00b4ff 0%, rgba(0,180,255,0.1) 100%)",
        flexShrink: 0,
        boxShadow: "0 0 12px rgba(0,180,255,0.45)",
      }} />
      <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
        <span style={{
          fontFamily: "var(--font-sans)",
          fontSize: "1.875rem",
          fontWeight: 800,
          letterSpacing: "0.03em",
          color: "#e8f2ff",
          lineHeight: 1.1,
          textTransform: "uppercase",
        }}>
          {label}
        </span>
        {sub && (
          <span style={{
            fontFamily: "var(--font-sans)",
            fontSize: "0.9rem",
            fontWeight: 400,
            color: "#7da8cc",
            letterSpacing: "0.02em",
          }}>
            {sub}
          </span>
        )}
      </div>
      <div style={{
        flex: 1,
        height: "1px",
        background: "linear-gradient(90deg, rgba(0,180,255,0.25) 0%, transparent 80%)",
        marginLeft: "0.5rem",
      }} />
    </motion.div>
  );
}

// ── Glass section card ────────────────────────────────────────────────────────

function GlassSection({ children, accent = false }: { children: React.ReactNode; accent?: boolean }) {
  return (
    <motion.div
      variants={itemVariants}
      style={{
        background: accent
          ? "rgba(0, 10, 28, 0.78)"
          : "rgba(5, 10, 22, 0.72)",
        backdropFilter: "blur(28px) saturate(1.6)",
        WebkitBackdropFilter: "blur(28px) saturate(1.6)",
        border: `1.5px solid rgba(0, 180, 255, ${accent ? "0.35" : "0.18"})`,
        borderRadius: "18px",
        padding: "1.75rem",
        boxShadow: accent
          ? `0 0 0 1px rgba(0,180,255,0.10) inset,
             0 12px 56px rgba(0,0,0,0.70),
             0 0 48px rgba(0,180,255,0.12),
             0 1px 0 rgba(0,180,255,0.25) inset`
          : `0 0 0 1px rgba(0,180,255,0.07) inset,
             0 8px 40px rgba(0,0,0,0.60),
             0 0 24px rgba(0,180,255,0.06)`,
        display: "flex",
        flexDirection: "column" as const,
        gap: "1.25rem",
        position: "relative" as const,
        overflow: "hidden" as const,
      }}
    >
      {/* Subtle inner top shimmer */}
      <div style={{
        position: "absolute", top: 0, left: "2rem", right: "2rem", height: "1px",
        background: "linear-gradient(90deg, transparent, rgba(0,180,255,0.30), transparent)",
        pointerEvents: "none",
      }} />
      {children}
    </motion.div>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────────

export default function DashboardPage() {
  return (
    <>
    {/* ── Animated perspective-grid background ────────────────────────── */}
    <CyberGrid opacity={0.55} speed={0.9} />

    <FlightModeOverlay />
    <PageTransition>
      <motion.div
        variants={containerVariants}
        initial="hidden"
        animate="visible"
        style={{
          position: "relative",
          zIndex: 1,
          maxWidth: "1400px",
          margin: "0 auto",
          padding: "2rem 1.5rem 3rem",
          display: "flex",
          flexDirection: "column",
          gap: "2rem",
        }}
      >
        <PowerProfileBar />

        {/* ── System Resilience ────────────────────────────────────────────── */}
        <GlassSection accent>
          <SectionLabel
            label="מדד חוסן מערכת"
            sub="System Resilience · סורק ארביטראז' · עדכון כל 5 שניות"
          />
          <div style={{
            display: "grid",
            gridTemplateColumns: "minmax(320px, 1.6fr) minmax(240px, 1fr)",
            gap: "1.25rem",
            alignItems: "start",
          }}>
            <StabilityGauge />
            <EmergencyPanel />
          </div>
        </GlassSection>

        {/* ── War Room — Master Trader intel ───────────────────────────────── */}
        <GlassSection accent>
          <SectionLabel
            label="WAR ROOM"
            sub="Master confidence · Race to 1000% · Fleet sentiment heatmap"
          />
          <WarRoomIntel />
        </GlassSection>

        {/* ── Network Topology ─────────────────────────────────────────────── */}
        <GlassSection>
          <SectionLabel label="CLUSTER TOPOLOGY" sub="Physical network map" />
          <TopologyVisual />
        </GlassSection>

        {/* ── HITL Queue ───────────────────────────────────────────────────── */}
        <GlassSection>
          <SectionLabel label="HUMAN-IN-THE-LOOP" sub="Pending approvals" />
          <HitlManager />
        </GlassSection>

        {/* ── Swarm Social Synthesis — community identity + AI warmer state ─ */}
        <GlassSection accent>
          <SectionLabel
            label="SWARM SOCIAL SYNTHESIS"
            sub="Community identity · group description · emerging vibe (Gemini 1.5 Flash)"
          />
          <CommunityIdentity />
        </GlassSection>

        {/* ── Financial Engine ─────────────────────────────────────────────── */}
        <GlassSection accent>
          <SectionLabel label="FINANCIAL ENGINE" sub="Revenue & session health" />
          <div style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "1.25rem",
            alignItems: "start",
          }}>
            <FinancialPulseWidget />
            <SessionHealthGauge />
          </div>
        </GlassSection>

        {/* ── Cross-Exchange Predictor ──────────────────────────────────────── */}
        <GlassSection>
          <SectionLabel label="CROSS-EXCHANGE PREDICTOR" sub="Binance order flow vs Polymarket odds" />
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(280px, 1.2fr) minmax(260px, 1fr)",
              gap: "1.25rem",
              alignItems: "start",
            }}
          >
            <PredictorWidget />
            <PolymarketBotPnL />
          </div>
        </GlassSection>

        {/* ── Prediction Market (BTC vs Poly + CI + manual kill) ─────────────── */}
        <GlassSection accent>
          <SectionLabel
            label="PREDICTION MARKET"
            sub="BTC spot vs Polymarket YES · AI confidence band · volatility override"
          />
          <PredictionMarketWidget />
        </GlassSection>

        {/* ── Ultimate Scalper (5m) ─────────────────────────────────────────── */}
        <GlassSection>
          <SectionLabel
            label="ULTIMATE SCALPER · 5M"
            sub="Simulation vs real · Binance velocity + OpenClaw news · Race to 1000%"
          />
          <UltimateScalperPanel />
        </GlassSection>

        {/* ── NEXUS-POLY-SCALPER-5M (dedicated event + Telefix + WS velocity) ─ */}
        <GlassSection accent>
          <SectionLabel
            label="POLY 5M SCALPER"
            sub="Binance WS velocity · telefix.db · CLOB · max $5 / cycle · 3-loss panic"
          />
          <Poly5mScalperWidget />
        </GlassSection>

        {/* ── Arbitrage Visualizer + Win-Rate Tracker ───────────────────────── */}
        <GlassSection accent>
          <SectionLabel label="סורק ארביטראז'" sub="Binance vs Polymarket — Real Time · אחוז הצלחה" />
          <div style={{
            display:             "grid",
            gridTemplateColumns: "2fr 1fr",
            gap:                 "1.25rem",
            alignItems:          "start",
          }}>
            <ArbitrageGraph />
            <WinRateWidget />
          </div>
        </GlassSection>

        {/* ── Live Trade Log ───────────────────────────────────────────────── */}
        <GlassSection>
          <SectionLabel label="LIVE OPS - REAL-TIME EXECUTION" sub="Execution telemetry and real trade stream" />
          <VirtualTradeLog />
        </GlassSection>

        {/* ── Control Panel ────────────────────────────────────────────────── */}
        <GlassSection accent>
          <SectionLabel label="CONTROL PANEL" sub="Budget · Autonomy · Deploy" />
          <div style={{
            display: "grid",
            gridTemplateColumns: "minmax(240px,1fr) minmax(280px,1fr) 2fr",
            gap: "1.25rem",
            alignItems: "start",
          }}>
            <BudgetWidget />
            <AutonomyControl />
            <DeployTerminal />
          </div>
        </GlassSection>

        {/* ── Intelligence ─────────────────────────────────────────────────── */}
        <GlassSection>
          <SectionLabel label="INTELLIGENCE" sub="Market & content signals" />
          <IntelDashboard />
          <ContentPreview />
        </GlassSection>

        {/* ── Analytics ────────────────────────────────────────────────────── */}
        <GlassSection accent>
          <SectionLabel label="ANALYTICS" sub="Profit heatmap & agent logs" />
          <ProfitHeatmap />
          <AgentThinkingLog />
        </GlassSection>

        {/* ── Digital Twin / Hardware grid (Matrix ops) ───────────────────── */}
        <GlassSection>
          <SectionLabel label="DIGITAL TWIN" sub="Hardware grid · Redis probes · live CPU" />
          <FleetStatus />
          <FleetScanProgress />
          <GroupHealth />
          <ModuleHealth />
        </GlassSection>

        <div style={{ width: "100%" }}>
          <MatrixOpsFooter />
        </div>
      </motion.div>
    </PageTransition>
    </>
  );
}
