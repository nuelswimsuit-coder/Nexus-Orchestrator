"use client";

import type { ReactNode } from "react";
import { NexusProvider } from "@/lib/nexus-context";
import { useI18n } from "@/lib/i18n";
import Sidebar from "@/components/Sidebar";
import GlobalErrorOverlay from "@/components/GlobalErrorOverlay";

const SIDEBAR_COLLAPSED = 56; // px — matches Sidebar W_COLLAPSED

export default function NexusLayout({ children }: { children: ReactNode }) {
  const { isRTL } = useI18n();

  return (
    <NexusProvider>
      {/* System-wide offline overlay — renders above all content when master is down */}
      <GlobalErrorOverlay />
      <Sidebar />
      <div
        style={{
          marginRight: isRTL ? SIDEBAR_COLLAPSED : 0,
          marginLeft:  isRTL ? 0 : SIDEBAR_COLLAPSED,
          minHeight: "calc(100vh - 56px)",
          transition: "margin 0.3s",
        }}
      >
        {children}
      </div>
    </NexusProvider>
  );
}
