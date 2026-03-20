import type { Metadata } from "next";
import { Inter, Assistant, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { StealthProvider } from "@/lib/stealth";
import { I18nProvider } from "@/lib/i18n";
import { ThemeProvider } from "@/lib/theme";
import Header from "@/components/Header";
import { MobileHitlButton } from "@/components/MobileHitl";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin", "latin-ext"],
  display: "swap",
  weight: ["300", "400", "500", "600", "700", "800"],
});

// Assistant — designed for Hebrew; excellent RTL rendering
const assistant = Assistant({
  variable: "--font-assistant",
  subsets: ["latin", "hebrew"],
  display: "swap",
  weight: ["300", "400", "500", "600", "700", "800"],
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains",
  subsets: ["latin"],
  display: "swap",
  weight: ["400", "500", "700"],
});

export const metadata: Metadata = {
  title: "מרכז שליטה אופרטיבי — Nexus OS",
  description: "מערכת זרימת עבודה אוטונומית מבוזרת — v2.0-Alpha",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html
      lang="he"
      className={`${inter.variable} ${assistant.variable} ${jetbrainsMono.variable} h-full dark`}
      data-theme="dark"
    >
      <body dir="rtl" className="min-h-full flex flex-col antialiased bg-[#050505] text-white">
        <ThemeProvider>
          <I18nProvider>
            <StealthProvider>
              <Header />
              <main style={{ paddingTop: "56px", flex: 1 }}>{children}</main>
              <MobileHitlButton />
            </StealthProvider>
          </I18nProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
