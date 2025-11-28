import "./globals.css";
import type { Metadata } from "next";
import { Space_Grotesk } from "next/font/google";
import type { ReactNode } from "react";

import Providers from "@/components/providers";

const display = Space_Grotesk({
  subsets: ["latin"],
  variable: "--font-display",
});

export const metadata: Metadata = {
  title: "LitData Viewer",
  description: "Inspect LitData shard indexes and chunk payloads with a Tauri + Next.js desktop UI.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className={display.variable}>
      <body className="select-none cursor-default h-screen w-screen overflow-hidden bg-slate-50 text-slate-900 antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
