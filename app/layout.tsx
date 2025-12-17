import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";

import Providers from "@/components/providers";

export const metadata: Metadata = {
  title: "Dataset Inspector",
  description: "Inspect LitData shards and Hugging Face streaming previews with a Tauri + Next.js desktop UI.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="select-none cursor-default h-screen w-screen overflow-hidden bg-slate-50 text-slate-900 antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
