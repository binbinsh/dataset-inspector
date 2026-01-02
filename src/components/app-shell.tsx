import type { ReactNode } from "react";

import UpdateBanner from "@/components/update-banner";

export default function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="text-slate-900 select-none cursor-default h-screen overflow-hidden flex flex-col bg-gradient-to-b from-slate-50 to-slate-100">
      <div className="flex-1 overflow-hidden p-2">{children}</div>
      <UpdateBanner />
    </div>
  );
}
