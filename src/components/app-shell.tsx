import type { ReactNode } from "react";

import UpdateBanner from "@/components/update-banner";

export default function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="atlas text-foreground select-none cursor-default h-screen overflow-hidden flex flex-col">
      <div className="flex-1 overflow-hidden p-2">{children}</div>
      <UpdateBanner />
    </div>
  );
}
