import { ScrollShadow } from "@heroui/react";
import type { ComponentPropsWithoutRef } from "react";
import { cn } from "@/lib/utils";

type ScrollAreaProps = ComponentPropsWithoutRef<typeof ScrollShadow> & {
  className?: string;
};

export function ScrollArea({ className, ...props }: ScrollAreaProps) {
  return (
    <ScrollShadow
      hideScrollBar
      className={cn("overflow-y-auto", className)}
      {...props}
    />
  );
}
