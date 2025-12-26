import { Chip, type ChipProps } from "@heroui/react";
import { cn } from "@/lib/utils";

type BadgeVariant = "default" | "secondary";

type BadgeProps = Omit<ChipProps, "variant" | "size" | "radius"> & {
  variant?: BadgeVariant;
};

export function Badge({ className, variant = "default", ...props }: BadgeProps) {
  const mappedVariant: ChipProps["variant"] = variant === "secondary" ? "flat" : "solid";
  return (
    <Chip
      radius="full"
      size="sm"
      variant={mappedVariant}
      className={cn("text-xs font-semibold", className)}
      {...props}
    />
  );
}
