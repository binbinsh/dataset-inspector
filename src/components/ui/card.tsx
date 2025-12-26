import {
  Card as HeroCard,
  CardBody,
  CardHeader,
  type CardProps,
} from "@heroui/react";
import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

type CardContentProps = {
  children: ReactNode;
  className?: string;
};

export function Card({ className, ...props }: CardProps) {
  return <HeroCard className={cn("border-none shadow-none", className)} {...props} />;
}

export { CardHeader };

export function CardContent({ className, ...props }: CardContentProps) {
  return <CardBody className={className} {...props} />;
}

export function CardTitle({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("text-base font-semibold", className)}>{children}</div>;
}
