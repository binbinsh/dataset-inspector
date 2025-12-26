import { Button as HeroButton, type ButtonProps } from "@heroui/react";

type LegacyVariant = "default" | "outline" | "ghost" | "secondary";

type LegacyButtonProps = Omit<ButtonProps, "variant" | "size" | "isDisabled"> & {
  variant?: LegacyVariant;
  size?: "sm" | "md" | "lg";
  disabled?: boolean;
};

const mapVariant = (variant?: LegacyVariant): ButtonProps["variant"] => {
  switch (variant) {
    case "outline":
      return "bordered";
    case "ghost":
      return "light";
    case "secondary":
      return "flat";
    default:
      return "solid";
  }
};

export function Button({ variant, size = "md", disabled, ...props }: LegacyButtonProps) {
  return <HeroButton variant={mapVariant(variant)} size={size} isDisabled={disabled} {...props} />;
}
