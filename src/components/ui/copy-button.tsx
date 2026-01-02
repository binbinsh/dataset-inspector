import * as React from "react";
import { Check, Copy } from "lucide-react";

import { Button, type ButtonProps } from "@/components/ui/button";
import { cn } from "@/lib/utils";

async function writeToClipboard(text: string) {
  if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  if (typeof document === "undefined") {
    throw new Error("Clipboard is unavailable.");
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.top = "0";
  textarea.style.left = "0";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();

  const ok = document.execCommand("copy");
  document.body.removeChild(textarea);

  if (!ok) {
    throw new Error("Copy failed.");
  }
}

export type CopyButtonProps = Omit<ButtonProps, "onClick" | "children"> & {
  text: string;
  copiedDurationMs?: number;
  idleLabel?: string;
  copiedLabel?: string;
};

export function CopyButton({
  text,
  copiedDurationMs = 1500,
  idleLabel = "Copy",
  copiedLabel = "Copied",
  type = "button",
  size = "sm",
  variant = "ghost",
  className,
  disabled,
  ...props
}: CopyButtonProps) {
  const [copied, setCopied] = React.useState(false);

  const handleCopy = React.useCallback(() => {
    if (!text) return;
    void writeToClipboard(text).catch(() => {
      // ignore
    });
    setCopied(true);
  }, [text]);

  React.useEffect(() => {
    if (!copied) return;
    const timer = setTimeout(() => setCopied(false), copiedDurationMs);
    return () => clearTimeout(timer);
  }, [copied, copiedDurationMs]);

  return (
    <Button
      type={type}
      size={size}
      variant={variant}
      className={cn(className)}
      onClick={handleCopy}
      disabled={disabled || !text}
      {...props}
    >
      {copied ? (
        <Check className="mr-1 h-3.5 w-3.5 text-emerald-600" />
      ) : (
        <Copy className="mr-1 h-3.5 w-3.5" />
      )}
      {copied ? copiedLabel : idleLabel}
    </Button>
  );
}

