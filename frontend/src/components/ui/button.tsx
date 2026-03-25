"use client";

import { forwardRef, type ButtonHTMLAttributes } from "react";
import { cn } from "@/lib/cn";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "default" | "outline" | "ghost" | "destructive";
  size?: "sm" | "md" | "lg";
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "default", size = "md", ...props }, ref) => {
    return (
      <button
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center gap-2 rounded-lg font-mono text-xs font-semibold uppercase tracking-wider transition-all focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-accent)] disabled:pointer-events-none disabled:opacity-40",
          variant === "default" &&
            "border border-[var(--color-accent)]/50 bg-[var(--color-accent)]/15 text-[var(--color-accent-bright)] shadow-[0_0_20px_rgba(0,180,255,0.12)] hover:bg-[var(--color-accent)]/25",
          variant === "outline" &&
            "border border-[var(--color-surface-4)] bg-[var(--color-surface-2)]/80 text-[var(--color-text-secondary)] hover:border-[var(--color-accent)]/40 hover:text-[var(--color-text-primary)]",
          variant === "ghost" &&
            "border border-transparent bg-transparent text-[var(--color-text-secondary)] hover:bg-[var(--color-surface-3)]/60",
          variant === "destructive" &&
            "border border-[var(--color-danger)]/50 bg-[var(--color-danger)]/10 text-[var(--color-danger)] hover:bg-[var(--color-danger)]/20",
          size === "sm" && "h-8 px-3",
          size === "md" && "h-9 px-4",
          size === "lg" && "h-11 px-5 text-sm",
          className,
        )}
        {...props}
      />
    );
  },
);
Button.displayName = "Button";
