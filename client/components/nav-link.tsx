"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import type { LucideIcon } from "lucide-react";

interface NavLinkProps {
  href: string;
  icon: LucideIcon;
  label: string;
}

export function NavLink({ href, icon: Icon, label }: NavLinkProps) {
  const pathname = usePathname();
  const isActive = pathname === href || pathname.startsWith(href + "/");

  return (
    <Link
      href={href}
      className={cn(
        "flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors border-l-2",
        isActive
          ? "border-primary bg-surface-raised text-foreground font-medium"
          : "border-transparent text-muted-foreground hover:text-foreground hover:bg-surface-raised"
      )}
    >
      <Icon className="w-4 h-4 shrink-0" />
      <span>{label}</span>
    </Link>
  );
}

export function MobileNavLink({ href, icon: Icon, label }: NavLinkProps) {
  const pathname = usePathname();
  const isActive = pathname === href || pathname.startsWith(href + "/");

  return (
    <Link
      href={href}
      className={cn(
        "flex flex-col items-center justify-center gap-1 flex-1 py-2 text-xs transition-colors",
        isActive ? "text-primary" : "text-muted-foreground"
      )}
    >
      <Icon className="w-5 h-5" />
      {isActive && <span className="text-[10px] font-medium">{label}</span>}
    </Link>
  );
}
