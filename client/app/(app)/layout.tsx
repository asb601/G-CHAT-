"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { MessageSquare, FolderOpen, LogOut, PanelLeftClose, PanelLeft, Database, UserCircle } from "lucide-react";
import { NavLink, MobileNavLink } from "@/components/nav-link";
import { AuthProvider, useAuth } from "@/components/auth-provider";

interface NavItem {
  href: string;
  icon: typeof MessageSquare;
  label: string;
}

function AppShellInner({ children }: { children: React.ReactNode }) {
  const { user, loading, logout } = useAuth();
  const router = useRouter();
  const [sidebarOpen, setSidebarOpen] = useState(true);

  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center bg-background">
        <p className="text-sm text-muted-foreground">Loading…</p>
      </div>
    );
  }

  if (!user) {
    router.replace("/login");
    return null;
  }

  const navItems: NavItem[] = [
    { href: "/chat", icon: MessageSquare, label: "Chat" },
    { href: "/folders", icon: FolderOpen, label: "Folders" },
    ...(user.is_admin
      ? [
          { href: "/admin/containers", icon: Database, label: "Containers" },
        ]
      : []),
    { href: "/profile", icon: UserCircle, label: "Profile" },
  ];

  return (
    <div className="flex h-screen bg-background overflow-hidden">
      {/* Sidebar toggle when collapsed */}
      {!sidebarOpen && (
        <button
          onClick={() => setSidebarOpen(true)}
          className="hidden md:flex fixed top-4 left-4 z-40 items-center justify-center w-8 h-8 rounded-md bg-surface border border-border text-muted-foreground hover:text-foreground transition-colors"
        >
          <PanelLeft className="w-4 h-4" />
        </button>
      )}

      {/* Desktop sidebar */}
      <aside
        className={`hidden md:flex flex-col shrink-0 bg-surface border-r border-border h-screen sticky top-0 transition-[width] duration-200 ${sidebarOpen ? "w-[220px]" : "w-0 overflow-hidden border-r-0"}`}
      >
        <div className="px-4 py-5 border-b border-border flex items-start justify-between">
          <div className="min-w-0">
            <p className="font-semibold text-foreground text-base">Gen-Chatbot</p>
            <p className="text-xs text-muted-foreground mt-0.5 truncate">
              {user.email}
            </p>
            {user.is_admin && (
              <span className="inline-block mt-1 px-1.5 py-0.5 text-[10px] font-medium rounded bg-primary/15 text-primary">
                Admin
              </span>
            )}
          </div>
          <button
            onClick={() => setSidebarOpen(false)}
            className="mt-0.5 p-1 rounded text-muted-foreground hover:text-foreground transition-colors"
          >
            <PanelLeftClose className="w-4 h-4" />
          </button>
        </div>

        <nav className="flex-1 px-3 py-4 flex flex-col gap-1">
          {navItems.map((item) => (
            <NavLink key={item.href} {...item} />
          ))}
        </nav>

        <div className="px-3 py-4 border-t border-border">
          <button
            onClick={logout}
            className="w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm text-muted-foreground hover:text-foreground hover:bg-surface-raised transition-colors"
          >
            <LogOut className="w-4 h-4" />
            Sign out
          </button>
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <main className="flex-1 overflow-y-auto pb-16 md:pb-0">{children}</main>
      </div>

      {/* Mobile bottom nav */}
      <nav className="md:hidden fixed bottom-0 left-0 right-0 bg-surface border-t border-border flex items-stretch z-50">
        {navItems.map((item) => (
          <MobileNavLink key={item.href} {...item} />
        ))}
      </nav>
    </div>
  );
}

export default function AppShellLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <AuthProvider>
      <AppShellInner>{children}</AppShellInner>
    </AuthProvider>
  );
}
