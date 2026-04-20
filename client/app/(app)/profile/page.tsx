"use client";

import { useState, useCallback } from "react";
import useSWR from "swr";
import { UserCircle, Users, Shield, ShieldOff, Loader2, Database, RefreshCw, CheckCircle2, AlertTriangle } from "lucide-react";
import { useAuth } from "@/components/auth-provider";
import { apiFetch } from "@/lib/auth";
import { cn } from "@/lib/utils";

/* ── types ───────────────────────────────────────────────────────────────── */

interface UserItem {
  id: string;
  email: string;
  name: string | null;
  picture: string | null;
  is_admin: boolean;
  created_at: string;
  file_count: number;
}

/* ── fetcher ─────────────────────────────────────────────────────────────── */

const usersFetcher = async (): Promise<UserItem[]> => {
  const res = await apiFetch("/api/users");
  if (!res.ok) return [];
  return res.json();
};

/* ── tabs ────────────────────────────────────────────────────────────────── */

type Tab = "profile" | "users" | "parquet";

/* ── page ────────────────────────────────────────────────────────────────── */

export default function ProfilePage() {
  const { user } = useAuth();
  const [tab, setTab] = useState<Tab>("profile");

  if (!user) return null;

  const tabs: { id: Tab; label: string; icon: typeof UserCircle; adminOnly?: boolean }[] = [
    { id: "profile", label: "Profile", icon: UserCircle },
    { id: "users", label: "Users", icon: Users, adminOnly: true },
    { id: "parquet", label: "Parquet Status", icon: Database, adminOnly: true },
  ];

  const visibleTabs = tabs.filter((t) => !t.adminOnly || user.is_admin);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="shrink-0 px-6 py-4 border-b border-border">
        <div className="flex items-center gap-3">
          <UserCircle className="w-5 h-5 text-foreground" />
          <h1 className="text-lg font-semibold text-foreground">Profile</h1>
        </div>

        {/* Tab bar */}
        <div className="flex gap-4 mt-4">
          {visibleTabs.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={cn(
                "flex items-center gap-2 pb-2 text-sm border-b-2 transition-colors",
                tab === t.id
                  ? "border-primary text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              )}
            >
              <t.icon className="w-4 h-4" />
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6">
        {tab === "profile" && <ProfileTab />}
        {tab === "users" && user.is_admin && <UsersTab currentUserId={user.id} />}
        {tab === "parquet" && user.is_admin && <ParquetTab />}
      </div>
    </div>
  );
}

/* ── Profile tab ─────────────────────────────────────────────────────────── */

function ProfileTab() {
  const { user } = useAuth();
  if (!user) return null;

  return (
    <div className="max-w-md space-y-6">
      <div className="flex items-center gap-4">
        {user.picture ? (
          <img
            src={user.picture}
            alt=""
            className="w-16 h-16 rounded-full border border-border"
            referrerPolicy="no-referrer"
          />
        ) : (
          <div className="w-16 h-16 rounded-full bg-surface-raised border border-border flex items-center justify-center">
            <UserCircle className="w-8 h-8 text-muted-foreground" />
          </div>
        )}
        <div>
          <p className="text-base font-medium text-foreground">{user.name || "—"}</p>
          <p className="text-sm text-muted-foreground">{user.email}</p>
          {user.is_admin && (
            <span className="inline-block mt-1 px-2 py-0.5 text-[10px] font-medium rounded bg-primary/15 text-primary">
              Admin
            </span>
          )}
        </div>
      </div>

      <div className="space-y-3">
        <Field label="Name" value={user.name || "—"} />
        <Field label="Email" value={user.email} />
        <Field label="Role" value={user.is_admin ? "Admin" : "Member"} />
      </div>
    </div>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-sm text-foreground px-3 py-2 rounded-md bg-surface border border-border">
        {value}
      </span>
    </div>
  );
}

/* ── Users tab (admin only) ──────────────────────────────────────────────── */

function UsersTab({ currentUserId }: { currentUserId: string }) {
  const { data: users, mutate } = useSWR("users-list", usersFetcher, {
    revalidateOnFocus: false,
  });
  const [togglingId, setTogglingId] = useState<string | null>(null);

  const handleToggleAdmin = useCallback(
    async (userId: string) => {
      setTogglingId(userId);
      try {
        const res = await apiFetch(`/api/users/${userId}/toggle-admin`, {
          method: "PATCH",
        });
        if (res.ok) {
          mutate();
        }
      } finally {
        setTogglingId(null);
      }
    },
    [mutate]
  );

  if (!users) {
    return (
      <div className="flex items-center justify-center h-40">
        <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {users.map((u) => {
        const isCurrent = u.id === currentUserId;
        const toggling = togglingId === u.id;

        return (
          <div
            key={u.id}
            className="flex items-center justify-between gap-4 px-4 py-3 rounded-xl border border-border bg-surface"
          >
            <div className="flex items-center gap-3 min-w-0">
              {u.picture ? (
                <img
                  src={u.picture}
                  alt=""
                  className="w-9 h-9 rounded-full border border-border"
                  referrerPolicy="no-referrer"
                />
              ) : (
                <div className="w-9 h-9 rounded-full bg-surface-raised border border-border flex items-center justify-center">
                  <UserCircle className="w-5 h-5 text-muted-foreground" />
                </div>
              )}
              <div className="min-w-0">
                <p className="text-sm font-medium text-foreground truncate">
                  {u.name || u.email}
                </p>
                <p className="text-xs text-muted-foreground truncate">{u.email}</p>
              </div>
            </div>

            <div className="flex items-center gap-3 shrink-0">
              <span className="text-xs text-muted-foreground">
                {u.file_count} file{u.file_count !== 1 && "s"}
              </span>

              {u.is_admin ? (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 text-[10px] font-medium rounded bg-primary/15 text-primary">
                  <Shield className="w-3 h-3" />
                  Admin
                </span>
              ) : (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 text-[10px] font-medium rounded bg-surface-raised text-muted-foreground">
                  Member
                </span>
              )}

              {!isCurrent && (
                <button
                  onClick={() => handleToggleAdmin(u.id)}
                  disabled={toggling}
                  title={u.is_admin ? "Remove admin" : "Make admin"}
                  className={cn(
                    "p-1.5 rounded transition-colors",
                    toggling
                      ? "text-muted-foreground cursor-not-allowed"
                      : "text-muted-foreground hover:text-foreground hover:bg-surface-raised"
                  )}
                >
                  {toggling ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : u.is_admin ? (
                    <ShieldOff className="w-4 h-4" />
                  ) : (
                    <Shield className="w-4 h-4" />
                  )}
                </button>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ── Parquet status tab (admin only) ─────────────────────────────────────── */

interface MissingFile {
  file_id: string;
  name: string;
  blob_path: string;
  has_analytics: boolean;
}

function ParquetTab() {
  const { data, error, isLoading, mutate } = useSWR<{ files: MissingFile[]; count: number }>(
    "/api/admin/missing-parquet",
    (url: string) => apiFetch(url).then((r) => r.json()),
  );
  const [retrying, setRetrying] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const retryAll = useCallback(async () => {
    setRetrying(true);
    setResult(null);
    try {
      const res = await apiFetch("/api/admin/retry-parquet", { method: "POST" });
      const body = await res.json();
      setResult(body.message + ` (${body.count} files)`);
      setTimeout(() => mutate(), 5000);
    } catch {
      setResult("Failed to start retry");
    } finally {
      setRetrying(false);
    }
  }, [mutate]);

  if (isLoading) return <div className="flex justify-center py-12"><Loader2 className="w-6 h-6 animate-spin text-zinc-400" /></div>;
  if (error) return <p className="text-red-400 p-4">Failed to load parquet status.</p>;

  const files = data?.files ?? [];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-zinc-100">Missing Parquet Conversions</h3>
          <p className="text-sm text-zinc-400">{files.length} file(s) without parquet</p>
        </div>
        {files.length > 0 && (
          <button
            onClick={retryAll}
            disabled={retrying}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
          >
            {retrying ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
            Retry All
          </button>
        )}
      </div>

      {result && (
        <div className="flex items-center gap-2 p-3 bg-blue-500/10 border border-blue-500/20 rounded-lg text-sm text-blue-300">
          <CheckCircle2 className="w-4 h-4 shrink-0" />
          {result}
        </div>
      )}

      {files.length === 0 ? (
        <div className="flex flex-col items-center py-12 text-zinc-400">
          <CheckCircle2 className="w-8 h-8 mb-2 text-green-400" />
          <p>All files have parquet conversions.</p>
        </div>
      ) : (
        <div className="space-y-2">
          {files.map((f) => (
            <div key={f.file_id} className="flex items-center justify-between p-3 bg-zinc-800/50 rounded-lg border border-zinc-700/50">
              <div className="min-w-0">
                <p className="text-sm font-medium text-zinc-200 truncate">{f.name}</p>
                <p className="text-xs text-zinc-500 truncate">{f.blob_path}</p>
              </div>
              <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0 ml-3" />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
