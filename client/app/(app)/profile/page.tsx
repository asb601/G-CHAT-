"use client";

import { useState, useCallback, useEffect } from "react";
import useSWR from "swr";
import { UserCircle, Users, Shield, ShieldOff, Loader2, Database, RefreshCw, CheckCircle2, AlertTriangle, Tag, X, Plus, Sparkles, FolderOpen, FileText, ChevronDown, ChevronRight, Trash2, Clock, UserCheck, UserX, Mail } from "lucide-react";
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
  allowed_domains: string[] | null;
}

interface EligibleFile {
  file_id: string;
  name: string;
  folder_id: string | null;
  current_domain: string | null;
  ai_description: string | null;
  good_for: string[];
  ingest_status: string;
}

interface DeptFile {
  file_id: string;
  name: string;
  ingest_status: string;
}

interface AccessRequestItem {
  id: string;
  user_id: string;
  user_email: string;
  user_name: string | null;
  user_picture: string | null;
  status: string;
  message: string | null;
  requested_at: string;
}

interface AccessRequestItem {
  id: string;
  user_id: string;
  user_email: string;
  user_name: string | null;
  user_picture: string | null;
  status: string;
  message: string | null;
  requested_at: string;
}

/* ── fetchers ────────────────────────────────────────────────────────────── */

const usersFetcher = async (): Promise<UserItem[]> => {
  const res = await apiFetch("/api/users");
  if (!res.ok) return [];
  return res.json();
};

const domainsFetcher = async (): Promise<string[]> => {
  const res = await apiFetch("/api/admin/domains");
  if (!res.ok) return [];
  const data = await res.json();
  return data.domains ?? [];
};

const accessRequestsFetcher = async (): Promise<AccessRequestItem[]> => {
  const res = await apiFetch("/api/access-requests");
  if (!res.ok) return [];
  return res.json();
};

/* ── tabs ────────────────────────────────────────────────────────────────── */

type Tab = "profile" | "users" | "domains" | "parquet";

/* ── page ────────────────────────────────────────────────────────────────── */

export default function ProfilePage() {
  const { user } = useAuth();
  const [tab, setTab] = useState<Tab>("profile");

  if (!user) return null;

  const tabs: { id: Tab; label: string; icon: typeof UserCircle; adminOnly?: boolean }[] = [
    { id: "profile", label: "Profile", icon: UserCircle },
    { id: "users", label: "Users", icon: Users, adminOnly: true },
    { id: "domains", label: "Domains", icon: Tag, adminOnly: true },
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
        {tab === "domains" && user.is_admin && <DomainsTab />}
        {tab === "parquet" && user.is_admin && <ParquetTab />}
      </div>
    </div>
  );
}

/* ── Profile tab ─────────────────────────────────────────────────────────── */

function ProfileTab() {
  const { user } = useAuth();
  const [editingDomains, setEditingDomains] = useState(false);

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

      {/* Departments section (non-admin users) */}
      {!user.is_admin && (
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-xs text-muted-foreground">My Departments</span>
            <button
              onClick={() => setEditingDomains(true)}
              className="text-xs text-primary hover:underline"
            >
              Edit
            </button>
          </div>
          {user.allowed_domains && user.allowed_domains.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {user.allowed_domains.map((d) => (
                <span
                  key={d}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-primary/10 text-primary border border-primary/20"
                >
                  <Tag className="w-3 h-3" />
                  {d}
                </span>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No departments set.</p>
          )}
        </div>
      )}

      {editingDomains && (
        <DomainPickerModal
          current={user.allowed_domains ?? []}
          onClose={() => setEditingDomains(false)}
        />
      )}
    </div>
  );
}

/* ── Domain picker modal (for regular users editing their own) ───────────── */

function DomainPickerModal({
  current,
  onClose,
}: {
  current: string[];
  onClose: () => void;
}) {
  const [domains, setDomains] = useState<string[]>([]);
  const [selected, setSelected] = useState<string[]>(current);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    apiFetch("/api/users/domains")
      .then((r) => r.json())
      .then((d) => setDomains(d.domains ?? []))
      .catch(() => {});
  }, []);

  const toggle = (d: string) =>
    setSelected((prev) =>
      prev.includes(d) ? prev.filter((x) => x !== d) : [...prev, d]
    );

  const handleSave = async () => {
    setSaving(true);
    try {
      await apiFetch("/api/users/me/domains", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ allowed_domains: selected.length > 0 ? selected : null }),
      });
      // Reload page to refresh auth context
      window.location.reload();
    } catch {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="w-full max-w-sm bg-surface border border-border rounded-2xl shadow-xl p-6 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-base font-semibold text-foreground">Edit Departments</h2>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="grid grid-cols-2 gap-2">
          {domains.map((d) => {
            const active = selected.includes(d);
            return (
              <button
                key={d}
                onClick={() => toggle(d)}
                className={cn(
                  "flex items-center gap-2 px-3 py-2 rounded-lg border text-sm text-left transition-all",
                  active
                    ? "border-primary bg-primary/10 text-foreground"
                    : "border-border text-muted-foreground hover:text-foreground"
                )}
              >
                {active && <CheckCircle2 className="w-3.5 h-3.5 text-primary shrink-0" />}
                <span className="truncate">{d}</span>
              </button>
            );
          })}
        </div>
        <div className="flex gap-2 pt-1">
          <button
            onClick={onClose}
            className="flex-1 py-2 rounded-lg border border-border text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex-1 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium disabled:opacity-50 hover:opacity-90 transition-opacity flex items-center justify-center gap-1.5"
          >
            {saving && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
            Save
          </button>
        </div>
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
  const { data: pendingRequests, mutate: mutateRequests } = useSWR(
    "access-requests",
    accessRequestsFetcher,
    { revalidateOnFocus: true, refreshInterval: 30000 },
  );
  const [togglingId, setTogglingId] = useState<string | null>(null);
  const [reviewingId, setReviewingId] = useState<string | null>(null);

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

  const handleReview = useCallback(
    async (requestId: string, action: "approve" | "decline") => {
      setReviewingId(requestId);
      try {
        const res = await apiFetch(`/api/access-requests/${requestId}/${action}`, {
          method: "PATCH",
        });
        if (res.ok) {
          mutateRequests();
          mutate(); // refresh users list too
        }
      } finally {
        setReviewingId(null);
      }
    },
    [mutate, mutateRequests]
  );

  if (!users) {
    return (
      <div className="flex items-center justify-center h-40">
        <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-6">

      {/* ── Pending access requests ── */}
      {pendingRequests && pendingRequests.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <Clock className="w-4 h-4 text-yellow-500" />
            <h3 className="text-sm font-semibold text-foreground">
              Pending Access Requests
              <span className="ml-2 px-1.5 py-0.5 text-[10px] font-medium rounded-full bg-yellow-500/15 text-yellow-600">
                {pendingRequests.length}
              </span>
            </h3>
          </div>

          {pendingRequests.map((req) => {
            const reviewing = reviewingId === req.id;
            return (
              <div
                key={req.id}
                className="flex items-start justify-between gap-4 px-4 py-3 rounded-xl border border-yellow-500/20 bg-yellow-500/5"
              >
                <div className="flex items-start gap-3 min-w-0">
                  {req.user_picture ? (
                    <img
                      src={req.user_picture}
                      alt=""
                      className="w-9 h-9 rounded-full border border-border mt-0.5 shrink-0"
                      referrerPolicy="no-referrer"
                    />
                  ) : (
                    <div className="w-9 h-9 rounded-full bg-surface-raised border border-border flex items-center justify-center shrink-0 mt-0.5">
                      <UserCircle className="w-5 h-5 text-muted-foreground" />
                    </div>
                  )}
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-foreground truncate">
                      {req.user_name || req.user_email}
                    </p>
                    <p className="text-xs text-muted-foreground truncate">{req.user_email}</p>
                    {req.message && (
                      <p className="text-xs text-muted-foreground mt-1 italic">
                        &ldquo;{req.message}&rdquo;
                      </p>
                    )}
                  </div>
                </div>

                <div className="flex items-center gap-2 shrink-0 mt-0.5">
                  <button
                    onClick={() => handleReview(req.id, "approve")}
                    disabled={reviewing}
                    title="Approve"
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-green-500/10 text-green-600 hover:bg-green-500/20 transition-colors disabled:opacity-40"
                  >
                    {reviewing ? (
                      <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    ) : (
                      <UserCheck className="w-3.5 h-3.5" />
                    )}
                    Approve
                  </button>
                  <button
                    onClick={() => handleReview(req.id, "decline")}
                    disabled={reviewing}
                    title="Decline"
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium bg-destructive/10 text-destructive hover:bg-destructive/20 transition-colors disabled:opacity-40"
                  >
                    <UserX className="w-3.5 h-3.5" />
                    Decline
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* ── Existing users ── */}
      <div className="space-y-3">
        {pendingRequests && pendingRequests.length > 0 && (
          <h3 className="text-sm font-semibold text-foreground">Members</h3>
        )}
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
      </div>  {/* end members list */}
    </div>
  );
}

/* ── Parquet status tab (admin only) ─────────────────────────────────────── */

interface MissingFile {
  file_id: string;
  name: string;
  blob_path: string;
  has_analytics: boolean;
  job_status: string | null;
  job_error: string | null;
  last_attempt: string | null;
}

function ParquetTab() {
  const { data, error, isLoading, mutate } = useSWR<{ files: MissingFile[]; count: number }>(
    "/api/admin/missing-parquet",
    (url: string) => apiFetch(url).then((r) => r.json()),
    { refreshInterval: 0 },
  );
  const [retrying, setRetrying] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [pollCount, setPollCount] = useState(0);

  // Auto-refresh after retry: poll at 5s, 12s, 25s, 45s intervals
  useEffect(() => {
    if (pollCount === 0) return;
    const delays = [5000, 12000, 25000, 45000];
    const timers = delays.map((d) => setTimeout(() => mutate(), d));
    return () => timers.forEach(clearTimeout);
  }, [pollCount, mutate]);

  const retryAll = useCallback(async () => {
    setRetrying(true);
    setResult(null);
    try {
      const res = await apiFetch("/api/admin/retry-parquet", { method: "POST" });
      const body = await res.json();
      const total = body.total ?? body.missing_parquet ?? body.count ?? 0;
      setResult((body.message ?? "Started") + ` (${total} files)`);
      setPollCount((n) => n + 1);
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
            <div key={f.file_id} className="p-3 bg-zinc-800/50 rounded-lg border border-zinc-700/50 space-y-1">
              <div className="flex items-center justify-between gap-3">
                <p className="text-sm font-medium text-zinc-200 truncate min-w-0">{f.name}</p>
                {f.job_status === "failed" && (
                  <span className="shrink-0 text-[10px] font-medium px-1.5 py-0.5 rounded bg-red-500/20 text-red-400 border border-red-500/30">failed</span>
                )}
                {f.job_status === "running" && (
                  <span className="shrink-0 flex items-center gap-1 text-[10px] font-medium px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-400 border border-amber-500/30">
                    <Loader2 className="w-2.5 h-2.5 animate-spin" />running
                  </span>
                )}
                {!f.job_status && (
                  <AlertTriangle className="w-4 h-4 text-amber-400 shrink-0" />
                )}
              </div>
              <p className="text-xs text-zinc-500 truncate">{f.blob_path}</p>
              {f.job_error && (
                <p className="text-xs text-red-400 break-all">{f.job_error}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ── Domains tab (admin only) ────────────────────────────────────────────── */

function DomainsTab() {
  const { data: domains, mutate: mutateDomains } = useSWR<string[]>(
    "admin-domains",
    domainsFetcher,
    { revalidateOnFocus: false }
  );
  const { data: users, mutate: mutateUsers } = useSWR<UserItem[]>(
    "users-list",
    usersFetcher,
    { revalidateOnFocus: false }
  );

  // Create domain
  const [newDomain, setNewDomain] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  // Per-user domain assignment
  const [pendingDomains, setPendingDomains] = useState<Record<string, string[]>>({});
  const [savingUser, setSavingUser] = useState<string | null>(null);

  const handleCreateDomain = useCallback(async () => {
    const name = newDomain.trim();
    if (!name) return;
    setCreating(true);
    setCreateError(null);
    try {
      const res = await apiFetch("/api/admin/domains", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        setCreateError(body.detail ?? "Failed to create domain");
      } else {
        setNewDomain("");
        mutateDomains();
      }
    } catch {
      setCreateError("Network error");
    } finally {
      setCreating(false);
    }
  }, [newDomain, mutateDomains]);

  const getUserDomains = (userId: string, fallback: string[] | null) =>
    pendingDomains[userId] ?? fallback ?? [];

  const toggleUserDomain = (userId: string, domain: string, current: string[] | null) => {
    const base = pendingDomains[userId] ?? current ?? [];
    const next = base.includes(domain)
      ? base.filter((d) => d !== domain)
      : [...base, domain];
    setPendingDomains((p) => ({ ...p, [userId]: next }));
  };

  const handleSaveUserDomains = useCallback(
    async (userId: string) => {
      setSavingUser(userId);
      const domains = pendingDomains[userId] ?? [];
      try {
        await apiFetch(`/api/admin/users/${userId}/domains`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ allowed_domains: domains.length > 0 ? domains : null }),
        });
        setPendingDomains((p) => {
          const next = { ...p };
          delete next[userId];
          return next;
        });
        mutateUsers();
      } finally {
        setSavingUser(null);
      }
    },
    [pendingDomains, mutateUsers]
  );

  const isLoading = !domains || !users;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-40">
        <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
      </div>
    );
  }

  return (
    <div className="space-y-8 max-w-2xl">

      {/* ── Create domain ── */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold text-foreground">Create Department</h2>
        <p className="text-xs text-muted-foreground">
          Adding a department creates a top-level folder tagged with that name. Users can then be assigned to it.
        </p>
        <div className="flex gap-2">
          <input
            type="text"
            value={newDomain}
            onChange={(e) => setNewDomain(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleCreateDomain()}
            placeholder="e.g. Finance, HR, Engineering"
            className="flex-1 px-3 py-2 text-sm rounded-lg border border-border bg-surface text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
          />
          <button
            onClick={handleCreateDomain}
            disabled={!newDomain.trim() || creating}
            className="flex items-center gap-1.5 px-4 py-2 text-sm bg-primary text-primary-foreground rounded-lg disabled:opacity-40 hover:opacity-90 transition-opacity"
          >
            {creating ? <Loader2 className="w-4 h-4 animate-spin" /> : <Plus className="w-4 h-4" />}
            Add
          </button>
        </div>
        {createError && <p className="text-xs text-destructive">{createError}</p>}

        {/* Existing domains */}
        {domains.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {domains.map((d) => (
              <span
                key={d}
                className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-primary/10 text-primary border border-primary/20"
              >
                <Tag className="w-3 h-3" />
                {d}
              </span>
            ))}
          </div>
        )}
      </section>

      {/* ── User domain assignments ── */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold text-foreground">User Access</h2>
        <p className="text-xs text-muted-foreground">
          Assign departments to users. Users with no departments are unrestricted.
        </p>

        {domains.length === 0 ? (
          <p className="text-sm text-muted-foreground py-4 text-center">
            No departments yet. Create one above first.
          </p>
        ) : (
          <div className="space-y-3">
            {users.filter((u) => !u.is_admin).map((u) => {
              const current = getUserDomains(u.id, u.allowed_domains);
              const isDirty = !!pendingDomains[u.id];

              return (
                <div
                  key={u.id}
                  className="p-4 rounded-xl border border-border bg-surface space-y-3"
                >
                  {/* User info */}
                  <div className="flex items-center gap-3">
                    {u.picture ? (
                      <img src={u.picture} alt="" className="w-8 h-8 rounded-full border border-border" referrerPolicy="no-referrer" />
                    ) : (
                      <div className="w-8 h-8 rounded-full bg-surface-raised border border-border flex items-center justify-center">
                        <UserCircle className="w-4 h-4 text-muted-foreground" />
                      </div>
                    )}
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-foreground truncate">{u.name || u.email}</p>
                      <p className="text-xs text-muted-foreground truncate">{u.email}</p>
                    </div>
                  </div>

                  {/* Domain pills */}
                  <div className="flex flex-wrap gap-1.5">
                    {domains.map((d) => {
                      const active = current.includes(d);
                      return (
                        <button
                          key={d}
                          onClick={() => toggleUserDomain(u.id, d, u.allowed_domains)}
                          className={cn(
                            "inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium border transition-all",
                            active
                              ? "bg-primary/15 text-primary border-primary/30"
                              : "bg-surface-raised text-muted-foreground border-border hover:border-muted-foreground"
                          )}
                        >
                          {active && <CheckCircle2 className="w-3 h-3" />}
                          {d}
                        </button>
                      );
                    })}
                  </div>

                  {current.length === 0 && (
                    <p className="text-xs text-amber-500">No departments — user sees all data</p>
                  )}

                  {/* Save button (only if changed) */}
                  {isDirty && (
                    <button
                      onClick={() => handleSaveUserDomains(u.id)}
                      disabled={savingUser === u.id}
                      className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-primary text-primary-foreground rounded-lg disabled:opacity-50 hover:opacity-90 transition-opacity"
                    >
                      {savingUser === u.id ? <Loader2 className="w-3 h-3 animate-spin" /> : <CheckCircle2 className="w-3 h-3" />}
                      Save changes
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* ── Department file assignment ── */}
      <section className="space-y-3">
        <h2 className="text-sm font-semibold text-foreground">File Assignment by Department</h2>
        <p className="text-xs text-muted-foreground">
          Select a department to see its files. Use AI Sort to auto-assign based on file content, or add files manually.
        </p>

        {domains.length === 0 ? (
          <p className="text-sm text-muted-foreground py-4 text-center">
            No departments yet. Create one above first.
          </p>
        ) : (
          <div className="space-y-2">
            {domains.map((domain) => (
              <DepartmentFilePanel key={domain} domain={domain} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

/* ── Department file panel ────────────────────────────────────────────────── */

function DepartmentFilePanel({ domain }: { domain: string }) {
  const [expanded, setExpanded] = useState(false);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState<{ assigned_count: number; assigned_files: string[] } | null>(null);
  const [showPicker, setShowPicker] = useState(false);

  const { data, mutate, isLoading } = useSWR<{ files: DeptFile[]; count: number }>(
    expanded ? `/api/admin/departments/${encodeURIComponent(domain)}/files` : null,
    (url: string) => apiFetch(url).then((r) => r.json()),
  );

  const handleAiSort = async () => {
    setAiLoading(true);
    setAiResult(null);
    try {
      const res = await apiFetch(`/api/admin/departments/${encodeURIComponent(domain)}/ai-assign`, {
        method: "POST",
      });
      const body = await res.json();
      setAiResult(body);
      mutate();
    } catch {
      setAiResult({ assigned_count: -1, assigned_files: [] });
    } finally {
      setAiLoading(false);
    }
  };

  const handleRemoveFile = async (fileId: string) => {
    await apiFetch(`/api/admin/departments/${encodeURIComponent(domain)}/files/${fileId}`, {
      method: "DELETE",
    });
    mutate();
  };

  return (
    <div className="rounded-xl border border-border bg-surface overflow-hidden">
      {/* Header row */}
      <button
        onClick={() => setExpanded((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-surface-raised transition-colors"
      >
        <div className="flex items-center gap-2">
          <FolderOpen className="w-4 h-4 text-primary" />
          <span className="text-sm font-medium text-foreground">{domain}</span>
          {data && (
            <span className="text-xs text-muted-foreground">({data.count} file{data.count !== 1 ? "s" : ""})</span>
          )}
        </div>
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="w-4 h-4 text-muted-foreground" />
        )}
      </button>

      {expanded && (
        <div className="px-4 pb-4 space-y-3 border-t border-border">
          {/* Action buttons */}
          <div className="flex gap-2 pt-3">
            <button
              onClick={handleAiSort}
              disabled={aiLoading}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs bg-primary text-primary-foreground rounded-lg disabled:opacity-50 hover:opacity-90 transition-opacity"
            >
              {aiLoading ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <Sparkles className="w-3.5 h-3.5" />
              )}
              AI Sort
            </button>
            <button
              onClick={() => setShowPicker(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs border border-border text-muted-foreground rounded-lg hover:text-foreground hover:border-muted-foreground transition-colors"
            >
              <Plus className="w-3.5 h-3.5" />
              Add Files
            </button>
          </div>

          {/* AI result banner */}
          {aiResult && (
            <div className={cn(
              "flex items-start gap-2 p-3 rounded-lg text-xs border",
              aiResult.assigned_count === -1
                ? "bg-destructive/10 border-destructive/20 text-destructive"
                : "bg-primary/10 border-primary/20 text-primary"
            )}>
              {aiResult.assigned_count === -1 ? (
                <AlertTriangle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
              ) : (
                <Sparkles className="w-3.5 h-3.5 mt-0.5 shrink-0" />
              )}
              <div>
                {aiResult.assigned_count === -1
                  ? "AI sort failed — please try again."
                  : aiResult.assigned_count === 0
                  ? "No matching files found for this department."
                  : (
                    <>
                      <span className="font-medium">AI assigned {aiResult.assigned_count} file{aiResult.assigned_count !== 1 ? "s" : ""}:</span>{" "}
                      {aiResult.assigned_files.join(", ")}
                    </>
                  )}
              </div>
            </div>
          )}

          {/* File list */}
          {isLoading ? (
            <div className="flex justify-center py-4">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
            </div>
          ) : data && data.files.length === 0 ? (
            <p className="text-xs text-muted-foreground py-2">No files assigned yet.</p>
          ) : (
            <div className="space-y-1.5">
              {(data?.files ?? []).map((f) => (
                <div
                  key={f.file_id}
                  className="flex items-center justify-between gap-2 px-3 py-2 rounded-lg bg-surface-raised"
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <FileText className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                    <span className="text-xs text-foreground truncate">{f.name}</span>
                    {f.ingest_status !== "ingested" && (
                      <span className="text-[10px] text-amber-500 shrink-0">{f.ingest_status}</span>
                    )}
                  </div>
                  <button
                    onClick={() => handleRemoveFile(f.file_id)}
                    className="p-1 text-muted-foreground hover:text-destructive transition-colors shrink-0"
                    title="Remove from department"
                  >
                    <Trash2 className="w-3 h-3" />
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {showPicker && (
        <FilePickerModal
          domain={domain}
          onClose={() => setShowPicker(false)}
          onAssigned={() => { mutate(); setShowPicker(false); }}
        />
      )}
    </div>
  );
}

/* ── File picker modal ────────────────────────────────────────────────────── */

function FilePickerModal({
  domain,
  onClose,
  onAssigned,
}: {
  domain: string;
  onClose: () => void;
  onAssigned: () => void;
}) {
  const [files, setFiles] = useState<EligibleFile[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [search, setSearch] = useState("");

  useEffect(() => {
    apiFetch("/api/admin/files/eligible")
      .then((r) => r.json())
      .then((d) => {
        // Show only files not already in this domain, sorted by name
        const eligible = (d.files as EligibleFile[]).filter(
          (f) => f.current_domain !== domain
        );
        setFiles(eligible);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [domain]);

  const toggle = (fileId: string) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(fileId)) next.delete(fileId);
      else next.add(fileId);
      return next;
    });

  const handleSave = async () => {
    if (selected.size === 0) return;
    setSaving(true);
    try {
      await apiFetch(`/api/admin/departments/${encodeURIComponent(domain)}/assign`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_ids: Array.from(selected) }),
      });
      onAssigned();
    } catch {
      setSaving(false);
    }
  };

  const filtered = files.filter((f) =>
    f.name.toLowerCase().includes(search.toLowerCase()) ||
    (f.ai_description ?? "").toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="w-full max-w-lg bg-surface border border-border rounded-2xl shadow-xl flex flex-col max-h-[80vh]">
        <div className="flex items-center justify-between px-5 py-4 border-b border-border">
          <div>
            <h2 className="text-sm font-semibold text-foreground">Add Files to "{domain}"</h2>
            <p className="text-xs text-muted-foreground mt-0.5">
              {selected.size} file{selected.size !== 1 ? "s" : ""} selected
            </p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Search */}
        <div className="px-5 py-3 border-b border-border">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search by name or description..."
            className="w-full px-3 py-1.5 text-sm rounded-lg border border-border bg-surface text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
          />
        </div>

        {/* File list */}
        <div className="flex-1 overflow-y-auto px-5 py-3 space-y-1.5">
          {loading ? (
            <div className="flex justify-center py-8">
              <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
            </div>
          ) : filtered.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">
              {search ? "No files match your search." : "All files are already assigned to this department."}
            </p>
          ) : (
            filtered.map((f) => {
              const active = selected.has(f.file_id);
              return (
                <button
                  key={f.file_id}
                  onClick={() => toggle(f.file_id)}
                  className={cn(
                    "w-full flex items-start gap-3 px-3 py-2.5 rounded-lg border text-left transition-all",
                    active
                      ? "border-primary bg-primary/10"
                      : "border-border hover:border-muted-foreground"
                  )}
                >
                  <div className={cn(
                    "w-4 h-4 mt-0.5 rounded border-2 shrink-0 flex items-center justify-center",
                    active ? "bg-primary border-primary" : "border-muted-foreground"
                  )}>
                    {active && <CheckCircle2 className="w-3 h-3 text-primary-foreground" />}
                  </div>
                  <div className="min-w-0">
                    <p className="text-xs font-medium text-foreground truncate">{f.name}</p>
                    {f.current_domain && (
                      <p className="text-[10px] text-amber-500">Currently in: {f.current_domain}</p>
                    )}
                    {f.ai_description && (
                      <p className="text-[10px] text-muted-foreground line-clamp-1 mt-0.5">{f.ai_description}</p>
                    )}
                    {f.good_for && f.good_for.length > 0 && (
                      <p className="text-[10px] text-muted-foreground mt-0.5 line-clamp-1">
                        Good for: {f.good_for.slice(0, 3).join(", ")}
                      </p>
                    )}
                  </div>
                </button>
              );
            })
          )}
        </div>

        {/* Footer */}
        <div className="flex gap-2 px-5 py-4 border-t border-border">
          <button
            onClick={onClose}
            className="flex-1 py-2 rounded-lg border border-border text-sm text-muted-foreground hover:text-foreground transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={selected.size === 0 || saving}
            className="flex-1 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium disabled:opacity-40 hover:opacity-90 transition-opacity flex items-center justify-center gap-1.5"
          >
            {saving && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
            Assign {selected.size > 0 ? `(${selected.size})` : ""}
          </button>
        </div>
      </div>
    </div>
  );
}
