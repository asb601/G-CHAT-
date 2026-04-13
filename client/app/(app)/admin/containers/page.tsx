"use client";

import { useState, useCallback } from "react";
import useSWR from "swr";
import {
  Database,
  Plus,
  RefreshCw,
  Trash2,
  Cloud,
  Loader2,
  X,
} from "lucide-react";
import { apiFetch } from "@/lib/auth";
import { cn } from "@/lib/utils";

/* ── types ───────────────────────────────────────────────────────────────── */

interface Container {
  id: string;
  name: string;
  container_name: string;
  last_synced_at: string | null;
  file_count: number;
  created_at: string;
}

/* ── fetcher ─────────────────────────────────────────────────────────────── */

const containersFetcher = async (): Promise<Container[]> => {
  const res = await apiFetch("/api/containers");
  if (!res.ok) return [];
  return res.json();
};

/* ── relative time helper ────────────────────────────────────────────────── */

function timeAgo(dateStr: string): string {
  const now = Date.now();
  const then = new Date(dateStr).getTime();
  const diff = Math.max(0, now - then);
  const seconds = Math.floor(diff / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/* ── main page ───────────────────────────────────────────────────────────── */

export default function ContainersPage() {
  const { data: containers, mutate } = useSWR("containers", containersFetcher, {
    revalidateOnFocus: false,
  });
  const [showForm, setShowForm] = useState(false);
  const [syncingIds, setSyncingIds] = useState<Set<string>>(new Set());

  /* ── sync handler ── */
  const handleSync = useCallback(
    async (id: string) => {
      setSyncingIds((prev) => new Set(prev).add(id));
      try {
        await apiFetch(`/api/containers/${id}/sync`, { method: "POST" });
        // Poll until last_synced_at changes
        const before = containers?.find((c) => c.id === id)?.last_synced_at;
        const poll = setInterval(async () => {
          const res = await apiFetch(`/api/containers/${id}`);
          if (res.ok) {
            const updated: Container = await res.json();
            if (updated.last_synced_at !== before) {
              clearInterval(poll);
              setSyncingIds((prev) => {
                const next = new Set(prev);
                next.delete(id);
                return next;
              });
              mutate();
            }
          }
        }, 3000);
        // Safety timeout: stop after 2 minutes
        setTimeout(() => {
          clearInterval(poll);
          setSyncingIds((prev) => {
            const next = new Set(prev);
            next.delete(id);
            return next;
          });
          mutate();
        }, 120_000);
      } catch {
        setSyncingIds((prev) => {
          const next = new Set(prev);
          next.delete(id);
          return next;
        });
      }
    },
    [containers, mutate]
  );

  /* ── delete handler ── */
  const handleDelete = useCallback(
    async (id: string) => {
      try {
        await apiFetch(`/api/containers/${id}`, { method: "DELETE" });
        mutate();
      } catch {
        mutate();
      }
    },
    [mutate]
  );

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="shrink-0 px-6 py-4 border-b border-border flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Database className="w-5 h-5 text-foreground" />
          <h1 className="text-lg font-semibold text-foreground">Azure Containers</h1>
        </div>
        <button
          onClick={() => setShowForm(true)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm bg-primary text-background hover:bg-primary/90 transition-colors"
        >
          <Plus className="w-4 h-4" />
          Add Container
        </button>
      </div>

      {/* Container list */}
      <div className="flex-1 overflow-y-auto p-6">
        {!containers ? (
          <div className="flex items-center justify-center h-40">
            <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
          </div>
        ) : containers.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-60 gap-3">
            <Cloud className="w-10 h-10 text-muted-foreground" strokeWidth={1} />
            <p className="text-sm text-foreground">No containers configured</p>
            <p className="text-xs text-muted-foreground">
              Add an Azure Blob container to sync files
            </p>
          </div>
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {containers.map((c) => {
              const syncing = syncingIds.has(c.id);
              return (
                <div
                  key={c.id}
                  className="rounded-xl border border-border bg-surface p-5 flex flex-col gap-3"
                >
                  <div className="flex items-start justify-between">
                    <div className="min-w-0">
                      <p className="text-sm font-medium text-foreground truncate">
                        {c.name}
                      </p>
                      <p className="text-xs text-muted-foreground mt-0.5 truncate">
                        {c.container_name}
                      </p>
                    </div>
                    <button
                      onClick={() => handleDelete(c.id)}
                      className="p-1.5 rounded text-muted-foreground hover:text-foreground transition-colors"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>

                  <div className="flex items-center gap-4 text-xs text-muted-foreground">
                    <span>{c.file_count} file{c.file_count !== 1 && "s"}</span>
                    <span>
                      {c.last_synced_at
                        ? `Synced ${timeAgo(c.last_synced_at)}`
                        : "Never synced"}
                    </span>
                  </div>

                  <button
                    onClick={() => handleSync(c.id)}
                    disabled={syncing}
                    className={cn(
                      "mt-auto flex items-center justify-center gap-2 px-3 py-1.5 rounded-md text-xs border transition-colors",
                      syncing
                        ? "border-border text-muted-foreground cursor-not-allowed"
                        : "border-border text-foreground hover:bg-surface-raised"
                    )}
                  >
                    <RefreshCw
                      className={cn("w-3.5 h-3.5", syncing && "animate-spin")}
                    />
                    {syncing ? "Syncing…" : "Sync Now"}
                  </button>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ── Add container modal ── */}
      {showForm && (
        <AddContainerModal
          onClose={() => setShowForm(false)}
          onSuccess={() => {
            setShowForm(false);
            mutate();
          }}
        />
      )}
    </div>
  );
}

/* ── Add container form modal ────────────────────────────────────────────── */

function AddContainerModal({
  onClose,
  onSuccess,
}: {
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [name, setName] = useState("");
  const [containerName, setContainerName] = useState("");
  const [connectionString, setConnectionString] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const res = await apiFetch("/api/containers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          container_name: containerName,
          connection_string: connectionString,
        }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({ detail: "Request failed" }));
        setError(body.detail || "Request failed");
        setLoading(false);
        return;
      }
      onSuccess();
    } catch {
      setError("Network error");
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <form
        onSubmit={handleSubmit}
        className="relative z-10 w-full max-w-md bg-surface border border-border rounded-xl p-6 flex flex-col gap-4"
      >
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-foreground">
            Add Azure Container
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="p-1 rounded text-muted-foreground hover:text-foreground"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        <label className="flex flex-col gap-1">
          <span className="text-xs text-muted-foreground">Display Name</span>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            placeholder="e.g. Client A Data"
            className="px-3 py-2 rounded-md border border-border bg-background text-sm text-foreground outline-none focus:border-primary"
          />
        </label>

        <label className="flex flex-col gap-1">
          <span className="text-xs text-muted-foreground">Container Name</span>
          <input
            value={containerName}
            onChange={(e) => setContainerName(e.target.value)}
            required
            placeholder="e.g. uploads"
            className="px-3 py-2 rounded-md border border-border bg-background text-sm text-foreground outline-none focus:border-primary"
          />
        </label>

        <label className="flex flex-col gap-1">
          <span className="text-xs text-muted-foreground">
            Azure Connection String
          </span>
          <textarea
            value={connectionString}
            onChange={(e) => setConnectionString(e.target.value)}
            required
            rows={3}
            placeholder="DefaultEndpointsProtocol=https;AccountName=..."
            className="px-3 py-2 rounded-md border border-border bg-background text-sm text-foreground outline-none focus:border-primary resize-none font-mono text-xs"
          />
        </label>

        {error && (
          <p className="text-xs text-foreground bg-surface-raised rounded px-3 py-2 border border-border">
            {error}
          </p>
        )}

        <button
          type="submit"
          disabled={loading}
          className={cn(
            "flex items-center justify-center gap-2 px-4 py-2 rounded-md text-sm transition-colors",
            loading
              ? "bg-primary/50 text-background cursor-not-allowed"
              : "bg-primary text-background hover:bg-primary/90"
          )}
        >
          {loading && <Loader2 className="w-4 h-4 animate-spin" />}
          {loading ? "Adding…" : "Add Container"}
        </button>
      </form>
    </div>
  );
}
