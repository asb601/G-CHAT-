"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { Building2, CheckCircle2, Loader2 } from "lucide-react";
import { apiFetch } from "@/lib/auth";
import { useAuth } from "@/components/auth-provider";
import { cn } from "@/lib/utils";

export default function OnboardingPage() {
  const { user, loading } = useAuth();
  const router = useRouter();

  const [domains, setDomains] = useState<string[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [fetchingDomains, setFetchingDomains] = useState(true);

  // If admin or already has domains, skip onboarding
  useEffect(() => {
    if (!loading && user) {
      if (user.is_admin || user.allowed_domains) {
        router.replace("/chat");
      }
    }
  }, [loading, user, router]);

  // Load available domains
  useEffect(() => {
    apiFetch("/api/users/domains")
      .then((r) => r.json())
      .then((data) => setDomains(data.domains ?? []))
      .catch(() => setDomains([]))
      .finally(() => setFetchingDomains(false));
  }, []);

  const toggle = (d: string) =>
    setSelected((prev) =>
      prev.includes(d) ? prev.filter((x) => x !== d) : [...prev, d]
    );

  const handleSave = async () => {
    if (selected.length === 0) return;
    setSaving(true);
    try {
      await apiFetch("/api/users/me/domains", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ allowed_domains: selected }),
      });
      router.replace("/chat");
    } catch {
      setSaving(false);
    }
  };

  if (loading || (user && (user.is_admin || user.allowed_domains))) {
    return (
      <div className="flex h-screen items-center justify-center bg-background">
        <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-background items-center justify-center p-6">
      <div className="w-full max-w-md space-y-8">
        {/* Header */}
        <div className="text-center space-y-2">
          <div className="flex justify-center">
            <div className="w-14 h-14 rounded-2xl bg-primary/10 flex items-center justify-center">
              <Building2 className="w-7 h-7 text-primary" />
            </div>
          </div>
          <h1 className="text-2xl font-semibold text-foreground">
            Welcome{user?.name ? `, ${user.name.split(" ")[0]}` : ""}
          </h1>
          <p className="text-sm text-muted-foreground">
            Select your department(s) so we can show you the most relevant data.
          </p>
        </div>

        {/* Domain picker */}
        <div className="space-y-3">
          {fetchingDomains ? (
            <div className="flex justify-center py-8">
              <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
            </div>
          ) : domains.length === 0 ? (
            <p className="text-center text-sm text-muted-foreground py-8">
              No departments configured yet.
              <br />
              <span className="text-xs">Ask your admin to set up departments.</span>
            </p>
          ) : (
            <div className="grid grid-cols-2 gap-2">
              {domains.map((d) => {
                const active = selected.includes(d);
                return (
                  <button
                    key={d}
                    onClick={() => toggle(d)}
                    className={cn(
                      "flex items-center gap-2 px-4 py-3 rounded-xl border text-sm font-medium transition-all text-left",
                      active
                        ? "border-primary bg-primary/10 text-foreground"
                        : "border-border bg-surface text-muted-foreground hover:text-foreground hover:border-muted-foreground"
                    )}
                  >
                    {active && <CheckCircle2 className="w-4 h-4 text-primary shrink-0" />}
                    <span className="truncate">{d}</span>
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* Actions */}
        <div className="space-y-3">
          <button
            onClick={handleSave}
            disabled={selected.length === 0 || saving || domains.length === 0}
            className="w-full py-2.5 rounded-xl bg-primary text-primary-foreground text-sm font-medium disabled:opacity-40 hover:opacity-90 transition-opacity flex items-center justify-center gap-2"
          >
            {saving && <Loader2 className="w-4 h-4 animate-spin" />}
            {saving ? "Saving…" : `Continue with ${selected.length > 0 ? selected.join(", ") : "selected"}`}
          </button>
          <p className="text-center text-xs text-muted-foreground">
            You can change this later from your profile.
          </p>
        </div>
      </div>
    </div>
  );
}
