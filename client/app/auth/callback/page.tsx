"use client";

import { Suspense, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { setToken, fetchMe } from "@/lib/auth";

function CallbackHandler() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const token = searchParams.get("token");
    if (!token) {
      router.replace("/login?error=no_token");
      return;
    }
    setToken(token);
    document.cookie = `token=${token}; path=/; max-age=${60 * 60 * 24 * 7}; SameSite=Lax`;

    // Fetch user to decide where to send them
    fetchMe().then((user) => {
      if (user && !user.is_admin && !user.allowed_domains) {
        // First-time user: no domain selected yet — send to onboarding
        router.replace("/onboarding");
      } else {
        router.replace("/chat");
      }
    }).catch(() => {
      router.replace("/chat");
    });
  }, [searchParams, router]);

  return null;
}

export default function AuthCallbackPage() {
  return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <Suspense fallback={<p className="text-muted-foreground text-sm">Signing you in…</p>}>
        <CallbackHandler />
      </Suspense>
      <p className="text-muted-foreground text-sm">Signing you in…</p>
    </div>
  );
}
