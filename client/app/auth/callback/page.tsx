"use client";

import { Suspense, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { setToken } from "@/lib/auth";

function CallbackHandler() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const token = searchParams.get("token");
    if (token) {
      setToken(token);
      // Also set a cookie so middleware can read it (localStorage isn't available in middleware)
      document.cookie = `token=${token}; path=/; max-age=${60 * 60 * 24 * 7}; SameSite=Lax`;
      router.replace("/chat");
    } else {
      router.replace("/login?error=no_token");
    }
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
