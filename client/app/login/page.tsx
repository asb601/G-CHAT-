"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { getGoogleLoginUrl } from "@/lib/auth";

function LoginForm() {
  const searchParams = useSearchParams();
  const hasError = !!searchParams.get("error");

  return (
    <>
      {hasError && (
        <div className="mb-4 px-3 py-2 rounded-md bg-danger-bg border border-danger/20 text-sm text-danger">
          Something went wrong. Please try again.
        </div>
      )}

      <a
        href={getGoogleLoginUrl()}
        className="w-full flex items-center justify-center gap-3 h-10 px-4 rounded-md bg-primary text-primary-foreground text-sm font-medium transition-opacity hover:opacity-90"
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 24 24"
          className="w-4 h-4 shrink-0"
          aria-hidden="true"
        >
          <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4" />
          <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853" />
          <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05" />
          <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335" />
        </svg>
        Continue with Google
      </a>
    </>
  );
}

export default function LoginPage() {
  return (
    <div className="min-h-screen bg-background flex items-center justify-center px-4">
      <div className="bg-surface border border-border rounded-lg p-8 w-full max-w-sm">
        <div className="mb-6">
          <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider mb-1">
            Gen-Chatbot
          </p>
          <h1 className="text-xl font-semibold text-foreground">
            Welcome back
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            Sign in with your Google account to continue
          </p>
        </div>

        <Suspense>
          <LoginForm />
        </Suspense>

        <p className="text-xs text-subtle-foreground text-center mt-5">
          By signing in you agree to our terms of service
        </p>
      </div>
    </div>
  );
}
