const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export interface User {
  id: string;
  email: string;
  name: string | null;
  picture: string | null;
  is_admin: boolean;
}

/** Retrieve stored JWT token */
export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem("token");
}

/** Store JWT token */
export function setToken(token: string): void {
  localStorage.setItem("token", token);
}

/** Clear stored token */
export function clearToken(): void {
  localStorage.removeItem("token");
}

/** Authenticated fetch wrapper */
export async function apiFetch(path: string, init?: RequestInit): Promise<Response> {
  const token = getToken();
  const headers = new Headers(init?.headers);
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  return fetch(`${API_URL}${path}`, { ...init, headers });
}

/** Fetch current user from the backend */
export async function fetchMe(): Promise<User | null> {
  const token = getToken();
  if (!token) return null;
  try {
    const res = await apiFetch("/api/auth/me");
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

/** Get the Google OAuth login URL (points at FastAPI) */
export function getGoogleLoginUrl(): string {
  return `${API_URL}/api/auth/google/login`;
}
