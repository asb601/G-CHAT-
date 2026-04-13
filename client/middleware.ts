import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  // We can't read localStorage in middleware, so we also set a cookie.
  // The auth callback page sets this cookie after storing the JWT.
  const token = request.cookies.get("token")?.value;
  const { pathname } = request.nextUrl;

  const isLoginPage = pathname === "/login";
  const isLoggedIn = !!token;

  // If logged in and on login page, redirect to chat
  if (isLoggedIn && isLoginPage) {
    return NextResponse.redirect(new URL("/chat", request.url));
  }

  // If not logged in and on a protected page, redirect to login
  if (!isLoggedIn && !isLoginPage) {
    return NextResponse.redirect(new URL("/login", request.url));
  }
}

export const config = {
  matcher: ["/chat/:path*", "/folders/:path*", "/admin/:path*", "/profile/:path*", "/login"],
};
