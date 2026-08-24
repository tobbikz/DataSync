import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import { verifySession } from "@/lib/auth";

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const token = request.cookies.get("datasync_session")?.value;
  const valid = token ? await verifySession(token) : false;

  if (pathname.startsWith("/dashboard") && !valid) {
    return NextResponse.redirect(new URL("/login", request.url));
  }

  if (pathname === "/login" && valid) {
    return NextResponse.redirect(new URL("/dashboard", request.url));
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/dashboard/:path*", "/login"],
};
