import { NextRequest, NextResponse } from "next/server";
import {
  createSession,
  sessionCookieOptions,
  validateCredentials,
} from "@/lib/auth";

export async function POST(request: NextRequest) {
  const body = (await request.json()) as { email?: string; password?: string };
  const email = body.email?.trim() ?? "";
  const password = body.password ?? "";

  if (!validateCredentials(email, password)) {
    return NextResponse.json({ error: "Invalid credentials" }, { status: 401 });
  }

  const token = await createSession(email);
  const response = NextResponse.json({ ok: true, email });
  response.cookies.set("datasync_session", token, sessionCookieOptions());
  return response;
}
