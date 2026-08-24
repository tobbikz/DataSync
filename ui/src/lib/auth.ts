import { SignJWT, jwtVerify } from "jose";

const secret = new TextEncoder().encode(
  process.env.DATASYNC_UI_SECRET ?? "datasync-dev-secret-change-me",
);

export async function createSession(email: string): Promise<string> {
  return new SignJWT({ email })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt()
    .setExpirationTime("7d")
    .sign(secret);
}

export async function verifySession(token: string): Promise<boolean> {
  try {
    await jwtVerify(token, secret);
    return true;
  } catch {
    return false;
  }
}

export function validateCredentials(email: string, password: string): boolean {
  const user = process.env.DATASYNC_UI_USER ?? "admin";
  const pass = process.env.DATASYNC_UI_PASSWORD ?? "datasync";
  return email === user && password === pass;
}

export function sessionCookieOptions(maxAge = 60 * 60 * 24 * 7) {
  return {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax" as const,
    path: "/",
    maxAge,
  };
}
