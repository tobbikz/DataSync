import { NextResponse } from "next/server";
import { getHealthStatus } from "@/lib/health";

export async function GET() {
  const health = await getHealthStatus();
  return NextResponse.json(health);
}
