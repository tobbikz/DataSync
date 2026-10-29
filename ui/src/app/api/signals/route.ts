import { NextRequest, NextResponse } from "next/server";
import {
  enqueueSignal,
  isSignalAction,
  listSignals,
} from "@/lib/signals";
import { validateConnId } from "@/lib/actions";

export async function GET(request: NextRequest) {
  const limit = Math.min(
    Number(request.nextUrl.searchParams.get("limit") ?? "50") || 50,
    200,
  );
  const result = await listSignals(limit);
  if (!result.ok) {
    return NextResponse.json(
      { error: result.error ?? "Database unavailable" },
      { status: 503 },
    );
  }
  return NextResponse.json({ items: result.rows });
}

export async function POST(request: NextRequest) {
  let body: {
    action?: string;
    connId?: string;
    catalogId?: number;
    payload?: Record<string, unknown>;
    confirm?: boolean;
  };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  if (!body.confirm) {
    return NextResponse.json(
      { error: "Confirmation required (confirm: true)" },
      { status: 400 },
    );
  }

  const action = body.action?.trim() ?? "";
  if (!isSignalAction(action)) {
    return NextResponse.json({ error: "Invalid action" }, { status: 400 });
  }

  const connId = body.connId?.trim() || undefined;
  if (connId && !validateConnId(connId)) {
    return NextResponse.json({ error: "Invalid connId" }, { status: 400 });
  }

  const catalogId =
    body.catalogId != null && Number.isFinite(Number(body.catalogId))
      ? Number(body.catalogId)
      : undefined;

  if (
    (action === "execute_incremental" || action === "execute_blocking") &&
    !connId &&
    !catalogId
  ) {
    return NextResponse.json(
      { error: "execute_* requires connId and/or catalogId" },
      { status: 400 },
    );
  }

  const result = await enqueueSignal({
    action,
    connId,
    catalogId,
    payload: body.payload,
  });

  if (!result.ok) {
    return NextResponse.json({ error: result.error }, { status: 500 });
  }

  return NextResponse.json({ ok: true, signal_id: result.signal_id, action });
}
