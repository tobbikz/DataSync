import { NextRequest, NextResponse } from "next/server";
import {
  listJobs,
  runnerAvailable,
  startAction,
  type ActionKind,
} from "@/lib/actions";
import { pipelineActionById } from "@/lib/pipeline-actions";

const ACTION_KINDS: ActionKind[] = [
  "discover",
  "full-load",
  "onboard-pending",
  "ddl-sync",
  "kafka-apply",
  "capture",
  "daemon",
];

export async function GET() {
  return NextResponse.json({
    jobs: listJobs(),
    runnerAvailable: runnerAvailable(),
  });
}

export async function POST(request: NextRequest) {
  const body = (await request.json()) as {
    action?: ActionKind;
    connId?: string;
    hotOnly?: boolean;
    coldOnly?: boolean;
    onboardTier?: "all" | "hot" | "cold";
    skipOnboard?: boolean;
    schema?: string;
    table?: string;
    confirm?: boolean;
  };

  if (!body.confirm) {
    return NextResponse.json(
      { error: "Confirmation required (confirm: true)" },
      { status: 400 },
    );
  }

  const action = body.action;
  if (!action || !ACTION_KINDS.includes(action)) {
    return NextResponse.json({ error: "Invalid action" }, { status: 400 });
  }

  const def = pipelineActionById(action);
  if (!def) {
    return NextResponse.json({ error: "Unknown action" }, { status: 400 });
  }

  const connId = body.connId?.trim() || undefined;
  if (def.requiresConn && !connId) {
    return NextResponse.json(
      { error: `${action} requires connId` },
      { status: 400 },
    );
  }

  const result = startAction({
    action,
    connId,
    hotOnly: body.hotOnly,
    coldOnly: body.coldOnly,
    onboardTier: body.onboardTier,
    skipOnboard: body.skipOnboard,
    schema: body.schema?.trim() || undefined,
    table: body.table?.trim() || undefined,
  });

  if ("error" in result) {
    return NextResponse.json({ error: result.error }, { status: 503 });
  }

  return NextResponse.json({ job: result.job });
}
