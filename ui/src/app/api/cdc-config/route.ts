import { NextRequest, NextResponse } from "next/server";
import {
  readCdcTuning,
  writeCdcTuning,
  type CdcTuning,
  configFilePath,
} from "@/lib/config-file";
import { enqueueSignal } from "@/lib/signals";
import fs from "fs";

export async function GET() {
  try {
    if (!fs.existsSync(configFilePath())) {
      return NextResponse.json(
        { error: `Config not found: ${configFilePath()}` },
        { status: 503 },
      );
    }
    return NextResponse.json({
      path: configFilePath(),
      cdc: readCdcTuning(),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "read failed";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: NextRequest) {
  let body: { cdc?: CdcTuning; reload?: boolean; confirm?: boolean };
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

  if (!body.cdc || typeof body.cdc !== "object") {
    return NextResponse.json({ error: "cdc object required" }, { status: 400 });
  }

  try {
    if (!fs.existsSync(configFilePath())) {
      return NextResponse.json(
        { error: `Config not found: ${configFilePath()}` },
        { status: 503 },
      );
    }

    const cdc = { ...body.cdc };
    if (cdc.content_routing != null && !Array.isArray(cdc.content_routing)) {
      return NextResponse.json(
        { error: "content_routing must be an array" },
        { status: 400 },
      );
    }
    if (
      cdc.message_filter_exclude_ops != null &&
      !Array.isArray(cdc.message_filter_exclude_ops)
    ) {
      return NextResponse.json(
        { error: "message_filter_exclude_ops must be an array" },
        { status: 400 },
      );
    }

    const saved = writeCdcTuning(cdc);

    let signalId: number | undefined;
    if (body.reload !== false) {
      const sig = await enqueueSignal({ action: "reload_config" });
      if (sig.ok) signalId = sig.signal_id;
    }

    return NextResponse.json({
      ok: true,
      cdc: saved,
      reload_signal_id: signalId ?? null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "write failed";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
