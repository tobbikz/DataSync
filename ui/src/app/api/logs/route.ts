import { NextResponse } from "next/server";
import { query } from "@/lib/db";
import { parsePagination } from "@/lib/pagination";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const level = searchParams.get("level");
  const conn = searchParams.get("conn");
  const logId = searchParams.get("log_id");
  const { page, limit, offset } = parsePagination(searchParams);

  const conditions: string[] = [];
  const params: unknown[] = [];
  let idx = 1;

  if (logId) {
    conditions.push(`log_id = $${idx++}`);
    params.push(Number(logId));
  }
  if (level) {
    conditions.push(`level = $${idx++}`);
    params.push(level);
  }
  if (conn) {
    conditions.push(`conn_id = $${idx++}`);
    params.push(conn);
  }

  const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  params.push(limit, offset);
  const result = await query<{
    log_id: number;
    logged_at: string;
    level: string;
    component: string;
    message: string;
    conn_id: string | null;
    total_count: string;
  }>(`
    SELECT log_id, logged_at, level, component, message, conn_id,
           COUNT(*) OVER() AS total_count
    FROM cdc_catalog.logs
    ${where}
    ORDER BY logged_at DESC
    LIMIT $${idx++} OFFSET $${idx}
  `, params);

  if (!result.ok) {
    return NextResponse.json({ items: [], total: 0, page, limit });
  }

  const total = result.rows.length
    ? Number(result.rows[0].total_count)
    : 0;

  const items = result.rows.map(({ total_count: _total, ...row }) => row);

  return NextResponse.json({ items, total, page, limit });
}
