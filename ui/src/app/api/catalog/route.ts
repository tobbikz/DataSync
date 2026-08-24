import { NextResponse } from "next/server";
import { buildCatalogListQuery } from "@/lib/catalog-query";
import type { CatalogFilters, CatalogRow } from "@/lib/catalog-types";
import { parsePagination } from "@/lib/pagination";
import { query } from "@/lib/db";

type CatalogListRow = CatalogRow & { total_count: string };

function parseFilters(searchParams: URLSearchParams): CatalogFilters {
  const { limit, offset } = parsePagination(searchParams);
  const cdc = searchParams.get("cdc");
  const quarantined = searchParams.get("quarantined");
  const needsFullLoad = searchParams.get("needs_full_load");

  return {
    conn: searchParams.get("conn") ?? undefined,
    status: searchParams.get("status") ?? undefined,
    cdc: cdc === "true" ? true : cdc === "false" ? false : undefined,
    rag: searchParams.get("rag") ?? undefined,
    quarantined:
      quarantined === "true"
        ? true
        : quarantined === "false"
          ? false
          : undefined,
    needsFullLoad:
      needsFullLoad === "true"
        ? true
        : needsFullLoad === "false"
          ? false
          : undefined,
    q: searchParams.get("q") ?? undefined,
    limit,
    offset,
  };
}

export async function GET(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const { page, limit } = parsePagination(searchParams);
  const filters = parseFilters(searchParams);
  const { sql, params } = buildCatalogListQuery(filters);

  const result = await query<CatalogListRow>(sql, params);

  if (!result.ok) {
    return NextResponse.json({ items: [], total: 0, page, limit });
  }

  const total = result.rows.length
    ? Number(result.rows[0].total_count)
    : 0;

  const items = result.rows.map(({ total_count: _total, ...row }) => ({
    ...row,
    kafka_lag: row.kafka_lag != null ? Number(row.kafka_lag) : null,
    capture_lag_seconds:
      row.capture_lag_seconds != null ? Number(row.capture_lag_seconds) : null,
    apply_lag_seconds:
      row.apply_lag_seconds != null ? Number(row.apply_lag_seconds) : null,
  }));

  return NextResponse.json({ items, total, page, limit });
}
