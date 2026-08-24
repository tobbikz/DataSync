export const PAGE_SIZE = 10;

export function parsePagination(searchParams: URLSearchParams) {
  const page = Math.max(1, Number(searchParams.get("page") ?? "1") || 1);
  const limit = Math.min(
    Math.max(1, Number(searchParams.get("limit") ?? String(PAGE_SIZE)) || PAGE_SIZE),
    100,
  );
  const offset = (page - 1) * limit;
  return { page, limit, offset };
}

export function paginateSlice<T>(items: T[], offset: number, limit: number) {
  return {
    items: items.slice(offset, offset + limit),
    total: items.length,
  };
}

export function totalPages(total: number, pageSize: number) {
  return Math.max(1, Math.ceil(total / pageSize));
}
