import { totalPages } from "@/lib/pagination";

export function Pagination({
  page,
  total,
  pageSize,
  onPageChange,
}: {
  page: number;
  total: number;
  pageSize: number;
  onPageChange: (page: number) => void;
}) {
  const pages = totalPages(total, pageSize);
  const start = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const end = Math.min(page * pageSize, total);

  return (
    <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-2">
      <span className="font-mono text-[10px] text-foreground-muted">
        {total === 0 ? "0 rows" : `${start}–${end} of ${total}`}
      </span>
      <div className="flex items-center gap-1.5">
        <button
          type="button"
          className="btn-secondary py-1 text-[10px]"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          Prev
        </button>
        <span className="font-mono text-[10px] text-foreground-muted">
          {page} / {pages}
        </span>
        <button
          type="button"
          className="btn-secondary py-1 text-[10px]"
          disabled={page >= pages}
          onClick={() => onPageChange(page + 1)}
        >
          Next
        </button>
      </div>
    </div>
  );
}
