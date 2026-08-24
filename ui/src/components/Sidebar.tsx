"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { usePolling } from "@/lib/use-polling";

interface Overview {
  errors24h: number;
}

const nav = [
  {
    href: "/dashboard/ops",
    label: "Ops",
    icon: (
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M3 3v18h18" />
        <path d="M7 16l4-8 4 5 4-9" />
      </svg>
    ),
  },
  {
    href: "/dashboard/flow",
    label: "Flow",
    icon: (
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="5" cy="12" r="2" />
        <circle cx="12" cy="6" r="2" />
        <circle cx="19" cy="12" r="2" />
        <circle cx="12" cy="18" r="2" />
        <path d="M7 12h3M14 12h3M12 8v3M12 13v3" />
      </svg>
    ),
  },
  {
    href: "/dashboard/connections",
    label: "Connections",
    icon: (
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M3 5v14a9 3 0 0 0 18 0V5" />
        <path d="M3 12a9 3 0 0 0 18 0" />
      </svg>
    ),
  },
  {
    href: "/dashboard/catalog",
    label: "Catalog",
    icon: (
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 6h16M4 10h16M4 14h16M4 18h16" />
      </svg>
    ),
  },
  {
    href: "/dashboard/logs",
    label: "Logs",
    icon: (
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
        <path d="M14 2v6h6" />
      </svg>
    ),
  },
];

export function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const { data: overview } = usePolling<Overview>(
    () => fetch("/api/overview").then((r) => r.json()),
    60_000,
  );

  const errorCount = overview?.errors24h ?? 0;

  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  }

  return (
    <aside className="flex h-screen w-[220px] shrink-0 flex-col border-r border-border bg-surface">
      <div className="flex items-center gap-2.5 border-b border-border px-4 py-3.5">
        <div className="flex size-7 items-center justify-center bg-accent text-[11px] font-bold text-white">
          DS
        </div>
        <div>
          <p className="text-[13px] font-semibold leading-none text-foreground">
            DataSync
          </p>
          <p className="mt-0.5 text-[10px] text-foreground-muted">
            Control Plane
          </p>
        </div>
      </div>

      <nav className="flex-1 px-2 py-3">
        <p className="mb-1.5 px-2 text-[10px] font-semibold uppercase tracking-wider text-foreground-muted">
          Operations
        </p>
        {nav.map((item) => {
          const active =
            pathname === item.href || pathname.startsWith(`${item.href}/`);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`mb-px flex items-center gap-2.5 px-2 py-2 text-[12px] font-medium transition-colors ${
                active
                  ? "border-l-2 border-accent bg-accent-subtle pl-[6px] text-accent"
                  : "border-l-2 border-transparent pl-[6px] text-foreground-secondary hover:bg-surface-muted hover:text-foreground"
              }`}
            >
              <span className={active ? "text-accent" : "text-foreground-muted"}>
                {item.icon}
              </span>
              {item.label}
              {item.href === "/dashboard/logs" && errorCount > 0 ? (
                <span className="tag ml-auto border-error/30 bg-error-subtle text-error">
                  {errorCount > 99 ? "99+" : errorCount}
                </span>
              ) : null}
            </Link>
          );
        })}
      </nav>

      <div className="border-t border-border p-3">
        <div className="mb-2 flex items-center gap-2 px-1 text-[11px] text-foreground-muted">
          <span className="status-dot status-dot--ok" />
          <span>admin</span>
        </div>
        <button type="button" onClick={logout} className="btn-secondary w-full">
          Sign out
        </button>
      </div>
    </aside>
  );
}
