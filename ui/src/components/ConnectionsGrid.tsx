"use client";

import { Fragment, useCallback, useEffect, useState } from "react";
import { AddConnectionWizard } from "./AddConnectionWizard";
import { ConnectionDetailPanel } from "./ConnectionDetailPanel";
import { PageMeta, PanelSkeleton } from "./DashboardShell";
import { Pagination } from "./Pagination";
import { PAGE_SIZE } from "@/lib/pagination";

interface Connection {
  alias: string;
  db_engine: string;
  host: string;
  port: number;
  db_name: string;
  username: string;
  active: boolean;
}

function engineLabel(engine: string) {
  if (engine === "mariadb") return "MariaDB";
  if (engine === "mssql") return "SQL Server";
  if (engine === "mongodb") return "MongoDB";
  return engine;
}

export function ConnectionsGrid() {
  const [items, setItems] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [wizardOpen, setWizardOpen] = useState(false);
  const [editAlias, setEditAlias] = useState<string | null>(null);
  const [deleteAlias, setDeleteAlias] = useState<string | null>(null);
  const [selectedAlias, setSelectedAlias] = useState<string | null>(null);
  const [busyAlias, setBusyAlias] = useState<string | null>(null);
  const [actionError, setActionError] = useState("");

  const refresh = useCallback(() => {
    setLoading(true);
    return fetch(`/api/connections?page=${page}&limit=${PAGE_SIZE}`)
      .then((r) => r.json())
      .then((d) => {
        setItems(d.items ?? []);
        setTotal(d.total ?? 0);
      })
      .finally(() => setLoading(false));
  }, [page]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const firstTime = !loading && total === 0;
  const showWizard = wizardOpen || firstTime || !!editAlias;

  function handleSaved() {
    setEditAlias(null);
    setWizardOpen(false);
    if (page === 1) {
      refresh();
    } else {
      setPage(1);
    }
  }

  function openEdit(alias: string) {
    setEditAlias(alias);
    setWizardOpen(false);
    setActionError("");
  }

  async function confirmDelete() {
    if (!deleteAlias) return;
    setBusyAlias(deleteAlias);
    setActionError("");
    try {
      const res = await fetch(
        `/api/connections/${encodeURIComponent(deleteAlias)}`,
        { method: "DELETE" },
      );
      const data = await res.json();
      if (!res.ok) {
        setActionError(data.error ?? "Failed to delete connection");
        return;
      }
      setDeleteAlias(null);
      await refresh();
    } finally {
      setBusyAlias(null);
    }
  }

  return (
    <>
      <PageMeta label="Connections" actions={
          !loading && !firstTime ? (
            <button
              type="button"
              onClick={() => {
                setEditAlias(null);
                setWizardOpen(true);
              }}
              disabled={false}
              className="btn-primary py-1 text-[11px]"
            >
              Add connection
            </button>
          ) : null
        }
      />

      {actionError ? (
        <pre className="whitespace-pre-wrap font-mono text-[11px] text-error">
          {actionError}
        </pre>
      ) : null}

      {loading ? (
        <PanelSkeleton rows={5} />
      ) : (
        <div className="relative flex min-h-0 flex-1 flex-col">
          <AddConnectionWizard
            open={showWizard}
            firstTime={firstTime && !editAlias}
            editAlias={editAlias}
            onClose={() => {
              setWizardOpen(false);
              setEditAlias(null);
            }}
            onCreated={handleSaved}
          />

          {!firstTime ? (
            <>
              <div className="panel overflow-hidden">
                <div className="panel-header py-2">
                  <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                    Registered sources
                  </p>
                </div>

                {items.length === 0 ? (
                  <p className="py-10 text-center font-mono text-[11px] text-foreground-muted">
                    no connections yet
                  </p>
                ) : (
                  <div className="resource-list resource-list--connections">
                    <div className="resource-list-head">
                      <span>Source</span>
                      <span>Endpoint</span>
                      <span>Replication</span>
                      <span>Manage</span>
                    </div>
                    {items.map((c) => {
                      const busy = busyAlias === c.alias;
                      const expanded = selectedAlias === c.alias;

                      return (
                        <Fragment key={c.alias}>
                          <div
                            role="button"
                            tabIndex={0}
                            className={`resource-row group cursor-pointer ${expanded ? "is-expanded" : ""}`}
                            onClick={() =>
                              setSelectedAlias((prev) =>
                                prev === c.alias ? null : c.alias,
                              )
                            }
                            onKeyDown={(e) => {
                              if (e.key === "Enter" || e.key === " ") {
                                e.preventDefault();
                                setSelectedAlias((prev) =>
                                  prev === c.alias ? null : c.alias,
                                );
                              }
                            }}
                          >
                          <div>
                            <div className="resource-row__title">{c.alias}</div>
                            <div className="resource-row__sub">
                              {engineLabel(c.db_engine)}
                              {c.username ? (
                                <span className="text-foreground-muted">
                                  {" "}
                                  · {c.username}
                                </span>
                              ) : null}
                            </div>
                          </div>

                          <div>
                            <div className="resource-row__mono">
                              {c.host}
                              <span className="text-foreground-muted">
                                :{c.port}
                              </span>
                            </div>
                            <div className="resource-row__mono-muted">
                              {c.db_name}
                            </div>
                          </div>

                          <div>
                            <span className="resource-row__status text-foreground-muted">
                              Replicating
                            </span>
                          </div>

                          <div
                            className="resource-row__actions"
                            onClick={(e) => e.stopPropagation()}
                          >
                            <button
                              type="button"
                              className="table-action"
                              disabled={busy}
                              onClick={() => openEdit(c.alias)}
                            >
                              Edit
                            </button>
                            <span className="table-action-sep">·</span>
                            <button
                              type="button"
                              className="table-action table-action--danger"
                              disabled={busy}
                              onClick={() => {
                                setDeleteAlias(c.alias);
                                setActionError("");
                              }}
                            >
                              Delete
                            </button>
                          </div>
                        </div>
                        {expanded ? (
                          <ConnectionDetailPanel alias={c.alias} />
                        ) : null}
                      </Fragment>
                      );
                    })}
                  </div>
                )}
                <Pagination
                  page={page}
                  total={total}
                  pageSize={PAGE_SIZE}
                  onPageChange={setPage}
                />
              </div>
            </>
          ) : null}
        </div>
      )}

      {deleteAlias ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/30 p-4">
          <div className="panel w-full max-w-md p-4">
            <h3 className="text-[13px] font-semibold text-foreground">
              Delete connection
            </h3>
            <p className="mt-2 text-[12px] text-foreground-secondary">
              Remove <span className="font-mono">{deleteAlias}</span>? Catalog
              tables referencing this source must be removed first.
            </p>
            <div className="mt-4 flex justify-end gap-2">
              <button
                type="button"
                className="btn-secondary"
                onClick={() => setDeleteAlias(null)}
                disabled={busyAlias === deleteAlias}
              >
                Cancel
              </button>
              <button
                type="button"
                className="btn-primary bg-error hover:bg-error"
                onClick={confirmDelete}
                disabled={busyAlias === deleteAlias}
              >
                {busyAlias === deleteAlias ? "Deleting…" : "Delete"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
