import { Suspense } from "react";
import { CatalogTable } from "@/components/CatalogTable";
import { PanelSkeleton } from "@/components/DashboardShell";

export default function CatalogPage() {
  return (
    <Suspense fallback={<PanelSkeleton rows={8} />}>
      <CatalogTable />
    </Suspense>
  );
}
