import { Suspense } from "react";
import { OpsDashboard } from "@/components/OpsDashboard";
import { PanelSkeleton } from "@/components/DashboardShell";

export default function OpsPage() {
  return (
    <Suspense fallback={<PanelSkeleton rows={6} />}>
      <OpsDashboard />
    </Suspense>
  );
}
