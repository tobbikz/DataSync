import { Suspense } from "react";
import { ControlPanel } from "@/components/ControlPanel";
import { PanelSkeleton } from "@/components/DashboardShell";

export default function ControlPage() {
  return (
    <Suspense fallback={<PanelSkeleton rows={8} />}>
      <ControlPanel />
    </Suspense>
  );
}
