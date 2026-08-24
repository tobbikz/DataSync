import { Suspense } from "react";
import { FlowDashboard } from "@/components/FlowDashboard";

export default function FlowPage() {
  return (
    <Suspense fallback={<p className="p-4 font-mono text-[11px] text-foreground-muted">loading flow…</p>}>
      <FlowDashboard />
    </Suspense>
  );
}
