"use client";

import { CHART } from "@/lib/chart-theme";
import { useFlowPulses, type FlowPulse } from "@/lib/flow-pulse";

function PulseDot({
  path,
  pulse,
  color,
}: {
  path: string;
  pulse: FlowPulse;
  color: string;
}) {
  return (
    <circle r={2} fill={color} opacity={0}>
      <animate attributeName="opacity" values="0;0.95;0.95;0" dur={`${pulse.durationSec}s`} begin={`${pulse.delayMs}ms`} fill="freeze" />
      <animateMotion
        dur={`${pulse.durationSec}s`}
        begin={`${pulse.delayMs}ms`}
        repeatCount="1"
        path={path}
        fill="freeze"
      />
    </circle>
  );
}

export function FlowEdgePulse({
  path,
  activityCount,
  live,
  edgePhase = 0,
  color = CHART.accent,
  strokeWidth = 1.2,
  dimmed = false,
}: {
  path: string;
  activityCount: number;
  live: boolean;
  edgePhase?: number;
  color?: string;
  strokeWidth?: number;
  dimmed?: boolean;
}) {
  const pulses = useFlowPulses(activityCount, live, edgePhase);

  return (
    <g>
      <path
        d={path}
        fill="none"
        stroke={dimmed ? CHART.grid : color}
        strokeWidth={strokeWidth}
        opacity={dimmed ? 0.45 : live ? 0.75 : 0.4}
        strokeDasharray={live ? undefined : "3 4"}
      />
      {live
        ? pulses.map((pulse) => (
            <PulseDot key={pulse.id} path={path} pulse={pulse} color={color} />
          ))
        : null}
    </g>
  );
}
