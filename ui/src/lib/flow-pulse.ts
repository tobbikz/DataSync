"use client";

import { useEffect, useRef, useState } from "react";

export interface FlowPulse {
  id: number;
  delayMs: number;
  durationSec: number;
}

/** Spawns short-lived pulses when activity count rises or while live traffic continues. */
export function useFlowPulses(
  activityCount: number,
  live: boolean,
  edgePhase = 0,
): FlowPulse[] {
  const [pulses, setPulses] = useState<FlowPulse[]>([]);
  const prevCount = useRef(activityCount);

  useEffect(() => {
    const delta = activityCount - prevCount.current;
    prevCount.current = activityCount;

    if (delta > 0) {
      const burst = Math.min(delta, 4);
      const base = Date.now();
      setPulses((current) => [
        ...current.slice(-10),
        ...Array.from({ length: burst }, (_, i) => ({
          id: base + i + edgePhase * 17,
          delayMs: edgePhase * 180 + i * (120 + Math.random() * 280),
          durationSec: 1.1 + Math.random() * 0.7,
        })),
      ]);
    }
  }, [activityCount, edgePhase]);

  useEffect(() => {
    if (!live || activityCount <= 0) return;

    const tick = () => {
      if (Math.random() > 0.55) return;
      setPulses((current) => [
        ...current.slice(-10),
        {
          id: Date.now() + Math.random(),
          delayMs: edgePhase * 220 + Math.random() * 900,
          durationSec: 1 + Math.random() * 0.8,
        },
      ]);
    };

    const intervalMs = 2800 + edgePhase * 400 + Math.random() * 2200;
    const id = window.setInterval(tick, intervalMs);
    return () => window.clearInterval(id);
  }, [live, activityCount, edgePhase]);

  useEffect(() => {
    if (pulses.length === 0) return;
    const id = window.setInterval(() => {
      const cutoff = Date.now() - 5000;
      setPulses((current) => current.filter((p) => p.id > cutoff));
    }, 1200);
    return () => window.clearInterval(id);
  }, [pulses.length]);

  return pulses;
}
