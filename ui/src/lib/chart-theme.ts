/** Chart colors aligned with app CSS variables in globals.css */

export const CHART = {
  accent: "#005eb8",
  accentMuted: "#b8d4f0",
  success: "#0a6e42",
  warning: "#9a4a00",
  error: "#b01818",
  foreground: "#141619",
  foregroundSecondary: "#4a525c",
  muted: "#7a828c",
  grid: "#cdd3da",
  surface: "#ffffff",
  surfaceMuted: "#f2f4f6",
  border: "#cdd3da",
  series: ["#005eb8", "#3378c4", "#6699d4", "#4a525c", "#004a92"],
  blues: ["#005eb8", "#3378c4", "#6699d4", "#99bbdf", "#004a92"],
  rag: {
    GREEN: "#0a6e42",
    AMBER: "#9a4a00",
    RED: "#b01818",
    UNKNOWN: "#7a828c",
  },
  /** RAG segments in charts — blue UI palette */
  ragChart: {
    GREEN: "#005eb8",
    AMBER: "#3378c4",
    RED: "#004a92",
    UNKNOWN: "#7a828c",
  },
  dml: {
    insert: "#6699d4",
    update: "#005eb8",
    delete: "#004a92",
  },
  pipeline: {
    ok: "#005eb8",
    warn: "#9a4a00",
    idle: "#7a828c",
    error: "#b01818",
    okBg: "#e6f0fa",
    warnBg: "#fdf0e0",
    idleBg: "#f2f4f6",
    errorBg: "#fce8e8",
  },
} as const;

export const chartAxisTick = { fontSize: 10, fill: CHART.muted };
export const chartGrid = { stroke: CHART.grid, vertical: false };
export const chartGridHorizontal = { stroke: CHART.grid, horizontal: false };

export const chartTooltip = {
  contentStyle: {
    background: CHART.surface,
    border: `1px solid ${CHART.border}`,
    borderRadius: "2px",
    fontSize: "11px",
    fontFamily: "var(--font-jetbrains), monospace",
    color: CHART.foreground,
  },
  labelStyle: { color: CHART.muted, fontWeight: 500 },
};
