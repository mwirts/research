/** Chart factory with Oikos theme for the FIP-IE Dashboard */

// Chart color palette (matches Oikos design system)
const CHART_COLORS = {
  green: '#6EC26A',
  greenDark: '#008D44',
  blue: '#495BAF',
  blueLight: '#8FA4E3',
  red: '#CC3A33',
  yellow: '#D9C300',
  gray: '#919398',
  grayLight: '#A7A8A8',
};

// Fund-specific colors
const FUND_COLORS = {
  PFIN11: '#6EC26A',
  AZIN11: '#495BAF',
  PPEI11: '#D9C300',
  VIGT11: '#8FA4E3',
  PICE11: '#CC3A33',
  BRZP11: '#B7E583',
};

// Shared chart defaults
function getChartDefaults() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: getThemeVar('--chart-text'),
          font: { family: "'Aeonik','Inter','DM Sans',sans-serif", size: 11 },
          padding: 12,
        },
      },
      tooltip: {
        backgroundColor: getThemeVar('--card'),
        titleColor: getThemeVar('--chart-text'),
        bodyColor: getThemeVar('--chart-text'),
        borderColor: getThemeVar('--border'),
        borderWidth: 1,
        padding: 10,
        titleFont: { family: "'Aeonik',sans-serif", size: 12, weight: '500' },
        bodyFont: { family: "'Aeonik','Consolas',monospace", size: 12 },
      },
    },
    scales: {
      x: {
        ticks: { color: getThemeVar('--chart-text-muted'), font: { size: 10 } },
        grid: { color: getThemeVar('--chart-grid') },
      },
      y: {
        ticks: { color: getThemeVar('--chart-text-muted'), font: { size: 10 } },
        grid: { color: getThemeVar('--chart-grid') },
      },
    },
  };
}

// Registry of active charts (for cleanup on re-render)
const chartRegistry = {};

function destroyChart(id) {
  if (chartRegistry[id]) {
    chartRegistry[id].destroy();
    delete chartRegistry[id];
  }
}

/** Create a line chart */
function createLineChart(canvasId, labels, datasets, overrides = {}) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const defaults = getChartDefaults();
  const config = {
    type: 'line',
    data: { labels, datasets },
    options: {
      ...defaults,
      ...overrides,
      plugins: { ...defaults.plugins, ...(overrides.plugins || {}) },
      scales: { ...defaults.scales, ...(overrides.scales || {}) },
    },
  };

  chartRegistry[canvasId] = new Chart(ctx, config);
  return chartRegistry[canvasId];
}

/** Create a bar chart */
function createBarChart(canvasId, labels, datasets, overrides = {}) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const defaults = getChartDefaults();
  const config = {
    type: 'bar',
    data: { labels, datasets },
    options: {
      ...defaults,
      ...overrides,
      plugins: { ...defaults.plugins, ...(overrides.plugins || {}) },
      scales: { ...defaults.scales, ...(overrides.scales || {}) },
    },
  };

  chartRegistry[canvasId] = new Chart(ctx, config);
  return chartRegistry[canvasId];
}

/** Create a horizontal bar chart (for rankings) */
function createHBarChart(canvasId, labels, data, color = CHART_COLORS.green) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const defaults = getChartDefaults();
  chartRegistry[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data,
        backgroundColor: color + '80',
        borderColor: color,
        borderWidth: 1,
        borderRadius: 3,
      }],
    },
    options: {
      ...defaults,
      indexAxis: 'y',
      plugins: {
        ...defaults.plugins,
        legend: { display: false },
      },
    },
  });
  return chartRegistry[canvasId];
}

/** Create a scatter/bubble chart */
function createScatterChart(canvasId, datasets, overrides = {}) {
  destroyChart(canvasId);
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  const defaults = getChartDefaults();
  chartRegistry[canvasId] = new Chart(ctx, {
    type: 'bubble',
    data: { datasets },
    options: {
      ...defaults,
      ...overrides,
      plugins: { ...defaults.plugins, ...(overrides.plugins || {}) },
      scales: { ...defaults.scales, ...(overrides.scales || {}) },
    },
  });
  return chartRegistry[canvasId];
}
