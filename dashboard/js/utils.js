/** Utility functions for the FIP-IE Dashboard */

// --- Theme Toggle ---
function initTheme() {
  const saved = localStorage.getItem('oikos-theme');
  if (saved === 'light') document.documentElement.classList.add('light');
}

function toggleTheme() {
  document.documentElement.classList.toggle('light');
  const isLight = document.documentElement.classList.contains('light');
  localStorage.setItem('oikos-theme', isLight ? 'light' : 'dark');
  // Re-render charts with new theme
  if (window.reloadCharts) window.reloadCharts();
}

// --- CSS Variable Reader ---
function getThemeVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// --- Number Formatting (Brazilian) ---
function formatBRL(value) {
  if (value == null) return '-';
  return value.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function formatBRLCompact(value) {
  if (value == null) return '-';
  if (Math.abs(value) >= 1e9) return 'R$ ' + (value / 1e9).toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + ' bi';
  if (Math.abs(value) >= 1e6) return 'R$ ' + (value / 1e6).toLocaleString('pt-BR', { maximumFractionDigits: 0 }) + ' mi';
  if (Math.abs(value) >= 1e3) return 'R$ ' + (value / 1e3).toLocaleString('pt-BR', { maximumFractionDigits: 0 }) + ' mil';
  return formatBRL(value);
}

function formatPct(value, decimals = 1) {
  if (value == null) return '-';
  return value.toLocaleString('pt-BR', { minimumFractionDigits: decimals, maximumFractionDigits: decimals }) + '%';
}

function formatNumber(value, decimals = 2) {
  if (value == null) return '-';
  return value.toLocaleString('pt-BR', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function formatInt(value) {
  if (value == null) return '-';
  return Math.round(value).toLocaleString('pt-BR');
}

// --- Data Fetching ---
async function fetchAPI(path) {
  const res = await fetch(`/api/${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

// --- Tab Navigation ---
function initTabs(tabsSelector, contentsSelector) {
  const tabs = document.querySelectorAll(tabsSelector);
  const contents = document.querySelectorAll(contentsSelector);

  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      contents.forEach(c => c.classList.remove('active'));
      tab.classList.add('active');
      const target = document.getElementById(tab.dataset.tab);
      if (target) target.classList.add('active');
    });
  });
}

// --- Date Formatting ---
function formatRefDate(dateStr) {
  if (!dateStr) return '-';
  const [year, month] = dateStr.split('-');
  const months = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
  return `${months[parseInt(month) - 1]}/${year.slice(-2)}`;
}

// --- CSS class for positive/negative ---
function valueClass(value) {
  if (value == null) return 'muted';
  if (value > 0) return 'positive';
  if (value < 0) return 'negative';
  return '';
}

// Init theme on load
initTheme();
