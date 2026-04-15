/** Main application logic for the FIP-IE Dashboard */

let fundsData = [];
let selectedFund = null;
let fundDetail = null;

document.addEventListener('DOMContentLoaded', async () => {
  initTabs('.tab', '.tab-content');
  await loadOverview();
  document.querySelector('[data-tab="comparative"]')
    ?.addEventListener('click', () => loadComparative());
});

// =====================================================================
// Tab: Visao Geral
// =====================================================================

async function loadOverview() {
  try {
    fundsData = await fetchAPI('funds');
    renderKPIs(fundsData);
    renderFundsTable(fundsData);
    renderOverviewCharts(fundsData);
    renderFundPills(fundsData);
  } catch (e) {
    console.error('loadOverview error:', e);
    document.getElementById('kpi-grid').innerHTML =
      '<div class="empty" style="grid-column:1/-1">Erro ao carregar dados. Servidor rodando?</div>';
  }
}

function renderKPIs(funds) {
  const el = document.getElementById('kpi-grid');
  if (!el) return;

  const withPrice = funds.filter(f => f.market_price);
  const withDiscount = funds.filter(f => f.discount_premium_pct != null);
  const withDY = funds.filter(f => f.dividend_yield_ltm != null && f.dividend_yield_ltm > 0);
  const withTir = funds.filter(f => f.irr_real != null);

  const avg = (arr, key) => arr.length ? arr.reduce((s, f) => s + (f[key] || 0), 0) / arr.length : null;

  el.innerHTML = `
    <div class="kpi-card green">
      <div class="kpi-label">Fundos com Cotacao</div>
      <div class="kpi-value">${withPrice.length} / ${funds.length}</div>
    </div>
    <div class="kpi-card ${(avg(withDiscount, 'discount_premium_pct') || 0) < 0 ? 'red' : 'green'}">
      <div class="kpi-label">Desconto Medio</div>
      <div class="kpi-value">${withDiscount.length ? formatPct(avg(withDiscount, 'discount_premium_pct')) : '-'}</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">DY Medio (LTM)</div>
      <div class="kpi-value">${withDY.length ? formatPct(avg(withDY, 'dividend_yield_ltm')) : '-'}</div>
    </div>
    <div class="kpi-card blue">
      <div class="kpi-label">TIR Media (IPCA+)</div>
      <div class="kpi-value">${withTir.length ? formatPct(avg(withTir, 'irr_real')) : '-'}</div>
    </div>
  `;
}

function renderFundsTable(funds) {
  const tbody = document.getElementById('funds-table-body');
  if (!tbody) return;

  tbody.innerHTML = funds.map(f => {
    const noData = !f.market_price;
    return `
    <tr class="${noData ? 'muted' : ''}" onclick="selectFund('${f.ticker}')" style="cursor:pointer">
      <td class="ticker">${f.ticker}</td>
      <td class="text">${shortName(f.fund_name)}</td>
      <td class="text">${f.manager || '-'}</td>
      <td class="right">${f.nav_per_unit ? formatNumber(f.nav_per_unit) : '-'}</td>
      <td class="right">${f.market_price ? formatNumber(f.market_price) : '-'}</td>
      <td class="right ${valueClass(f.discount_premium_pct)}">${f.discount_premium_pct != null ? formatPct(f.discount_premium_pct) : '-'}</td>
      <td class="right">${f.irr_real != null ? formatPct(f.irr_real) : '-'}</td>
      <td class="right">${f.dividend_yield_ltm != null ? formatPct(f.dividend_yield_ltm) : '-'}</td>
      <td class="right">${f.div_total != null ? formatNumber(f.div_total) : '-'}</td>
      <td class="center muted" style="font-size:11px">${f.market_date || f.report_date || '-'}</td>
    </tr>`;
  }).join('');
}

function shortName(name) {
  if (!name) return '-';
  // "Perfin Apollo Energia FIP-IE" -> "Perfin Apollo Energia"
  return name.replace(/\s+FIP[\s-]*IE$/i, '').trim();
}

function renderOverviewCharts(funds) {
  // TIR ranking (horizontal bars)
  const withTir = funds.filter(f => f.irr_real != null)
    .sort((a, b) => b.irr_real - a.irr_real);

  if (withTir.length) {
    createHBarChart('chart-tir-ranking',
      withTir.map(f => f.ticker),
      withTir.map(f => f.irr_real),
      CHART_COLORS.green
    );
  }

  // DY ranking (horizontal bars)
  const withDY = funds.filter(f => f.dividend_yield_ltm != null && f.dividend_yield_ltm > 0)
    .sort((a, b) => b.dividend_yield_ltm - a.dividend_yield_ltm);

  if (withDY.length) {
    createHBarChart('chart-dy-ranking',
      withDY.map(f => f.ticker),
      withDY.map(f => f.dividend_yield_ltm),
      CHART_COLORS.blue
    );
  }
}

function renderFundPills(funds) {
  const el = document.getElementById('fund-pills');
  if (!el) return;
  el.innerHTML = funds.map(f =>
    `<button class="fund-pill" data-ticker="${f.ticker}" onclick="selectFund('${f.ticker}')">${f.ticker}</button>`
  ).join('');
}

// =====================================================================
// Tab: Analise Individual
// =====================================================================

async function selectFund(ticker) {
  selectedFund = ticker;

  // Switch tab
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.querySelector('[data-tab="individual"]').classList.add('active');
  document.getElementById('individual').classList.add('active');

  // Highlight pill
  document.querySelectorAll('.fund-pill').forEach(p =>
    p.classList.toggle('active', p.dataset.ticker === ticker)
  );

  const container = document.getElementById('fund-detail-content');
  container.innerHTML = '<div class="loading">Carregando...</div>';

  try {
    fundDetail = await fetchAPI(`funds/${ticker}`);
    renderFundDetail(fundDetail);
  } catch (e) {
    console.error('selectFund error:', e);
    container.innerHTML = `<div class="empty">Erro ao carregar ${ticker}: ${e.message}</div>`;
  }
}

function renderFundDetail(data) {
  const container = document.getElementById('fund-detail-content');
  if (!container) return;

  const fund = data.fund;
  const prices = data.price_history || [];
  const divs = data.dividends || [];
  const snaps = data.snapshots || [];
  const tir = data.tir;

  const latestSnap = snaps.length ? snaps[snaps.length - 1] : {};
  const latestPrice = prices.length ? prices[prices.length - 1].close : null;
  const navPerUnit = latestSnap.nav_per_unit || null;

  // Compute DY from dividends
  const now = new Date();
  const oneYearAgo = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
  const ltmDivs = divs.filter(d => new Date(d.ex_date) >= oneYearAgo);
  const ltmAmount = ltmDivs.reduce((s, d) => s + d.amount, 0);
  const dy = latestPrice && ltmAmount > 0 ? (ltmAmount / latestPrice * 100) : null;

  // Compute discount
  const discount = navPerUnit && latestPrice
    ? ((latestPrice - navPerUnit) / navPerUnit * 100) : null;

  const fundColor = FUND_COLORS[fund.ticker] || CHART_COLORS.green;

  container.innerHTML = `
    <div class="kpi-grid mb-24">
      <div class="kpi-card green">
        <div class="kpi-label">Cota Mercado</div>
        <div class="kpi-value">${latestPrice ? formatBRL(latestPrice) : '-'}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">VP / Cota</div>
        <div class="kpi-value" style="color:var(--text)">${navPerUnit ? formatBRL(navPerUnit) : '-'}</div>
      </div>
      <div class="kpi-card ${discount != null && discount < 0 ? 'red' : 'green'}">
        <div class="kpi-label">Desconto</div>
        <div class="kpi-value">${discount != null ? formatPct(discount) : '-'}</div>
      </div>
      <div class="kpi-card blue">
        <div class="kpi-label">TIR (IPCA+)</div>
        <div class="kpi-value">${tir ? formatPct(tir.ipca_plus_pct) : '-'}</div>
      </div>
      <div class="kpi-card green">
        <div class="kpi-label">DY (LTM)</div>
        <div class="kpi-value">${dy ? formatPct(dy) : '-'}</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Dividendos Acum.</div>
        <div class="kpi-value" style="color:var(--text)">R$ ${divs.length ? formatNumber(divs.reduce((s, d) => s + d.amount, 0)) : '-'}</div>
      </div>
    </div>

    <div class="chart-grid">
      <div class="chart-card full-width">
        <h3>Cotacao de Mercado</h3>
        <canvas id="chart-price-history" height="300"></canvas>
      </div>
      <div class="chart-card">
        <h3>Distribuicoes (R$/cota)</h3>
        <canvas id="chart-distributions" height="280"></canvas>
      </div>
      <div class="chart-card">
        <h3>Volume Financeiro Mensal (R$)</h3>
        <canvas id="chart-volume" height="280"></canvas>
      </div>
    </div>
  `;

  // --- Price chart ---
  if (prices.length > 1) {
    const step = Math.max(1, Math.floor(prices.length / 300));
    const sampled = prices.filter((_, i) => i % step === 0 || i === prices.length - 1);

    createLineChart('chart-price-history',
      sampled.map(p => p.date),
      [{
        label: 'Fechamento (R$)',
        data: sampled.map(p => p.close),
        borderColor: fundColor,
        backgroundColor: fundColor + '15',
        fill: true,
        tension: 0.2,
        pointRadius: 0,
        borderWidth: 2,
      }],
      {
        scales: {
          x: { ticks: { color: getThemeVar('--chart-text-muted'), maxTicksLimit: 10, maxRotation: 0 }, grid: { color: getThemeVar('--chart-grid') } },
          y: { ticks: { color: getThemeVar('--chart-text-muted'), callback: v => 'R$ ' + v.toFixed(0) }, grid: { color: getThemeVar('--chart-grid') } },
        },
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => 'R$ ' + ctx.raw.toFixed(2) } } },
      }
    );
  } else {
    document.getElementById('chart-price-history')?.parentElement
      ?.insertAdjacentHTML('beforeend', '<div class="empty">Sem dados de cotacao</div>');
  }

  // --- Distributions chart ---
  if (divs.length) {
    createBarChart('chart-distributions',
      divs.map(d => d.ex_date),
      [{
        data: divs.map(d => d.amount),
        backgroundColor: fundColor + '70',
        borderColor: fundColor,
        borderWidth: 1,
        borderRadius: 2,
      }],
      {
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: getThemeVar('--chart-text-muted'), maxTicksLimit: 12, maxRotation: 45 }, grid: { display: false } },
          y: { ticks: { color: getThemeVar('--chart-text-muted'), callback: v => 'R$ ' + v.toFixed(2) }, grid: { color: getThemeVar('--chart-grid') } },
        },
      }
    );
  }

  // --- Volume chart ---
  if (prices.length > 1) {
    const monthlyVol = {};
    for (const p of prices) {
      const m = p.date.substring(0, 7);
      if (!monthlyVol[m]) monthlyVol[m] = { sum: 0, count: 0 };
      monthlyVol[m].sum += (p.close || 0) * (p.volume || 0);
      monthlyVol[m].count++;
    }
    const months = Object.keys(monthlyVol).sort();
    const avgVol = months.map(m => monthlyVol[m].count > 0
      ? Math.round(monthlyVol[m].sum / monthlyVol[m].count) : 0);

    createBarChart('chart-volume',
      months.map(m => formatRefDate(m)),
      [{
        data: avgVol,
        backgroundColor: CHART_COLORS.blue + '50',
        borderColor: CHART_COLORS.blue,
        borderWidth: 1,
        borderRadius: 2,
      }],
      {
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: getThemeVar('--chart-text-muted'), maxTicksLimit: 12, maxRotation: 45 }, grid: { display: false } },
          y: { ticks: { color: getThemeVar('--chart-text-muted'), callback: v => v >= 1e6 ? (v/1e6).toFixed(1)+'M' : v >= 1e3 ? (v/1e3).toFixed(0)+'K' : v }, grid: { color: getThemeVar('--chart-grid') } },
        },
      }
    );
  }
}

// =====================================================================
// Tab: Comparativo
// =====================================================================

async function loadComparative() {
  if (!fundsData.length) fundsData = await fetchAPI('funds');
  renderComparative(fundsData);
}

function renderComparative(funds) {
  const withPrice = funds.filter(f => f.market_price);

  // DY ranking (all funds with dividends)
  const withDY = withPrice.filter(f => f.dividend_yield_ltm != null && f.dividend_yield_ltm > 0)
    .sort((a, b) => b.dividend_yield_ltm - a.dividend_yield_ltm);
  if (withDY.length) {
    createHBarChart('chart-ranking-dy',
      withDY.map(f => f.ticker),
      withDY.map(f => f.dividend_yield_ltm),
      CHART_COLORS.green
    );
  }

  // Discount ranking
  const withDiscount = funds.filter(f => f.discount_premium_pct != null)
    .sort((a, b) => a.discount_premium_pct - b.discount_premium_pct);
  if (withDiscount.length) {
    createHBarChart('chart-ranking-discount',
      withDiscount.map(f => f.ticker),
      withDiscount.map(f => f.discount_premium_pct),
      CHART_COLORS.red
    );
  }

  // Dividends total ranking
  const withDivs = withPrice.filter(f => f.div_total > 0)
    .sort((a, b) => b.div_total - a.div_total);
  if (withDivs.length) {
    createHBarChart('chart-ranking-divs',
      withDivs.map(f => f.ticker),
      withDivs.map(f => f.div_total),
      CHART_COLORS.blue
    );
  }
}

// =====================================================================
// Theme
// =====================================================================

window.reloadCharts = () => {
  if (fundsData.length) renderOverviewCharts(fundsData);
  if (fundDetail) renderFundDetail(fundDetail);
};
