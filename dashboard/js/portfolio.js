/** Portfolio / Carteira tab — detailed fund view with retroactive date selection */

let portDates = [];
let portSnapshot = null;
let portTicker = null;

document.addEventListener('DOMContentLoaded', () => {
  document.querySelector('[data-tab="portfolio"]')
    ?.addEventListener('click', initPortfolioTab);
});

function initPortfolioTab() {
  if (!fundsData.length) return;
  const pills = document.getElementById('portfolio-pills');
  if (pills && !pills.dataset.init) {
    pills.innerHTML = fundsData.map(f =>
      `<button class="fund-pill" data-ticker="${f.ticker}" onclick="loadPortfolioFund('${f.ticker}')">${f.ticker}</button>`
    ).join('');
    pills.dataset.init = '1';
  }
}

async function loadPortfolioFund(ticker) {
  portTicker = ticker;
  document.querySelectorAll('#portfolio-pills .fund-pill').forEach(p =>
    p.classList.toggle('active', p.dataset.ticker === ticker));

  const container = document.getElementById('portfolio-content');
  container.innerHTML = '<div class="loading">Carregando carteira...</div>';

  try {
    const [dates, snapshot] = await Promise.all([
      fetchAPI(`portfolio/${ticker}/dates`),
      fetchAPI(`portfolio/${ticker}/snapshot`),
    ]);
    portDates = dates;
    portSnapshot = snapshot;
    renderPortfolio(ticker, snapshot, dates);
  } catch (e) {
    container.innerHTML = `<div class="empty">Erro: ${e.message}</div>`;
  }
}

async function changePortfolioDate(ticker, date) {
  try {
    const snapshot = await fetchAPI(`portfolio/${ticker}/snapshot?date=${date}`);
    portSnapshot = snapshot;
    renderPortfolio(ticker, snapshot, portDates);
  } catch (e) {
    document.getElementById('portfolio-content').innerHTML =
      `<div class="empty">Erro ao carregar ${date}: ${e.message}</div>`;
  }
}

function renderPortfolio(ticker, snap, dates) {
  const container = document.getElementById('portfolio-content');
  const s = snap.snapshot || {};
  const currentDate = snap.date;

  let html = '';

  // --- Date selector ---
  html += `<div class="card" style="padding:12px 20px;display:flex;align-items:center;gap:16px;flex-wrap:wrap">
    <span style="color:var(--text-muted);font-size:12px;text-transform:uppercase;letter-spacing:1px">Relatorio de:</span>
    <select id="port-date-select" onchange="changePortfolioDate('${ticker}', this.value)"
      style="background:var(--input-bg);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:6px 12px;font-family:inherit;font-size:13px">
      ${dates.slice().reverse().map(d => `<option value="${d}" ${d === currentDate ? 'selected' : ''}>${formatRefDate(d)}</option>`).join('')}
    </select>
    <span style="color:var(--green-500);font-weight:500;font-size:14px">${snap.fund_info?.fund_name || ticker}</span>
    <span style="color:var(--text-muted);font-size:12px;margin-left:auto">${snap.fund_info?.manager || ''} | ${snap.fund_info?.segment || ''}</span>
  </div>`;

  // --- Assets ---
  const hasTrans = snap.transmission?.length;
  const hasGen = snap.generation?.length;
  const hasPort = snap.port?.length;
  const hasHold = snap.holdings?.length;

  if (hasTrans || hasGen || hasPort || hasHold) {
    html += '<div class="card"><h2>Ativos em Carteira</h2>';

    if (hasTrans) {
      html += sectionTitle('Transmissao');
      html += `<div class="sheet-wrap" style="margin-bottom:16px"><table class="sheet">
        <thead><tr><th>Ativo</th><th>Extensao</th><th>RAP Anual</th><th>Concessao</th><th>Disponib.</th><th>EBITDA</th><th>Receita</th></tr></thead><tbody>
        ${snap.transmission.map(a => `<tr>
          <td class="text ticker">${a.asset_name}</td>
          <td class="right">${a.extension_km ? a.extension_km + ' km' : '-'}</td>
          <td class="right">${a.rap_annual_brl ? formatBRLCompact(a.rap_annual_brl) : '-'}</td>
          <td class="center">${a.concession_end || '-'}</td>
          <td class="right ${a.availability_pct >= 99 ? 'positive' : ''}">${a.availability_pct != null ? formatPct(a.availability_pct) : '-'}</td>
          <td class="right">${a.ebitda_brl ? formatBRLCompact(a.ebitda_brl) : '-'}</td>
          <td class="right">${a.revenue_brl ? formatBRLCompact(a.revenue_brl) : '-'}</td>
        </tr>`).join('')}
      </tbody></table></div>`;
    }

    if (hasGen) {
      html += sectionTitle('Geracao');
      html += `<div class="sheet-wrap" style="margin-bottom:16px"><table class="sheet">
        <thead><tr><th>Ativo</th><th>Tipo</th><th>Capac.</th><th>Geracao</th><th>Disponib.</th><th>Curtailm.</th><th>PPA</th><th>Venc. PPA</th></tr></thead><tbody>
        ${snap.generation.map(a => `<tr>
          <td class="text ticker">${a.asset_name}</td>
          <td class="center">${({eolica:'Eolica',solar:'Solar',hidrica:'Hidrica'})[a.gen_type] || a.gen_type}</td>
          <td class="right">${a.capacity_mw ? a.capacity_mw + ' MW' : '-'}</td>
          <td class="right">${a.generation_mwm != null ? formatNumber(a.generation_mwm, 1) + ' MWm' : '-'}</td>
          <td class="right ${a.availability_pct >= 99 ? 'positive' : ''}">${a.availability_pct != null ? formatPct(a.availability_pct) : '-'}</td>
          <td class="right ${a.curtailment_mwm > 0 ? 'negative' : ''}">${a.curtailment_mwm != null ? formatNumber(a.curtailment_mwm, 1) + ' MWm' : '-'}</td>
          <td class="right">${a.ppa_price_brl_mwh ? 'R$ ' + formatNumber(a.ppa_price_brl_mwh, 0) : '-'}</td>
          <td class="center">${a.ppa_end_date || '-'}</td>
        </tr>`).join('')}
      </tbody></table></div>`;
    }

    if (hasPort) {
      html += sectionTitle('Portuario');
      html += `<div class="sheet-wrap" style="margin-bottom:16px"><table class="sheet">
        <thead><tr><th>Ativo</th><th>TEUs/mes</th><th>Receita</th><th>EBITDA</th><th>Margem</th><th>Lucro Liq.</th><th>Div.Liq/EBITDA</th></tr></thead><tbody>
        ${snap.port.map(a => `<tr>
          <td class="text ticker">${a.asset_name}</td>
          <td class="right">${a.teus_month ? formatInt(a.teus_month) : '-'}</td>
          <td class="right">${a.revenue_brl ? formatBRLCompact(a.revenue_brl) : '-'}</td>
          <td class="right">${a.ebitda_brl ? formatBRLCompact(a.ebitda_brl) : '-'}</td>
          <td class="right">${a.ebitda_margin_pct != null ? formatPct(a.ebitda_margin_pct) : '-'}</td>
          <td class="right">${a.net_income_brl ? formatBRLCompact(a.net_income_brl) : '-'}</td>
          <td class="right">${a.net_debt_ebitda != null ? formatNumber(a.net_debt_ebitda, 1) + 'x' : '-'}</td>
        </tr>`).join('')}
      </tbody></table></div>`;
    }

    if (hasHold) {
      html += sectionTitle('Portfolio de Credito');
      html += `<div class="sheet-wrap" style="margin-bottom:16px"><table class="sheet">
        <thead><tr><th>Emissor</th><th>Segmento</th><th>Tipo</th><th>Ticker</th><th>% PL</th><th>Valor</th><th>Duration</th><th>Indexador</th><th>Spread</th></tr></thead><tbody>
        ${snap.holdings.map(a => `<tr>
          <td class="text">${a.issuer}</td>
          <td class="text center" style="font-size:11px">${a.segment || '-'}</td>
          <td class="center" style="font-size:11px">${a.instrument_type || '-'}</td>
          <td class="ticker center">${a.ticker || '-'}</td>
          <td class="right">${a.pct_pl != null ? formatPct(a.pct_pl) : '-'}</td>
          <td class="right">${a.amount_brl ? formatBRLCompact(a.amount_brl) : '-'}</td>
          <td class="right">${a.duration_years != null ? formatNumber(a.duration_years, 1) + 'a' : '-'}</td>
          <td class="center">${a.indexer || '-'}</td>
          <td class="right">${a.spread_pct != null ? formatPct(a.spread_pct) : '-'}</td>
        </tr>`).join('')}
      </tbody></table></div>`;
    }

    html += '</div>';
  }

  // --- Commentary ---
  const comm = snap.commentaries || {};
  if (Object.keys(comm).length) {
    html += '<div class="card"><h2>Comentarios do Gestor</h2>';
    for (const [section, text] of Object.entries(comm)) {
      const label = ({macro:'Cenario Macro',strategy:'Estrategia',portfolio:'Portfolio',highlights:'Destaques'})[section] || section;
      html += `<h3 style="color:var(--green-300);margin:12px 0 6px;font-size:12px;text-transform:uppercase;letter-spacing:1px">${label}</h3>
        <p style="color:var(--text);font-size:13px;line-height:1.7;white-space:pre-line;max-height:400px;overflow-y:auto;padding-right:8px">${text}</p>`;
    }
    html += '</div>';
  }

  container.innerHTML = html;
}

function sectionTitle(text) {
  return `<h3 style="color:var(--green-300);margin:12px 0 8px;font-size:13px;text-transform:uppercase;letter-spacing:1px">${text}</h3>`;
}
