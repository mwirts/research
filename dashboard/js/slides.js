/** Presentation / Slides mode */

let slides = [];
let currentSlide = 0;

function generateSlides() {
  if (!fundsData.length) return [];

  const today = new Date().toLocaleDateString('pt-BR');
  const withPrice = fundsData.filter(f => f.market_price);
  const withTir = fundsData.filter(f => f.irr_real != null).sort((a, b) => b.irr_real - a.irr_real);
  const withDY = fundsData.filter(f => f.dividend_yield_ltm > 0).sort((a, b) => b.dividend_yield_ltm - a.dividend_yield_ltm);

  const sl = [];

  // Slide 1: Cover
  sl.push({
    title: 'Analise FIP-IE',
    content: `
      <div style="text-align:center;padding:60px 0">
        <h1 style="font-size:48px;font-weight:300;letter-spacing:6px;color:var(--green-500);margin-bottom:16px">ANALISE FIP-IE</h1>
        <p style="font-size:20px;color:var(--text-muted);letter-spacing:3px;text-transform:uppercase">Comite de Investimentos</p>
        <p style="font-size:14px;color:var(--text-muted);margin-top:40px">${today}</p>
        <p style="font-size:12px;color:var(--text-muted);margin-top:8px">${withPrice.length} fundos monitorados</p>
      </div>`,
  });

  // Slide 2: Overview table
  const tableRows = fundsData.map(f => `
    <tr>
      <td class="ticker">${f.ticker}</td>
      <td class="text">${shortName(f.fund_name)}</td>
      <td class="right">${f.market_price ? formatBRL(f.market_price) : '-'}</td>
      <td class="right">${f.irr_real != null ? formatPct(f.irr_real) : '-'}</td>
      <td class="right">${f.dividend_yield_ltm != null ? formatPct(f.dividend_yield_ltm) : '-'}</td>
      <td class="right">${f.discount_premium_pct != null ? formatPct(f.discount_premium_pct) : '-'}</td>
      <td class="right">${f.div_total != null ? 'R$ ' + formatNumber(f.div_total) : '-'}</td>
    </tr>`).join('');

  sl.push({
    title: 'Visao Geral',
    content: `
      <div class="sheet-wrap">
        <table class="sheet">
          <thead><tr><th>Ticker</th><th>Fundo</th><th>Preco</th><th>TIR (IPCA+)</th><th>DY (LTM)</th><th>Desconto</th><th>Div. Acum.</th></tr></thead>
          <tbody>${tableRows}</tbody>
        </table>
      </div>`,
  });

  // Slide 3: Highlights
  const highlights = [];
  if (withTir.length) highlights.push(`Maior TIR implicita: <strong>${withTir[0].ticker}</strong> com IPCA+ ${formatPct(withTir[0].irr_real)}`);
  if (withDY.length) highlights.push(`Maior DY: <strong>${withDY[0].ticker}</strong> com ${formatPct(withDY[0].dividend_yield_ltm)} (LTM)`);
  const discounted = fundsData.filter(f => f.discount_premium_pct != null && f.discount_premium_pct < 0)
    .sort((a, b) => a.discount_premium_pct - b.discount_premium_pct);
  if (discounted.length) highlights.push(`Maior desconto: <strong>${discounted[0].ticker}</strong> com ${formatPct(discounted[0].discount_premium_pct)}`);

  sl.push({
    title: 'Destaques',
    content: `
      <ul style="list-style:none;padding:0">
        ${highlights.map(h => `<li style="padding:16px 0;border-bottom:1px solid var(--border);font-size:18px;color:var(--text)">${h}</li>`).join('')}
      </ul>`,
  });

  // Slides 4+: One per fund
  for (const f of fundsData) {
    if (!f.market_price) continue;
    const metrics = [
      ['Preco Mercado', f.market_price ? formatBRL(f.market_price) : '-'],
      ['VP / Cota', f.nav_per_unit ? formatBRL(f.nav_per_unit) : '-'],
      ['Desconto', f.discount_premium_pct != null ? formatPct(f.discount_premium_pct) : '-'],
      ['TIR (IPCA+)', f.irr_real != null ? formatPct(f.irr_real) : '-'],
      ['DY (LTM)', f.dividend_yield_ltm != null ? formatPct(f.dividend_yield_ltm) : '-'],
      ['Div. Acumulados', f.div_total != null ? 'R$ ' + formatNumber(f.div_total) : '-'],
    ];

    sl.push({
      title: f.ticker,
      content: `
        <p style="color:var(--text-muted);margin-bottom:20px;font-size:14px">${f.fund_name} | ${f.manager} | ${f.segment}</p>
        <div class="kpi-grid" style="margin-bottom:24px">
          ${metrics.map(([label, value]) => `
            <div class="kpi-card">
              <div class="kpi-label">${label}</div>
              <div class="kpi-value" style="font-size:18px;color:var(--text)">${value}</div>
            </div>`).join('')}
        </div>`,
    });
  }

  // Last slide
  sl.push({
    title: 'Obrigado',
    content: `
      <div style="text-align:center;padding:80px 0">
        <p style="font-size:24px;color:var(--text-muted);letter-spacing:2px">FIM DA APRESENTACAO</p>
        <p style="font-size:14px;color:var(--text-muted);margin-top:20px">${today}</p>
      </div>`,
  });

  return sl;
}

function renderSlidesPreview() {
  slides = generateSlides();
  const container = document.getElementById('slides-preview');
  if (!container) return;

  container.innerHTML = slides.map((s, i) => `
    <div class="card" style="cursor:pointer;margin-bottom:12px" onclick="startPresentation(${i})">
      <div style="display:flex;align-items:center;gap:16px">
        <span style="color:var(--text-muted);font-size:12px;min-width:30px">${i + 1}.</span>
        <span style="color:var(--green-500);font-weight:500">${s.title}</span>
      </div>
    </div>
  `).join('');
}

function startPresentation(startAt) {
  slides = generateSlides();
  if (!slides.length) return;
  currentSlide = startAt || 0;
  document.getElementById('slide-overlay').style.display = 'block';
  showSlide(currentSlide);
  document.addEventListener('keydown', handleSlideKey);
}

function exitPresentation() {
  document.getElementById('slide-overlay').style.display = 'none';
  document.removeEventListener('keydown', handleSlideKey);
}

function showSlide(idx) {
  if (idx < 0 || idx >= slides.length) return;
  currentSlide = idx;
  const s = slides[idx];
  document.getElementById('slide-container').innerHTML = `
    <div style="max-width:1000px;width:100%">
      <h2 style="font-size:28px;font-weight:300;color:var(--green-500);letter-spacing:3px;text-transform:uppercase;margin-bottom:24px;padding-bottom:12px;border-bottom:2px solid var(--green-900)">${s.title}</h2>
      ${s.content}
    </div>`;
  document.getElementById('slide-counter').textContent = `${idx + 1} / ${slides.length}`;
}

function nextSlide() { if (currentSlide < slides.length - 1) showSlide(currentSlide + 1); }
function prevSlide() { if (currentSlide > 0) showSlide(currentSlide - 1); }

function handleSlideKey(e) {
  if (e.key === 'ArrowRight' || e.key === ' ') nextSlide();
  else if (e.key === 'ArrowLeft') prevSlide();
  else if (e.key === 'Escape') exitPresentation();
}

// Load slides preview when tab is clicked
document.addEventListener('DOMContentLoaded', () => {
  document.querySelector('[data-tab="presentation"]')
    ?.addEventListener('click', () => renderSlidesPreview());
});
