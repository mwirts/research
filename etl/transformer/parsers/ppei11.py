"""Parser for PPEI11 (Prisma Proton Energia FIP-IE) monthly reports.

Layout (12 pages, stable):
  Page 1: Fund characteristics (left) + Highlights/financial summary (right)
  Page 2: Operational results (generation by asset, ranking)
  Page 3: Generation charts per asset (Angico, Malta, Esmeralda, Sobrado)
  Page 4: Availability charts per asset
  Page 5: Financial results consolidated (P&L)
  Page 6: Debt composition, cash position, enterprise value
  Page 7: Performance (cota chart, volume)
  Page 8: Dividend history, dividend projections
  Page 9: Dividend projections 2026
  Page 10: TIR calculator
  Page 11: Spread over NTN-B
  Page 12: Corporate structure, asset details
"""

from __future__ import annotations

import re
from datetime import date

from etl.transformer.models import (
    Distribution,
    Fund,
    FundMetricKV,
    FundReport,
    GenerationAsset,
    ManagerCommentary,
    MonthlySnapshot,
)
from etl.transformer.parsers.base import (
    BaseFundParser,
    parse_brl,
    parse_date_pt,
    parse_full_date,
    parse_integer,
    parse_pct,
)


class PPEI11Parser(BaseFundParser):
    FUND_ID = "ppei11"
    TICKER = "PPEI11"
    PARSER_VERSION = "1.0"

    # Known asset capacities from page 12 (stable data)
    ASSET_CAPACITIES = {
        "Angico": {"capacity_mw": 27.2, "capacity_mwp": 31.6},
        "Malta": {"capacity_mw": 27.2, "capacity_mwp": 31.6},
        "Esmeralda": {"capacity_mw": 29.0, "capacity_mwp": 34.6},
        "Sobrado": {"capacity_mw": 30.0, "capacity_mwp": 35.5},
    }

    def parse_raw(self, raw_data: dict) -> FundReport:
        meta = self._get_metadata(raw_data)
        ref_date = f"{meta['year']:04d}-{meta['month']:02d}"
        source_pdf = meta.get("source_pdf", "")

        page1 = self._get_page_text(raw_data, 1)
        page2 = self._get_page_text(raw_data, 2)
        page4 = self._get_page_text(raw_data, 4)
        page5 = self._get_page_text(raw_data, 5)
        page6 = self._get_page_text(raw_data, 6)
        page7 = self._get_page_text(raw_data, 7)
        page8 = self._get_page_text(raw_data, 8)
        page12 = self._get_page_text(raw_data, 12)

        # -- Fund info --
        fund = self._parse_fund_info(page1, page12)

        # -- Monthly snapshot --
        snapshot = self._parse_snapshot(
            page1, page5, page6, page7, page8, raw_data, ref_date, source_pdf, meta,
        )

        # -- Generation assets --
        generation_assets = self._parse_generation_assets(
            page2, page4, page12, raw_data, ref_date, source_pdf,
        )

        # -- Distributions --
        distributions = self._parse_distributions(page8, raw_data, ref_date)

        # -- Commentary --
        commentaries = self._parse_commentary(page1, ref_date)

        # -- Extra metrics --
        extra_metrics = self._parse_extra_metrics(
            page1, page5, page6, page7, ref_date, source_pdf,
        )

        return FundReport(
            fund=fund,
            snapshot=snapshot,
            distributions=distributions,
            generation_assets=generation_assets,
            commentaries=commentaries,
            extra_metrics=extra_metrics,
        )

    def _parse_fund_info(self, page1: str, page12: str) -> Fund:
        """Parse fund info from page 1."""
        return Fund(
            fund_id=self.FUND_ID,
            ticker=self.TICKER,
            fund_name="Prisma Proton Energia FIP-IE",
            manager="Prisma Private Equity",
            administrator="BTG Pactual Serviços Financeiros",
            segment="geracao_solar",
            inception_date=date(2020, 10, 28),
            fund_term="Indeterminado",
            target_audience="Investidores Qualificados",
            admin_fee_pct=0.10,
            mgmt_fee_pct=1.00,
            perf_fee_pct=None,
        )

    def _parse_snapshot(
        self,
        page1: str,
        page5: str,
        page6: str,
        page7: str,
        page8: str,
        raw_data: dict,
        ref_date: str,
        source_pdf: str,
        meta: dict,
    ) -> MonthlySnapshot:
        # --- Page 1: Key metrics ---
        # Número de Cotistas: 2,350
        num_investors = None
        m = re.search(r"N[úu]mero\s+de\s+Cotistas:\s*([\d.,]+)", page1)
        if m:
            num_investors = parse_integer(m.group(1).replace(",", "."))

        # "Valor2 Patrimonial da Cota | do Fundo: ..."
        # "R$ 77.10 | R$ 340.0 mm"
        nav_per_unit = None
        nav_total = None
        m = re.search(
            r"Valor\d*\s+Patrimonial\s+da\s+Cota\s*\|\s*do\s+Fundo:.*?\n\s*R\$\s*([\d.,]+)\s*\|\s*R\$\s*([\d.,]+)\s*mm",
            page1, re.DOTALL,
        )
        if m:
            nav_per_unit = parse_brl(m.group(1))
            val = parse_brl(m.group(2))
            if val:
                nav_total = val * 1_000_000

        # "Valor de Mercado da Cota | do Fundo: ..."
        # "R$ 84.45 | R$ 380.0 mm"
        market_price = None
        market_cap = None
        m = re.search(
            r"Valor\s+de\s+Mercado\s+da\s+Cota\s*\|\s*do\s+Fundo:.*?\n\s*R\$\s*([\d.,]+)\s*\|\s*R\$\s*([\d.,]+)\s*mm",
            page1, re.DOTALL,
        )
        if m:
            market_price = parse_brl(m.group(1))
            val = parse_brl(m.group(2))
            if val:
                market_cap = val * 1_000_000

        # Units: "4,500,000" (US-style commas as thousands separators)
        units = None
        m = re.search(r"Quantidade\s+de\s+Cotas:\s*([\d,]+)", page1)
        if m:
            try:
                units = int(m.group(1).replace(",", ""))
            except ValueError:
                pass

        # --- Page 6: Enterprise value table ---
        # "Valor da Cota em 28-fev-26 [R$] 80.25"
        # This is sometimes a more accurate market_price
        ev_tables = self._get_page_tables(raw_data, 6)
        if ev_tables:
            for table in ev_tables:
                for row in table:
                    cells = [str(c).strip() if c else "" for c in row]
                    joined = " ".join(cells)
                    if "Valor da Cota" in joined:
                        m = re.search(r"([\d.]+)", cells[-2] if len(cells) > 1 else cells[-1])
                        if m:
                            val = parse_brl(m.group(1))
                            if val and 50 < val < 200:
                                # This is the "Valor da Cota" at month end
                                pass  # market_price from page 1 is more reliable

        # --- Page 7: Performance ---
        # "o Fundo distribuiu R$ 163.8 mm em dividendos acumulados, ou R$ 36.39/cota"
        dist_total_accum = None
        dist_per_unit_accum = None
        m = re.search(r"distribui.*?R\$\s*([\d.,]+)\s*/\s*cota", page7)
        if m:
            dist_per_unit_accum = parse_brl(m.group(1))

        # Volume: "R$ 0.31 mm nos últimos 90 dias"
        avg_volume = None
        m = re.search(r"volume\s+m[ée]dio\s+di[áa]rio.*?R\$\s*([\d.,]+)\s*mm", page7, re.IGNORECASE)
        if m:
            val = parse_brl(m.group(1))
            if val:
                avg_volume = val * 1_000_000

        # --- Page 8: Distributions ---
        # Total distributions: "TOTAL R$ 36.39 R$ 160.48"
        m = re.search(r"TOTAL\s+R\$\s*([\d.,]+)\s+R\$\s*([\d.,]+)", page8)
        if m:
            dist_per_unit_accum = parse_brl(m.group(1))

        # Dividend yield from page 8: "dividend yield de 15.5%"
        dividend_yield = None
        m = re.search(r"dividend\s+yield\s+de\s+([\d.,]+)\s*%", page8, re.IGNORECASE)
        if m:
            dividend_yield = parse_pct(m.group(1) + "%")

        # Distribution projection 2026: "R$ 13.40/cota"
        dist_projection = None
        m = re.search(r"distribui[çc][ãa]o\s+total\s+de\s+dividendos\s+[ée]\s+de\s+R\$\s*([\d.,]+)\s*/\s*cota", page1, re.IGNORECASE)
        if not m:
            m = re.search(r"R\$\s*([\d.,]+)\s*/\s*cota\s+Classe\s+A", page8)
        if m:
            dist_projection = parse_brl(m.group(1))

        # Discount/Premium
        discount_pct = None
        if market_price and nav_per_unit and nav_per_unit > 0:
            discount_pct = round((market_price - nav_per_unit) / nav_per_unit * 100, 2)

        # Price to book
        price_to_book = None
        if market_price and nav_per_unit and nav_per_unit > 0:
            price_to_book = round(market_price / nav_per_unit, 2)

        return MonthlySnapshot(
            fund_id=self.FUND_ID,
            reference_date=ref_date,
            nav_total=nav_total,
            market_cap=market_cap,
            nav_per_unit=nav_per_unit,
            market_price=market_price,
            units_outstanding=units,
            discount_premium_pct=discount_pct,
            price_to_book=price_to_book,
            distribution_total_accum=dist_per_unit_accum,
            dividend_yield_ltm=dividend_yield,
            avg_daily_volume=avg_volume,
            num_investors=num_investors,
            source_pdf=source_pdf,
            page_count=meta.get("page_count"),
        )

    def _parse_generation_assets(
        self,
        page2: str,
        page4: str,
        page12: str,
        raw_data: dict,
        ref_date: str,
        source_pdf: str,
    ) -> list[GenerationAsset]:
        """Parse generation asset data from pages 2, 4, and 12."""
        assets = []

        # --- Page 2: Generation data table ---
        # Table format: Geração Média | Angico | Malta | Esmeralda | Sobrado
        gen_tables = self._get_page_tables(raw_data, 2)
        gen_realized = {}
        gen_contracted = {}

        if gen_tables:
            for table in gen_tables:
                for row in table:
                    cells = [str(c).strip() if c else "" for c in row]
                    joined = " ".join(cells).lower()

                    if "realizada" in joined and "mwm" in joined:
                        # "Geração Realizada (MWm) 7.85 7.88 7.46 8.15"
                        nums = []
                        for c in cells[1:]:
                            val = parse_brl(c)
                            if val is not None:
                                nums.append(val)
                        if len(nums) >= 4:
                            gen_realized = {
                                "Angico": nums[0],
                                "Malta": nums[1],
                                "Esmeralda": nums[2],
                                "Sobrado": nums[3],
                            }

                    if "contratada" in joined and "mwm" in joined:
                        nums = []
                        for c in cells[1:]:
                            val = parse_brl(c)
                            if val is not None:
                                nums.append(val)
                        if len(nums) >= 4:
                            gen_contracted = {
                                "Angico": nums[0],
                                "Malta": nums[1],
                                "Esmeralda": nums[2],
                                "Sobrado": nums[3],
                            }

        # Fallback: parse from text
        if not gen_realized:
            m = re.search(
                r"Gera[çc][ãa]o\s+Realizada\s+\(MWm\)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
                page2,
            )
            if m:
                gen_realized = {
                    "Angico": parse_brl(m.group(1)),
                    "Malta": parse_brl(m.group(2)),
                    "Esmeralda": parse_brl(m.group(3)),
                    "Sobrado": parse_brl(m.group(4)),
                }

        if not gen_contracted:
            m = re.search(
                r"Gera[çc][ãa]o\s+Contratada\s+\(MWm\)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
                page2,
            )
            if m:
                gen_contracted = {
                    "Angico": parse_brl(m.group(1)),
                    "Malta": parse_brl(m.group(2)),
                    "Esmeralda": parse_brl(m.group(3)),
                    "Sobrado": parse_brl(m.group(4)),
                }

        # --- Page 4: Availability ---
        availability = {}
        for asset_name in ["Angico", "Malta", "Esmeralda", "Sobrado"]:
            # Look for "Disponibilidade média realizada nos últimos 12 meses – 99.6%"
            m = re.search(
                rf"{asset_name}.*?Disponibilidade\s+m[ée]dia\s+realizada\s+nos\s+[úu]ltimos\s+12\s+meses\s*[–-]\s*([\d.,]+)\s*%",
                page4,
                re.DOTALL,
            )
            if m:
                availability[asset_name] = parse_pct(m.group(1) + "%")

        # --- Page 12: PPA prices ---
        # Format: "Angico\n• Munícipio/UF: ...\n• PPA até jul-2037 a R$499.3/MWh"
        # Each asset has its own section starting with "Name\n• Munícipio"
        ppa_prices = {}
        ppa_end_dates = {}
        for asset_name in ["Angico", "Malta", "Esmeralda", "Sobrado"]:
            # Find the section starting with "Name\n• Munícipio"
            for m_name in re.finditer(rf"{asset_name}\n", page12):
                snippet = page12[m_name.start():m_name.start() + 300]
                if "Mun" in snippet[:50]:
                    m = re.search(
                        r"PPA\s+at[ée]\s+(\S+)\s+a\s+R\$\s*([\d.,]+)\s*/?\s*MWh",
                        snippet,
                    )
                    if m:
                        ppa_end_dates[asset_name] = m.group(1)
                        ppa_prices[asset_name] = parse_brl(m.group(2))
                    break

        # Build assets
        for asset_name in ["Angico", "Malta", "Esmeralda", "Sobrado"]:
            caps = self.ASSET_CAPACITIES.get(asset_name, {})

            assets.append(GenerationAsset(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                asset_name=asset_name,
                gen_type="solar",
                capacity_mw=caps.get("capacity_mw"),
                generation_mwm=gen_realized.get(asset_name),
                contracted_mwm=gen_contracted.get(asset_name),
                availability_pct=availability.get(asset_name),
                ppa_price_brl_mwh=ppa_prices.get(asset_name),
                ppa_end_date=ppa_end_dates.get(asset_name),
                source_pdf=source_pdf,
            ))

        return assets

    def _parse_distributions(
        self, page8: str, raw_data: dict, ref_date: str,
    ) -> list[Distribution]:
        """Parse distribution history from page 8."""
        distributions = []

        # Try table first
        tables = self._get_page_tables(raw_data, 8)
        if tables:
            for table in tables:
                for row in table:
                    cells = [str(c).strip() if c else "" for c in row]
                    joined = " ".join(cells)

                    # Look for rows with dates and R$ values
                    date_match = re.search(r"(\d{2}-\w{3}-\d{2})", joined)
                    amount_match = re.search(r"R\$\s*([\d.,]+)", joined)

                    if date_match and amount_match:
                        amt = parse_brl(amount_match.group(1))
                        if amt and 0 < amt <= 10:  # filter out projection totals
                            # Parse the date (communication date)
                            raw_date = date_match.group(1)
                            # Convert "09-fev-21" to date
                            m2 = re.match(r"(\d{2})-(\w{3})-(\d{2})", raw_date)
                            if m2:
                                ref_month = parse_date_pt(f"{m2.group(2)}/{m2.group(3)}")
                                if ref_month:
                                    distributions.append(Distribution(
                                        fund_id=self.FUND_ID,
                                        reference_month=ref_month,
                                        amount_per_unit=amt,
                                    ))

        # Fallback: parse from text
        if not distributions:
            # Pattern: "1ª Distribuição 09-fev-21 ... R$ 1.89"
            for m in re.finditer(
                r"\d+[ªa]\s+Distribui[çc][ãa]o\s+(\d{2})-(\w{3})-(\d{2})\s+.*?R\$\s*([\d.,]+)",
                page8,
            ):
                day, month_abbr, year_short = m.group(1), m.group(2), m.group(3)
                amt = parse_brl(m.group(4))
                ref_month = parse_date_pt(f"{month_abbr}/{year_short}")
                if ref_month and amt and amt > 0:
                    distributions.append(Distribution(
                        fund_id=self.FUND_ID,
                        reference_month=ref_month,
                        amount_per_unit=amt,
                    ))

        return distributions

    def _parse_commentary(self, page1: str, ref_date: str) -> list[ManagerCommentary]:
        """Extract highlights from page 1."""
        commentaries = []

        # Highlights are bullet points after "DESTAQUES"
        m = re.search(
            r"DESTAQUES\s*\n(.*?)(?:DESTAQUES\s+DE\s+MERCADO|Em\s+R\$\s+mm)",
            page1,
            re.DOTALL,
        )
        if m:
            text = m.group(1).strip()
            # Extract bullet points
            bullets = re.findall(r"[▪•]\s*(.+?)(?=[▪•]|\Z)", text, re.DOTALL)
            if bullets:
                content = "\n".join(
                    "- " + re.sub(r"\s+", " ", b.strip()) for b in bullets if b.strip()
                )
                if content:
                    commentaries.append(ManagerCommentary(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        section="highlights",
                        content=content,
                    ))

        return commentaries

    def _parse_extra_metrics(
        self,
        page1: str,
        page5: str,
        page6: str,
        page7: str,
        ref_date: str,
        source_pdf: str,
    ) -> list[FundMetricKV]:
        """Extract extra metrics."""
        metrics = []

        # Total generation from page 5 P&L table:
        # "Geração (MWh) 19,010" - uses comma as thousands separator
        m = re.search(r"Gera[çc][ãa]o\s+\(MWh\)\s+([\d,]+)", page5)
        if m:
            val = parse_integer(m.group(1).replace(",", ""))
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="generation_mwh_month",
                    metric_value=float(val),
                    metric_unit="MWh",
                    source_pdf=source_pdf,
                ))

        # EBITDA from P&L
        m = re.search(r"EBITDA\s+([\d.,]+)", page5)
        if m:
            val = parse_brl(m.group(1))
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="ebitda_month_mm",
                    metric_value=val,
                    metric_unit="R$ mm",
                    source_pdf=source_pdf,
                ))

        # EBITDA margin
        m = re.search(r"Margem\s+EBITDA\s+([\d.,]+)\s*%", page5)
        if m:
            val = parse_pct(m.group(1) + "%")
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="ebitda_margin_pct",
                    metric_value=val,
                    metric_unit="%",
                    source_pdf=source_pdf,
                ))

        # Net income
        m = re.search(r"Lucro\s+L[íi]quido\s+([\d.,]+)", page5)
        if m:
            val = parse_brl(m.group(1))
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="net_income_month_mm",
                    metric_value=val,
                    metric_unit="R$ mm",
                    source_pdf=source_pdf,
                ))

        # Enterprise Value / EBITDA from page 6
        m = re.search(r"Enterprise\s+Value\s*/\s*EBITDA.*?([\d.,]+)\s*x", page6)
        if m:
            val = parse_brl(m.group(1))
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="ev_ebitda",
                    metric_value=val,
                    metric_unit="x",
                    source_pdf=source_pdf,
                ))

        # Dívida Líquida / EBITDA from page 6
        m = re.search(r"D[íi]vida\s+L[íi]quida.*?([\d.,]+)\s*x", page6)
        if not m:
            # Try from consolidated table
            m = re.search(r"([\d.,]+)\s*x\s*$", page6, re.MULTILINE)
        if m:
            val = parse_brl(m.group(1))
            if val and val < 10:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="net_debt_ebitda",
                    metric_value=val,
                    metric_unit="x",
                    source_pdf=source_pdf,
                ))

        # PPA price from page 5
        m = re.search(r"PPA\s+m[ée]dio\s+\(R\$/MWh\)\s+([\d.,]+)", page5)
        if m:
            val = parse_brl(m.group(1))
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="ppa_avg_price_brl_mwh",
                    metric_value=val,
                    metric_unit="R$/MWh",
                    source_pdf=source_pdf,
                ))

        # Distribution projection: "R$ 13.40/cota"
        m = re.search(r"distribui[çc][ãa]o\s+total\s+de\s+dividendos\s+[ée]\s+de\s+R\$\s*([\d.,]+)\s*/\s*cota", page1, re.IGNORECASE)
        if m:
            val = parse_brl(m.group(1))
            if val:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="distribution_projection_2026_per_unit",
                    metric_value=val,
                    metric_unit="R$/cota",
                    source_pdf=source_pdf,
                ))

        return metrics
