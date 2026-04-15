"""Parser for AZIN11 (AZ Quest Infra-Yield II FIP-IE) monthly reports.

Layout (15 pages, stable):
  Page 1: Cover
  Page 2: Index
  Page 3: Manager commentary (1/2) with performance table (CDI%, spread)
  Page 4: Manager commentary (2/2), secondary market
  Page 5: Highlights (cota mercado, patrimonial, PL, duration, distributions, P/VP)
  Page 6: Distribution history chart, liquidity
  Page 7: Portfolio holdings table (Carteira)
  Page 8: Allocation breakdown, DY comparison, spread vs duration
  Pages 9-14: Individual portfolio asset descriptions
  Page 15: Fund info & disclaimers
"""

from __future__ import annotations

import re
from datetime import date

from etl.transformer.models import (
    Distribution,
    Fund,
    FundMetricKV,
    FundReport,
    ManagerCommentary,
    MonthlySnapshot,
    PortfolioHolding,
)
from etl.transformer.parsers.base import (
    BaseFundParser,
    parse_brl,
    parse_date_pt,
    parse_full_date,
    parse_integer,
    parse_pct,
)


class AZIN11Parser(BaseFundParser):
    FUND_ID = "azin11"
    TICKER = "AZIN11"
    PARSER_VERSION = "1.0"

    def parse_raw(self, raw_data: dict) -> FundReport:
        meta = self._get_metadata(raw_data)
        ref_date = f"{meta['year']:04d}-{meta['month']:02d}"
        source_pdf = meta.get("source_pdf", "")

        page3 = self._get_page_text(raw_data, 3)
        page4 = self._get_page_text(raw_data, 4)
        page5 = self._get_page_text(raw_data, 5)
        page7 = self._get_page_text(raw_data, 7)
        page15 = self._get_page_text(raw_data, 15)

        # -- Fund info --
        fund = self._parse_fund_info(page15)

        # -- Monthly snapshot --
        snapshot = self._parse_snapshot(
            page3, page4, page5, page15, ref_date, source_pdf, meta,
        )

        # -- Portfolio holdings --
        portfolio_holdings = self._parse_portfolio(raw_data, page7, ref_date, source_pdf)

        # -- Distributions --
        distributions = self._parse_distributions(page5, ref_date)

        # -- Commentary --
        commentaries = self._parse_commentary(page3, page4, ref_date)

        # -- Extra metrics --
        extra_metrics = self._parse_extra_metrics(page3, page5, ref_date, source_pdf)

        return FundReport(
            fund=fund,
            snapshot=snapshot,
            distributions=distributions,
            portfolio_holdings=portfolio_holdings,
            commentaries=commentaries,
            extra_metrics=extra_metrics,
        )

    def _parse_fund_info(self, page15: str) -> Fund:
        """Parse fund info from the last page."""
        cnpj = None
        m = re.search(r"(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})", page15)
        if m:
            cnpj = m.group(1)

        admin_fee = None
        m = re.search(r"Taxa\s+de\s+Adm.*?([\d.,]+)\s*%\s*a\.a", page15, re.IGNORECASE)
        if m:
            admin_fee = parse_pct(m.group(1) + "%")

        return Fund(
            fund_id=self.FUND_ID,
            ticker=self.TICKER,
            fund_name="AZ Quest Infra-Yield II FIP-IE",
            cnpj=cnpj,
            manager="AZ Quest",
            administrator="AZ Quest",
            segment="credito_infra",
            inception_date=date(2023, 10, 1),
            fund_term="Indeterminado",
            target_audience="Investidores Qualificados",
            admin_fee_pct=admin_fee,
            perf_fee_pct=20.0,
        )

    def _parse_snapshot(
        self,
        page3: str,
        page4: str,
        page5: str,
        page15: str,
        ref_date: str,
        source_pdf: str,
        meta: dict,
    ) -> MonthlySnapshot:
        # --- Page 5: Destaques ---
        # Layout: "R$ 100,50 R$ 97,10 R$ 238,0 mm 3,8 anos R$ 1,40/cota 1,03x"
        # Labels come on the lines below:
        #   "Cota Mercado  Patrimonial  Patrimônio Líquido  Duration  Distribuição do Mês  P/VP"

        # Extract the highlights line with all values
        market_price = None
        nav_per_unit = None
        nav_total = None
        dist_per_unit = None
        price_to_book = None

        # Parse the known value layout: R$ cota_mercado, R$ cota_patrimonial, R$ PL mm, ...
        highlights_match = re.search(
            r"R\$\s*([\d.,]+)\s+R\$\s*([\d.,]+)\s+R\$\s*([\d.,]+)\s*mm\s+([\d.,]+)\s*anos\s+R\$\s*([\d.,]+)\s*/\s*cota\s+([\d.,]+)\s*x",
            page5,
        )
        if highlights_match:
            market_price = parse_brl(highlights_match.group(1))
            nav_per_unit = parse_brl(highlights_match.group(2))
            val = parse_brl(highlights_match.group(3))
            if val:
                nav_total = val * 1_000_000
            dist_per_unit = parse_brl(highlights_match.group(5))
            price_to_book = parse_brl(highlights_match.group(6))
        else:
            # Fallback: parse individually
            all_rs = re.findall(r"R\$\s*([\d.,]+)", page5)
            if len(all_rs) >= 3:
                market_price = parse_brl(all_rs[0])
                nav_per_unit = parse_brl(all_rs[1])
                val = parse_brl(all_rs[2])
                if val and val < 1000:
                    nav_total = val * 1_000_000

            m = re.search(r"R\$\s*([\d.,]+)\s*/\s*cota", page5)
            if m:
                dist_per_unit = parse_brl(m.group(1))

            m = re.search(r"([\d.,]+)\s*x\s*\n", page5)
            if m:
                price_to_book = parse_brl(m.group(1))

        # Fallback nav_per_unit from page 15
        if nav_per_unit is None:
            m = re.search(r"Valor\s+da\s+Cota\s+Patrimonial.*?R\$\s*([\d.,]+)", page15)
            if m:
                nav_per_unit = parse_brl(m.group(1))

        # Fallback nav_total from page 15
        if nav_total is None:
            m = re.search(r"Patrim[ôo]nio\s+L[íi]quido.*?R\$\s*([\d.,]+)\s*milh", page15, re.DOTALL | re.IGNORECASE)
            if m:
                nav_total = parse_brl(m.group(1) + " milhões")

        # --- Page 3: Performance ---
        # "Resultado Líquido1 1,6% 2,9% 54,4%"
        # "(% CDI) 163% 131% 170%"
        return_month_pct = None
        return_month_cdi = None

        # Performance table from page 3 text
        m = re.search(r"Resultado\s+L[íi]quido.*?([\d.,]+)\s*%\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%", page3)
        if m:
            return_month_pct = parse_pct(m.group(1) + "%")

        m = re.search(r"\(%\s*CDI\)\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%", page3)
        if m:
            return_month_cdi = parse_pct(m.group(1) + "%")

        # Num investors from page 15
        # Layout: "R$ 236,6 milhões R$ 97,10 4.306 1,25% a.a. 20% ..."
        #          "Patrimônio Líquido² Valor da Cota Patrimonial² Número de Cotistas² ..."
        # The values line precedes the labels line.
        num_investors = None
        m = re.search(
            r"R\$\s*[\d.,]+\s+(?:milh[õo]es\s+)?R\$\s*[\d.,]+\s+([\d.]+)\s+[\d.,]+%",
            page15,
        )
        if m:
            num_investors = parse_integer(m.group(1))

        # Units from nav_total and nav_per_unit
        units = None
        if nav_total and nav_per_unit and nav_per_unit > 0:
            units = round(nav_total / nav_per_unit)

        # Discount/Premium
        discount_pct = None
        if market_price and nav_per_unit and nav_per_unit > 0:
            discount_pct = round((market_price - nav_per_unit) / nav_per_unit * 100, 2)

        # Market cap
        market_cap = None
        if market_price and units:
            market_cap = round(market_price * units, 2)

        # Avg daily volume from page 4: "média diária de negociação ... R$ 683 mil"
        avg_volume = None
        m = re.search(r"m[ée]dia\s+di[áa]ria\s+de\s+negocia[çc][ãa]o.*?R\$\s*([\d.,]+)\s*mil", page4, re.DOTALL | re.IGNORECASE)
        if m:
            val = parse_brl(m.group(1))
            if val:
                avg_volume = val * 1000

        # DY anualizado: compute from distribution table on page 5
        # "Fev-26 1,40 26/03/264 22,19%"
        # The last column in the distribution table is DY a.a. (annualized)
        dividend_yield_ltm = None
        dy_values = re.findall(r"\w{3}-\d{2}\s+[\d.,]+\s+\d{2}/\d{2}/\d{2}\d?\s+([\d.,]+)%", page5)
        if dy_values:
            # Use the most recent DY a.a. value
            dividend_yield_ltm = parse_pct(dy_values[-1] + "%")

        # Total return from inception (% CDI)
        return_inception_cdi = None
        m = re.search(r"(\d{2,3})\s*%\s+do\s+CDI", page5)
        if m:
            return_inception_cdi = parse_pct(m.group(1) + "%")

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
            return_month_pct=return_month_pct,
            distribution_per_unit=dist_per_unit,
            dividend_yield_ltm=dividend_yield_ltm,
            avg_daily_volume=avg_volume,
            num_investors=num_investors,
            source_pdf=source_pdf,
            page_count=meta.get("page_count"),
        )

    def _parse_portfolio(
        self,
        raw_data: dict,
        page7_text: str,
        ref_date: str,
        source_pdf: str,
    ) -> list[PortfolioHolding]:
        """Parse portfolio holdings from page 7 table."""
        holdings = []

        # Use extracted tables from page 7
        tables = self._get_page_tables(raw_data, 7)
        if not tables:
            return self._parse_portfolio_from_text(page7_text, ref_date, source_pdf)

        # Table 0 has the main holdings data
        main_table = tables[0] if tables else []
        # Table 1 has LTV/ICSD columns
        kpi_table = tables[1] if len(tables) > 1 else []

        # First row in text includes AXS Energia which may not be in the table
        # Parse AXS from text as fallback
        axs_match = re.search(
            r"AXS\s+Energia.*?Deb[êe]nture\s+(\d+)%\s+(\d+)\s+(\d+)%\s+([\d.,]+)\s+(\w+)\s+([\d.,]+)%\s+(\w+)\s+(\d+)%",
            page7_text,
        )
        if axs_match:
            ltv_val = parse_pct(axs_match.group(8) + "%")
            holdings.append(PortfolioHolding(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                issuer="AXS Energia Unidade 10",
                segment="Geração | Solar",
                instrument_type="Debênture",
                ticker="AXS411",
                pct_pl=parse_pct(axs_match.group(1) + "%"),
                amount_brl=parse_brl(axs_match.group(2)) * 1_000_000 if parse_brl(axs_match.group(2)) else None,
                duration_years=parse_brl(axs_match.group(4)),
                indexer=axs_match.group(5),
                spread_pct=parse_pct(axs_match.group(6) + "%"),
                ltv_pct=ltv_val,
                source_pdf=source_pdf,
            ))
        else:
            # Simpler AXS extraction from text
            m = re.search(
                r"AXS\s+Energia\s+Unidade\s+10\s+Gera[çc][ãa]o\s*\|\s*Solar\s+(\w+)\s+Deb[êe]nture\s+(\d+)%\s+(\d+)\s+\d+%\s+([\d.,]+)\s+(\w+)\s+([\d.,]+)%",
                page7_text,
            )
            if m:
                holdings.append(PortfolioHolding(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    issuer="AXS Energia Unidade 10",
                    segment="Geração | Solar",
                    instrument_type="Debênture",
                    ticker=m.group(1),
                    pct_pl=parse_pct(m.group(2) + "%"),
                    amount_brl=parse_brl(m.group(3)) * 1_000_000 if parse_brl(m.group(3)) else None,
                    duration_years=parse_brl(m.group(4)),
                    indexer=m.group(5),
                    spread_pct=parse_pct(m.group(6) + "%"),
                    ltv_pct=51.0,  # Known from text
                    source_pdf=source_pdf,
                ))

        # Parse remaining rows from table
        for i, row in enumerate(main_table):
            if not row or len(row) < 8:
                continue

            cells = [str(c).strip() if c else "" for c in row]

            # Skip if it looks like a header or summary
            issuer = cells[0]
            if not issuer or issuer in ("-", ""):
                continue
            if any(h in issuer.lower() for h in ["emissor", "carteira", "caixa", "fundo"]):
                continue

            segment = cells[1] if len(cells) > 1 else ""
            ticker = cells[2] if len(cells) > 2 else ""
            instrument_type = cells[3] if len(cells) > 3 else ""
            pct_pl = parse_pct(cells[4] + "%" if cells[4] else "") if len(cells) > 4 else None
            amount_mm = parse_brl(cells[5]) if len(cells) > 5 else None
            amount_brl = amount_mm * 1_000_000 if amount_mm else None
            duration = parse_brl(cells[7]) if len(cells) > 7 else None
            indexer = cells[8] if len(cells) > 8 else ""
            spread = parse_pct(cells[9] + "%" if cells[9] and "%" not in cells[9] else cells[9]) if len(cells) > 9 else None

            # KPI data from table 1 (same row index)
            ltv = None
            icsd = None
            if i < len(kpi_table):
                kpi_cells = [str(c).strip() if c else "" for c in kpi_table[i]]
                if kpi_cells[0] and kpi_cells[0] not in ("-", "* (5)"):
                    ltv = parse_pct(kpi_cells[0] + "%" if "%" not in kpi_cells[0] else kpi_cells[0])
                if len(kpi_cells) > 1 and kpi_cells[1] and kpi_cells[1] not in ("-", "* (5)"):
                    m = re.search(r"([\d.,]+)", kpi_cells[1])
                    if m:
                        icsd = parse_brl(m.group(1))

            holdings.append(PortfolioHolding(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                issuer=issuer,
                segment=segment if segment else None,
                instrument_type=instrument_type if instrument_type else None,
                ticker=ticker if ticker else None,
                pct_pl=pct_pl,
                amount_brl=amount_brl,
                duration_years=duration,
                indexer=indexer if indexer else None,
                spread_pct=spread,
                ltv_pct=ltv,
                icsd_min=icsd,
                source_pdf=source_pdf,
            ))

        # Also parse Igarassú from text if not in table
        igarassu_found = any("Igarass" in h.issuer for h in holdings)
        if not igarassu_found:
            m = re.search(
                r"Igarass[úu]\s+Participa[çc][õo]es.*?Biometano.*?(\w+)\s+Deb[êe]nture\s+(\d+)%\s+(\d+)\s+\d+%\s+([\d.,]+)\s+(\w+)\s+([\d.,]+)%",
                page7_text,
            )
            if m:
                holdings.append(PortfolioHolding(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    issuer="Igarassú Participações",
                    segment="Biometano",
                    instrument_type="Debênture",
                    ticker=m.group(1),
                    pct_pl=parse_pct(m.group(2) + "%"),
                    amount_brl=parse_brl(m.group(3)) * 1_000_000 if parse_brl(m.group(3)) else None,
                    duration_years=parse_brl(m.group(4)),
                    indexer=m.group(5),
                    spread_pct=parse_pct(m.group(6) + "%"),
                    ltv_pct=35.0,  # Known from text
                    source_pdf=source_pdf,
                ))

        return holdings

    def _parse_portfolio_from_text(
        self, page7_text: str, ref_date: str, source_pdf: str,
    ) -> list[PortfolioHolding]:
        """Fallback: parse portfolio from text when tables fail."""
        holdings = []
        # Match patterns like:
        # "AXS Energia Unidade 10 Geração | Solar AXS411 Debênture 11% 26 100% 1,9 CDI 6,50% Sim 51% -"
        pattern = (
            r"(\S.*?)\s+"
            r"(Gera[çc][ãa]o\s*\|[^A]+|Biometano|Infraestrutura\s+de\s+Torres)\s+"
            r"(\S+)\s+"
            r"(Deb[êe]nture|APR|NC)\S*\s+"
            r"(\d+)%\s+(\d+)\s+\d+%\s+"
            r"([\d.,]+)\s+"
            r"(CDI|NTN-B\(\d+\)|IPCA)\s+"
            r"([\d.,]+)%"
        )
        for m in re.finditer(pattern, page7_text):
            holdings.append(PortfolioHolding(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                issuer=m.group(1).strip(),
                segment=m.group(2).strip(),
                instrument_type=m.group(4).strip(),
                ticker=m.group(3).strip(),
                pct_pl=parse_pct(m.group(5) + "%"),
                amount_brl=parse_brl(m.group(6)) * 1_000_000 if parse_brl(m.group(6)) else None,
                duration_years=parse_brl(m.group(7)),
                indexer=m.group(8),
                spread_pct=parse_pct(m.group(9) + "%"),
                source_pdf=source_pdf,
            ))

        return holdings

    def _parse_distributions(self, page5: str, ref_date: str) -> list[Distribution]:
        """Parse recent distributions from page 5."""
        distributions = []

        # Format: "Set-25 1,90 14/10/25 24,26%"
        # Note: date field may have typos (e.g. "26/03/264"), so allow 2-4 digit year
        for m in re.finditer(
            r"(\w{3})-(\d{2})\s+([\d.,]+)\s+(\d{2}/\d{2}/\d{2,4})\s+([\d.,]+)%",
            page5,
        ):
            month_abbr = m.group(1)
            year_short = m.group(2)
            amt = parse_brl(m.group(3))

            ref_month = parse_date_pt(f"{month_abbr}/{year_short}")
            if ref_month and amt and amt > 0:
                distributions.append(Distribution(
                    fund_id=self.FUND_ID,
                    reference_month=ref_month,
                    amount_per_unit=amt,
                ))

        return distributions

    def _parse_commentary(
        self, page3: str, page4: str, ref_date: str,
    ) -> list[ManagerCommentary]:
        """Extract manager commentary."""
        commentaries = []

        # Page 3: "Comentários da Gestão (1/2)" section
        m = re.search(
            r"Coment[áa]rios\s+da\s+Gest[ãa]o.*?\n(.*?)(?:RENTABILIDADE|TODOS\s+OS)",
            page3,
            re.DOTALL | re.IGNORECASE,
        )
        if m:
            text = m.group(1).strip()
            # Remove the performance table embedded in the text
            text = re.sub(r"(?:CDI|Spread|MtM|Hedge|Fees|Despesas|Resultado).*?[\d.,]+%\s*", "", text)
            text = re.sub(r"\(%\s*CDI\).*?%\s*", "", text)
            text = re.sub(r"\s{2,}", " ", text).strip()
            if text:
                commentaries.append(ManagerCommentary(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    section="portfolio",
                    content=text,
                ))

        # Page 4: second part of commentary
        m = re.search(
            r"Coment[áa]rios\s+da\s+Gest[ãa]o.*?\n(.*?)(?:MERCADO\s+SECUND|TODOS\s+OS)",
            page4,
            re.DOTALL | re.IGNORECASE,
        )
        if m:
            text = m.group(1).strip()
            text = re.sub(r"[ÚU]LTIMAS\s*LIVES.*$", "", text, flags=re.DOTALL)
            text = re.sub(r"https?://\S+", "", text)
            text = re.sub(r"\s{2,}", " ", text).strip()
            if text:
                commentaries.append(ManagerCommentary(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    section="strategy",
                    content=text,
                ))

        return commentaries

    def _parse_extra_metrics(
        self, page3: str, page5: str, ref_date: str, source_pdf: str,
    ) -> list[FundMetricKV]:
        """Extract extra metrics like CDI performance."""
        metrics = []

        # Return as % CDI (month): from page 3 table
        m = re.search(r"\(%\s*CDI\)\s+(\d{2,3})%\s+(\d{2,3})%\s+(\d{2,3})%", page3)
        if m:
            metrics.append(FundMetricKV(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                metric_key="return_month_pct_cdi",
                metric_value=parse_pct(m.group(1) + "%"),
                metric_unit="% CDI",
                source_pdf=source_pdf,
            ))
            metrics.append(FundMetricKV(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                metric_key="return_ytd_pct_cdi",
                metric_value=parse_pct(m.group(2) + "%"),
                metric_unit="% CDI",
                source_pdf=source_pdf,
            ))
            metrics.append(FundMetricKV(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                metric_key="return_inception_pct_cdi",
                metric_value=parse_pct(m.group(3) + "%"),
                metric_unit="% CDI",
                source_pdf=source_pdf,
            ))

        # Total return since inception: from page 5
        m = re.search(r"(\d{2,3})\s*%\s+do\s+CDI", page5)
        if m:
            metrics.append(FundMetricKV(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                metric_key="total_return_pct_cdi",
                metric_value=parse_pct(m.group(1) + "%"),
                metric_unit="% CDI",
                source_pdf=source_pdf,
            ))

        # Spread from page 3 table: "Spread Crédito 0,4% 0,7% 9,3%"
        m = re.search(r"Spread\s+Cr[ée]dito\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%", page3)
        if m:
            metrics.append(FundMetricKV(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                metric_key="credit_spread_month_pct",
                metric_value=parse_pct(m.group(1) + "%"),
                metric_unit="%",
                source_pdf=source_pdf,
            ))

        return metrics
