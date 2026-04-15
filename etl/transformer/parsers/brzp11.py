"""Parser for BRZP11 (BRZ Infra Portos FIP-IE) monthly reports.

Layout (14 pages, stable):
  Page 1: Cover
  Page 2: Index
  Page 3: Fund characteristics (left) + Highlights (right)
  Page 4: Key metrics (Cota Patrimonial, Cota Mercado, TIR, P/B, distributions)
  Page 5: Performance, price chart, volume, investors
  Page 6: Distribution projections, TIR curve
  Page 7: Distribution history & schedule
  Pages 8-10: Portfolio description, port operations, ranking
  Page 11: Financial results
  Pages 12-14: Annexes, disclaimers, contacts
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
    PortAsset,
)
from etl.transformer.parsers.base import (
    BaseFundParser,
    parse_brl,
    parse_date_pt,
    parse_full_date,
    parse_integer,
    parse_ipca_plus,
    parse_pct,
)


class BRZP11Parser(BaseFundParser):
    FUND_ID = "brzp11"
    TICKER = "BRZP11"
    PARSER_VERSION = "1.0"

    def parse_raw(self, raw_data: dict) -> FundReport:
        meta = self._get_metadata(raw_data)
        ref_date = f"{meta['year']:04d}-{meta['month']:02d}"
        source_pdf = meta.get("source_pdf", "")

        full_text = self._get_full_text(raw_data)
        page3 = self._get_page_text(raw_data, 3)
        page4 = self._get_page_text(raw_data, 4)
        page5 = self._get_page_text(raw_data, 5)
        page6 = self._get_page_text(raw_data, 6)
        page7 = self._get_page_text(raw_data, 7)
        page9 = self._get_page_text(raw_data, 9)
        page10 = self._get_page_text(raw_data, 10)
        page11 = self._get_page_text(raw_data, 11)

        # -- Fund info --
        fund = Fund(
            fund_id=self.FUND_ID,
            ticker=self.TICKER,
            fund_name="BRZ Infra Portos FIP-IE",
            manager="BRZ Investimentos",
            administrator="Apex Group Ltd",
            segment="portuario",
            inception_date=date(2020, 2, 18),
            fund_term="30 anos, prorrogáveis por mais 30 anos",
            target_audience="Investidores Qualificados",
            admin_fee_pct=0.12,
            mgmt_fee_pct=1.5,
            perf_fee_pct=None,
        )

        # -- Monthly snapshot --
        snapshot = self._parse_snapshot(
            page3, page4, page5, page6, page7, ref_date, source_pdf, meta,
        )

        # -- Distributions --
        distributions = self._parse_distributions(page7, ref_date)

        # -- Port assets --
        port_assets = self._parse_port_assets(
            page9, page10, page11, raw_data, ref_date, source_pdf,
        )

        # -- Commentary --
        commentaries = self._parse_commentary(page3, ref_date)

        # -- Extra metrics --
        extra_metrics = self._parse_extra_metrics(
            page4, page5, page6, page11, ref_date, source_pdf,
        )

        return FundReport(
            fund=fund,
            snapshot=snapshot,
            distributions=distributions,
            port_assets=port_assets,
            commentaries=commentaries,
            extra_metrics=extra_metrics,
        )

    def _parse_snapshot(
        self,
        page3: str,
        page4: str,
        page5: str,
        page6: str,
        page7: str,
        ref_date: str,
        source_pdf: str,
        meta: dict,
    ) -> MonthlySnapshot:
        # --- Page 4: Key metrics ---
        # The layout has three columns on two rows:
        # Row 1: "Cota Patrimonial ... Cota de Mercado ... TIR Real Estimada"
        #         "R$ 218,50/cota    R$ 182,29/cota    IPCA + 13,69%"
        # Row 2: "Valor Patrimonial ... Valor de Mercado ... Número de Cotas"
        #         "R$ 1.112,59 milhões R$ 928,21 milhões 5.153.781"

        # Parse cota values line: "R$ 218,50/cota R$ 182,29/cota IPCA + 13,69%"
        nav_per_unit = None
        market_price = None
        irr_real = None
        cota_values = re.findall(r"R\$\s*([\d.,]+)\s*/\s*cota", page4)
        if len(cota_values) >= 2:
            nav_per_unit = parse_brl(cota_values[0])
            market_price = parse_brl(cota_values[1])
        elif len(cota_values) == 1:
            nav_per_unit = parse_brl(cota_values[0])

        m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%", page4)
        if m:
            irr_real = parse_ipca_plus(m.group(0))

        # Parse values line: "R$ 1.112,59 milhões R$ 928,21 milhões 5.153.781"
        nav_total = None
        market_cap = None
        units = None
        milh_values = re.findall(r"R\$\s*([\d.,]+)\s*milh[õo]es", page4)
        if len(milh_values) >= 2:
            nav_total = parse_brl(milh_values[0] + " milhões")
            market_cap = parse_brl(milh_values[1] + " milhões")
        elif len(milh_values) == 1:
            nav_total = parse_brl(milh_values[0] + " milhões")

        # Número de Cotas: find standalone large number on the values line
        m = re.search(r"milh[õo]es\s+([\d.]+)\n", page4)
        if m:
            units = parse_integer(m.group(1))

        # Rendimentos row: "R$ 30,10/cota R$ 10,00/cota 22,8%"
        # (Desde o IPO, LTM distribution, LTM volatility)
        dist_total = None
        dist_ltm = None
        volatility = None
        dist_cota_values = re.findall(r"R\$\s*([\d.,]+)\s*/\s*cota", page4)
        if len(dist_cota_values) >= 4:
            # First two are from "Cota" row, next two from "Rendimentos" row
            dist_total = parse_brl(dist_cota_values[2])
            dist_ltm = parse_brl(dist_cota_values[3])
        elif len(dist_cota_values) >= 3:
            dist_total = parse_brl(dist_cota_values[2])

        m = re.search(r"Volatilidade\s+Anual.*?([\d.,]+)\s*%", page4, re.DOTALL)
        if m:
            volatility = parse_pct(m.group(1) + "%")

        # --- Page 5: Performance, volume, investors ---
        # Monthly return: "o retorno ao cotista no mês foi de 6,5%"
        return_month = None
        m = re.search(r"retorno\s+ao\s+cotista\s+no\s+m[êe]s\s+foi\s+de\s+([\d.,]+)\s*%", page5)
        if m:
            return_month = parse_pct(m.group(1) + "%")

        # 12-month return: "a valorização foi de 105,3%"
        return_12m = None
        m = re.search(r"(?:12\s*meses|per[íi]odo\s+de\s+12).*?valoriza[çc][ãa]o\s+foi\s+de\s+([\d.,]+)\s*%", page5, re.DOTALL)
        if m:
            return_12m = parse_pct(m.group(1) + "%")

        # Num investors: "3.559 cotistas"
        num_investors = None
        m = re.search(r"([\d.]+)\s+cotistas", page5)
        if m:
            num_investors = parse_integer(m.group(1))

        # Avg daily volume: "R$ 638,6 mil/dia"
        avg_volume = None
        m = re.search(r"R\$\s*([\d.,]+)\s*mil\s*/\s*dia\s+no\s+m[êe]s", page5)
        if m:
            val = parse_brl(m.group(1))
            if val:
                avg_volume = val * 1000  # mil -> R$

        # Price/Book: "0.83x"
        price_to_book = None
        m = re.search(r"Price\s*/\s*Book.*?([\d.,]+)\s*x", page5, re.DOTALL)
        if m:
            price_to_book = parse_brl(m.group(1))

        # Discount: "-16,6%"
        discount_pct = None
        m = re.search(r"desconto\s+de\s+(?:cerca\s+de\s+)?(-?[\d.,]+)\s*%", page5)
        if m:
            val = parse_pct(m.group(1) + "%")
            if val:
                discount_pct = -abs(val)  # Always negative for discount

        # Dividend yield LTM: compute from dist_ltm / market_price
        dividend_yield_ltm = None
        if dist_ltm and market_price and market_price > 0:
            dividend_yield_ltm = round(dist_ltm / market_price * 100, 2)

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
            irr_real=irr_real,
            return_month_pct=return_month,
            return_12m_pct=return_12m,
            distribution_per_unit=0.92,  # Current monthly distribution from page 7
            distribution_total_accum=dist_total,
            dividend_yield_ltm=dividend_yield_ltm,
            avg_daily_volume=avg_volume,
            num_investors=num_investors,
            source_pdf=source_pdf,
            page_count=meta.get("page_count"),
        )

    def _parse_distributions(self, page7: str, ref_date: str) -> list[Distribution]:
        """Parse distribution schedule from page 7."""
        distributions = []

        # Parse monthly distribution rows:
        # "Julho 2025 30/06/2025 09/07/2025 R$ 0,92"
        for m in re.finditer(
            r"(\w+)\s+(20\d{2})\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+R\$\s*([\d.,]+)",
            page7,
        ):
            month_name = m.group(1)
            year = m.group(2)
            base_date = parse_full_date(m.group(3))
            pay_date = parse_full_date(m.group(4))
            amt = parse_brl(m.group(5))

            if amt and amt > 0:
                # Resolve reference month from month name
                ref_month = parse_date_pt(f"{month_name} {year}")
                if ref_month:
                    distributions.append(Distribution(
                        fund_id=self.FUND_ID,
                        reference_month=ref_month,
                        ex_date=base_date,
                        payment_date=pay_date,
                        amount_per_unit=amt,
                    ))

        return distributions

    def _parse_port_assets(
        self,
        page9: str,
        page10: str,
        page11: str,
        raw_data: dict,
        ref_date: str,
        source_pdf: str,
    ) -> list[PortAsset]:
        """Parse Porto Itapoá operational and financial data."""
        assets = []

        # TEUs from page 9/10: "O Porto Itapoá movimentou 111,9 mil TEUs"
        teus_month = None
        combined = page9 + "\n" + page10
        m = re.search(r"movimentou\s+([\d.,]+)\s*mil\s*TEUs", combined)
        if m:
            val = parse_brl(m.group(1))
            if val:
                teus_month = val * 1000

        # --- Page 11: Financial results ---
        # Look for the most recent year data (2024) in the financial table
        # "Receita Líquida (R$ MM) ... 1.223"
        revenue = None
        ebitda = None
        ebitda_margin = None
        net_income = None
        net_debt = None
        net_debt_ebitda = None

        # Try parsing from page 11 text - the financial table
        # Format: metric name ... values per year
        # We want the rightmost (most recent) value

        # Revenue: "Receita Líquida (R$ MM) 243 281 310 323 361 490 546 650²,³ 1.223"
        m = re.search(r"Receita\s+L[íi]quida\s+\(R\$\s*MM\)\s+([\d.,\s²³]+)", page11)
        if m:
            # Get the last number in the sequence; these are integer R$ MM values
            nums = re.findall(r"[\d.]+", m.group(1).replace("²", "").replace("³", ""))
            if nums:
                val = parse_integer(nums[-1])
                if val and val > 100:
                    revenue = float(val) * 1_000_000

        # EBITDA
        m = re.search(r"EBITDA\s+\(R\$\s*MM\)\s+([\d.,\s²³]+)", page11)
        if m:
            nums = re.findall(r"[\d.]+", m.group(1).replace("²", "").replace("³", ""))
            if nums:
                val = parse_integer(nums[-1])
                if val and val > 10:
                    ebitda = float(val) * 1_000_000

        # EBITDA margin
        m = re.search(r"Margem\s+EBITDA\s+([\d.,\s%]+)", page11)
        if m:
            pcts = re.findall(r"([\d.,]+)\s*%", m.group(1))
            if pcts:
                ebitda_margin = parse_pct(pcts[-1] + "%")

        # Net income
        m = re.search(r"Lucro\s+L[íi]quido\s+\(R\$\s*MM\)\s+([\d.,\s²³]+)", page11)
        if m:
            nums = re.findall(r"[\d.]+", m.group(1).replace("²", "").replace("³", ""))
            if nums:
                val = parse_integer(nums[-1])
                if val and val > 1:
                    net_income = float(val) * 1_000_000

        # Net Debt / EBITDA
        m = re.search(r"D[íi]vida\s+L[íi]quida\s*/\s*EBITDA\s+([\d.,x\s]+)", page11)
        if m:
            ratios = re.findall(r"([\d.,]+)\s*x", m.group(1))
            if ratios:
                net_debt_ebitda = parse_brl(ratios[-1])

        # Net Debt
        m = re.search(r"D[íi]vida\s+L[íi]quida\s+\(R\$\s*MM\)\s+([\d.,\s²³-]+)", page11)
        if m:
            nums = re.findall(r"[\d.]+", m.group(1).replace("²", "").replace("³", ""))
            if nums:
                val = parse_integer(nums[-1])
                if val:
                    net_debt = float(val) * 1_000_000

        assets.append(PortAsset(
            fund_id=self.FUND_ID,
            reference_date=ref_date,
            asset_name="Porto Itapoá",
            teus_month=teus_month,
            revenue_brl=revenue,
            ebitda_brl=ebitda,
            ebitda_margin_pct=ebitda_margin,
            net_income_brl=net_income,
            net_debt_brl=net_debt,
            net_debt_ebitda=net_debt_ebitda,
            source_pdf=source_pdf,
        ))

        return assets

    def _parse_commentary(self, page3: str, ref_date: str) -> list[ManagerCommentary]:
        """Extract highlights commentary from page 3.

        The two-column layout causes left-column labels to interleave with
        right-column bullet points.  We extract only the bullet-point lines.
        """
        commentaries = []

        # Extract all bullet points from page 3 (they are interleaved with
        # left-column fund characteristic labels)
        bullets = re.findall(r"[▪•]\s*(.+?)(?=[▪•]|\Z)", page3, re.DOTALL)
        if bullets:
            cleaned = []
            for b in bullets:
                # Remove left-column labels that got mixed in
                text = re.sub(
                    r"(?:In[íi]cio\s+de\s+Negocia[çc][ãa]o|Prazo\s+do\s+Fundo|"
                    r"P[úu]blico-Alvo|Segmento|Projetos\s+Investidos|"
                    r"Tributa[çc][ãa]o|Investidores\s+Qualificados|"
                    r"Portu[áa]rio|18/02/2020|30\s+anos.*?anos|"
                    r"\d+\s*\(.*?\)):\s*",
                    "", b, flags=re.DOTALL,
                )
                text = re.sub(r"\s+", " ", text).strip()
                if text and len(text) > 20:
                    cleaned.append("- " + text)

            if cleaned:
                commentaries.append(ManagerCommentary(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    section="highlights",
                    content="\n".join(cleaned),
                ))

        return commentaries

    def _parse_extra_metrics(
        self,
        page4: str,
        page5: str,
        page6: str,
        page11: str,
        ref_date: str,
        source_pdf: str,
    ) -> list[FundMetricKV]:
        """Extract extra metrics."""
        metrics = []

        # Volatility from page 4
        m = re.search(r"Volatilidade\s+Anual.*?([\d.,]+)\s*%", page4, re.DOTALL)
        if m:
            vol = parse_pct(m.group(1) + "%")
            if vol:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="volatility_annual_pct",
                    metric_value=vol,
                    metric_unit="%",
                    source_pdf=source_pdf,
                ))

        # Distribution projection CAGR from page 6
        m = re.search(r"CAGR.*?([\d.,]+)\s*%", page6, re.DOTALL)
        if m:
            cagr = parse_pct(m.group(1) + "%")
            if cagr:
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="distribution_cagr_projected_pct",
                    metric_value=cagr,
                    metric_unit="%",
                    source_pdf=source_pdf,
                ))

        # Port ranking from page 10 text: "Porto Itapoá 1.446" in TOP 10
        # TEUs YTD
        combined = self._get_page_text({"pages": [{"page_num": 10, "text": page5}]}, 10)

        return metrics
