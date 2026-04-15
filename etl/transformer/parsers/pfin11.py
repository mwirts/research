"""Parser for PFIN11 (Perfin Apollo Energia FIP-IE) monthly reports.

Layout (stable since inception):
  Page 1: Fund characteristics (left) + Manager commentary (right)
  Page 2: Distribution table + Historical performance + Spread/TIR
  Page 3: Trading volume + Investor base
  Page 4: Transmission assets map table
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
    TransmissionAsset,
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


class PFIN11Parser(BaseFundParser):
    FUND_ID = "pfin11"
    TICKER = "PFIN11"
    PARSER_VERSION = "1.0"

    def parse_raw(self, raw_data: dict) -> FundReport:
        meta = self._get_metadata(raw_data)
        ref_date = f"{meta['year']:04d}-{meta['month']:02d}"
        source_pdf = meta.get("source_pdf", "")

        page1 = self._get_page_text(raw_data, 1)
        page2 = self._get_page_text(raw_data, 2)
        page3 = self._get_page_text(raw_data, 3)
        page4 = self._get_page_text(raw_data, 4)

        # -- Fund info (from page 1) --
        fund = Fund(
            fund_id=self.FUND_ID,
            ticker=self.TICKER,
            fund_name="Perfin Apollo Energia FIP-IE",
            manager="Perfin",
            administrator="Perfin Administração de Recursos",
            segment="transmissao",
            inception_date=date(2019, 12, 1),
            fund_term="Indeterminado",
            target_audience="Investidores Qualificados",
            admin_fee_pct=0.60,
            perf_fee_pct=None,
        )

        # -- Monthly snapshot --
        snapshot = self._parse_snapshot(page1, page2, page3, ref_date, source_pdf, meta)

        # -- Distributions --
        distributions = self._parse_distributions(page2, raw_data, ref_date)

        # -- Transmission assets (page 4) --
        transmission_assets = self._parse_transmission_assets(page4, raw_data, ref_date, source_pdf)

        # -- Manager commentary (page 1) --
        commentaries = self._parse_commentary(page1, ref_date)

        # -- Extra metrics --
        extra_metrics = self._parse_extra_metrics(page2, ref_date, source_pdf)

        return FundReport(
            fund=fund,
            snapshot=snapshot,
            distributions=distributions,
            transmission_assets=transmission_assets,
            commentaries=commentaries,
            extra_metrics=extra_metrics,
        )

    def _parse_snapshot(
        self, page1: str, page2: str, page3: str,
        ref_date: str, source_pdf: str, meta: dict,
    ) -> MonthlySnapshot:
        # --- Page 1 fields ---
        # Two-column layout causes text interleaving, so search for R$ value
        # within a window after the label
        nav_total = self._extract_nearby_brl(page1, r"PATRIM[ÔO]NIO\s+L[ÍI]QUIDO")
        market_cap = self._extract_nearby_brl(page1, r"VALOR\s+DE\s+MERCADO")
        units = self._extract_labeled_value(page1, r"QUANTIDADE\s+DE\s+COTAS", parse_integer)

        # --- Page 2 fields ---
        # Market price: "cotação de R$ 87,70/cota"
        market_price = None
        m = re.search(r"cota[çc][ãa]o\s+de\s+R\$\s*([\d.,]+)\s*/\s*cota", page2)
        if m:
            market_price = parse_brl(m.group(1))

        # Monthly return: "valorização de 5,35%" - specifically near "mês de"
        # or "cotação a mercado das cotas do Fundo teve valorização de X%"
        return_month = None
        m = re.search(r"(?:cota[çc][ãa]o\s+a\s+mercado|m[êe]s\s+de\s+\w+\s+de\s+\d{4}).*?(?:valoriza[çc][ãa]o|varia[çc][ãa]o)\s+de\s+(-?[\d.,]+)\s*%", page2, re.DOTALL)
        if m:
            return_month = parse_pct(m.group(1) + "%")

        # Total accumulated distributions
        dist_total = None
        m = re.search(r"(?:distribui[çc][õo]es|distribu[íi]dos).*?(?:perfazem|totalizam|totalizando)\s+R\$\s*([\d.,]+)\s*/\s*cota", page2)
        if not m:
            # Fallback: look for "Total X,XX" in distribution table area
            m = re.search(r"Total\s+([\d.,]+)", page2)
        if m:
            dist_total = parse_brl(m.group(1))

        # Since-inception return: "variação de X,XX%"
        return_inception = None
        m = re.search(r"cotas\s+tiveram\s+varia[çc][ãa]o\s+de\s+(-?[\d.,]+)\s*%", page2)
        if m:
            return_inception = parse_pct(m.group(1) + "%")

        # --- Page 3 fields ---
        # Avg daily volume: "GLOBAL R$ 1.359.658,72"
        avg_volume = None
        m = re.search(r"GLOBAL\s+R\$\s*([\d.,]+)", page3)
        if m:
            avg_volume = parse_brl(m.group(1))

        # Investors: "4.360 investidores"
        num_investors = None
        m = re.search(r"([\d.]+)\s+investidores", page3)
        if m:
            num_investors = parse_integer(m.group(1))

        # Retail %: "93,83% de pessoas físicas"
        pct_retail = None
        m = re.search(r"([\d.,]+)\s*%\s+de\s+pessoas\s+f[íi]sicas", page3)
        if m:
            pct_retail = parse_pct(m.group(1) + "%")

        # Compute NAV per unit and discount
        nav_per_unit = None
        if nav_total and units:
            nav_per_unit = round(nav_total / units, 2)

        discount_pct = None
        if market_price and nav_per_unit and nav_per_unit > 0:
            discount_pct = round((market_price - nav_per_unit) / nav_per_unit * 100, 2)

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
            return_month_pct=return_month,
            return_since_inception_pct=return_inception,
            distribution_total_accum=dist_total,
            avg_daily_volume=avg_volume,
            num_investors=num_investors,
            pct_retail=pct_retail,
            source_pdf=source_pdf,
            page_count=meta.get("page_count"),
        )

    def _parse_distributions(self, page2: str, raw_data: dict, ref_date: str) -> list[Distribution]:
        """Parse distribution table from page 2."""
        distributions = []

        # Try to use the extracted table first
        tables = self._get_page_tables(raw_data, 2)
        if tables:
            dist_table = tables[0]  # First table on page 2 is the distribution table
            for row in dist_table:
                if not row:
                    continue
                # Clean row cells
                cells = [str(c).strip() if c else "" for c in row]

                # Skip header rows and "Total" row
                if any(h in " ".join(cells).lower() for h in ["data base", "r$/cota", "total"]):
                    continue

                # Try to find year (2020, 2021, ...) or specific date
                year_str = None
                has_specific_date = False

                for cell in cells:
                    if re.match(r"^20\d{2}$", cell):
                        year_str = cell
                    if re.match(r"\d{2}/\d{2}/\d{4}", cell):
                        has_specific_date = True

                if has_specific_date:
                    continue  # Handled by specific date regex below

                # For annual rows, R$/cota is the FIRST numeric value after "-"
                # Table layout: [_, year, "-", _, "-", R$/cota, _, DY%, _]
                # Take first cell matching X,XX pattern (the amount, not DY)
                amount = None
                if year_str:
                    found_dash = False
                    for cell in cells:
                        if cell == "-":
                            found_dash = True
                        elif found_dash and re.match(r"^\d{1,2},\d{2}$", cell):
                            amount = parse_brl(cell)
                            break  # Take FIRST match (R$/cota), not second (DY)

                if amount and amount > 0 and year_str:
                    distributions.append(Distribution(
                        fund_id=self.FUND_ID,
                        reference_month=f"{year_str}-12",
                        amount_per_unit=amount,
                    ))

        # Fallback: regex on text
        if not distributions:
            # Parse annual entries: "2020 - - 3,22 3,14"
            for m in re.finditer(r"(20\d{2})\s+-\s+-\s+([\d.,]+)", page2):
                yr = m.group(1)
                amt = parse_brl(m.group(2))
                if amt and amt > 0:
                    distributions.append(Distribution(
                        fund_id=self.FUND_ID,
                        reference_month=f"{yr}-12",
                        amount_per_unit=amt,
                    ))

            # Parse specific date entries: "21/01/2026 22/01/2026 28/01/2026 0,90"
            for m in re.finditer(
                r"(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+([\d.,]+)",
                page2,
            ):
                base_date = parse_full_date(m.group(1))
                ex_date = parse_full_date(m.group(2))
                pay_date = parse_full_date(m.group(3))
                amt = parse_brl(m.group(4))
                if amt and amt > 0 and base_date:
                    ref_month = base_date[:7]  # YYYY-MM
                    distributions.append(Distribution(
                        fund_id=self.FUND_ID,
                        reference_month=ref_month,
                        ex_date=ex_date,
                        payment_date=pay_date,
                        amount_per_unit=amt,
                    ))

        return distributions

    def _parse_transmission_assets(
        self, page4: str, raw_data: dict, ref_date: str, source_pdf: str,
    ) -> list[TransmissionAsset]:
        """Parse transmission assets table from page 4."""
        assets = []

        # Use the first table from page 4
        tables = self._get_page_tables(raw_data, 4)
        if not tables:
            return assets

        table = tables[0]
        i = 0
        while i < len(table):
            row = table[i]
            cells = [str(c).strip() if c else "" for c in row]
            joined = " ".join(cells)

            # Skip header rows and TOTAL
            if any(h in joined.lower() for h in ["mapa", "ativos", "extensão", "estado", "total"]):
                i += 1
                continue

            # Look for asset rows: number, name, state, km, ...
            # Asset names: TME, EDTE, ETB, TPE, TCC, TSM, CGI
            name_match = re.search(r"\b(TME|EDTE|ETB|TPE|TCC|TSM|CGI)\b", joined)
            if name_match:
                asset_name = name_match.group(1)

                # Extract km
                km = None
                km_match = re.search(r"\b(\d{2,3})\b", joined)
                if km_match:
                    km_val = int(km_match.group(1))
                    if 50 < km_val < 600:  # reasonable km range
                        km = float(km_val)

                # Extract participation % - may be on next row
                participation = None
                for offset in range(3):  # check current and next 2 rows
                    if i + offset < len(table):
                        check_row = " ".join(str(c) for c in table[i + offset] if c)
                        pct_match = re.search(r"(\d{1,3},\d{2})%", check_row)
                        if pct_match:
                            participation = parse_pct(pct_match.group(0))
                            break

                # Extract RAP
                rap = None
                rap_match = re.search(r"(\d{1,3},\d)", joined)
                if rap_match:
                    val = parse_brl(rap_match.group(1))
                    if val and 5 < val < 200:  # reasonable RAP range in R$ MM
                        rap = val * 1_000_000  # Convert MM to R$

                assets.append(TransmissionAsset(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    asset_name=asset_name,
                    extension_km=km,
                    rap_annual_brl=rap,
                    source_pdf=source_pdf,
                ))

            i += 1

        return assets

    def _parse_commentary(self, page1: str, ref_date: str) -> list[ManagerCommentary]:
        """Extract manager commentary from page 1."""
        commentaries = []

        # Commentary is in the right column, after "COMENTÁRIOS DO GESTOR"
        m = re.search(
            r"COMENT[ÁA]RIOS\s+DO\s+GESTOR\s*(.*?)(?:QUANTIDADE\s+DE\s+COTAS|CONSTITUI[ÇC][ÃA]O)",
            page1,
            re.DOTALL | re.IGNORECASE,
        )
        if m:
            text = m.group(1).strip()
            # Clean up: remove "Objetivo" section that's from left column
            text = re.sub(r"^Objetivo\s+.*?patrimônio\.\s*", "", text, flags=re.DOTALL | re.IGNORECASE)
            if text:
                commentaries.append(ManagerCommentary(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    section="portfolio",
                    content=text,
                ))

        return commentaries

    def _parse_extra_metrics(self, page2: str, ref_date: str, source_pdf: str) -> list[FundMetricKV]:
        """Extract extra metrics like spread and TIR."""
        metrics = []

        # Spread: "+426bps"
        m = re.search(r"\+?(\d+)\s*bps", page2)
        if m:
            metrics.append(FundMetricKV(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                metric_key="spread_vs_ntnb_bps",
                metric_value=float(m.group(1)),
                metric_unit="bps",
                source_pdf=source_pdf,
            ))

        # TIR implied: look for the number near "retorno implícito" or after the spread section
        m = re.search(r"(?:retorno\s+impl[íi]cito|TIR).*?(\d{1,2},\d{2})", page2, re.IGNORECASE)
        if m:
            tir = parse_brl(m.group(1))
            if tir and 3 < tir < 25:  # reasonable TIR range
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="tir_implied_ipca_plus",
                    metric_value=tir,
                    metric_unit="% IPCA+",
                    source_pdf=source_pdf,
                ))

        return metrics

    @staticmethod
    def _extract_labeled_value(text: str, label_pattern: str, parser_fn):
        """Extract a value that appears on the line(s) after a label."""
        m = re.search(label_pattern + r"[\s\S]{0,5}?\n(.+)", text, re.IGNORECASE)
        if m:
            return parser_fn(m.group(1).strip())
        return None

    @staticmethod
    def _extract_nearby_brl(text: str, label_pattern: str, max_chars: int = 300) -> float | None:
        """Extract an R$ value within a window after a label.

        Handles two-column layouts where text from other columns may appear
        between the label and its value.
        """
        m = re.search(label_pattern, text, re.IGNORECASE)
        if not m:
            return None
        window = text[m.end():m.end() + max_chars]
        # Find the first R$ value in the window
        val_match = re.search(r"R\$\s*([\d.,]+)", window)
        if val_match:
            return parse_brl(val_match.group(1))
        return None
