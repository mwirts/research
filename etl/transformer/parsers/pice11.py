"""Parser for PICE11 (Patria Infraestrutura Energia Core FIP-IE) monthly reports.

Layout (8 pages):
  Page 1: Cover
  Page 2: Manager letter (left) + Fund info / general information (right)
  Page 3: Distribution history table + expected events timeline
  Page 4: Highlights (cota patrimonial, cota mercado, TIR sensitivity table)
  Page 5: Portfolio - 3 wind parks (Serrote, Serra do Mato, Afonso Bezerra)
  Page 6: Portfolio generation LTM chart
  Page 7: Disclaimers
  Page 8: Back cover
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
    parse_integer,
    parse_ipca_plus,
    parse_pct,
)


class PICE11Parser(BaseFundParser):
    FUND_ID = "pice11"
    TICKER = "PICE11"
    PARSER_VERSION = "1.0"

    # Known wind parks with their capacities
    WIND_PARKS = {
        "Serrote": {"capacity_mw": 206.0, "gen_type": "eolica"},
        "Serra do Mato": {"capacity_mw": 122.0, "gen_type": "eolica"},
        "Afonso Bezerra": {"capacity_mw": 160.0, "gen_type": "eolica"},
    }

    def parse_raw(self, raw_data: dict) -> FundReport:
        meta = self._get_metadata(raw_data)
        ref_date = f"{meta['year']:04d}-{meta['month']:02d}"
        source_pdf = meta.get("source_pdf", "")

        full_text = self._get_full_text(raw_data)
        page2 = self._get_page_text(raw_data, 2)
        page3 = self._get_page_text(raw_data, 3)
        page4 = self._get_page_text(raw_data, 4)
        page5 = self._get_page_text(raw_data, 5)

        # -- Fund info --
        fund = Fund(
            fund_id=self.FUND_ID,
            ticker=self.TICKER,
            fund_name="Patria Infraestrutura Energia Core FIP-IE",
            manager="Patria",
            administrator="BTG Pactual",
            segment="geracao_eolica",
            inception_date=date(2021, 1, 13),
            fund_term="Indeterminado",
            target_audience="Investidores Qualificados",
            admin_fee_pct=self._parse_admin_fee(page2),
        )

        # -- Monthly snapshot --
        snapshot = self._parse_snapshot(page2, page3, page4, ref_date, source_pdf, meta)

        # -- Distributions --
        distributions = self._parse_distributions(page3, raw_data, ref_date)

        # -- Generation assets (page 5) --
        generation_assets = self._parse_generation_assets(page5, ref_date, source_pdf)

        # -- Manager commentary (page 2) --
        commentaries = self._parse_commentary(page2, ref_date)

        # -- Extra metrics --
        extra_metrics = self._parse_extra_metrics(page4, ref_date, source_pdf)

        return FundReport(
            fund=fund,
            snapshot=snapshot,
            distributions=distributions,
            generation_assets=generation_assets,
            commentaries=commentaries,
            extra_metrics=extra_metrics,
        )

    @staticmethod
    def _parse_brl_mixed(text: str) -> float | None:
        """Parse a BRL value that may have mixed comma/dot formatting.

        Handles formats like "1,016.608.392,25" where the first comma is a
        thousands separator (not decimal). This happens when the PDF text
        has inconsistent number formatting.
        """
        if not text:
            return None
        text = text.strip()
        # Count commas: if there are 2+ commas, the first one(s) are thousands separators
        comma_count = text.count(",")
        if comma_count >= 2:
            # Remove all commas except the last one (which is the decimal separator)
            last_comma = text.rfind(",")
            cleaned = text[:last_comma].replace(",", "") + text[last_comma:]
            return parse_brl(cleaned)
        return parse_brl(text)

    def _parse_admin_fee(self, page2: str) -> float | None:
        """Extract admin fee for Classe A from page 2."""
        try:
            m = re.search(r"Classe\s+A:\s*([\d.,]+)\s*%\s*a\.a\.", page2)
            if m:
                return parse_pct(m.group(1) + "%")
        except Exception:
            pass
        return None

    def _parse_snapshot(
        self, page2: str, page3: str, page4: str,
        ref_date: str, source_pdf: str, meta: dict,
    ) -> MonthlySnapshot:
        # --- Page 2: Fund info panel ---
        nav_total = self._extract_nav_total(page2)
        market_cap = self._extract_market_cap(page2)
        units = self._extract_units(page2)
        num_investors = self._extract_num_investors(page2)

        # NAV per unit from page 2 or page 4
        nav_per_unit = self._extract_nav_per_unit(page2, page4)

        # Market price from page 2 or page 4
        market_price = self._extract_market_price(page2, page4)

        # --- Page 4: TIR and performance ---
        irr_real = self._extract_irr(page2, page4)

        # Distribution totals from page 3
        dist_total = None
        m = re.search(r"Rendimentos\s+Totais\s+Acumulados\s+([\d.,]+)", page3)
        if m:
            dist_total = parse_brl(m.group(1))

        # DY LTM from page 4
        dy_ltm = None
        m = re.search(r"YIELD\s+LTM.*?([\d.,]+)\s*%", page4, re.DOTALL)
        if m:
            dy_ltm = parse_pct(m.group(1) + "%")

        # Distribution per unit (latest)
        dist_per_unit = None
        m = re.search(r"R\$\s*([\d.,]+)\s*/\s*\n?\s*cota", page4)
        if not m:
            m = re.search(r"LTIMO\s+RENDIMENTO.*?R\$\s*([\d.,]+)", page4, re.DOTALL)
        if m:
            dist_per_unit = parse_brl(m.group(1))

        # Return metrics from page 4
        return_month = None
        m = re.search(r"varia[çc][ãa]o\s+ajustada.*?de\s+(-?[\d.,]+)\s*%\s*no\s+m[êe]s", page4)
        if m:
            return_month = parse_pct(m.group(1) + "%")

        return_12m = None
        m = re.search(r"(-?[\d.,]+)\s*%\s*nos\s+[úu]ltimos\s+12\s+meses", page4)
        if m:
            return_12m = parse_pct(m.group(1) + "%")

        return_inception = None
        m = re.search(r"(-?[\d.,]+)\s*%\s*desde\s*\n?\s*o\s+lan[çc]amento", page4)
        if m:
            return_inception = parse_pct(m.group(1) + "%")

        # Compute derived metrics
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
            irr_real=irr_real,
            return_month_pct=return_month,
            return_12m_pct=return_12m,
            return_since_inception_pct=return_inception,
            distribution_per_unit=dist_per_unit,
            distribution_total_accum=dist_total,
            dividend_yield_ltm=dy_ltm,
            num_investors=num_investors,
            source_pdf=source_pdf,
            page_count=meta.get("page_count"),
        )

    def _extract_nav_total(self, page2: str) -> float | None:
        """Extract Patrimonio Liquido from page 2.

        Two-column layout causes interleaving: the label "Patrimônio Líquido:"
        may be separated from "R$ R$ 1,016.608.392,25" by other text.
        Search broadly for R$ values near the label.
        """
        try:
            # Find the label position, then search forward for the R$ value
            m = re.search(r"Patrim[ôo]nio\s+L[íi]quido\s*:", page2, re.IGNORECASE)
            if m:
                # Search forward up to 500 chars for R$ value
                window = page2[m.end():m.end() + 500]
                val_match = re.search(r"R\$\s*(?:R\$\s*)?([\d.,]+)", window)
                if val_match:
                    return self._parse_brl_mixed(val_match.group(1))
        except Exception:
            pass
        return None

    def _extract_market_cap(self, page2: str) -> float | None:
        """Extract Valor de Mercado (total) from page 2."""
        try:
            m = re.search(r"Valor\s+de\s+Mercado\s*[²2]?\s*:", page2, re.IGNORECASE)
            if m:
                window = page2[m.end():m.end() + 500]
                val_match = re.search(r"R\$\s*(?:R\$\s*)?([\d.,]+)", window)
                if val_match:
                    return self._parse_brl_mixed(val_match.group(1))
        except Exception:
            pass
        return None

    def _extract_units(self, page2: str) -> int | None:
        """Extract total cotas from page 2.

        Two-column layout: "Quantidade de Cotas3:" may be followed by
        interleaved text before the actual number like "7.572.098".
        """
        try:
            m = re.search(r"Quantidade\s+de\s+Cotas\s*[³3]?\s*:", page2, re.IGNORECASE)
            if m:
                # Search forward for a standalone large number (thousands-formatted)
                window = page2[m.end():m.end() + 500]
                # Look for a number with dots (e.g. "7.572.098")
                val_match = re.search(r"\b(\d{1,3}(?:\.\d{3})+)\b", window)
                if val_match:
                    return parse_integer(val_match.group(1))
                # Fallback: any plain number
                val_match = re.search(r"\b(\d{4,})\b", window)
                if val_match:
                    return int(val_match.group(1))
        except Exception:
            pass
        return None

    def _extract_num_investors(self, page2: str) -> int | None:
        """Extract number of investors from page 2.

        Two-column interleaving means the value may be several lines after the label.
        """
        try:
            m = re.search(r"N[úu]mero\s+de\s+Cotistas\s*:", page2, re.IGNORECASE)
            if m:
                window = page2[m.end():m.end() + 500]
                # Look for a standalone number (e.g. "2.835" or "2835")
                val_match = re.search(r"\b(\d{1,3}(?:\.\d{3})*)\b", window)
                if val_match:
                    return parse_integer(val_match.group(1))
        except Exception:
            pass
        return None

    def _extract_nav_per_unit(self, page2: str, page4: str) -> float | None:
        """Extract cota patrimonial from page 2 or page 4."""
        try:
            # Page 2: "Classe A: R$ 132,55"
            m = re.search(
                r"Valor\s+Patrimonial\s+da\s+Cota\s*:?\s*\n?\s*Classe\s+A\s*:\s*R\$\s*([\d.,]+)",
                page2, re.IGNORECASE,
            )
            if m:
                return parse_brl(m.group(1))

            # Page 4: "COTA PATRIMONIAL\nR$ 135,55" or similar
            m = re.search(r"COTA\s+PATRIMONIAL\s*\n?\s*R\$\s*([\d.,]+)", page4, re.IGNORECASE)
            if m:
                return parse_brl(m.group(1))

            # Fallback: "valor patrimonial da cota... era de R$ X"
            m = re.search(
                r"valor\s+patrimonial\s+da\s+cota.*?R\$\s*([\d.,]+)",
                page2 + page4, re.IGNORECASE | re.DOTALL,
            )
            if m:
                return parse_brl(m.group(1))
        except Exception:
            pass
        return None

    def _extract_market_price(self, page2: str, page4: str) -> float | None:
        """Extract cota de mercado for PICE11 (Classe A)."""
        try:
            # Page 2: "PICE11: R$ 31,00"
            m = re.search(r"PICE11\s*:\s*R\$\s*([\d.,]+)", page2)
            if m:
                return parse_brl(m.group(1))

            # Page 4: "COTA MERCADO\nR$ 31,00"
            m = re.search(r"COTA\s+MERCADO\s*\n?\s*R\$\s*([\d.,]+)", page4, re.IGNORECASE)
            if m:
                return parse_brl(m.group(1))
        except Exception:
            pass
        return None

    def _extract_irr(self, page2: str, page4: str) -> float | None:
        """Extract TIR real implied (IPCA+) from page 2 or page 4."""
        try:
            # Page 2: "IPCA + 30,70%"
            m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%", page2)
            if m:
                val = parse_pct(m.group(1) + "%")
                if val and 5 < val < 100:
                    return val

            # Page 4: "IPCA + 30,7%" or "RETORNO NO PRECO DE MERCADO ... IPCA + 30,7%"
            m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%", page4)
            if m:
                val = parse_pct(m.group(1) + "%")
                if val and 5 < val < 100:
                    return val
        except Exception:
            pass
        return None

    def _parse_distributions(self, page3: str, raw_data: dict, ref_date: str) -> list[Distribution]:
        """Parse distribution history table from page 3."""
        distributions = []

        # Strategy: use regex on the text which has clear "year jan feb ... dec total" rows
        # Pattern: "2021 - - - - - 1,00 1,00 ..."
        months_labels = [
            "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
            "Jul", "Ago", "Set", "Out", "Nov", "Dez",
        ]

        # Find annual distribution rows
        for m in re.finditer(
            r"(20\d{2})\s+"
            r"([\d.,]+|-)\s+([\d.,]+|-)\s+([\d.,]+|-)\s+([\d.,]+|-)\s+"
            r"([\d.,]+|-)\s+([\d.,]+|-)\s+([\d.,]+|-)\s+([\d.,]+|-)\s+"
            r"([\d.,]+|-)\s+([\d.,]+|-)\s+([\d.,]+|-)\s+([\d.,]+|-)",
            page3,
        ):
            year = m.group(1)
            for i in range(12):
                val_str = m.group(i + 2).strip()
                if val_str == "-" or not val_str:
                    continue
                amt = parse_brl(val_str)
                if amt and amt > 0:
                    month_num = i + 1
                    distributions.append(Distribution(
                        fund_id=self.FUND_ID,
                        reference_month=f"{year}-{month_num:02d}",
                        amount_per_unit=amt,
                    ))

        # Also try table-based extraction as fallback
        if not distributions:
            tables = self._get_page_tables(raw_data, 3)
            for table in tables:
                for row in table:
                    cells = [str(c).strip() if c else "" for c in row]
                    if not cells:
                        continue
                    # Check if first cell is a year
                    year_match = re.match(r"^(20\d{2})$", cells[0])
                    if year_match:
                        year = year_match.group(1)
                        # Cells 1-12 are months (jan-dec), cell 13 is annual total
                        for i in range(1, min(13, len(cells))):
                            val_str = cells[i].strip()
                            if val_str == "-" or not val_str:
                                continue
                            amt = parse_brl(val_str)
                            if amt and amt > 0:
                                distributions.append(Distribution(
                                    fund_id=self.FUND_ID,
                                    reference_month=f"{year}-{i:02d}",
                                    amount_per_unit=amt,
                                ))

        return distributions

    def _parse_generation_assets(
        self, page5: str, ref_date: str, source_pdf: str,
    ) -> list[GenerationAsset]:
        """Parse wind generation assets from page 5."""
        assets = []

        for park_name, info in self.WIND_PARKS.items():
            try:
                # Find the section for this park
                pattern = re.escape(park_name) + r".*?(?=" + "|".join(
                    re.escape(n) for n in self.WIND_PARKS if n != park_name
                ) + r"|$)"
                section_match = re.search(pattern, page5, re.DOTALL | re.IGNORECASE)
                if not section_match:
                    # Still create with known static data
                    assets.append(GenerationAsset(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        asset_name=park_name,
                        gen_type=info["gen_type"],
                        capacity_mw=info["capacity_mw"],
                        source_pdf=source_pdf,
                    ))
                    continue

                section = section_match.group(0)

                # Capacity: "206 MW"
                capacity = info["capacity_mw"]  # use known value
                cap_match = re.search(r"(\d+)\s*MW", section)
                if cap_match:
                    capacity = float(cap_match.group(1))

                # Availability: "95, 6%" or "96,3%" or "97,1%"
                # Note: PDF extraction may insert spaces in numbers like "95, 6%"
                availability = None
                avail_match = re.search(r"DISPONIBILIDADE.*?(\d{2,3})\s*,\s*(\d+)\s*%", section, re.DOTALL)
                if avail_match:
                    clean_pct = avail_match.group(1) + "," + avail_match.group(2)
                    availability = parse_pct(clean_pct + "%")

                # Debenture disbursement
                debenture_val = None
                deb_match = re.search(r"R\$\s*([\d.,]+)\s*M\b", section)
                if deb_match:
                    debenture_val = parse_brl(deb_match.group(1))
                    if debenture_val:
                        debenture_val = debenture_val * 1_000_000

                assets.append(GenerationAsset(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    asset_name=park_name,
                    gen_type=info["gen_type"],
                    capacity_mw=capacity,
                    availability_pct=availability,
                    source_pdf=source_pdf,
                ))
            except Exception:
                # Fallback: add with static data only
                assets.append(GenerationAsset(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    asset_name=park_name,
                    gen_type=info["gen_type"],
                    capacity_mw=info["capacity_mw"],
                    source_pdf=source_pdf,
                ))

        return assets

    def _parse_commentary(self, page2: str, ref_date: str) -> list[ManagerCommentary]:
        """Extract manager commentary from page 2 (Carta do Gestor).

        The two-column layout interleaves the manager letter (left column)
        with the fund objective and general info (right column). We extract
        the full block then clean out the right-column text.
        """
        commentaries = []
        try:
            # Commentary starts after "Carta do Gestor" and goes until "Informações Gerais"
            m = re.search(
                r"Carta\s+do\s+Gestor\s*\d?\s*(.*?)(?:Informa[çc][õo]es\s+Gerais|In[íi]cio\s+das\s+atividades)",
                page2,
                re.DOTALL | re.IGNORECASE,
            )
            if m:
                text = m.group(1).strip()
                # Remove right-column "Objetivo do Fundo" section
                text = re.sub(
                    r"Objetivo\s+do\s+Fundo.*?Regulamento\.",
                    "", text, flags=re.DOTALL | re.IGNORECASE,
                )
                # Remove right-column fund objective continuation
                text = re.sub(
                    r"O Fundo tem por objetivo.*?Regulamento\.",
                    "", text, flags=re.DOTALL | re.IGNORECASE,
                )
                # Remove sentence fragments about fund purpose
                text = re.sub(
                    r"proporcionar aos seus\s+cotistas.*?(?:estrat[ée]gica|gest[ãa]o)\.",
                    "", text, flags=re.DOTALL | re.IGNORECASE,
                )
                text = re.sub(r"\s{2,}", " ", text).strip()
                if text and len(text) > 50:
                    commentaries.append(ManagerCommentary(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        section="portfolio",
                        content=text,
                    ))
        except Exception:
            pass
        return commentaries

    def _parse_extra_metrics(self, page4: str, ref_date: str, source_pdf: str) -> list[FundMetricKV]:
        """Extract extra metrics from page 4 (sensitivity table, duration, etc.)."""
        metrics = []

        try:
            # Duration: "duration do portfólio é de 8,6 anos"
            m = re.search(r"duration.*?(\d+,\d+)\s*anos", page4, re.IGNORECASE)
            if m:
                val = parse_brl(m.group(1))
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="portfolio_duration_years",
                        metric_value=val,
                        metric_unit="years",
                        source_pdf=source_pdf,
                    ))

            # Upside: "+R$ 101,55" or "327,6%"
            m = re.search(r"UPSIDE.*?R\$\s*([\d.,]+)", page4, re.DOTALL)
            if m:
                val = parse_brl(m.group(1))
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="upside_vs_market_brl",
                        metric_value=val,
                        metric_unit="R$/cota",
                        source_pdf=source_pdf,
                    ))

            m = re.search(r"([\d.,]+)\s*%\s*\n.*?RETORNO\s+NO\s+PRE", page4, re.DOTALL)
            if m:
                val = parse_pct(m.group(1) + "%")
                if val and val > 50:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="upside_vs_market_pct",
                        metric_value=val,
                        metric_unit="%",
                        source_pdf=source_pdf,
                    ))
        except Exception:
            pass

        return metrics
