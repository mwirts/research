"""Parser for VIGT11 (Vinci Energia FIP-IE) monthly reports.

Layout (16 pages):
  Page 1: Cover
  Page 2: Table of contents
  Page 3: General info (fund overview) + Highlights
  Page 4: Macro commentary
  Page 5: Strategy & Destaques + operational summary
  Page 6: Financial performance (EBITDA, debt schedule)
  Page 7: Fund overview / performance / distributions
  Page 8: Rentabilidade chart + distribution schedule
  Page 9: B3 trading data (market cap, investors, volume)
  Page 10: Transmission assets (LEST, Arcoverde, TPAE)
  Page 11: Hydro generation (ESPRA PCHs)
  Page 12: Wind generation (Mangue Seco 1, 3, 4)
  Page 13: Curtailment data and discussion
  Page 14: Curtailment chart continuation
  Page 15: Contact / disclaimers
  Page 16: Back cover
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
    TransmissionAsset,
)
from etl.transformer.parsers.base import (
    BaseFundParser,
    parse_brl,
    parse_date_pt,
    parse_integer,
    parse_ipca_plus,
    parse_pct,
)


class VIGT11Parser(BaseFundParser):
    FUND_ID = "vigt11"
    TICKER = "VIGT11"
    PARSER_VERSION = "1.0"

    # Known transmission assets
    TRANSMISSION_ASSETS = ["LEST", "Arcoverde", "TPAE"]

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
        page8 = self._get_page_text(raw_data, 8)
        page9 = self._get_page_text(raw_data, 9)
        page10 = self._get_page_text(raw_data, 10)
        page11 = self._get_page_text(raw_data, 11)
        page12 = self._get_page_text(raw_data, 12)
        page13 = self._get_page_text(raw_data, 13)

        # -- Fund info --
        fund = Fund(
            fund_id=self.FUND_ID,
            ticker=self.TICKER,
            fund_name="Vinci Energia FIP-IE",
            manager="Vinci",
            administrator="BTG Pactual Servicos Financeiros S.A. DTVM",
            segment="energia_mista",
            inception_date=date(2020, 1, 1),
            fund_term="Indeterminado",
            target_audience="Investidores em Geral",
            admin_fee_pct=self._parse_admin_fee(page3),
        )

        # -- Monthly snapshot --
        snapshot = self._parse_snapshot(
            page3, page5, page6, page7, page8, page9,
            ref_date, source_pdf, meta,
        )

        # -- Distributions --
        distributions = self._parse_distributions(page8, raw_data, ref_date)

        # -- Transmission assets (page 10) --
        transmission_assets = self._parse_transmission_assets(page10, raw_data, ref_date, source_pdf)

        # -- Generation assets: wind + hydro --
        generation_assets = self._parse_generation_assets(
            page5, page11, page12, raw_data, ref_date, source_pdf,
        )

        # -- Manager commentary --
        commentaries = self._parse_commentary(page4, page5, ref_date)

        # -- Extra metrics --
        extra_metrics = self._parse_extra_metrics(
            page5, page6, page13, ref_date, source_pdf,
        )

        return FundReport(
            fund=fund,
            snapshot=snapshot,
            distributions=distributions,
            transmission_assets=transmission_assets,
            generation_assets=generation_assets,
            commentaries=commentaries,
            extra_metrics=extra_metrics,
        )

    def _parse_admin_fee(self, page3: str) -> float | None:
        """Extract admin fee from page 3.

        Layout: "Taxa de Administração²\nVIGT11 1,5% a.a."
        The fee value may appear on the next line due to two-column interleaving.
        """
        try:
            m = re.search(r"Taxa\s+de\s+Administra[çc][ãa]o", page3, re.IGNORECASE)
            if m:
                window = page3[m.end():m.end() + 200]
                val_match = re.search(r"([\d.,]+)\s*%\s*a\.a\.", window)
                if val_match:
                    return parse_pct(val_match.group(1) + "%")
        except Exception:
            pass
        return None

    def _parse_snapshot(
        self, page3: str, page5: str, page6: str,
        page7: str, page8: str, page9: str,
        ref_date: str, source_pdf: str, meta: dict,
    ) -> MonthlySnapshot:
        # --- Page 3: Key metrics ---
        nav_per_unit = None
        m = re.search(r"Valor\s+Patrimonial\s+da\s+Cota.*?\n?\s*R\$\s*([\d.,]+)", page3, re.IGNORECASE)
        if m:
            nav_per_unit = parse_brl(m.group(1))

        market_price = None
        m = re.search(r"Valor\s+de\s+Mercado\s+da\s+Cota.*?\n?\s*R\$\s*([\d.,]+)", page3, re.IGNORECASE)
        if m:
            market_price = parse_brl(m.group(1))

        units = None
        # Two-column layout: "Quantidade de Cotas\n...interleaved text... 8.674.669"
        m = re.search(r"Quantidade\s+de\s+Cotas", page3, re.IGNORECASE)
        if m:
            window = page3[m.end():m.end() + 200]
            # Look for a large formatted number (thousands-separated)
            val_match = re.search(r"\b(\d{1,3}(?:\.\d{3})+)\b", window)
            if val_match:
                units = parse_integer(val_match.group(1))

        num_investors = None
        # Two-column layout: "Número de Cotistas (27-02-26)\nBTG Pactual... 5.282"
        m = re.search(r"N[úu]mero\s+de\s+Cotistas", page3, re.IGNORECASE)
        if m:
            window = page3[m.end():m.end() + 200]
            # Look for a standalone number (e.g. "5.282") - skip dates like "27-02-26"
            val_match = re.search(r"\b(\d{1,3}(?:\.\d{3})+)\b", window)
            if val_match:
                num_investors = parse_integer(val_match.group(1))
        if not num_investors:
            # Fallback: page 9
            m = re.search(r"N[úu]mero\s+de\s+Cotistas\s+([\d.,]+)", page9)
            if m:
                num_investors = parse_integer(m.group(1))

        # --- Page 3: Highlights ---
        # TIR real: "14,8" near "TIR REAL"
        irr_real = None
        m = re.search(r"TIR\s+REAL\s+IMPL[ÍI]C.*?(\d{1,2},\d+)", page3, re.IGNORECASE | re.DOTALL)
        if m:
            irr_real = parse_pct(m.group(1) + "%")
        if not irr_real:
            # Fallback: "IPCA + 14,8%"
            m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%", page3)
            if m:
                irr_real = parse_pct(m.group(1) + "%")
        if not irr_real:
            m = re.search(r"IPCA\s*\+\s*([\d.,]+)\s*%", page5)
            if m:
                irr_real = parse_pct(m.group(1) + "%")

        # --- Page 9: Market data ---
        market_cap = None
        m = re.search(r"Valor\s+de\s+Mercado\s*\(R\$\s*mil\)\s*([\d.,]+)", page9)
        if m:
            val = parse_integer(m.group(1))
            if val:
                market_cap = float(val) * 1000  # convert from R$ mil to R$

        avg_volume = None
        # Two-column interleaving: "Volume Diário Médio Negociado 33,6%...discount text...\n520,61\n(R$ mil)"
        # The actual volume value is "520,61" on its own line, OR
        # "R$ 520,6 mil" in the description text, OR from the table
        m = re.search(r"volume\s+m[ée]dio\s+di[áa]rio\s+de\s+negocia[çc][ãa]o.*?R\$\s*([\d.,]+)\s*mil", page9, re.IGNORECASE | re.DOTALL)
        if not m:
            # Try to find the standalone value near (R$ mil)
            m = re.search(r"\n([\d.,]+)\n\(R\$\s*mil\)", page9)
        if m:
            val = parse_brl(m.group(1))
            if val:
                avg_volume = val * 1000  # R$ mil to R$

        # --- Page 7: Performance metrics ---
        # Layout: "desde o Início no Ano no mês\nYTD\nDesde Início: R$ 42,71\n-11,0% 4,0% 4,6%"
        # The 3 percentages are: since inception, YTD, month (in that order)
        return_inception = None
        return_ytd = None
        return_month = None

        pct_line = re.search(r"(-?[\d.,]+)\s*%\s+(-?[\d.,]+)\s*%\s+(-?[\d.,]+)\s*%", page7)
        if pct_line:
            return_inception = parse_pct(pct_line.group(1) + "%")
            return_ytd = parse_pct(pct_line.group(2) + "%")
            return_month = parse_pct(pct_line.group(3) + "%")

        # Distribution totals from page 7
        dist_total = None
        m = re.search(r"Desde\s+In[íi]cio\s*:\s*R\$\s*([\d.,]+)", page7, re.IGNORECASE)
        if m:
            dist_total = parse_brl(m.group(1))

        dist_ytd = None
        m = re.search(r"No\s+Ano\s*:\s*R\$\s*([\d.,]+)", page7, re.IGNORECASE)
        if m:
            dist_ytd = parse_brl(m.group(1))

        # Compute NAV total
        nav_total = None
        if nav_per_unit and units:
            nav_total = round(nav_per_unit * units, 2)

        # Discount/premium
        discount_pct = None
        # Try to get from page text: "desconto de 33,6%"
        m = re.search(r"desconto\s+de\s+([\d.,]+)\s*%", page3 + page5, re.IGNORECASE)
        if m:
            discount_pct = -1 * parse_pct(m.group(1) + "%")  # negative = discount
        elif market_price and nav_per_unit and nav_per_unit > 0:
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
            return_ytd_pct=return_ytd,
            return_since_inception_pct=return_inception,
            distribution_total_accum=dist_total,
            avg_daily_volume=avg_volume,
            num_investors=num_investors,
            source_pdf=source_pdf,
            page_count=meta.get("page_count"),
        )

    def _parse_distributions(self, page8: str, raw_data: dict, ref_date: str) -> list[Distribution]:
        """Parse distribution schedule from page 8."""
        distributions = []

        try:
            # Pattern: "DD/MM/YYYY DD/MM/YYYY 0,24"
            for m in re.finditer(
                r"(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+([\d.,]+)",
                page8,
            ):
                announce_date_str = m.group(1)
                payment_date_str = m.group(2)
                amount = parse_brl(m.group(3))
                if amount and amount > 0:
                    # Parse reference month from announcement date
                    parts = announce_date_str.split("/")
                    if len(parts) == 3:
                        ref_month = f"{parts[2]}-{parts[1]}"
                        distributions.append(Distribution(
                            fund_id=self.FUND_ID,
                            reference_month=ref_month,
                            ex_date=f"{parts[2]}-{parts[1]}-{parts[0]}",
                            payment_date=f"{m.group(2)[6:10]}-{m.group(2)[3:5]}-{m.group(2)[0:2]}",
                            amount_per_unit=amount,
                        ))
        except Exception:
            pass

        return distributions

    def _parse_transmission_assets(
        self, page10: str, raw_data: dict, ref_date: str, source_pdf: str,
    ) -> list[TransmissionAsset]:
        """Parse transmission assets from page 10."""
        assets = []

        # Extract data from the structured table on page 10
        tables = self._get_page_tables(raw_data, 10)

        # The main table has columns: Transmissao, LEST, Arcoverde, TPAE
        # Rows: Extensao, RAP, Fim da Concessao, etc.
        asset_data = {"LEST": {}, "Arcoverde": {}, "TPAE": {}}

        # Parse from tables
        for table in tables:
            for row in table:
                cells = [str(c).strip() if c else "" for c in row]
                if len(cells) < 3:
                    continue

                joined = " ".join(cells)

                # Extension: "198 km", "139 km", "12 km"
                if "Extens" in joined:
                    for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                        if i + 1 < len(cells):
                            km_match = re.search(r"(\d+)\s*km", cells[i + 1])
                            if km_match:
                                asset_data[name]["extension_km"] = float(km_match.group(1))

                # Concession end
                if "Concess" in joined:
                    for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                        if i + 1 < len(cells):
                            yr_match = re.search(r"(20\d{2})", cells[i + 1])
                            if yr_match:
                                asset_data[name]["concession_end"] = yr_match.group(1)

                # EBITDA participation
                if "EBITDA" in joined:
                    for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                        if i + 1 < len(cells):
                            pct_match = re.search(r"([\d.,]+)\s*%", cells[i + 1])
                            if pct_match:
                                asset_data[name]["ebitda_pct"] = parse_pct(pct_match.group(0))

        # Also parse from text as fallback/primary
        try:
            # RAP: "RAP anual ciclo 25-26 (R$\n67 37 12\nmilhões)"
            # The format has R$ on one line, values on next, "milhões)" on the line after
            m = re.search(r"RAP\s+anual.*?R\$\s*\)?\s*\n?\s*(\d+)\s+(\d+)\s+(\d+)", page10, re.IGNORECASE | re.DOTALL)
            if m:
                for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                    val = float(m.group(i + 1))
                    asset_data[name]["rap_annual_brl"] = val * 1_000_000

            # Extension from text: "Extensão 198 km 139 km 12 km"
            m = re.search(r"Extens[ãa]o\s+(\d+)\s*km\s+(\d+)\s*km\s+(\d+)\s*km", page10, re.IGNORECASE)
            if m:
                for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                    asset_data[name].setdefault("extension_km", float(m.group(i + 1)))

            # Concession end from text: "Fim da Concessão 2047 2047 2039"
            m = re.search(r"Fim\s+da\s+Concess[ãa]o\s+(20\d{2})\s+(20\d{2})\s+(20\d{2})", page10, re.IGNORECASE)
            if m:
                for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                    asset_data[name].setdefault("concession_end", m.group(i + 1))

            # Net debt from text: "Dívida Líquida¹\n216 157 28\n(R$ milhões)"
            m = re.search(r"D[íi]vida\s+L[íi]quida.*?\n\s*(\d+)\s+(\d+)\s+(\d+)", page10, re.IGNORECASE)
            if m:
                for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                    asset_data[name]["net_debt_mm"] = float(m.group(i + 1))

            # EBITDA participation: "32,2% 14,1% 3,8%"
            m = re.search(r"EBITDA.*?Portf[óo]lio\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%\s+([\d.,]+)\s*%", page10, re.IGNORECASE | re.DOTALL)
            if m:
                for i, name in enumerate(["LEST", "Arcoverde", "TPAE"]):
                    asset_data[name]["ebitda_pct"] = parse_pct(m.group(i + 1) + "%")

        except Exception:
            pass

        # Build TransmissionAsset objects
        for name in ["LEST", "Arcoverde", "TPAE"]:
            data = asset_data[name]
            assets.append(TransmissionAsset(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                asset_name=name,
                extension_km=data.get("extension_km"),
                rap_annual_brl=data.get("rap_annual_brl"),
                concession_end=data.get("concession_end"),
                source_pdf=source_pdf,
            ))

        return assets

    def _parse_generation_assets(
        self, page5: str, page11: str, page12: str,
        raw_data: dict, ref_date: str, source_pdf: str,
    ) -> list[GenerationAsset]:
        """Parse generation assets: Mangue Seco (wind) and ESPRA (hydro)."""
        assets = []

        # --- Wind: Mangue Seco (page 12) ---
        try:
            # Total capacity: 3 x 26 MW = 78 MW
            capacity_mw = 78.0
            cap_matches = re.findall(r"(\d+)\s*MW", page12)
            if cap_matches:
                # Sum all MW values that are capacity (26 MW each for 3 parks)
                cap_vals = [float(x) for x in cap_matches if 10 <= float(x) <= 50]
                if cap_vals:
                    capacity_mw = sum(cap_vals)

            # Generation: "geração eólica no mês foi de 12,3 MWm" from page 3/5
            generation_mwm = None
            page3_text = self._get_page_text(raw_data, 3)
            # Search in page 3 (highlights), page 5 (strategy), and page 12 (wind detail)
            search_text = page3_text + "\n" + page5 + "\n" + page12
            m = re.search(r"gera[çc][ãa]o\s+e[óo]lica\s+no\s+m[êe]s\s+foi\s+de\s+([\d.,]+)\s*MWm", search_text, re.IGNORECASE)
            if not m:
                m = re.search(r"GERA[ÇC][ÃA]O\s+E[ÓO]LICA\s*\(MWm\)\s*\n?\s*(?:A\s+gera.*?de\s+)?([\d.,]+)", search_text, re.IGNORECASE)
            if not m:
                m = re.search(r"Mangue\s+Seco\s+registraram\s+gera[çc][ãa]o\s+de\s+([\d.,]+)\s*\n?\s*MWm", search_text, re.IGNORECASE)
            if m:
                generation_mwm = parse_brl(m.group(1))

            # Curtailment: "frustração de 2,5 MWm" from page 3 or page 5
            curtailment_mwm = None
            m = re.search(r"frustra[çc][ãa]o\s+de\s+([\d.,]+)\s*MWm", search_text, re.IGNORECASE)
            if not m:
                m = re.search(r"cortes.*?alcan[çc]aram\s+([\d.,]+)\s*MWm", search_text, re.IGNORECASE | re.DOTALL)
            if m:
                curtailment_mwm = parse_brl(m.group(1))

            # PPA price: R$ 363/MWh
            ppa_price = None
            m = re.search(r"PPA.*?R\$\s*([\d.,]+)\s*/\s*MWh", page12)
            if m:
                ppa_price = parse_brl(m.group(1))

            # PPA end: jun/32
            ppa_end = None
            m = re.search(r"Fim\s+PPA\s+(\w+/\d{2})", page12)
            if m:
                ppa_end = m.group(1)

            assets.append(GenerationAsset(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                asset_name="Mangue Seco",
                gen_type="eolica",
                capacity_mw=capacity_mw,
                generation_mwm=generation_mwm,
                curtailment_mwm=curtailment_mwm,
                ppa_price_brl_mwh=ppa_price,
                ppa_end_date=ppa_end,
                source_pdf=source_pdf,
            ))
        except Exception:
            assets.append(GenerationAsset(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                asset_name="Mangue Seco",
                gen_type="eolica",
                capacity_mw=78.0,
                source_pdf=source_pdf,
            ))

        # --- Hydro: ESPRA PCHs (page 11) ---
        try:
            # Total capacity: 14.8 + 11 + 16 = 41.8 MW
            capacity_mw = 41.8
            # Extract all MW values from page 11 (e.g. "14,8 MW", "11 MW", "16 MW")
            cap_matches = re.findall(r"([\d.,]+)\s*MW", page11)
            if cap_matches:
                cap_vals = [parse_brl(x) for x in cap_matches]
                cap_vals = [v for v in cap_vals if v and 5 <= v <= 50]
                if cap_vals:
                    capacity_mw = sum(cap_vals)

            # Generation: "10,8 MWm" from page 5
            generation_mwm = None
            m = re.search(r"ESPRA\s+apresentaram\s+gera[çc][ãa]o.*?(\d+,\d+)\s*MWm", page5, re.IGNORECASE | re.DOTALL)
            if m:
                generation_mwm = parse_brl(m.group(1))

            # PPA price from page 11: R$ 472/MWh
            ppa_price = None
            m = re.search(r"PPA.*?R\$\s*([\d.,]+)\s*/\s*MWh", page11)
            if not m:
                m = re.search(r"(\d{3})\s+(\d{3})\s+(\d{3})", page11)
                if m:
                    # All 3 PCHs have same price
                    ppa_price = float(m.group(1))
            if m and not ppa_price:
                ppa_price = parse_brl(m.group(1))

            # EBITDA participation
            ebitda_pct = None
            m = re.search(r"Participa[çc][ãa]o\s+no\s+EBITDA.*?(\d+,\d+)\s*%", page11, re.IGNORECASE | re.DOTALL)
            if m:
                ebitda_pct = parse_pct(m.group(1) + "%")

            assets.append(GenerationAsset(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                asset_name="ESPRA",
                gen_type="hidrica",
                capacity_mw=capacity_mw,
                generation_mwm=generation_mwm,
                ppa_price_brl_mwh=ppa_price,
                source_pdf=source_pdf,
            ))
        except Exception:
            assets.append(GenerationAsset(
                fund_id=self.FUND_ID,
                reference_date=ref_date,
                asset_name="ESPRA",
                gen_type="hidrica",
                capacity_mw=41.8,
                source_pdf=source_pdf,
            ))

        return assets

    def _parse_commentary(self, page4: str, page5: str, ref_date: str) -> list[ManagerCommentary]:
        """Extract manager commentary: macro (page 4) and strategy (page 5)."""
        commentaries = []

        # Macro commentary from page 4
        try:
            m = re.search(
                r"Cen[áa]rio\s+Macroecon[ôo]mico\s*(.*?)(?:Relat[óo]rio\s+de\s+Desempenho|$)",
                page4, re.DOTALL | re.IGNORECASE,
            )
            if m:
                text = m.group(1).strip()
                text = re.sub(r"\s{2,}", " ", text).strip()
                if text and len(text) > 50:
                    commentaries.append(ManagerCommentary(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        section="macro",
                        content=text,
                    ))
        except Exception:
            pass

        # Strategy from page 5
        try:
            m = re.search(
                r"Estrat[ée]gia\s+e\s+Destaques\s*(.*?)(?:Resultado\s+e\s+Indicadores|Relat[óo]rio\s+de\s+Desempenho|$)",
                page5, re.DOTALL | re.IGNORECASE,
            )
            if m:
                text = m.group(1).strip()
                text = re.sub(r"\s{2,}", " ", text).strip()
                if text and len(text) > 50:
                    commentaries.append(ManagerCommentary(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        section="strategy",
                        content=text,
                    ))
        except Exception:
            pass

        return commentaries

    def _parse_extra_metrics(
        self, page5: str, page6: str, page13: str,
        ref_date: str, source_pdf: str,
    ) -> list[FundMetricKV]:
        """Extract extra metrics: EBITDA, debt, curtailment."""
        metrics = []

        try:
            # EBITDA from page 6: "EBITDA de R$ 16,2 milhões"
            m = re.search(r"EBITDA\s+de\s+R\$\s*([\d.,]+)\s*milh", page6, re.IGNORECASE)
            if m:
                val = parse_brl(m.group(1))
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="ebitda_monthly_mm",
                        metric_value=val,
                        metric_unit="R$ milhoes",
                        source_pdf=source_pdf,
                    ))

            # EBITDA margin: "margem de 81%"
            m = re.search(r"margem\s+de\s+(\d+)\s*%", page6, re.IGNORECASE)
            if m:
                val = float(m.group(1))
                metrics.append(FundMetricKV(
                    fund_id=self.FUND_ID,
                    reference_date=ref_date,
                    metric_key="ebitda_margin_pct",
                    metric_value=val,
                    metric_unit="%",
                    source_pdf=source_pdf,
                ))

            # Net debt: "dívida líquida consolidada... R$ 641,2 milhões"
            m = re.search(r"d[íi]vida\s+l[íi]quida\s+consolidada.*?R\$\s*([\d.,]+)\s*milh", page6, re.IGNORECASE | re.DOTALL)
            if m:
                val = parse_brl(m.group(1))
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="net_debt_mm",
                        metric_value=val,
                        metric_unit="R$ milhoes",
                        source_pdf=source_pdf,
                    ))

            # Net debt / EBITDA: "Dívida Líquida/EBITDA de 3,2x"
            m = re.search(r"D[íi]vida\s+L[íi]quida\s*/\s*EBITDA\s+de\s+([\d.,]+)\s*x", page6, re.IGNORECASE)
            if m:
                val = parse_brl(m.group(1))
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="net_debt_ebitda_ratio",
                        metric_value=val,
                        metric_unit="x",
                        source_pdf=source_pdf,
                    ))

            # Total debt from page 6: "Total 780,3"
            m = re.search(r"Total\s+([\d.,]+)\s*-\s*-", page6)
            if not m:
                m = re.search(r"Total\s+([\d.,]+)", page6)
            if m:
                val = parse_brl(m.group(1))
                if val and val > 100:  # should be > 100 MM
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="total_debt_mm",
                        metric_value=val,
                        metric_unit="R$ milhoes",
                        source_pdf=source_pdf,
                    ))

            # Transmission availability from page 5: "99,83% de disponibilidade"
            m = re.search(r"transmissoras\s+operaram\s+com\s+([\d.,]+)\s*%\s*de\s+disponibilidade", page5, re.IGNORECASE)
            if m:
                val = parse_pct(m.group(1) + "%")
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="transmission_availability_pct",
                        metric_value=val,
                        metric_unit="%",
                        source_pdf=source_pdf,
                    ))

            # Curtailment impact percentage from page 13 or 14
            m = re.search(r"curtailment\s+representou\s+cerca\s+de\s+([\d.,]+)\s*%", page13, re.IGNORECASE)
            if m:
                val = parse_pct(m.group(1) + "%")
                if val:
                    metrics.append(FundMetricKV(
                        fund_id=self.FUND_ID,
                        reference_date=ref_date,
                        metric_key="curtailment_pct_of_potential",
                        metric_value=val,
                        metric_unit="%",
                        source_pdf=source_pdf,
                    ))

            # Portfolio composition from page 5
            for asset_type, pattern in [
                ("portfolio_pct_transmission", r"Transmiss[ãa]o.*?(\d{1,2})\s*%"),
                ("portfolio_pct_hydro", r"Gera[çc][ãa]o\s+H[íi]drica.*?(\d{1,2})\s*%"),
                ("portfolio_pct_wind", r"Gera[çc][ãa]o\s+E[óo]lica.*?(\d{1,2})\s*%"),
                ("portfolio_pct_cash", r"Caixa.*?(\d{1,2})\s*%"),
            ]:
                # Look at the composition section on page 5
                comp_match = re.search(r"Composi[çc][ãa]o.*?Fundo.*?\n(.*?)Resultado", page5, re.DOTALL | re.IGNORECASE)
                if comp_match:
                    section = comp_match.group(1)
                    # Extract percentages: "46%", "29%", "23%", "2%"
                    pct_matches = re.findall(r"(\d{1,3})\s*%", section)
                    # These are typically in order: Transmission, Hydro, Wind, Cash
                    # But layout varies; skip structured extraction here

        except Exception:
            pass

        return metrics
