"""
Download monthly reports (Relatório Gerencial / Mensal) for FIP-IE funds.

Primary source: B3 SIG API (api-trading.b3.com.br).
Fallback sources for funds without reports on B3 SIG:
  - BRZP: brzinfraportos.com.br (ASP.NET server-rendered)
  - PICE: MZIQ filemanager API (pice11.com.br backend)

Usage:
    python etl/downloader/b3_monthly_reports.py PFIN --dry-run
    python etl/downloader/b3_monthly_reports.py PFIN AZIN PPEI VIGT PICE BRZP
    python etl/downloader/b3_monthly_reports.py --all
    python etl/downloader/b3_monthly_reports.py --list-funds
"""

import argparse
import base64
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------
SIG_LISTING = "https://api-trading.b3.com.br/api/sig/issuers/funds/{sigla}/documents"
SIG_DOWNLOAD = "https://bvmf.bmfbovespa.com.br/sig/FormConsultaPdfDocumentoFundos.asp"
B3_LIST_FUNDS = "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/GetListFunds"

BRZP_LIST = "https://www.brzinfraportos.com.br/list.aspx"
BRZP_DOWNLOAD = "https://www.brzinfraportos.com.br/Download.aspx"
BRZP_CHANNEL_ID = "ZRRfgnbDZdmBw9EhGwen/w=="

MZIQ_API = "https://apicatalog.mziq.com/filemanager/company"
PICE_MZIQ_COMPANY = "8163f194-c17f-4123-9601-759a461bdf41"
PICE_MZIQ_CATEGORY = "relatórios-mensais"

SIG_DOC_TYPES = [1, 3]

# Default funds to download when no arguments are given
DEFAULT_FUNDS = ["PFIN", "AZIN", "PPEI", "VIGT", "PICE", "BRZP"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Month parsing (shared across all sources)
# ---------------------------------------------------------------------------
MONTH_MAP = {
    "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03",
    "abril": "04", "maio": "05", "junho": "06",
    "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "outrubro": "10", "novembro": "11", "dezembro": "12",
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
}

_MONTH_NAMES = (
    r"Janeiro|Fevereiro|Mar[cç]o|Abril|Maio|Junho"
    r"|Julho|Agosto|Setembro|Outrubro|Outubro|Novembro|Dezembro"
    r"|Jan|Fev|Mar|Abr|Mai|Jun|Jul|Ago|Set|Out|Nov|Dez"
)
RE_MONTH_YEAR = re.compile(rf"({_MONTH_NAMES})[/\s]*(?:de\s+)?(\d{{2,4}})", re.IGNORECASE)
RE_NUMERIC_DATE = re.compile(r"\b(\d{1,2})[./](\d{4})\b")
RE_MONTH_ONLY = re.compile(rf"({_MONTH_NAMES})\s*$", re.IGNORECASE)

RE_RELATORIO = re.compile(
    r"Relat[oó]rio\s+(?:Gerencial|Mensal)|Carta\s+d[ao]\s+Gestor[a]?",
    re.IGNORECASE,
)
RE_EXCLUDE = re.compile(
    r"\d[TQ]\d{4}|Laudo\s+de\s+Avalia[cç][aã]o",
    re.IGNORECASE,
)


def _resolve_month(name):
    key = name.lower().replace("ç", "c")
    return MONTH_MAP.get(name.lower()) or MONTH_MAP.get(key)


def _month_before(timestamp):
    y, m = int(timestamp[:4]), int(timestamp[5:7])
    m -= 1
    if m == 0:
        m, y = 12, y - 1
    return str(y), f"{m:02d}"


def extract_month_year(nome, data_timestamp=""):
    """Extract (year, month) from a document name string."""
    match = RE_MONTH_YEAR.search(nome)
    if match:
        year = match.group(2)
        if len(year) == 2:
            year = "20" + year
        return year, _resolve_month(match.group(1))

    match = RE_NUMERIC_DATE.search(nome)
    if match:
        return match.group(2), match.group(1).zfill(2)

    match = RE_MONTH_ONLY.search(nome)
    if match and data_timestamp:
        month = _resolve_month(match.group(1))
        year = data_timestamp[:4]
        if month:
            upload_month = int(data_timestamp[5:7])
            if int(month) > upload_month:
                year = str(int(year) - 1)
        return year, month

    if data_timestamp:
        return _month_before(data_timestamp)

    return None, None


def build_filename(sigla, doc):
    ticker = (sigla + "11").lower()
    year, month = extract_month_year(doc["nome"], doc["data"])
    if year and month:
        return f"{ticker}_relatorio_mensal_{year}_{month}.pdf"
    safe = doc["data"].replace(":", "-").replace("T", "_")
    return f"{ticker}_relatorio_{safe}.pdf"


# ---------------------------------------------------------------------------
# HTTP client (shared session)
# ---------------------------------------------------------------------------
def _make_session(max_retries=5):
    s = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=max_retries, backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
        )
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })
    return s


def _download_pdf(session, url, output_path, timeout=60, **kwargs):
    """Download a PDF to output_path. Returns True if downloaded, False if skipped."""
    if output_path.exists():
        logger.debug("Skip (exists): %s", output_path.name)
        return False

    resp = session.get(url, timeout=timeout, stream=True, **kwargs)
    resp.raise_for_status()

    first = next(resp.iter_content(chunk_size=4), b"")
    if first != b"%PDF":
        logger.warning("%s: might not be PDF (first bytes: %r)", output_path.name, first)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(".tmp")
    with open(tmp, "wb") as f:
        f.write(first)
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    tmp.rename(output_path)

    logger.info("Downloaded: %s (%s bytes)", output_path.name, f"{output_path.stat().st_size:,}")
    return True


# ===================================================================
# Source 1: B3 SIG API (primary — works for most FIP-IE funds)
# ===================================================================
class B3SIGSource:
    def __init__(self, session):
        self.session = session

    def list_reports(self, sigla):
        by_timestamp = {}
        for dt in SIG_DOC_TYPES:
            url = SIG_LISTING.format(sigla=sigla)
            logger.debug("GET %s?type=%d", url, dt)
            resp = self.session.get(url, params={"type": dt},
                                    timeout=60, verify=False)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            for node in root.findall(".//ARQUIVO"):
                nome = (node.findtext("NOME") or "").strip()
                data = (node.findtext("DATA") or "").strip()
                data_com = (node.findtext("DATA_COMUNICADO") or "").strip()
                if nome and data and RE_RELATORIO.search(nome) and not RE_EXCLUDE.search(nome):
                    by_timestamp[data] = {"nome": nome, "data": data, "data_comunicado": data_com}
            logger.info("[%s] SIG type=%d → %d doc(s)", sigla, dt, len(root.findall(".//ARQUIVO")))

        by_filename = {}
        for ts, doc in sorted(by_timestamp.items()):
            fn = build_filename(sigla, doc)
            by_filename[fn] = (ts, doc)
        return sorted(by_filename.values())

    def download(self, sigla, doc, output_path):
        return _download_pdf(
            self.session, SIG_DOWNLOAD, output_path,
            params={"strSigla": sigla, "strData": doc["data"]},
        )


# ===================================================================
# Source 2: BRZP website (brzinfraportos.com.br)
# ===================================================================
class BRZPWebsiteSource:
    """Scrape brzinfraportos.com.br for BRZP monthly reports.

    The site is ASP.NET with year filter: list.aspx?idCanal=...&ano=YYYY
    Download links use: Download.aspx?Arquivo=<base64-token>
    """
    YEARS = range(2020, 2030)

    def __init__(self, session):
        self.session = session

    def list_reports(self, sigla):
        if sigla != "BRZP":
            return []

        re_link = re.compile(
            r'Download\.aspx\?Arquivo=([^"&]+).*?'
            r'Relat[oó]rio\s+Mensal\s*[–-]\s*'
            rf'({_MONTH_NAMES})\s+(?:de\s+)?(\d{{4}})',
            re.IGNORECASE | re.DOTALL,
        )
        all_docs = {}
        for year in self.YEARS:
            resp = self.session.get(
                BRZP_LIST,
                params={"idCanal": BRZP_CHANNEL_ID, "ano": year},
                timeout=60,
            )
            if resp.status_code != 200:
                continue
            for m in re_link.finditer(resp.text):
                arquivo_token, month_name, doc_year = m.group(1), m.group(2), m.group(3)
                month = _resolve_month(month_name)
                if month:
                    doc = {
                        "nome": f"Relatório Mensal – {month_name} de {doc_year}",
                        "data": f"{doc_year}-{month}-01T00:00:00.000",
                        "data_comunicado": "",
                        "_arquivo": arquivo_token,
                    }
                    fn = build_filename(sigla, doc)
                    all_docs[fn] = (doc["data"], doc)
            logger.info("[BRZP] year=%d → %d report(s) cumulative", year, len(all_docs))
            if not resp.text or "Nenhum" in resp.text:
                continue

        return sorted(all_docs.values())

    def download(self, sigla, doc, output_path):
        return _download_pdf(
            self.session, BRZP_DOWNLOAD, output_path,
            params={"Arquivo": doc["_arquivo"]},
        )


# ===================================================================
# Source 3: MZIQ API (pice11.com.br backend)
# ===================================================================
class MZIQSource:
    """Fetch reports from MZIQ filemanager API (used by Pátria funds).

    POST https://apicatalog.mziq.com/filemanager/company/{id}/filter/categories/meta
    Returns JSON with document_metas containing file_url for direct download.
    """
    def __init__(self, session, company_id, category):
        self.session = session
        self.company_id = company_id
        self.category = category

    def list_reports(self, sigla):
        url = f"{MZIQ_API}/{self.company_id}/filter/categories/meta"
        payload = {
            "company": self.company_id,
            "categories": [self.category],
            "categoryInternalNames": [self.category],
            "language": "pt_BR",
            "published": True,
        }
        logger.debug("POST %s", url)
        resp = self.session.post(url, json=payload, timeout=60, verify=False)
        resp.raise_for_status()

        data = resp.json().get("data", {})
        metas = data.get("document_metas", [])
        logger.info("[%s] MZIQ → %d document(s)", sigla, len(metas))

        by_filename = {}
        for m in metas:
            title = m.get("file_title", "")
            pub_date = m.get("file_published_date", "")  # "2026-03-17T00:00:00.000Z"
            file_url = m.get("file_url", "")
            if not file_url:
                continue
            doc = {
                "nome": title,
                "data": pub_date,
                "data_comunicado": "",
                "_url": file_url,
            }
            fn = build_filename(sigla, doc)
            by_filename[fn] = (pub_date, doc)

        return sorted(by_filename.values())

    def download(self, sigla, doc, output_path):
        return _download_pdf(
            self.session, doc["_url"], output_path, verify=False,
        )


# ===================================================================
# Fallback registry
# ===================================================================
# Map sigla → fallback source factory.  Called only when B3 SIG returns 0 reports.
FALLBACK_SOURCES = {
    "BRZP": lambda session: BRZPWebsiteSource(session),
    "PICE": lambda session: MZIQSource(session, PICE_MZIQ_COMPANY, PICE_MZIQ_CATEGORY),
}


# ===================================================================
# B3 fund list
# ===================================================================
def list_fip_funds(session):
    all_funds = []
    for page in range(1, 20):
        payload = json.dumps({"language": "pt-br", "typeFund": "FIP",
                              "pageNumber": page, "pageSize": 50})
        encoded = base64.b64encode(payload.encode()).decode()
        resp = session.get(f"{B3_LIST_FUNDS}/{encoded}", timeout=30)
        if resp.status_code != 200:
            break
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        all_funds.extend(results)
        if page >= data.get("page", {}).get("totalPages", 1):
            break
    return all_funds


# ===================================================================
# Process one fund
# ===================================================================
def process_fund(session, sigla, base_dir, dry_run=False):
    ticker = f"{sigla}11"
    out_dir = base_dir / ticker.lower() / "monthly_report"

    logger.info("=" * 60)
    logger.info("Fund: %s (%s)", sigla, ticker)
    logger.info("=" * 60)

    # Try B3 SIG first
    source = B3SIGSource(session)
    reports = source.list_reports(sigla)

    # Fallback if SIG returned nothing
    if not reports and sigla in FALLBACK_SOURCES:
        logger.info("[%s] No reports on B3 SIG, trying fallback source...", sigla)
        source = FALLBACK_SOURCES[sigla](session)
        reports = source.list_reports(sigla)

    logger.info("[%s] %d unique monthly report(s)", sigla, len(reports))

    if not reports:
        logger.warning("[%s] No monthly reports found on any source.", sigla)
        return 0, 0

    if dry_run:
        print(f"\n  {sigla} ({ticker}) — {len(reports)} report(s)")
        print(f"  Output: {out_dir}")
        print(f"  {'#':>4}  {'Filename':<45}  {'Published':<12}  Name")
        print(f"  {'-'*105}")
        for i, (_, doc) in enumerate(reports, 1):
            fn = build_filename(sigla, doc)
            pub = doc.get("data_comunicado") or doc.get("data", "")[:10]
            print(f"  {i:>4}  {fn:<45}  {pub:<12}  {doc['nome']}")
        return len(reports), 0

    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded, skipped = 0, 0
    for _, doc in reports:
        fn = build_filename(sigla, doc)
        if source.download(sigla, doc, out_dir / fn):
            downloaded += 1
        else:
            skipped += 1
        time.sleep(0.3)

    logger.info("[%s] Done: %d downloaded, %d skipped", sigla, downloaded, skipped)
    return downloaded, skipped


# ===================================================================
# CLI
# ===================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Download monthly reports for FIP-IE funds",
    )
    parser.add_argument(
        "funds", nargs="*", metavar="SIGLA",
        help="Fund sigla(s) without '11' suffix (e.g. PFIN AZIN VIGT)",
    )
    parser.add_argument("--all", action="store_true", help="All listed FIP funds on B3")
    parser.add_argument("--list-funds", action="store_true", help="List available FIP funds and exit")
    parser.add_argument(
        "--output-dir", type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "funds",
        help="Base directory for fund data (default: data/raw/funds/)",
    )
    parser.add_argument("--dry-run", action="store_true", help="List documents without downloading")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = _make_session()

    if args.list_funds:
        funds = list_fip_funds(session)
        print(f"\n{'Sigla':<8} {'Ticker':<10} Fund Name")
        print("-" * 80)
        for f in funds:
            acr = f["acronym"]
            print(f"{acr:<8} {acr + '11':<10} {f['fundName']}")
        print(f"\nTotal: {len(funds)} FIP fund(s)")
        return

    if args.all:
        siglas = [f["acronym"] for f in list_fip_funds(session)]
    elif args.funds:
        siglas = [s.upper().replace("11", "") for s in args.funds]
    else:
        siglas = DEFAULT_FUNDS

    total_dl, total_sk = 0, 0
    for sigla in siglas:
        dl, sk = process_fund(session, sigla, args.output_dir, args.dry_run)
        total_dl += dl
        total_sk += sk

    if not args.dry_run and len(siglas) > 1:
        logger.info("=" * 60)
        logger.info("TOTAL: %d downloaded, %d skipped across %d fund(s)", total_dl, total_sk, len(siglas))


if __name__ == "__main__":
    main()
