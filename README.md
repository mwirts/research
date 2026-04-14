# Research

Pipeline de pesquisa e análise de dados financeiros, com foco em fundos FIP-IE (Fundos de Investimento em Participações em Infraestrutura) listados na B3.

## Estrutura do Projeto

```
research/
├── data/
│   ├── raw/funds/           # Relatórios mensais por fundo (PDFs)
│   │   ├── pfin11/monthly_report/
│   │   ├── azin11/monthly_report/
│   │   ├── ppei11/monthly_report/
│   │   ├── vigt11/monthly_report/
│   │   ├── pice11/monthly_report/
│   │   └── brzp11/monthly_report/
│   └── processed/           # Dados limpos e transformados
├── etl/
│   ├── downloader/          # Scripts de download
│   │   └── b3_monthly_reports.py
│   └── transformer/         # Scripts de transformação
├── requirements.txt
└── README.md
```

## Visão Geral

O projeto implementa um pipeline ETL (Extract, Transform, Load) para:

1. **Extract** — Baixar relatórios gerenciais mensais de fundos FIP-IE
2. **Transform** — Limpar, normalizar e estruturar os dados brutos
3. **Load** — Armazenar dados processados para análise

## Fundos Monitorados

| Ticker | Fundo | Fonte |
|--------|-------|-------|
| PFIN11 | Perfin Apollo Energia FIP-IE | B3 SIG |
| AZIN11 | AZ Quest Infra-Yield II FIP-IE | B3 SIG |
| PPEI11 | Prisma Proton Energia FIP-IE | B3 SIG |
| VIGT11 | Vinci Energia FIP-IE | B3 SIG |
| PICE11 | Pátria Infra Energia Core FIP-IE | MZIQ API (fallback) |
| BRZP11 | BRZ Infra Portos FIP-IE | brzinfraportos.com.br (fallback) |

## Como Usar

```bash
pip install -r requirements.txt

# Baixar relatórios de todos os fundos monitorados
python etl/downloader/b3_monthly_reports.py

# Apenas listar sem baixar
python etl/downloader/b3_monthly_reports.py --dry-run

# Fundos específicos
python etl/downloader/b3_monthly_reports.py PFIN VIGT

# Listar todos os FIPs disponíveis na B3
python etl/downloader/b3_monthly_reports.py --list-funds

# Todos os FIPs listados na B3
python etl/downloader/b3_monthly_reports.py --all
```

Re-execuções são seguras — arquivos já baixados são ignorados automaticamente.

### Fontes de dados

O downloader usa a **API SIG da B3** (`api-trading.b3.com.br`) como fonte primária. Fundos que não publicam relatórios gerenciais na B3 têm fallback automático para o site da gestora (BRZP) ou API MZIQ (PICE).

## Changelog

- **2026-04-14** — Downloader de relatórios mensais FIP-IE (B3 SIG + fallbacks para BRZP e PICE)
- **2026-04-14** — Estrutura inicial do projeto
