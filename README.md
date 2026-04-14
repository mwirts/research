# Research

Pipeline de pesquisa e análise de dados financeiros, com foco em fundos de investimento imobiliário (FIIs) brasileiros.

## Estrutura do Projeto

```
research/
├── data/
│   ├── raw/              # Dados brutos baixados das fontes
│   │   └── funds/
│   │       └── pfin11/
│   │           └── monthly_report/
│   └── processed/        # Dados limpos e transformados
├── etl/
│   ├── downloader/       # Scripts de download de dados externos
│   └── transformer/      # Scripts de transformação e limpeza de dados
├── CLAUDE.md             # Regras de configuração do Claude Code
└── README.md
```

## Visão Geral

O projeto implementa um pipeline ETL (Extract, Transform, Load) para:

1. **Extract** — Baixar relatórios e dados de fundos de investimento de fontes públicas
2. **Transform** — Limpar, normalizar e estruturar os dados brutos
3. **Load** — Armazenar dados processados para análise

## Fundos Monitorados

| Ticker | Tipo | Status |
|--------|------|--------|
| PFIN11 | FII  | Em andamento |

## Como Usar

> Em desenvolvimento — instruções serão adicionadas conforme o projeto evolui.

## Changelog

- **2026-04-14** — Estrutura inicial do projeto criada (diretórios `data/`, `etl/`, configuração do Claude Code)
