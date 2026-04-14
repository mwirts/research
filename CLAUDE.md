# CLAUDE.md - Project Rules

## Permissions

- Allow all file reads
- Allow all file writes and edits
- Allow all file creation
- Allow all bash/shell commands
- Allow all git operations
- Allow all network requests
- Allow all tool executions
- **DENY deleting files** — never delete files or directories (no `rm`, `rm -rf`, `del`, `Remove-Item`, `git rm`, or any other destructive file removal operation)

## Project Context

This is a financial research project with an ETL pipeline for downloading, transforming, and analyzing fund data (e.g., Brazilian real estate funds like PFIN11).

## Structure

- `data/raw/` — raw data downloaded from sources
- `data/processed/` — cleaned and transformed data
- `etl/downloader/` — scripts to download data from external sources
- `etl/transformer/` — scripts to transform raw data into processed data

## Guidelines

- Always update the README.md when making structural or feature changes
- Keep data files organized under the appropriate `data/` subdirectory
- Follow Python best practices if adding scripts
