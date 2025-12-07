# SchemaHub (Coinbase MVP)

SchemaHub is a lightweight data platform for unifying crypto exchange trades into Iceberg tables. This first cut focuses on **Coinbase** ingestion so you can start experimenting locally before wiring up other exchanges.

## Features (current state)

- Coinbase connector that fetches recent trades via the public REST API.
- CLI entry point to ingest Coinbase trades into a local JSONL file that mirrors the `raw_coinbase_trades` shape.
- Mapping config stub for Coinbase → `trades_unified` to guide future transformation work.

## Quickstart

1. Install dependencies (Python 3.11+ recommended):

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Fetch the latest trades for a product (e.g., BTC-USD) and write them to `data/raw_coinbase_trades.jsonl`:

   ```bash
   python -m schemahub.cli ingest BTC-USD --limit 50
   ```

   Use `--before` or `--after` with a `trade_id` to paginate.

3. Inspect the output:

   ```bash
   head data/raw_coinbase_trades.jsonl
   ```

## Repository layout

```
schemahub/
  cli.py                 # CLI entry point (ingest coinbase)
  connectors/
    coinbase.py          # Coinbase connector + raw record conversion
  raw_writer.py          # JSONL helper for raw outputs
config/
  mappings/
    coinbase_trades.yaml # Mapping stub for future unification
```

## Next steps

- Add Binance/Kraken connectors following the same pattern.
- Implement Iceberg writers for raw tables instead of local files.
- Build the unifier that reads raw tables and writes to `trades_unified` using the mapping configs.
