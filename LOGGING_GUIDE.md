# Logging Guide

This project uses Python's built-in `logging` module to help debug and monitor the ingest process. Comprehensive logging has been added throughout the codebase.

## Running with Logging

### Default (INFO level)
By default, logs are printed to stderr at INFO level:

```bash
python3 -m schemahub.cli ingest BTC-USD
```

This will show important events like:
- Starting ingest for products
- Batch fetches and trade counts
- S3 write locations and status
- Checkpoint saves
- API request details
- Errors with full tracebacks

### Verbose (DEBUG level)
For detailed debugging information when the process stalls, use the `-v` or `--verbose` flag:

```bash
python3 -m schemahub.cli ingest BTC-USD -v
# or
python3 -m schemahub.cli ingest BTC-USD --verbose
```

Debug logs include:
- Individual trade filtering
- API timeout and connection details
- Response parsing
- Detailed batch processing
- Comprehensive error context

## Log Format

All logs follow this format:
```
TIMESTAMP - MODULE_NAME - LOG_LEVEL - MESSAGE
```

Example:
```
2025-12-16 14:30:45,123 - schemahub.cli - INFO - Starting ingest for BTC-USD: limit=1000, before=None, after=None, cutoff_minutes=45
2025-12-16 14:30:45,456 - schemahub.connectors.coinbase - DEBUG - Using public Coinbase API (no authentication)
2025-12-16 14:30:46,789 - schemahub.cli - INFO - Collected 50 trades for BTC-USD
```

## Log Levels

- **DEBUG**: Fine-grained diagnostic information
  - API request details
  - Cursor positions
  - Checkpoint operations
  - Parsed record counts

- **INFO**: General informational messages
  - Starting/completing operations
  - Number of records processed
  - S3 write locations
  - Product counts

- **ERROR**: Error events with full exception tracebacks
  - API failures
  - S3 write failures
  - Missing configuration

## Logging Modules

Logging is configured in the following modules:

- `schemahub.cli` - Main CLI orchestration
- `schemahub.connectors.coinbase` - Coinbase API interactions
- `schemahub.raw_writer` - S3 write operations

## Troubleshooting with Logs

When something goes wrong, enable verbose logging:

```bash
python3 -m schemahub.cli ingest BTC-USD -v 2>&1 | tee ingest.log
```

This will:
1. Show all DEBUG and INFO logs
2. Save them to `ingest.log` for later inspection
3. Display them in real-time

Common issues to look for in logs:

- **"API request failed"** - Check network connectivity, Coinbase API status
- **"Failed to write to S3"** - Check AWS credentials and bucket permissions
- **"Product seed file not found"** - Check that seed file exists at specified path
- **"No more trades fetched"** - May indicate API rate limiting or no new data

## Example Workflow

```bash
# 1. Try ingest with defaults
python3 -m schemahub.cli ingest BTC-USD

# 2. If something fails, try with verbose logging
python3 -m schemahub.cli ingest BTC-USD -v

# 3. Save logs for analysis
python3 -m schemahub.cli ingest BTC-USD -v 2>&1 | tee debug.log

# 4. Search logs for errors
grep -i error debug.log
grep -i "failed" debug.log
```

## Adding Custom Logging

To add logging to your own code:

```python
import logging

logger = logging.getLogger(__name__)

# Use logger in your functions
logger.info(f"Processing product: {product_id}")
logger.debug(f"Trade count: {len(trades)}")
logger.error(f"Failed to fetch: {error}", exc_info=True)
```

The logging configuration will automatically route your logs to stderr with appropriate formatting and level filtering.
