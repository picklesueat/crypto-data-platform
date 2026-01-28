# Production Data Pipeline Dashboard with comprehensive operational metrics
# Shows pipeline health, job success/failure, API health, and data quality

resource "aws_cloudwatch_dashboard" "data_quality" {
  count          = var.create_athena_resources ? 1 : 0
  dashboard_name = "schemahub-data-quality"

  dashboard_body = jsonencode({
    widgets = [
      # ========== ROW 0: HEADLINE STATS ==========
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# SchemaHub Crypto Data Pipeline - Production Dashboard"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 1
        width  = 4
        height = 3
        properties = {
          title  = "Total Records"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "TotalRecords", { "stat" : "Average" }]
          ]
          period    = 3600
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 4
        y      = 1
        width  = 4
        height = 3
        properties = {
          title  = "Parquet Size (GB)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ParquetSizeGB", { "stat" : "Average" }]
          ]
          period    = 3600
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 1
        width  = 4
        height = 3
        properties = {
          title  = "Data Span (Years)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            [{ "expression" : "m1/365", "label" : "Years", "id" : "e1" }],
            ["SchemaHub/DataQuality", "DataSpanDays", { "stat" : "Average", "id" : "m1", "visible" : false }]
          ]
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 1
        width  = 4
        height = 3
        properties = {
          title  = "Active Products"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ProductCount", { "stat" : "Average" }]
          ]
          period    = 300
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 1
        width  = 4
        height = 3
        properties = {
          title  = "Health Score"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "OverallHealthScore", { "stat" : "Average", "color" : "#2ca02c" }]
          ]
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 20
        y      = 1
        width  = 4
        height = 3
        properties = {
          title  = "Avg Records/Day"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "AvgDailyGrowth", { "stat" : "Average" }]
          ]
          period = 3600
        }
      },

      # ========== ROW 1: PIPELINE OPERATIONS ==========
      {
        type   = "text"
        x      = 0
        y      = 4
        width  = 24
        height = 1
        properties = {
          markdown = "## Pipeline Operations"
        }
      },
      # Ingest Success Rate
      {
        type   = "metric"
        x      = 0
        y      = 5
        width  = 4
        height = 4
        properties = {
          title  = "Ingest Jobs (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "IngestSuccess", "Source", "coinbase", { "stat" : "Sum", "label" : "Success", "color" : "#2ca02c" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 4
        y      = 5
        width  = 4
        height = 4
        properties = {
          title  = "Trades Ingested (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "IngestTotalTrades", "Source", "coinbase", { "stat" : "Sum" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 5
        width  = 4
        height = 4
        properties = {
          title  = "Products Processed"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "IngestProductCount", "Source", "coinbase", { "stat" : "Sum" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 5
        width  = 4
        height = 4
        properties = {
          title  = "Lambda Invocations (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", "schemahub-data-quality", { "stat" : "Sum", "color" : "#1f77b4" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 5
        width  = 4
        height = 4
        properties = {
          title  = "Lambda Errors (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["AWS/Lambda", "Errors", "FunctionName", "schemahub-data-quality", { "stat" : "Sum", "color" : "#d62728" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 20
        y      = 5
        width  = 4
        height = 4
        properties = {
          title  = "Lambda Duration (Avg)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            [{ "expression" : "m1/1000", "label" : "Seconds", "id" : "e1" }],
            ["AWS/Lambda", "Duration", "FunctionName", "schemahub-data-quality", { "stat" : "Average", "id" : "m1", "visible" : false }]
          ]
          period = 3600
        }
      },
      # Pipeline Activity Over Time
      {
        type   = "metric"
        x      = 0
        y      = 9
        width  = 8
        height = 5
        properties = {
          title  = "Ingest Jobs Over Time"
          view   = "timeSeries"
          region = var.aws_region
          stacked = true
          metrics = [
            ["SchemaHub", "IngestSuccess", "Source", "coinbase", { "stat" : "Sum", "label" : "Success", "color" : "#2ca02c" }],
            ["SchemaHub", "IngestFailure", "Source", "coinbase", { "stat" : "Sum", "label" : "Failure", "color" : "#d62728" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Count" }
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 9
        width  = 8
        height = 5
        properties = {
          title  = "Trades Ingested Per Hour"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "IngestTotalTrades", "Source", "coinbase", { "stat" : "Sum", "label" : "Trades/Hour", "color" : "#1f77b4" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Trades" }
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 9
        width  = 8
        height = 5
        properties = {
          title  = "Lambda Executions"
          view   = "timeSeries"
          region = var.aws_region
          stacked = true
          metrics = [
            ["AWS/Lambda", "Invocations", "FunctionName", "schemahub-data-quality", { "stat" : "Sum", "label" : "Invocations", "color" : "#2ca02c" }],
            ["AWS/Lambda", "Errors", "FunctionName", "schemahub-data-quality", { "stat" : "Sum", "label" : "Errors", "color" : "#d62728" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Count" }
          }
        }
      },

      # ========== ROW 2: API HEALTH ==========
      {
        type   = "text"
        x      = 0
        y      = 14
        width  = 24
        height = 1
        properties = {
          markdown = "## API Health & Reliability"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 15
        width  = 4
        height = 4
        properties = {
          title  = "API Success (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "APISuccessCount", "Source", "coinbase", { "stat" : "Sum", "color" : "#2ca02c" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 4
        y      = 15
        width  = 4
        height = 4
        properties = {
          title  = "Rate Limit Errors (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "RateLimitErrors", "Source", "coinbase", { "stat" : "Sum", "color" : "#ff7f0e" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 15
        width  = 4
        height = 4
        properties = {
          title  = "Server Errors (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "ServerErrors", "Source", "coinbase", { "stat" : "Sum", "color" : "#d62728" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 15
        width  = 4
        height = 4
        properties = {
          title  = "Timeout Errors (24h)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "TimeoutErrors", "Source", "coinbase", { "stat" : "Sum", "color" : "#9467bd" }]
          ]
          period    = 86400
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 15
        width  = 4
        height = 4
        properties = {
          title  = "Circuit Breaker State"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "CircuitBreakerState", "Source", "coinbase", { "stat" : "Average", "label" : "0=Closed" }]
          ]
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 20
        y      = 15
        width  = 4
        height = 4
        properties = {
          title  = "Avg Response Time (ms)"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "ExchangeResponseTime", "Source", "coinbase", { "stat" : "Average" }]
          ]
          period    = 300
          sparkline = true
        }
      },
      # API Health Over Time
      {
        type   = "metric"
        x      = 0
        y      = 19
        width  = 8
        height = 5
        properties = {
          title  = "API Errors Over Time"
          view   = "timeSeries"
          region = var.aws_region
          stacked = true
          metrics = [
            ["SchemaHub", "RateLimitErrors", "Source", "coinbase", { "stat" : "Sum", "label" : "Rate Limit (429)", "color" : "#ff7f0e" }],
            ["SchemaHub", "ServerErrors", "Source", "coinbase", { "stat" : "Sum", "label" : "Server (5xx)", "color" : "#d62728" }],
            ["SchemaHub", "TimeoutErrors", "Source", "coinbase", { "stat" : "Sum", "label" : "Timeout", "color" : "#9467bd" }],
            ["SchemaHub", "ConnectionErrors", "Source", "coinbase", { "stat" : "Sum", "label" : "Connection", "color" : "#8c564b" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Errors" }
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 19
        width  = 8
        height = 5
        properties = {
          title  = "API Response Time (ms)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "ExchangeResponseTime", "Source", "coinbase", { "stat" : "Average", "label" : "Avg", "color" : "#1f77b4" }],
            ["SchemaHub", "ExchangeResponseTime", "Source", "coinbase", { "stat" : "p99", "label" : "p99", "color" : "#ff7f0e" }]
          ]
          period = 300
          yAxis = {
            left = { min = 0, label = "ms" }
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 19
        width  = 8
        height = 5
        properties = {
          title  = "Circuit Breaker Activity"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "CircuitBreakerOpens", "Source", "coinbase", { "stat" : "Sum", "label" : "Opens", "color" : "#d62728" }],
            ["SchemaHub", "CircuitBreakerState", "Source", "coinbase", { "stat" : "Average", "label" : "State (0=OK)", "color" : "#2ca02c", "yAxis" : "right" }]
          ]
          period = 300
          yAxis = {
            left  = { min = 0, label = "Opens" }
            right = { min = 0, max = 1, label = "State" }
          }
        }
      },

      # ========== ROW 3: DATA QUALITY ==========
      {
        type   = "text"
        x      = 0
        y      = 24
        width  = 24
        height = 1
        properties = {
          markdown = "## Data Quality & Health"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 25
        width  = 6
        height = 5
        properties = {
          title  = "Overall Health Score"
          view   = "gauge"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "OverallHealthScore", { "stat" : "Average" }]
          ]
          yAxis = {
            left = { min = 0, max = 100 }
          }
          annotations = {
            horizontal = [
              { value = 80, color = "#2ca02c", label = "Healthy" },
              { value = 50, color = "#ff7f0e", label = "Warning" },
              { value = 0, color = "#d62728", label = "Critical" }
            ]
          }
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 25
        width  = 6
        height = 5
        properties = {
          title  = "Duplicate Trades"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "DuplicateTradesTotal", { "stat" : "Average", "color" : "#ff7f0e" }]
          ]
          period    = 300
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 25
        width  = 12
        height = 5
        properties = {
          title  = "Health Score Trend"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "OverallHealthScore", { "stat" : "Average", "color" : "#2ca02c" }]
          ]
          period = 300
          yAxis = {
            left = { min = 0, max = 100 }
          }
        }
      },

      # ========== ROW 4: DATA FRESHNESS ==========
      {
        type   = "text"
        x      = 0
        y      = 30
        width  = 24
        height = 1
        properties = {
          markdown = "## Data Freshness"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 31
        width  = 8
        height = 5
        properties = {
          title  = "Average Data Freshness (Minutes)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "AvgDataFreshnessMinutes", { "stat" : "Average", "label" : "Avg Freshness" }],
            ["SchemaHub/DataQuality", "MaxDataFreshnessMinutes", { "stat" : "Average", "label" : "Max Freshness" }]
          ]
          period = 300
          yAxis = {
            left = { min = 0, label = "Minutes" }
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 31
        width  = 8
        height = 5
        properties = {
          title  = "Stale Products (>6 hours old)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "StaleProductCount", { "stat" : "Average", "color" : "#d62728" }]
          ]
          period = 300
          annotations = {
            horizontal = [
              { value = 5, color = "#ff7f0e", label = "Warning Threshold" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 31
        width  = 8
        height = 5
        properties = {
          title  = "Products with Duplicates"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ProductsWithDuplicates", { "stat" : "Average", "color" : "#9467bd" }]
          ]
          period = 300
        }
      },

      # ========== ROW 5: DATA COMPLETENESS ==========
      {
        type   = "text"
        x      = 0
        y      = 36
        width  = 24
        height = 1
        properties = {
          markdown = "## Data Completeness"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 37
        width  = 8
        height = 5
        properties = {
          title  = "Data Completeness (%)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "MinCompletenessPct", { "stat" : "Average", "label" : "Min Completeness", "color" : "#d62728" }],
            ["SchemaHub/DataQuality", "AvgCompletenessPct", { "stat" : "Average", "label" : "Avg Completeness", "color" : "#2ca02c" }]
          ]
          period = 300
          yAxis = {
            left = { min = 95, max = 100, label = "%" }
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 37
        width  = 8
        height = 5
        properties = {
          title  = "Missing Trades (from Trade ID Gaps)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "MissingTradesTotal", { "stat" : "Average", "color" : "#ff7f0e" }]
          ]
          period = 300
          annotations = {
            horizontal = [
              { value = 0, color = "#2ca02c", label = "Perfect" },
              { value = 10, color = "#ff7f0e", label = "Warning" },
              { value = 100, color = "#d62728", label = "Critical" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 37
        width  = 8
        height = 5
        properties = {
          title  = "Total Records Growth"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "TotalRecords", { "stat" : "Average", "color" : "#1f77b4" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Records" }
          }
        }
      },

      # ========== ROW 6: DATA FLOW ==========
      {
        type   = "text"
        x      = 0
        y      = 42
        width  = 24
        height = 1
        properties = {
          markdown = "## Data Flow & Storage"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 43
        width  = 12
        height = 5
        properties = {
          title  = "Daily Records Written"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "DailyRecordsWritten", { "stat" : "Average", "label" : "Records/Day" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Records" }
          }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 43
        width  = 12
        height = 5
        properties = {
          title  = "Parquet Size Over Time"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ParquetSizeGB", { "stat" : "Average", "label" : "Size (GB)" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "GB" }
          }
        }
      },

      # ========== ROW 7: PER-PRODUCT ACTIVITY ==========
      {
        type   = "text"
        x      = 0
        y      = 48
        width  = 24
        height = 1
        properties = {
          markdown = "## Per-Product Activity"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 49
        width  = 12
        height = 6
        properties = {
          title  = "Trades Ingested by Product (24h)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "ProductIngestCount", "Product", "SOL-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "SOL-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "MATIC-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "MATIC-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "XRP-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "XRP-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "ADA-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "ADA-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "AVAX-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "AVAX-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "DOT-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "DOT-USD" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Trades" }
          }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 49
        width  = 12
        height = 6
        properties = {
          title  = "Trades Ingested by Product (24h) - Continued"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub", "ProductIngestCount", "Product", "AAVE-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "AAVE-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "ACH-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "ACH-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "ABT-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "ABT-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "WLD-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "WLD-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "W-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "W-USD" }],
            ["SchemaHub", "ProductIngestCount", "Product", "PYTH-USD", "Source", "coinbase", { "stat" : "Sum", "label" : "PYTH-USD" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Trades" }
          }
        }
      },

      # ========== ROW 8: INSTRUCTIONS ==========
      {
        type   = "text"
        x      = 0
        y      = 55
        width  = 24
        height = 4
        properties = {
          markdown = <<-EOT
## Metrics Reference

| Section | Metrics | Description |
|---------|---------|-------------|
| **Pipeline Ops** | Ingest Jobs, Trades Ingested, Lambda Invocations | Real-time job execution counts and throughput |
| **API Health** | Success, Rate Limit (429), Server (5xx), Timeout | API reliability and error tracking |
| **Data Quality** | Health Score, Duplicates, Completeness | Data integrity metrics from Athena queries |
| **Freshness** | Avg/Max Freshness, Stale Products | How current the data is |

**Schedules:** Ingest & Transform every 3 hours | Data Quality every 6 hours

**Athena Queries:** For detailed analysis, use the saved queries in Athena workgroup `schemahub`
EOT
        }
      }
    ]
  })
}
