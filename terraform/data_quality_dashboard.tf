# Separate Data Quality Dashboard with detailed metrics and table outputs

resource "aws_cloudwatch_dashboard" "data_quality" {
  count          = var.create_athena_resources ? 1 : 0
  dashboard_name = "schemahub-data-quality"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: Health Score and Key Metrics
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "🏥 Overall Health Score"
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
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "📊 Total Records"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "TotalRecords", { "stat" : "Average" }]
          ]
          period = 3600
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "🪙 Active Products"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ProductCount", { "stat" : "Average" }]
          ]
          period = 3600
          sparkline = true
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 0
        width  = 6
        height = 6
        properties = {
          title  = "⚠️ Duplicate Trades"
          view   = "singleValue"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "DuplicateTradesTotal", { "stat" : "Average" }]
          ]
          period = 3600
          sparkline = true
        }
      },

      # Row 2: Data Freshness
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "⏰ Average Data Freshness (Minutes)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "AvgDataFreshnessMinutes", { "stat" : "Average", "label" : "Avg Freshness" }],
            ["SchemaHub/DataQuality", "MaxDataFreshnessMinutes", { "stat" : "Average", "label" : "Max Freshness" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, label = "Minutes" }
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "🔴 Stale Products (>60 min old)"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "StaleProductCount", { "stat" : "Average", "color" : "#d62728" }]
          ]
          period = 3600
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
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "📈 Products with Duplicates"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ProductsWithDuplicates", { "stat" : "Average", "color" : "#9467bd" }]
          ]
          period = 3600
        }
      },

      # Row 3: Gap Detection
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "🕳️ Data Gaps by Severity"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "WarningGapsTotal", { "stat" : "Average", "label" : "Warning (>3σ)", "color" : "#ff7f0e" }],
            ["SchemaHub/DataQuality", "SevereGapsTotal", { "stat" : "Average", "label" : "Severe (>4σ)", "color" : "#d62728" }],
            ["SchemaHub/DataQuality", "ExtremeGapsTotal", { "stat" : "Average", "label" : "Extreme (>5σ)", "color" : "#7f0000" }]
          ]
          period = 3600
          stacked = true
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "🔥 Extreme Gaps Over Time"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "ExtremeGapsTotal", { "stat" : "Average", "color" : "#d62728" }]
          ]
          period = 3600
          annotations = {
            horizontal = [
              { value = 0, color = "#2ca02c", label = "Healthy" },
              { value = 5, color = "#ff7f0e", label = "Warning" },
              { value = 10, color = "#d62728", label = "Critical" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "📊 Health Score Trend"
          view   = "timeSeries"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "OverallHealthScore", { "stat" : "Average", "color" : "#2ca02c" }]
          ]
          period = 3600
          yAxis = {
            left = { min = 0, max = 100 }
          }
          annotations = {
            horizontal = [
              { value = 80, color = "#2ca02c", label = "Healthy" },
              { value = 50, color = "#ff7f0e", label = "Degraded" }
            ]
          }
        }
      },

      # Row 4: Per-Product Freshness (top products)
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "🕐 Freshness by Product (Minutes Since Last Trade)"
          view   = "bar"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "FreshnessMinutes", "ProductId", "BTC-USD", { "stat" : "Average" }],
            ["...", "ETH-USD", { "stat" : "Average" }],
            ["...", "SOL-USD", { "stat" : "Average" }],
            ["...", "DOGE-USD", { "stat" : "Average" }],
            ["...", "XRP-USD", { "stat" : "Average" }],
            ["...", "ADA-USD", { "stat" : "Average" }],
            ["...", "AVAX-USD", { "stat" : "Average" }],
            ["...", "LINK-USD", { "stat" : "Average" }]
          ]
          period = 3600
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "📦 Records by Product"
          view   = "bar"
          region = var.aws_region
          metrics = [
            ["SchemaHub/DataQuality", "RecordsPerProduct", "ProductId", "BTC-USD", { "stat" : "Average" }],
            ["...", "ETH-USD", { "stat" : "Average" }],
            ["...", "SOL-USD", { "stat" : "Average" }],
            ["...", "DOGE-USD", { "stat" : "Average" }],
            ["...", "XRP-USD", { "stat" : "Average" }],
            ["...", "ADA-USD", { "stat" : "Average" }],
            ["...", "AVAX-USD", { "stat" : "Average" }],
            ["...", "LINK-USD", { "stat" : "Average" }]
          ]
          period = 3600
        }
      },

      # Row 5: Lambda Logs Insights - Raw Data Tables
      {
        type   = "log"
        x      = 0
        y      = 24
        width  = 24
        height = 8
        properties = {
          title  = "📋 Latest Data Quality Report (Raw Table)"
          region = var.aws_region
          query  = <<-EOT
SOURCE '/aws/lambda/schemahub-data-quality'
| filter @message like /total_records/
| parse @message '"total_records": *,' as total_records
| parse @message '"product_count": *,' as product_count  
| parse @message '"avg_freshness_minutes": *,' as avg_freshness
| parse @message '"stale_products": *,' as stale_products
| parse @message '"warning_gaps": *,' as warning_gaps
| parse @message '"severe_gaps": *,' as severe_gaps
| parse @message '"extreme_gaps": *,' as extreme_gaps
| parse @message '"duplicates": *,' as duplicates
| parse @message '"health_score": *' as health_score
| display @timestamp, total_records, product_count, avg_freshness, stale_products, warning_gaps, severe_gaps, extreme_gaps, duplicates, health_score
| sort @timestamp desc
| limit 20
EOT
          view   = "table"
        }
      },

      # Row 6: Detailed Product Overview from Lambda logs
      {
        type   = "log"
        x      = 0
        y      = 32
        width  = 12
        height = 8
        properties = {
          title  = "📊 Per-Product Overview (from latest run)"
          region = var.aws_region
          query  = <<-EOT
SOURCE '/aws/lambda/schemahub-data-quality'
| filter @message like /"overview":/
| parse @message '"product_id": "*"' as product_id
| parse @message '"total_records": "*"' as total_records
| parse @message '"total_volume": "*"' as total_volume
| filter product_id != ""
| display product_id, total_records, total_volume
| sort total_records desc
| limit 50
EOT
          view   = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 32
        width  = 12
        height = 8
        properties = {
          title  = "⏰ Per-Product Freshness (from latest run)"
          region = var.aws_region
          query  = <<-EOT
SOURCE '/aws/lambda/schemahub-data-quality'
| filter @message like /"freshness":/
| parse @message '"product_id": "*"' as product_id
| parse @message '"minutes_since_last_trade": "*"' as minutes_since_last
| filter product_id != "" and minutes_since_last != ""
| display product_id, minutes_since_last
| sort minutes_since_last desc
| limit 50
EOT
          view   = "table"
        }
      },

      # Row 7: Gap and Duplicate Details
      {
        type   = "log"
        x      = 0
        y      = 40
        width  = 12
        height = 8
        properties = {
          title  = "🕳️ Gap Detection by Product"
          region = var.aws_region
          query  = <<-EOT
SOURCE '/aws/lambda/schemahub-data-quality'
| filter @message like /"gaps":/
| parse @message '"product_id": "*"' as product_id
| parse @message '"warning_gaps": "*"' as warning_gaps
| parse @message '"severe_gaps": "*"' as severe_gaps
| parse @message '"extreme_gaps": "*"' as extreme_gaps
| filter product_id != ""
| display product_id, warning_gaps, severe_gaps, extreme_gaps
| sort extreme_gaps desc
| limit 50
EOT
          view   = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 40
        width  = 12
        height = 8
        properties = {
          title  = "⚠️ Products with Duplicates"
          region = var.aws_region
          query  = <<-EOT
SOURCE '/aws/lambda/schemahub-data-quality'
| filter @message like /"duplicates":/
| parse @message '"product_id": "*"' as product_id
| parse @message '"duplicate_count": "*"' as duplicate_count
| filter product_id != "" and duplicate_count != "0"
| display product_id, duplicate_count
| sort duplicate_count desc
| limit 50
EOT
          view   = "table"
        }
      },

      # Row 8: Instructions
      {
        type   = "text"
        x      = 0
        y      = 48
        width  = 24
        height = 3
        properties = {
          markdown = <<-EOT
## 📖 Data Quality Metrics Guide

| Metric | Description | Thresholds |
|--------|-------------|------------|
| **Health Score** | Overall data health (0-100) | 🟢 >80 Healthy, 🟡 50-80 Degraded, 🔴 <50 Critical |
| **Freshness** | Minutes since last trade per product | 🟢 <15 min, 🟡 15-60 min, 🔴 >60 min (stale) |
| **Gap Detection** | Anomalous time gaps using z-scores | Warning >3σ, Severe >4σ, Extreme >5σ |
| **Duplicates** | Trades with same trade_id per product | Any duplicates indicate data issues |

**Athena Queries**: For detailed analysis, use the saved queries in Athena workgroup `schemahub`

**Data refreshes hourly** via Lambda function `schemahub-data-quality`
EOT
        }
      }
    ]
  })
}
