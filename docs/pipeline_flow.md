# Pipeline Flow Details

## Medallion Architecture

This project follows the **Bronze → Silver → Gold** medallion architecture pattern.

### Bronze Layer (Raw)
- **What**: Exact copy of source data, nothing removed or changed
- **Format**: Delta Lake with audit columns (_ingestion_timestamp, _source_file)
- **Partitioning**: By ingestion timestamp
- **Purpose**: Full data lineage, reprocessing capability

### Silver Layer (Cleansed)
- **What**: Deduplicated, type-cast, null-filtered, standardised data
- **Format**: Delta Lake with ACID merge (upsert on primary key)
- **Partitioning**: By business date (order_date, event_type)
- **Purpose**: Single source of truth for all downstream consumers

### Gold Layer (Aggregated)
- **What**: Business-level aggregations, analytics-ready tables
- **Format**: Delta Lake + mirrored in Azure SQL Database
- **Partitioning**: None (small aggregated tables)
- **Purpose**: Direct Power BI consumption, low-latency queries

## Spark Optimisations Applied

| Technique | Where Used | Impact |
|---|---|---|
| Partitioning by date | Silver orders | Reduces scan on date-filtered queries |
| Z-ordering | Gold daily_sales | Faster ORDER BY sales_date queries |
| Broadcast join | Customer lookup | Avoids shuffle on 500GB+ fact table |
| Delta auto-compact | All layers | Reduces small file problem |
| Caching | Repeated reads in Gold | Avoids recomputing Silver twice |

## Data Quality Checks

Each layer runs these checks before writing:
1. **Row count** — target must have ≥ 99% of source rows
2. **Null check** — critical columns (IDs, dates) must have zero nulls
3. **Duplicate check** — primary key must be unique in Silver+
4. **Schema check** — all expected columns must be present

## Error Handling

- All pipeline runs are logged to `dbo.pipeline_audit_log` in Azure SQL
- If any layer fails, downstream layers are blocked (ADF dependency conditions)
- Failed runs surface in ADF Monitor with full error stack trace
