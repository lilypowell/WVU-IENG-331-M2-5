# Milestone 2: Python Pipeline

**Team 5**: Olivia Donoghue, Lily Powell

## How to Run

Instructions to run the pipeline from a fresh clone:

```bash
#1. Clone the repository
git clone https://github.com/lilypowell/wvu-ieng-331-m2-5.git
cd wvu-ieng-331-m2-5
#2. Install dependencies using uv
uv sync
#3. Place olist.duckdb in the data/ directoryy
#4. Run the default analysis
uv run wvu-ieng-331-m2-5
#5. Run a parameterized analysis
uv run wvu-ieng-331-m2-5 --start-date 2024-01-01 --end-date 2024-12-31
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--db-path` | string | data/olist.duckdb | Path to the DuckDB database file |
| `--start-date` | date | None (no filter) | Filters orders occurring on or after this date (YYYY-MM-DD) |
| `--end-date` | date | None (no filter) | Filters orders occurring on or before this date (YYYY-MM-DD) |

## Outputs

- `summary.csv`: Aggregated table providing high-level metrics for quick review  
- `detail.parquet`: Comprehensive dataset containing the full analysis results  
- `chart.html`: Interactive Altair visualization of top product categories by revenue  

## Validation Checks

Schema Verification: Ensures all 9 tables exist in the database  

Completeness Check: Ensures key identity columns do not contain null values  

Temporal Sanity: Verifies that order dates are not empty  

Volume Threshold: Confirms core tables meet minimum row count requirements  

## Analysis Summary

This analysis focuses on identifying the primary drivers of revenue within the Olist dataset.

Top Categories: "bed_bath_table" and "health_beauty" generate the largest share of total revenue.

Revenue Concentration: A small number of categories contribute disproportionately to total revenue, indicating a concentrated sales distribution.

AOV Trends: Average order value varies significantly across product categories, suggesting differences in customer purchasing behavior across segments.

## Limitations & Caveats

Data Range Constraint: The dataset has been date-shifted. The valid order purchase date range is approximately **2023-11-05 to 2025-12-17**, so date filters must fall within this range. Using dates outside this window may return no results.

Data Quality: The pipeline assumes the underlying DuckDB file follows the Olist schema; significant schema changes require query updates.

Memory Constraint: Large-scale data ranges utilize in-memory Polars processing and performance may degrade.

Data Precision: Filters are applied based on the order_purchase_timestamp and results may vary slightly if comparing against shipping or delivery dates.
