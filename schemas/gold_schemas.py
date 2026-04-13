"""
Gold layer schema definitions.

# DECISION: Gold schemas represent business-facing aggregated tables. They are
# designed for consumption by analysts, dashboards, and reports. Column names
# are business-friendly (not technical snake_case with prefixes).

# TRADEOFF: Could use a star schema with fact/dimension tables. Chose flat
# aggregated tables because: (1) this is a DQ framework demo, not a data
# modeling project, and (2) flat tables are simpler to validate with DQ rules.

# FAILURE MODE: Gold tables can become stale if Silver pipeline fails silently.
# The freshness_check rule catches this by comparing max(last_updated) against
# the current timestamp and an SLA threshold.

# INTERVIEW: "How do you ensure Gold tables are trustworthy?"
# → "Three checks: row count anomaly detection (sudden drops), freshness SLA
#    (data isn't stale), and distribution analysis (no skew or manipulation).
#    Together they form a Gold layer data contract."

# SCALE: At 10K rows → simple aggregation. At 10M → partition by date.
# At 1B → pre-aggregate in Silver, use incremental Gold updates with MERGE,
# and consider Z-ordering on frequently filtered columns.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
    DateType,
)


# =============================================================================
# GOLD: DAILY REVENUE
# =============================================================================

GOLD_DAILY_REVENUE_SCHEMA = StructType([
    StructField("order_date", DateType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("order_count", LongType(), nullable=False),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("unique_customers", LongType(), nullable=False),
    StructField("unique_products", LongType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    # DECISION: Include currency in Gold to support multi-currency analytics.
    # Aggregation is per-currency to avoid mixing USD and EUR totals.
    # FAILURE MODE: If currency normalization fails in Silver, Gold revenue
    # numbers become meaningless. The distribution_anomaly_check catches this
    # by detecting sudden shifts in revenue distribution by currency.
    
    StructField("computed_at", TimestampType(), nullable=False),
    # When this aggregate was computed — for freshness SLA checks.
])

GOLD_DAILY_REVENUE_PRIMARY_KEY = ["order_date", "currency"]


# =============================================================================
# GOLD: PRODUCT PERFORMANCE
# =============================================================================

GOLD_PRODUCT_PERFORMANCE_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("total_quantity_sold", LongType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("avg_unit_price", DoubleType(), nullable=True),
    StructField("order_count", LongType(), nullable=False),
    StructField("unique_customers", LongType(), nullable=False),
    StructField("first_sold_date", DateType(), nullable=True),
    StructField("last_sold_date", DateType(), nullable=True),
    StructField("revenue_rank", IntegerType(), nullable=True),
    # DECISION: Pre-computed rank to avoid window function at query time.
    # SCALE: At 1B rows, window functions for ranking are expensive.
    # Pre-computing in Gold saves analysts from writing complex SQL.
    
    StructField("computed_at", TimestampType(), nullable=False),
])

GOLD_PRODUCT_PERFORMANCE_PRIMARY_KEY = ["product_id"]


# =============================================================================
# GOLD: CUSTOMER LIFETIME VALUE
# =============================================================================

GOLD_CUSTOMER_LTV_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("total_orders", LongType(), nullable=False),
    StructField("total_spend", DoubleType(), nullable=False),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("first_order_date", DateType(), nullable=True),
    StructField("last_order_date", DateType(), nullable=True),
    StructField("customer_tenure_days", IntegerType(), nullable=True),
    StructField("orders_per_month", DoubleType(), nullable=True),
    # DECISION: Derived metric for customer engagement frequency.
    # FAILURE MODE: Division by zero if tenure_days is 0 (same-day customer).
    # Silver pipeline handles this with a CASE WHEN guard.
    
    StructField("ltv_segment", StringType(), nullable=True),
    # DECISION: Pre-computed segment (high/medium/low) based on spend quantiles.
    # This supports dashboard filters without runtime computation.
    
    StructField("computed_at", TimestampType(), nullable=False),
])

GOLD_CUSTOMER_LTV_PRIMARY_KEY = ["customer_id"]
