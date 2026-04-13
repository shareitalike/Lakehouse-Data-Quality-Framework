"""
Silver layer schema definitions.

# DECISION: Silver schemas enforce strict types because data has been validated
# and cleaned. Timestamps are now TimestampType, IDs are non-nullable where
# business rules require it.

# TRADEOFF: Could keep everything as StringType for maximum flexibility, but
# that pushes type validation to every downstream consumer. Silver's job is to
# provide "trusted, typed data" — loose types defeat the purpose.

# FAILURE MODE: If Silver schema is too strict (e.g., non-nullable on a column
# that legitimately can be null), valid records get rejected. Always validate
# nullability rules against actual business requirements, not assumptions.

# INTERVIEW: "What's the difference between Bronze and Silver schemas?"
# → "Bronze accepts raw data as-is (strings, nullables). Silver enforces
#    business types after validation. This is the Medallion philosophy:
#    Bronze = raw fidelity, Silver = typed trust."

# SCALE: At 1B rows, strict Silver schemas enable predicate pushdown and
# columnar compression. A DoubleType column compresses 10x better than the
# same values stored as StringType.
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
# SILVER ORDERS SCHEMA
# =============================================================================

SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    # DECISION: Non-nullable in Silver because null customer_ids were
    # quarantined at Bronze→Silver boundary.
    
    StructField("product_id", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=False),
    # DECISION: Computed column (quantity * unit_price). Materialized in Silver
    # to avoid recomputation in every Gold query. This is a denormalization
    # decision — acceptable because Silver is "enriched trusted data."
    
    StructField("currency", StringType(), nullable=False),
    StructField("order_timestamp", TimestampType(), nullable=False),
    # DECISION: Now TimestampType — malformed timestamps were quarantined.
    
    StructField("order_date", DateType(), nullable=False),
    # DECISION: Pre-extracted date for partition-friendly Gold aggregations.
    # Avoids costly date extraction at query time across billions of rows.
    # SCALE: This single column saves ~30% query time in Gold date-based
    # aggregations because it enables partition pruning.
    
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    # DECISION: Track when record entered Silver for freshness monitoring.
    # This is different from order_timestamp (business time vs processing time).
    
    StructField("source_file", StringType(), nullable=True),
    # Lineage tracking — which Bronze batch this record came from.
])

SILVER_ORDERS_PRIMARY_KEY = ["order_id"]


# =============================================================================
# SILVER CUSTOMERS SCHEMA
# =============================================================================

SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("first_order_date", DateType(), nullable=True),
    StructField("last_order_date", DateType(), nullable=True),
    StructField("total_orders", LongType(), nullable=False),
    StructField("total_spend", DoubleType(), nullable=False),
    StructField("avg_order_value", DoubleType(), nullable=True),
    StructField("preferred_currency", StringType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

SILVER_CUSTOMERS_PRIMARY_KEY = ["customer_id"]


# =============================================================================
# SILVER PRODUCTS SCHEMA
# =============================================================================

SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("total_quantity_sold", LongType(), nullable=False),
    StructField("total_revenue", DoubleType(), nullable=False),
    StructField("avg_unit_price", DoubleType(), nullable=True),
    StructField("min_unit_price", DoubleType(), nullable=True),
    StructField("max_unit_price", DoubleType(), nullable=True),
    StructField("order_count", LongType(), nullable=False),
    StructField("last_sold_date", DateType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=False),
])

SILVER_PRODUCTS_PRIMARY_KEY = ["product_id"]
