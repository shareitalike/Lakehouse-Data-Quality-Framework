"""
Bronze layer schema definitions.

# DECISION: Define explicit StructType schemas for Bronze instead of relying
# on schema inference. Schema inference reads the entire file to determine
# types, which is expensive at scale and produces inconsistent results across
# batches (e.g., an all-null column might infer as StringType in one batch
# and IntegerType in another).

# TRADEOFF: Schema inference is convenient for ad-hoc exploration. But in
# production pipelines, explicit schemas are mandatory for reproducibility.

# FAILURE MODE: If the upstream source adds a new column and we don't update
# the schema here, the column is silently dropped in schema enforcement mode.
# This is actually DESIRED for Bronze — we want to control what enters the
# lakehouse. Schema drift detection catches and alerts on this.

# INTERVIEW: "Why do you define schemas explicitly instead of using inferSchema?"
# → "Schema inference is non-deterministic across batches and requires a full
#    data scan. Explicit schemas give us reproducibility, performance, and the
#    ability to detect schema drift."

# SCALE: At 10K rows, schema inference adds ~1 second. At 1B rows, it adds
# minutes and doubles memory usage. Explicit schemas are O(1) regardless of
# data size.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    LongType,
)


# =============================================================================
# BRONZE RAW EVENTS SCHEMA
# =============================================================================
# This is the "expected" schema for incoming raw order events.
# The schema drift detector compares actual data against this baseline.

BRONZE_RAW_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    # DECISION: customer_id is nullable in Bronze because we expect dirty data.
    # The not_null_check rule catches these; we don't want schema enforcement
    # to reject entire files because of a few null customer_ids.
    
    StructField("product_id", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    # DECISION: StringType for order_status instead of a restricted type.
    # Enum validation happens in the rule engine, not at the schema level.
    # This separates storage concerns from business logic.
    
    StructField("quantity", IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("order_timestamp", StringType(), nullable=True),
    # DECISION: order_timestamp is StringType in Bronze, not TimestampType.
    # Raw data often has malformed timestamps. If we enforce TimestampType
    # at Bronze, the entire row is rejected. Instead, we accept as String
    # and validate/parse in Silver transformation.
    # INTERVIEW: "Why store timestamps as strings in Bronze?"
    # → "Bronze is the raw, unprocessed layer. Enforcing strict types here
    #    causes data loss. We validate and cast in Silver."
])


# Valid order status values (used by accepted_values_check)
VALID_ORDER_STATUSES = [
    "pending",
    "confirmed",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
    "refunded",
]

# Valid currencies (used by accepted_values_check)
VALID_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD"]

# Expected primary key columns at Bronze
BRONZE_PRIMARY_KEY = ["order_id"]

# Columns that must not be null (critical fields)
BRONZE_CRITICAL_NOT_NULL_COLUMNS = ["order_id"]

# Columns that should not be null (warning level)
BRONZE_WARNING_NOT_NULL_COLUMNS = ["customer_id", "product_id", "order_timestamp"]
