"""
Layer schema configuration — data contracts per Medallion layer.

# DECISION: Wrap StructType schemas with metadata to form "data contracts."
# A data contract is more than a schema — it includes: expected columns,
# nullability rules, type constraints, AND quality expectations.

# INTERVIEW: "What's a data contract?"
# → "A formal agreement between data producers and consumers about the
#    structure, quality, and freshness of data. It includes schema (columns
#    and types), quality rules (null rates, uniqueness), and SLAs (freshness).
#    Without contracts, every team interprets data differently."

# FAILURE MODE: If the contract is outdated (e.g., upstream added a column),
# valid data gets incorrectly flagged as schema drift. Contracts must be
# versioned and updated in sync with upstream changes.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pyspark.sql.types import StructType

from schemas.bronze_schemas import BRONZE_RAW_EVENTS_SCHEMA
from schemas.silver_schemas import (
    SILVER_ORDERS_SCHEMA,
    SILVER_CUSTOMERS_SCHEMA,
    SILVER_PRODUCTS_SCHEMA,
)
from schemas.gold_schemas import (
    GOLD_DAILY_REVENUE_SCHEMA,
    GOLD_PRODUCT_PERFORMANCE_SCHEMA,
    GOLD_CUSTOMER_LTV_SCHEMA,
)


@dataclass
class DataContract:
    """
    A data contract defines expectations for a dataset.
    
    # DECISION: Combine schema expectations with quality expectations
    # in a single object. This ensures schema and quality are always
    # evaluated together — you can't have trusted data without both.
    """
    dataset_name: str
    layer: str
    schema: StructType
    primary_key_columns: List[str]
    
    # Schema contract
    enforce_schema: bool = True
    # If True, reject data that doesn't match schema (Bronze mode)
    # If False, allow schema evolution (Silver/Gold mode)
    
    allow_schema_evolution: bool = False
    # Can the schema grow over time?
    
    # Quality contract
    min_not_null_rate: Dict[str, float] = field(default_factory=dict)
    # Column → minimum non-null rate (e.g., {"customer_id": 0.95})
    
    max_duplicate_rate: float = 0.0
    # Maximum acceptable duplicate rate on primary key
    
    # SLA contract
    max_freshness_hours: float = 24.0
    min_row_count: int = 0
    max_row_count: Optional[int] = None
    
    version: str = "1.0"


# =============================================================================
# PRE-BUILT CONTRACTS PER LAYER
# =============================================================================

BRONZE_RAW_EVENTS_CONTRACT = DataContract(
    dataset_name="bronze_raw_events",
    layer="bronze",
    schema=BRONZE_RAW_EVENTS_SCHEMA,
    primary_key_columns=["order_id"],
    enforce_schema=True,
    allow_schema_evolution=False,
    # DECISION: Bronze enforces schema strictly. Any new columns from upstream
    # must be explicitly approved before acceptance. This prevents "schema
    # creep" where unexpected columns silently pollute the lakehouse.
    min_not_null_rate={"order_id": 1.0, "customer_id": 0.90},
    max_duplicate_rate=0.05,  # Allow up to 5% dupes in raw events
    max_freshness_hours=24.0,
    min_row_count=100,
    version="1.0",
)

SILVER_ORDERS_CONTRACT = DataContract(
    dataset_name="silver_orders",
    layer="silver",
    schema=SILVER_ORDERS_SCHEMA,
    primary_key_columns=["order_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    # DECISION: Silver allows schema evolution because new business columns
    # (e.g., "loyalty_tier") should be addable without pipeline changes.
    min_not_null_rate={"order_id": 1.0, "customer_id": 1.0, "product_id": 1.0},
    max_duplicate_rate=0.0,  # Zero duplicates after deduplication
    max_freshness_hours=4.0,
    min_row_count=50,
    version="1.0",
)

SILVER_CUSTOMERS_CONTRACT = DataContract(
    dataset_name="silver_customers",
    layer="silver",
    schema=SILVER_CUSTOMERS_SCHEMA,
    primary_key_columns=["customer_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"customer_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=4.0,
    version="1.0",
)

SILVER_PRODUCTS_CONTRACT = DataContract(
    dataset_name="silver_products",
    layer="silver",
    schema=SILVER_PRODUCTS_SCHEMA,
    primary_key_columns=["product_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"product_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=4.0,
    version="1.0",
)

GOLD_DAILY_REVENUE_CONTRACT = DataContract(
    dataset_name="gold_daily_revenue",
    layer="gold",
    schema=GOLD_DAILY_REVENUE_SCHEMA,
    primary_key_columns=["order_date", "currency"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"order_date": 1.0, "total_revenue": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=2.0,
    version="1.0",
)

GOLD_PRODUCT_PERFORMANCE_CONTRACT = DataContract(
    dataset_name="gold_product_performance",
    layer="gold",
    schema=GOLD_PRODUCT_PERFORMANCE_SCHEMA,
    primary_key_columns=["product_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"product_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=2.0,
    version="1.0",
)

GOLD_CUSTOMER_LTV_CONTRACT = DataContract(
    dataset_name="gold_customer_ltv",
    layer="gold",
    schema=GOLD_CUSTOMER_LTV_SCHEMA,
    primary_key_columns=["customer_id"],
    enforce_schema=False,
    allow_schema_evolution=True,
    min_not_null_rate={"customer_id": 1.0},
    max_duplicate_rate=0.0,
    max_freshness_hours=2.0,
    version="1.0",
)

# Registry of all contracts for easy lookup
ALL_CONTRACTS = {
    "bronze_raw_events": BRONZE_RAW_EVENTS_CONTRACT,
    "silver_orders": SILVER_ORDERS_CONTRACT,
    "silver_customers": SILVER_CUSTOMERS_CONTRACT,
    "silver_products": SILVER_PRODUCTS_CONTRACT,
    "gold_daily_revenue": GOLD_DAILY_REVENUE_CONTRACT,
    "gold_product_performance": GOLD_PRODUCT_PERFORMANCE_CONTRACT,
    "gold_customer_ltv": GOLD_CUSTOMER_LTV_CONTRACT,
}
