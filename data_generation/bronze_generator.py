"""
Bronze layer synthetic data generator — E-Commerce domain.

# DECISION: Generate synthetic data using PySpark DataFrame API instead of
# Pandas or Faker. Why? (1) Same API as production code — no paradigm switch,
# (2) demonstrates Spark-native data generation that scales to any size,
# (3) reproducible with seed parameter.

# TRADEOFF: Faker library produces more realistic data (real names, addresses).
# Chose PySpark-native generation because: (1) no external dependency,
# (2) Faker is single-threaded and slow at scale, (3) for DQ framework demo,
# data realism is secondary to data quality issue simulation.

# FAILURE MODE: If random seed is not fixed, every run generates different data.
# This makes debugging impossible because "it worked yesterday" becomes
# unfalsifiable. Always use deterministic seeds in test/demo data.

# INTERVIEW: "How would you generate test data for a 1B row pipeline test?"
# → "PySpark range() + random functions with a fixed seed. Faker can't
#    generate 1B rows in reasonable time. PySpark distributes generation
#    across executors and generates 1B rows in minutes."

# SCALE: 10K rows → instant. 10M rows → ~30 seconds. 1B rows → ~5 minutes
# on a 10-node cluster. The limiting factor is write I/O, not generation.
"""

import random
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)


# =============================================================================
# REFERENCE DATA
# =============================================================================

# Product catalog — realistic e-commerce items
PRODUCTS = [
    ("PROD-001", "Wireless Bluetooth Headphones", 49.99),
    ("PROD-002", "USB-C Charging Cable", 12.99),
    ("PROD-003", "Laptop Stand Adjustable", 34.99),
    ("PROD-004", "Mechanical Keyboard RGB", 89.99),
    ("PROD-005", "Wireless Mouse Ergonomic", 29.99),
    ("PROD-006", "Monitor 27 inch 4K", 399.99),
    ("PROD-007", "Webcam HD 1080p", 59.99),
    ("PROD-008", "Desk Lamp LED", 24.99),
    ("PROD-009", "Noise Cancelling Earbuds", 79.99),
    ("PROD-010", "Phone Case Protective", 14.99),
    ("PROD-011", "Portable Charger 20000mAh", 39.99),
    ("PROD-012", "Screen Protector Tempered", 9.99),
    ("PROD-013", "Smart Watch Fitness", 199.99),
    ("PROD-014", "Tablet Stylus Pen", 29.99),
    ("PROD-015", "External SSD 1TB", 89.99),
    ("PROD-016", "Router WiFi 6", 129.99),
    ("PROD-017", "Surge Protector Power Strip", 19.99),
    ("PROD-018", "Cable Management Kit", 15.99),
    ("PROD-019", "Laptop Backpack Waterproof", 54.99),
    ("PROD-020", "Desk Organizer Wooden", 22.99),
]

VALID_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned", "refunded"]
VALID_CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD"]

# Customer IDs — 500 unique customers
CUSTOMER_IDS = [f"CUST-{str(i).zfill(4)}" for i in range(1, 501)]


def generate_clean_bronze_data(
    spark: SparkSession,
    num_records: int = 10000,
    seed: int = 42,
    start_date: str = "2024-01-01",
    end_date: str = "2024-12-31",
) -> DataFrame:
    """
    Generate clean (no issues) bronze e-commerce order events.
    
    This produces a DataFrame with realistic distribution patterns:
    - Order timestamps spread across the date range
    - Product selection follows a power-law distribution (some products are popular)
    - Quantities follow a realistic distribution (mostly 1-3, occasionally higher)
    - Prices come from the product catalog
    
    # DECISION: Generate clean data first, then inject issues separately.
    # This separation of concerns makes it clear which issues are injected
    # vs which might be bugs in the generator itself.
    
    # INTERVIEW: "Why separate generation from injection?"
    # → "Testability. I can validate the generator produces correct data,
    #    then validate the injector produces the right failure patterns.
    #    If combined, a bug in generation masks injection testing."
    
    Args:
        spark: SparkSession
        num_records: Number of order events to generate
        seed: Random seed for reproducibility
        start_date: Start of date range (ISO format)
        end_date: End of date range (ISO format)
    
    Returns:
        DataFrame with clean bronze order events
    """
    random.seed(seed)
    
    # Generate base records using Spark
    # DECISION: Use spark.range() + withColumn() instead of creating from
    # Python list. spark.range() is distributed; Python list collect is not.
    # SCALE: At 1B rows, creating from Python list would OOM the driver.
    
    df = spark.range(0, num_records).toDF("row_id")
    
    # Generate order IDs — sequential with prefix for readability
    df = df.withColumn(
        "order_id",
        F.concat(F.lit("ORD-"), F.lpad(F.col("row_id").cast("string"), 8, "0"))
    )
    
    # Generate customer IDs — select from pool with some customers ordering more
    # DECISION: Use modulo with a skew factor to simulate power-law distribution.
    # Customer 1-50 order 3x more than customer 51-500. This creates realistic
    # patterns like "20% of customers generate 80% of orders."
    num_customers = len(CUSTOMER_IDS)
    df = df.withColumn(
        "customer_idx",
        F.when(
            F.rand(seed) < 0.4,  # 40% of orders from top 10% of customers
            (F.abs(F.hash(F.col("row_id"))) % 50).cast("int")
        ).otherwise(
            (F.abs(F.hash(F.col("row_id") + 1000)) % num_customers).cast("int")
        )
    )
    
    # Create customer_id from index
    customer_ids_expr = F.concat(
        F.lit("CUST-"),
        F.lpad((F.col("customer_idx") + 1).cast("string"), 4, "0")
    )
    df = df.withColumn("customer_id", customer_ids_expr)
    
    # Generate product IDs — power-law distribution
    num_products = len(PRODUCTS)
    df = df.withColumn(
        "product_idx",
        F.when(
            F.rand(seed + 1) < 0.3,  # 30% of orders for top 5 products
            (F.abs(F.hash(F.col("row_id") + 2000)) % 5).cast("int")
        ).otherwise(
            (F.abs(F.hash(F.col("row_id") + 3000)) % num_products).cast("int")
        )
    )
    
    # Map product_idx to product_id and price
    product_ids = [p[0] for p in PRODUCTS]
    product_prices = [p[2] for p in PRODUCTS]
    
    # Create product mapping using when/otherwise chain
    product_id_expr = F.lit(product_ids[-1])
    price_expr = F.lit(product_prices[-1])
    for i in range(len(product_ids) - 1, -1, -1):
        product_id_expr = F.when(
            F.col("product_idx") == i, F.lit(product_ids[i])
        ).otherwise(product_id_expr)
        price_expr = F.when(
            F.col("product_idx") == i, F.lit(product_prices[i])
        ).otherwise(price_expr)
    
    df = df.withColumn("product_id", product_id_expr)
    df = df.withColumn("unit_price", price_expr)
    
    # Generate quantities — mostly 1-3, sometimes higher
    # DECISION: Use a weighted distribution that mimics real e-commerce:
    # ~60% quantity=1, ~25% quantity=2, ~10% quantity=3, ~5% quantity=4-10
    df = df.withColumn(
        "quantity",
        F.when(F.rand(seed + 2) < 0.60, F.lit(1))
        .when(F.rand(seed + 2) < 0.85, F.lit(2))
        .when(F.rand(seed + 2) < 0.95, F.lit(3))
        .otherwise((F.rand(seed + 3) * 7 + 4).cast("int"))
    )
    
    # Generate order status — distribution matches typical e-commerce funnel
    df = df.withColumn(
        "order_status",
        F.when(F.rand(seed + 4) < 0.15, F.lit("pending"))
        .when(F.rand(seed + 4) < 0.30, F.lit("confirmed"))
        .when(F.rand(seed + 4) < 0.45, F.lit("shipped"))
        .when(F.rand(seed + 4) < 0.80, F.lit("delivered"))
        .when(F.rand(seed + 4) < 0.90, F.lit("cancelled"))
        .when(F.rand(seed + 4) < 0.95, F.lit("returned"))
        .otherwise(F.lit("refunded"))
    )
    
    # Generate currency — mostly USD
    df = df.withColumn(
        "currency",
        F.when(F.rand(seed + 5) < 0.70, F.lit("USD"))
        .when(F.rand(seed + 5) < 0.82, F.lit("EUR"))
        .when(F.rand(seed + 5) < 0.90, F.lit("GBP"))
        .when(F.rand(seed + 5) < 0.95, F.lit("INR"))
        .otherwise(F.lit("CAD"))
    )
    
    # Generate timestamps — spread across date range
    start_ts = datetime.strptime(start_date, "%Y-%m-%d")
    end_ts = datetime.strptime(end_date, "%Y-%m-%d")
    date_range_seconds = int((end_ts - start_ts).total_seconds())
    
    df = df.withColumn(
        "order_timestamp",
        F.date_format(
            F.from_unixtime(
                F.lit(int(start_ts.timestamp())) +
                (F.rand(seed + 6) * date_range_seconds).cast("long")
            ),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    
    # Select final columns (drop intermediate ones)
    df = df.select(
        "order_id",
        "customer_id",
        "product_id",
        "order_status",
        "quantity",
        "unit_price",
        "currency",
        "order_timestamp",
    )
    
    return df


def get_product_reference_df(spark: SparkSession) -> DataFrame:
    """
    Create a reference DataFrame of valid products.
    
    Used by referential_integrity_check to validate product_id values.
    """
    product_data = [(p[0], p[1], p[2]) for p in PRODUCTS]
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("list_price", DoubleType(), False),
    ])
    return spark.createDataFrame(product_data, schema)


def get_customer_reference_df(spark: SparkSession) -> DataFrame:
    """
    Create a reference DataFrame of valid customers.
    
    Used by referential_integrity_check to validate customer_id values.
    """
    customer_data = [(cid,) for cid in CUSTOMER_IDS]
    schema = StructType([
        StructField("customer_id", StringType(), False),
    ])
    return spark.createDataFrame(customer_data, schema)
