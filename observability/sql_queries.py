"""
SQL-based observability queries — dbt-style testing philosophy.

# =============================================================================
# WHY DBT SEPARATES MODELS AND TESTS
# =============================================================================
# dbt treats data transformations (models) and data validation (tests) as
# separate artifacts. Why?
# 1. SINGLE RESPONSIBILITY: Models define WHAT data looks like. Tests define
#    IF data is correct. Mixing them creates untestable, unreviewable code.
# 2. INDEPENDENT LIFECYCLE: Models change when business logic changes. Tests
#    change when quality expectations change. Different cadence, different 
#    reviewers.
# 3. REUSABILITY: A generic "unique" test can be applied to ANY column in
#    ANY model. If tests are embedded in models, you rewrite the same check
#    everywhere.

# =============================================================================
# WHY SQL-BASED EXPECTATIONS SCALE WELL
# =============================================================================
# SQL is the universal language of data. Benefits:
# 1. ANYONE CAN WRITE THEM: Data analysts, DBAs, and PMs can author SQL
#    checks without knowing Python or Spark internals.
# 2. PUSH-DOWN OPTIMIZATION: SQL checks can be pushed down to the storage
#    engine (Delta, Parquet). The engine optimizes execution plans.
# 3. DECLARATIVE: "SELECT * WHERE order_id IS NULL" describes WHAT to check,
#    not HOW to check. The engine decides the optimal execution.
# 4. PORTABLE: Same SQL runs on Spark, Snowflake, BigQuery, Postgres.
#    If you migrate data platforms, tests migrate with zero rewrite.

# =============================================================================
# WHY DECLARATIVE TESTS IMPROVE MAINTAINABILITY
# =============================================================================
# Imperative: "Loop through rows, check if order_id is null, count nulls..."
# Declarative: "SELECT COUNT(*) WHERE order_id IS NULL SHOULD BE 0"
# Declarative tests are:
# 1. READABLE: Non-engineers understand them
# 2. REVIEWABLE: PRs for test changes are trivial to review
# 3. COMPOSABLE: Complex checks = composition of simple checks
# 4. SELF-DOCUMENTING: The SQL IS the documentation

# DECISION: Implement SQL checks that mirror dbt test philosophy without
# requiring dbt itself. These are SparkSQL queries that can be run against
# Delta tables to validate data quality.

# INTERVIEW: "Why do you use SQL for data quality checks?"
# → "SQL is declarative, portable, and accessible to everyone on the team.
#    A data analyst can write 'SELECT * WHERE revenue < 0' without knowing
#    Python. The same check runs on Spark, Snowflake, or BigQuery. And the
#    query optimizer handles execution efficiency — I don't need to hand-
#    tune filter pushdown."
"""

from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame


class SQLObservabilityQueries:
    """
    SQL-based data quality checks inspired by dbt testing philosophy.
    
    Each method returns a SQL query string and optionally executes it.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def _execute(self, query: str) -> DataFrame:
        """Execute a SQL query and return results."""
        return self.spark.sql(query)
    
    # =========================================================================
    # REFERENTIAL INTEGRITY (dbt: relationships test)
    # =========================================================================
    
    def referential_integrity_sql(
        self,
        child_table: str,
        child_column: str,
        parent_table: str,
        parent_column: str,
    ) -> str:
        """
        SQL-based referential integrity check.
        
        dbt equivalent: relationships test
        Oracle equivalent: FOREIGN KEY constraint validation
        
        Returns SQL query that lists orphan records.
        """
        return f"""
        -- REFERENTIAL INTEGRITY CHECK
        -- dbt equivalent: relationships(to='ref("{parent_table}")', field='{parent_column}')
        -- Expectation: query should return 0 rows
        SELECT 
            child.{child_column},
            COUNT(*) as orphan_count
        FROM {child_table} child
        LEFT JOIN {parent_table} parent
            ON child.{child_column} = parent.{parent_column}
        WHERE parent.{parent_column} IS NULL
            AND child.{child_column} IS NOT NULL
        GROUP BY child.{child_column}
        ORDER BY orphan_count DESC
        """
    
    # =========================================================================
    # UNIQUENESS (dbt: unique test)
    # =========================================================================
    
    def uniqueness_sql(
        self,
        table_name: str,
        columns: List[str],
    ) -> str:
        """
        SQL-based uniqueness check.
        
        dbt equivalent: unique test
        Oracle equivalent: UNIQUE constraint validation
        
        Returns SQL query that lists duplicate key groups.
        """
        col_list = ", ".join(columns)
        return f"""
        -- UNIQUENESS CHECK
        -- dbt equivalent: unique(column_name='{col_list}')
        -- Expectation: query should return 0 rows
        SELECT 
            {col_list},
            COUNT(*) as occurrence_count
        FROM {table_name}
        GROUP BY {col_list}
        HAVING COUNT(*) > 1
        ORDER BY occurrence_count DESC
        LIMIT 100
        """
    
    # =========================================================================
    # ACCEPTED VALUES (dbt: accepted_values test)
    # =========================================================================
    
    def accepted_values_sql(
        self,
        table_name: str,
        column_name: str,
        accepted_values: List[str],
    ) -> str:
        """
        SQL-based accepted values check.
        
        dbt equivalent: accepted_values test
        Oracle equivalent: CHECK constraint validation
        
        Returns SQL query that lists records with invalid values.
        """
        values_str = ", ".join(f"'{v}'" for v in accepted_values)
        return f"""
        -- ACCEPTED VALUES CHECK
        -- dbt equivalent: accepted_values(values=[{values_str}])
        -- Expectation: query should return 0 rows
        SELECT 
            {column_name},
            COUNT(*) as violation_count
        FROM {table_name}
        WHERE {column_name} IS NOT NULL 
            AND LOWER({column_name}) NOT IN ({', '.join(f"'{v.lower()}'" for v in accepted_values)})
        GROUP BY {column_name}
        ORDER BY violation_count DESC
        """
    
    # =========================================================================
    # NOT NULL (dbt: not_null test)
    # =========================================================================
    
    def not_null_sql(
        self,
        table_name: str,
        column_name: str,
    ) -> str:
        """
        SQL-based not-null check.
        
        dbt equivalent: not_null test
        Oracle equivalent: NOT NULL constraint validation
        """
        return f"""
        -- NOT NULL CHECK
        -- dbt equivalent: not_null(column_name='{column_name}')
        -- Expectation: null_count should be 0
        SELECT 
            '{column_name}' as column_name,
            COUNT(*) as total_records,
            SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as null_count,
            ROUND(
                SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
                2
            ) as null_percentage
        FROM {table_name}
        """
    
    # =========================================================================
    # FRESHNESS (dbt: source freshness)
    # =========================================================================
    
    def freshness_sql(
        self,
        table_name: str,
        timestamp_column: str,
        max_age_hours: float = 24.0,
    ) -> str:
        """
        SQL-based freshness check.
        
        dbt equivalent: source freshness (warn_after, error_after)
        Oracle equivalent: monitoring job scheduler lag
        """
        return f"""
        -- FRESHNESS CHECK
        -- dbt equivalent: loaded_at_field='{timestamp_column}', error_after={{count: {max_age_hours}, period: hour}}
        -- Expectation: age_hours should be <= {max_age_hours}
        SELECT 
            MAX({timestamp_column}) as latest_record,
            current_timestamp() as check_time,
            ROUND(
                (unix_timestamp(current_timestamp()) - unix_timestamp(MAX({timestamp_column}))) / 3600.0,
                2
            ) as age_hours,
            {max_age_hours} as sla_hours,
            CASE 
                WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(MAX({timestamp_column}))) / 3600.0 <= {max_age_hours}
                THEN 'PASS'
                ELSE 'FAIL - SLA BREACH'
            END as status
        FROM {table_name}
        """
    
    # =========================================================================
    # ROW COUNT TREND 
    # =========================================================================
    
    def row_count_trend_sql(
        self,
        metrics_table: str = "observability_metrics",
        dataset_name: str = None,
        limit: int = 30,
    ) -> str:
        """
        SQL query for row count trend analysis over recent runs.
        
        Queries the observability metrics table to show row count over time.
        """
        where_clause = ""
        if dataset_name:
            where_clause = f"AND dataset_name = '{dataset_name}'"
        
        return f"""
        -- ROW COUNT TREND
        -- Shows row count evolution over recent pipeline runs
        SELECT 
            run_timestamp,
            dataset_name,
            metric_value as row_count,
            LAG(metric_value) OVER (
                PARTITION BY dataset_name 
                ORDER BY run_timestamp
            ) as prev_count,
            ROUND(
                (metric_value - LAG(metric_value) OVER (
                    PARTITION BY dataset_name ORDER BY run_timestamp
                )) / NULLIF(LAG(metric_value) OVER (
                    PARTITION BY dataset_name ORDER BY run_timestamp
                ), 0) * 100,
                2
            ) as pct_change
        FROM {metrics_table}
        WHERE metric_name = 'row_count'
            {where_clause}
        ORDER BY run_timestamp DESC
        LIMIT {limit}
        """
    
    # =========================================================================
    # QUALITY DASHBOARD QUERY
    # =========================================================================
    
    def quality_dashboard_sql(
        self,
        metrics_table: str = "observability_metrics",
    ) -> str:
        """
        SQL query for a data quality dashboard view.
        
        Shows latest pass rates and failure counts per rule per dataset.
        """
        return f"""
        -- DATA QUALITY DASHBOARD
        -- Latest validation results across all datasets and rules
        WITH latest_run AS (
            SELECT 
                dataset_name,
                MAX(run_timestamp) as latest_run_ts
            FROM {metrics_table}
            GROUP BY dataset_name
        ),
        latest_metrics AS (
            SELECT m.*
            FROM {metrics_table} m
            INNER JOIN latest_run lr
                ON m.dataset_name = lr.dataset_name
                AND m.run_timestamp = lr.latest_run_ts
        )
        SELECT 
            dataset_name,
            layer,
            rule_name,
            metric_name,
            ROUND(metric_value, 4) as metric_value,
            passed,
            severity,
            total_records,
            failed_records,
            run_timestamp
        FROM latest_metrics
        WHERE metric_name IN ('failure_rate', 'pass_rate', 'null_rate', 
                             'freshness_lag_hours', 'distribution_z_score',
                             'row_count_delta_pct')
        ORDER BY 
            layer,
            dataset_name,
            CASE WHEN passed = false THEN 0 ELSE 1 END,
            rule_name
        """
    
    # =========================================================================
    # HELPER: Register DataFrame as temp view for SQL queries
    # =========================================================================
    
    def register_temp_views(
        self,
        tables: Dict[str, DataFrame],
    ) -> None:
        """
        Register DataFrames as temporary SQL views for querying.
        
        # DECISION: Temp views (not global) because they're session-scoped.
        # Global views persist across sessions and can cause naming conflicts.
        
        Args:
            tables: Dict of {view_name: DataFrame}
        """
        for name, df in tables.items():
            df.createOrReplaceTempView(name)
            print(f"[SQL] Registered temp view: {name} ({df.count()} rows)")
    
    def run_all_checks(
        self,
        table_name: str,
        checks: Dict[str, str],
    ) -> Dict[str, DataFrame]:
        """
        Execute multiple SQL checks and return results.
        
        Args:
            table_name: Name of the registered temp view
            checks: Dict of {check_name: sql_query}
        
        Returns:
            Dict of {check_name: result_DataFrame}
        """
        results = {}
        for check_name, query in checks.items():
            try:
                result_df = self._execute(query)
                results[check_name] = result_df
                count = result_df.count()
                status = "✅ PASS" if count == 0 else f"❌ FAIL ({count} violations)"
                print(f"  [{check_name}] {status}")
            except Exception as e:
                print(f"  [{check_name}] ⚠️ ERROR: {e}")
                results[check_name] = None
        
        return results
