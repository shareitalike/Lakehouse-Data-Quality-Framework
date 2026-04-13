"""
Pipeline-level configuration.

# DECISION: Centralize all pipeline tuning parameters in one place.
# This allows operations teams to adjust behavior without touching
# pipeline code. In production, these values would come from a config
# management system (e.g., Consul, AWS Parameter Store).

# TRADEOFF: Could load from YAML/JSON file for non-Python users.
# Chose dataclasses for: (1) type safety, (2) IDE support, (3) default
# values with documentation, (4) no file I/O at import time.

# FAILURE MODE: If pipeline config values are misconfigured (e.g.,
# quarantine_threshold=0.0), ALL records get quarantined and downstream
# tables receive no data. Mitigation: __post_init__ validates ranges.

# INTERVIEW: "How do you manage pipeline configurations across environments?"
# → "Dataclass configs with environment-specific overrides. Dev has loose
#    thresholds for experimentation, staging matches prod, prod has strict
#    thresholds reviewed by the platform team."

# SCALE: Config complexity grows with pipeline complexity, not data volume.
# Same config works for 10K and 1B rows.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List
from utils.path_resolver import LakehousePaths, PATHS


@dataclass
class QuarantineConfig:
    """
    Controls quarantine behavior.
    
    # DECISION: Quarantine records instead of dropping them.
    # Dropped records are gone forever. Quarantined records can be:
    # (1) investigated for root cause, (2) reprocessed after fixing upstream,
    # (3) reported to data producers for correction.
    """
    enabled: bool = True
    max_quarantine_pct: float = 10.0
    # DECISION: If more than 10% of records fail, something is fundamentally
    # wrong. At that point, quarantine the batch and alert — don't let 90%+
    # of data be quarantined records.
    
    alert_on_quarantine: bool = True
    include_failure_reason: bool = True
    max_records_to_store: int = 10000
    # DECISION: Cap stored quarantine records to prevent storage explosion.
    # If 100M records fail, storing all of them wastes storage and makes
    # investigation harder. Store a representative sample.
    # SCALE: At 1B rows with 5% failure rate = 50M failures. Storing all
    # would cost ~50GB. Cap at 10K representative samples instead.
    
    def __post_init__(self):
        if self.max_quarantine_pct < 0 or self.max_quarantine_pct > 100:
            raise ValueError("max_quarantine_pct must be between 0 and 100")


@dataclass
class SLAConfig:
    """
    Service Level Agreement thresholds.
    
    # DECISION: Define SLAs per layer because each layer has different
    # freshness requirements. Bronze can be minutes behind; Gold must be
    # within its published SLA.
    """
    bronze_freshness_hours: float = 24.0
    silver_freshness_hours: float = 4.0
    gold_freshness_hours: float = 2.0
    max_pipeline_duration_minutes: float = 60.0


@dataclass
class PipelineConfig:
    """
    Master pipeline configuration.
    """
    # Paths
    paths: LakehousePaths = field(default_factory=lambda: PATHS)
    
    # Quarantine settings
    quarantine: QuarantineConfig = field(default_factory=QuarantineConfig)
    
    # SLA settings
    sla: SLAConfig = field(default_factory=SLAConfig)
    
    # Pipeline identity
    pipeline_name: str = "lakehouse_dq_pipeline"
    pipeline_version: str = "1.0"
    
    # Data generation settings
    num_records: int = 10000
    # DECISION: 10K records is the sweet spot for demos — fast enough to run
    # in <1 minute on a laptop, large enough to show meaningful statistics.
    
    random_seed: int = 42
    # DECISION: Fixed seed for reproducibility. Same seed = same data = same
    # validation results. Critical for debugging and demos.
    
    # Validation behavior
    fail_pipeline_on_critical: bool = False
    # DECISION: Default False — pipeline continues even on critical failures.
    # Critical failures quarantine records and emit alerts, but don't stop
    # the pipeline. Stopping causes cascading failures downstream.
    # TRADEOFF: Some orgs require hard stops on critical failures for compliance.
    # Set this to True for those cases.
    
    enable_schema_evolution: bool = True
    # DECISION: Enable schema evolution in Silver+ layers.
    # Bronze uses schema enforcement (mergeSchema=False).
    # Silver+ uses schema evolution (mergeSchema=True) for backward compat.
    
    # Observability settings
    enable_observability: bool = True
    metrics_retention_days: int = 90
    
    @classmethod
    def default(cls) -> "PipelineConfig":
        """Create default configuration."""
        return cls()
    
    @classmethod
    def for_testing(cls) -> "PipelineConfig":
        """
        Configuration optimized for unit tests.
        
        # DECISION: Separate test config with smaller datasets and relaxed
        # thresholds. Tests should run in seconds, not minutes.
        """
        return cls(
            num_records=1000,
            quarantine=QuarantineConfig(
                max_quarantine_pct=50.0,  # Relaxed for test data
                max_records_to_store=100,
            ),
            sla=SLAConfig(
                bronze_freshness_hours=9999.0,  # Disable freshness in tests
                silver_freshness_hours=9999.0,
                gold_freshness_hours=9999.0,
            ),
        )
