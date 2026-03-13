"""External module — registry-based dispatcher.

In C# this used Assembly.LoadFrom() + reflection to load a DLL at runtime.
In Python we maintain a dict mapping typeName strings to callables.
Each callable has the signature:
    def execute(shared_state: dict[str, object]) -> dict[str, object]
"""

from __future__ import annotations

from typing import Callable

from etl.modules.base import Module

# Type alias for external step functions.
ExternalStepFn = Callable[[dict[str, object]], dict[str, object]]

# Registry: maps "ExternalModules.<ClassName>" -> callable.
_REGISTRY: dict[str, ExternalStepFn] = {}


def register(type_name: str, fn: ExternalStepFn) -> None:
    """Register an external step function under a typeName."""
    _REGISTRY[type_name] = fn


def _load_all() -> None:
    """Import all external module files to trigger registration."""
    # Each file in etl.modules.externals calls register() at import time.
    # fmt: off
    from etl.modules.externals import (  # noqa: F401
        account_customer_denormalizer,
        account_distribution_calculator,
        account_snapshot_builder,
        account_status_counter,
        account_velocity_tracker,
        bond_maturity_schedule_builder,
        branch_visit_enricher,
        card_customer_spending_processor,
        card_expiration_watch_processor,
        card_fraud_flags_processor,
        card_spending_by_merchant_processor,
        card_transaction_daily_processor,
        card_type_distribution_processor,
        communication_channel_mapper,
        compliance_event_summary_builder,
        compliance_open_items_builder,
        compliance_transaction_ratio_writer,
        covered_transaction_processor,
        credit_score_averager,
        credit_score_processor,
        cross_sell_candidate_finder,
        customer_360_snapshot_builder,
        customer_account_summary_builder,
        customer_address_delta_processor,
        customer_attrition_scorer,
        customer_branch_activity_builder,
        customer_compliance_risk_calculator,
        customer_contactability_processor,
        customer_credit_summary_builder,
        customer_demographics_builder,
        customer_investment_summary_builder,
        customer_txn_activity_builder,
        customer_value_calculator,
        daily_balance_movement_calculator,
        debit_credit_ratio_calculator,
        do_not_contact_processor,
        dormant_account_detector,
        executive_dashboard_builder,
        fee_revenue_daily_processor,
        full_profile_assembler,
        fund_allocation_writer,
        high_balance_filter,
        high_risk_merchant_activity_processor,
        holdings_by_sector_writer,
        inter_account_transfer_detector,
        investment_account_overview_builder,
        investment_risk_classifier,
        large_transaction_processor,
        large_wire_report_builder,
        loan_risk_calculator,
        loan_snapshot_builder,
        marketing_eligible_processor,
        monthly_revenue_breakdown_builder,
        overdraft_amount_distribution_processor,
        overdraft_by_account_type_processor,
        overdraft_customer_profile_processor,
        overdraft_daily_summary_processor,
        overdraft_recovery_rate_processor,
        peak_transaction_times_writer,
        peak_transaction_times_writer_v4,
        portfolio_concentration_calculator,
        portfolio_value_calculator,
        preference_by_segment_writer,
        preference_summary_counter,
        quarterly_executive_kpi_builder,
        regulatory_exposure_calculator,
        repeat_overdraft_customer_processor,
        suspicious_wire_flag_processor,
        transaction_anomaly_flagger,
        wealth_tier_analyzer,
        weekend_transaction_pattern_processor,
        wire_direction_summary_writer,
        wire_transfer_daily_processor,
    )
    # fmt: on


_loaded = False


class External(Module):
    def __init__(self, assembly_path: str, type_name: str) -> None:
        self.assembly_path = assembly_path
        self.type_name = type_name

    def execute(self, shared_state: dict[str, object]) -> dict[str, object]:
        global _loaded
        if not _loaded:
            _load_all()
            _loaded = True

        fn = _REGISTRY.get(self.type_name)
        if fn is None:
            raise ValueError(
                f"No registered external module for typeName '{self.type_name}'. "
                f"Known: {sorted(_REGISTRY.keys())}"
            )
        return fn(shared_state)
