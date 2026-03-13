"""Tests for V4 job SQL transformations — ported from V4JobTests.cs (27 tests)."""

import pandas as pd

from etl.modules.transformation import Transformation


# ====================================================================
# PeakTransactionTimes V4 Tests
# ====================================================================


def make_transactions_for_peak():
    return pd.DataFrame(
        {
            "txn_timestamp": [
                "2024-10-01T09:15:00",
                "2024-10-01T09:30:00",
                "2024-10-01T14:00:00",
                "2024-10-01T14:45:00",
                "2024-10-01T14:59:00",
                "2024-10-01T23:00:00",
            ],
            "amount": [100.50, 200.25, 50.75, 75.00, 25.00, 10.00],
            "ifw_effective_date": ["2024-10-01"] * 6,
        }
    )


PEAK_SQL = (
    "SELECT CAST(strftime('%H', txn_timestamp) AS INTEGER) AS hour_of_day, "
    "COUNT(*) AS txn_count, ROUND(SUM(amount), 2) AS total_amount "
    "FROM transactions GROUP BY strftime('%H', txn_timestamp) ORDER BY hour_of_day"
)


def test_peak_transaction_times_hourly_aggregation_groups_by_hour():
    state = {"transactions": make_transactions_for_peak()}
    result = Transformation("hourly_aggregation", PEAK_SQL).execute(state)

    df = result["hourly_aggregation"]
    assert len(df) == 3  # 3 distinct hours: 9, 14, 23

    # Hour 9: 2 transactions, 300.75
    hour9 = df[df["hour_of_day"] == 9].iloc[0]
    assert hour9["txn_count"] == 2
    assert round(hour9["total_amount"], 2) == 300.75

    # Hour 14: 3 transactions, 150.75
    hour14 = df[df["hour_of_day"] == 14].iloc[0]
    assert hour14["txn_count"] == 3
    assert round(hour14["total_amount"], 2) == 150.75

    # Hour 23: 1 transaction, 10.00
    hour23 = df[df["hour_of_day"] == 23].iloc[0]
    assert hour23["txn_count"] == 1
    assert round(hour23["total_amount"], 2) == 10.00


def test_peak_transaction_times_output_ordering_sorted_by_hour():
    state = {"transactions": make_transactions_for_peak()}
    result = Transformation("hourly_aggregation", PEAK_SQL).execute(state)

    df = result["hourly_aggregation"]
    hours = df["hour_of_day"].tolist()
    assert hours == sorted(hours)


def test_peak_transaction_times_empty_input_produces_zero_rows():
    empty = pd.DataFrame(columns=["txn_timestamp", "amount", "ifw_effective_date"])
    state = {"transactions": empty}
    result = Transformation("hourly_aggregation", PEAK_SQL).execute(state)

    assert len(result["hourly_aggregation"]) == 0


def test_peak_transaction_times_rounding_two_decimal_places():
    txns = pd.DataFrame(
        {
            "txn_timestamp": ["2024-10-01T10:00:00", "2024-10-01T10:30:00"],
            "amount": [1.111, 2.222],
            "ifw_effective_date": ["2024-10-01", "2024-10-01"],
        }
    )
    state = {"transactions": txns}
    result = Transformation("hourly_aggregation", PEAK_SQL).execute(state)

    df = result["hourly_aggregation"]
    total = df.iloc[0]["total_amount"]
    # 1.111 + 2.222 = 3.333, rounded to 3.33
    assert round(total, 2) == 3.33


def test_peak_transaction_times_no_accounts_sourcing_works_without_accounts():
    state = {"transactions": make_transactions_for_peak()}
    result = Transformation("hourly_aggregation", PEAK_SQL).execute(state)

    assert len(result["hourly_aggregation"]) == 3


# ====================================================================
# DailyBalanceMovement V4 Tests
# ====================================================================


def make_transactions_for_balance():
    return pd.DataFrame(
        {
            "account_id": [1001, 1001, 1001, 1002, 1002, 1003],
            "txn_type": ["Debit", "Credit", "Debit", "Credit", "Transfer", "Debit"],
            "amount": [100.0, 250.0, 50.0, 500.0, 75.0, 200.0],
            "ifw_effective_date": ["2024-10-01"] * 6,
        }
    )


def make_accounts_for_balance():
    return pd.DataFrame(
        {
            "account_id": [1001, 1002],
            "customer_id": [100, 200],
            "ifw_effective_date": ["2024-10-01", "2024-10-01"],
        }
    )


BALANCE_SQL = (
    "SELECT t.account_id, COALESCE(a.customer_id, 0) AS customer_id, "
    "SUM(CASE WHEN t.txn_type = 'Debit' THEN CAST(t.amount AS REAL) ELSE 0 END) AS debit_total, "
    "SUM(CASE WHEN t.txn_type = 'Credit' THEN CAST(t.amount AS REAL) ELSE 0 END) AS credit_total, "
    "SUM(CASE WHEN t.txn_type = 'Credit' THEN CAST(t.amount AS REAL) ELSE 0 END) - "
    "SUM(CASE WHEN t.txn_type = 'Debit' THEN CAST(t.amount AS REAL) ELSE 0 END) AS net_movement, "
    "MIN(t.ifw_effective_date) AS ifw_effective_date "
    "FROM transactions t LEFT JOIN accounts a ON t.account_id = a.account_id "
    "GROUP BY t.account_id, a.customer_id"
)


def test_daily_balance_movement_aggregation_correct_debit_credit_totals():
    state = {
        "transactions": make_transactions_for_balance(),
        "accounts": make_accounts_for_balance(),
    }
    result = Transformation("daily_balance_movement", BALANCE_SQL).execute(state)

    df = result["daily_balance_movement"]
    assert len(df) == 3

    # Account 1001: debit=150, credit=250, net=100
    a1001 = df[df["account_id"] == 1001].iloc[0]
    assert round(a1001["debit_total"], 2) == 150.0
    assert round(a1001["credit_total"], 2) == 250.0
    assert round(a1001["net_movement"], 2) == 100.0


def test_daily_balance_movement_net_movement_credit_minus_debit():
    state = {
        "transactions": make_transactions_for_balance(),
        "accounts": make_accounts_for_balance(),
    }
    result = Transformation("daily_balance_movement", BALANCE_SQL).execute(state)

    df = result["daily_balance_movement"]
    for _, row in df.iterrows():
        assert round(row["credit_total"] - row["debit_total"], 10) == round(
            row["net_movement"], 10
        )


def test_daily_balance_movement_unmatched_account_customer_id_defaults_to_zero():
    state = {
        "transactions": make_transactions_for_balance(),
        "accounts": make_accounts_for_balance(),
    }
    result = Transformation("daily_balance_movement", BALANCE_SQL).execute(state)

    df = result["daily_balance_movement"]
    # Account 1003 has no match in accounts — customer_id should be 0
    a1003 = df[df["account_id"] == 1003].iloc[0]
    assert int(a1003["customer_id"]) == 0


def test_daily_balance_movement_non_debit_credit_txn_type_silently_ignored():
    state = {
        "transactions": make_transactions_for_balance(),
        "accounts": make_accounts_for_balance(),
    }
    result = Transformation("daily_balance_movement", BALANCE_SQL).execute(state)

    df = result["daily_balance_movement"]
    # Account 1002 has one Credit (500) and one Transfer (75) — Transfer should NOT count
    a1002 = df[df["account_id"] == 1002].iloc[0]
    assert round(a1002["debit_total"], 2) == 0.0
    assert round(a1002["credit_total"], 2) == 500.0


def test_daily_balance_movement_empty_transactions_produces_zero_rows():
    empty = pd.DataFrame(
        columns=["account_id", "txn_type", "amount", "ifw_effective_date"]
    )
    state = {
        "transactions": empty,
        "accounts": make_accounts_for_balance(),
    }
    result = Transformation("daily_balance_movement", BALANCE_SQL).execute(state)

    assert len(result["daily_balance_movement"]) == 0


def test_daily_balance_movement_no_external_module_sql_produces_expected_schema():
    state = {
        "transactions": make_transactions_for_balance(),
        "accounts": make_accounts_for_balance(),
    }
    result = Transformation("daily_balance_movement", BALANCE_SQL).execute(state)

    df = result["daily_balance_movement"]
    assert "account_id" in df.columns
    assert "customer_id" in df.columns
    assert "debit_total" in df.columns
    assert "credit_total" in df.columns
    assert "net_movement" in df.columns
    assert "ifw_effective_date" in df.columns


# ====================================================================
# CreditScoreDelta V4 Tests
# ====================================================================


def make_todays_scores():
    return pd.DataFrame(
        {
            "customer_id": [2252, 2252, 2252, 2581, 2581, 2581, 2632, 2632, 2632],
            "bureau": [
                "Equifax", "Experian", "TransUnion",
                "Equifax", "Experian", "TransUnion",
                "Equifax", "Experian", "TransUnion",
            ],
            "score": [720, 710, 730, 680, 690, 670, 750, 745, 760],
            "ifw_effective_date": ["2024-10-02"] * 9,
        }
    )


def make_prior_scores():
    return pd.DataFrame(
        {
            "customer_id": [2252, 2252, 2252, 2581, 2581, 2581, 2632, 2632, 2632],
            "bureau": [
                "Equifax", "Experian", "TransUnion",
                "Equifax", "Experian", "TransUnion",
                "Equifax", "Experian", "TransUnion",
            ],
            "score": [715, 710, 725, 680, 690, 670, 740, 745, 755],
            "ifw_effective_date": ["2024-10-01"] * 9,
        }
    )


def make_customers_for_credit():
    return pd.DataFrame(
        {
            "id": [2252, 2581, 2632],
            "sort_name": ["Reyes Gabriel", "Chen Wei", "Patel Anita"],
            "ifw_effective_date": ["2024-10-01"] * 3,
        }
    )


CREDIT_SQL = (
    "SELECT t.customer_id, c.sort_name, t.bureau, t.score AS current_score, p.score AS prior_score "
    "FROM todays_scores t "
    "LEFT JOIN prior_scores p ON t.customer_id = p.customer_id AND t.bureau = p.bureau "
    "LEFT JOIN customers c ON t.customer_id = c.id "
    "WHERE p.score IS NULL OR t.score <> p.score "
    "ORDER BY t.customer_id, t.bureau"
)


def test_credit_score_delta_change_detection_excludes_unchanged_scores():
    state = {
        "todays_scores": make_todays_scores(),
        "prior_scores": make_prior_scores(),
        "customers": make_customers_for_credit(),
    }
    result = Transformation("credit_score_deltas", CREDIT_SQL).execute(state)

    df = result["credit_score_deltas"]
    # Changed: 2252-Equifax(715->720), 2252-TransUnion(725->730),
    #          2632-Equifax(740->750), 2632-TransUnion(755->760)
    assert len(df) == 4

    # Verify no unchanged scores included
    for _, row in df.iterrows():
        if pd.notna(row["prior_score"]):
            assert row["current_score"] != row["prior_score"]


def test_credit_score_delta_no_prior_all_rows_included_with_null_prior():
    empty_prior = pd.DataFrame(
        columns=["customer_id", "bureau", "score", "ifw_effective_date"]
    )
    state = {
        "todays_scores": make_todays_scores(),
        "prior_scores": empty_prior,
        "customers": make_customers_for_credit(),
    }
    result = Transformation("credit_score_deltas", CREDIT_SQL).execute(state)

    df = result["credit_score_deltas"]
    # All 9 rows included (3 customers x 3 bureaus) since all prior_score is NULL
    assert len(df) == 9
    assert all(df["prior_score"].isna())


def test_credit_score_delta_customer_name_enrichment_correct_sort_names():
    empty_prior = pd.DataFrame(
        columns=["customer_id", "bureau", "score", "ifw_effective_date"]
    )
    state = {
        "todays_scores": make_todays_scores(),
        "prior_scores": empty_prior,
        "customers": make_customers_for_credit(),
    }
    result = Transformation("credit_score_deltas", CREDIT_SQL).execute(state)

    df = result["credit_score_deltas"]
    c2252 = df[df["customer_id"] == 2252].iloc[0]
    assert c2252["sort_name"] == "Reyes Gabriel"


def test_credit_score_delta_output_ordering_customer_then_bureau():
    empty_prior = pd.DataFrame(
        columns=["customer_id", "bureau", "score", "ifw_effective_date"]
    )
    state = {
        "todays_scores": make_todays_scores(),
        "prior_scores": empty_prior,
        "customers": make_customers_for_credit(),
    }
    result = Transformation("credit_score_deltas", CREDIT_SQL).execute(state)

    df = result["credit_score_deltas"]
    customer_ids = df["customer_id"].tolist()
    assert customer_ids == sorted(customer_ids)


def test_credit_score_delta_customer_scope_only_three_customers():
    empty_prior = pd.DataFrame(
        columns=["customer_id", "bureau", "score", "ifw_effective_date"]
    )
    state = {
        "todays_scores": make_todays_scores(),
        "prior_scores": empty_prior,
        "customers": make_customers_for_credit(),
    }
    result = Transformation("credit_score_deltas", CREDIT_SQL).execute(state)

    df = result["credit_score_deltas"]
    distinct_customers = df["customer_id"].unique().tolist()
    assert len(distinct_customers) == 3
    assert 2252 in distinct_customers
    assert 2581 in distinct_customers
    assert 2632 in distinct_customers


# ====================================================================
# BranchVisitsByCustomerCsvAppendTrailer V4 Tests
# ====================================================================


def make_visits_for_branch():
    return pd.DataFrame(
        {
            "visit_id": [1, 2, 3, 4],
            "customer_id": [100, 200, 100, 1499],
            "branch_id": [5, 3, 5, 7],
            "visit_timestamp": [
                "2024-10-01T10:00:00",
                "2024-10-01T11:00:00",
                "2024-10-01T14:00:00",
                "2024-10-01T15:00:00",
            ],
            "visit_purpose": ["Deposit", "Withdrawal", "Inquiry", "Loan"],
            "ifw_effective_date": ["2024-10-01"] * 4,
        }
    )


def make_customers_for_branch():
    return pd.DataFrame(
        {
            "id": [100, 200, 1499],
            "sort_name": ["Smith John", "Jones Mary", "Brown Alice"],
            "ifw_effective_date": ["2024-10-01"] * 3,
        }
    )


BRANCH_SQL = (
    "SELECT v.visit_id, v.customer_id, c.sort_name, v.branch_id, "
    "v.visit_timestamp, v.visit_purpose "
    "FROM visits v LEFT JOIN customers c ON v.customer_id = c.id "
    "ORDER BY v.customer_id, v.visit_timestamp"
)


def test_branch_visits_customer_enrichment_joins_sort_name():
    state = {
        "visits": make_visits_for_branch(),
        "customers": make_customers_for_branch(),
    }
    result = Transformation("branch_visits_by_customer", BRANCH_SQL).execute(state)

    df = result["branch_visits_by_customer"]
    assert len(df) == 4

    first_row = df[df["visit_id"] == 1].iloc[0]
    assert first_row["sort_name"] == "Smith John"


def test_branch_visits_output_ordering_customer_then_timestamp():
    state = {
        "visits": make_visits_for_branch(),
        "customers": make_customers_for_branch(),
    }
    result = Transformation("branch_visits_by_customer", BRANCH_SQL).execute(state)

    df = result["branch_visits_by_customer"]
    customer_ids = df["customer_id"].tolist()
    assert customer_ids == sorted(customer_ids)


def test_branch_visits_all_columns_pass_through():
    state = {
        "visits": make_visits_for_branch(),
        "customers": make_customers_for_branch(),
    }
    result = Transformation("branch_visits_by_customer", BRANCH_SQL).execute(state)

    df = result["branch_visits_by_customer"]
    assert "visit_id" in df.columns
    assert "customer_id" in df.columns
    assert "sort_name" in df.columns
    assert "branch_id" in df.columns
    assert "visit_timestamp" in df.columns
    assert "visit_purpose" in df.columns


def test_branch_visits_missing_customer_null_sort_name():
    visits = pd.DataFrame(
        {
            "visit_id": [1],
            "customer_id": [9999],
            "branch_id": [1],
            "visit_timestamp": ["2024-10-01T10:00:00"],
            "visit_purpose": ["Deposit"],
            "ifw_effective_date": ["2024-10-01"],
        }
    )
    state = {
        "visits": visits,
        "customers": make_customers_for_branch(),
    }
    result = Transformation("branch_visits_by_customer", BRANCH_SQL).execute(state)

    df = result["branch_visits_by_customer"]
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["sort_name"])


def test_branch_visits_empty_visits_produces_zero_rows():
    empty = pd.DataFrame(
        columns=[
            "visit_id", "customer_id", "branch_id",
            "visit_timestamp", "visit_purpose", "ifw_effective_date",
        ]
    )
    state = {
        "visits": empty,
        "customers": make_customers_for_branch(),
    }
    result = Transformation("branch_visits_by_customer", BRANCH_SQL).execute(state)

    assert len(result["branch_visits_by_customer"]) == 0


# ====================================================================
# DansTransactionSpecial V4 Tests
# ====================================================================


def make_transactions_for_dans():
    return pd.DataFrame(
        {
            "transaction_id": [1, 2, 3],
            "account_id": [1001, 1001, 1002],
            "txn_timestamp": [
                "2024-10-01T09:00:00",
                "2024-10-01T10:00:00",
                "2024-10-01T11:00:00",
            ],
            "txn_type": ["Debit", "Credit", "Debit"],
            "amount": [100.0, 500.0, 200.0],
            "description": ["Purchase", "Deposit", "ATM"],
            "ifw_effective_date": ["2024-10-01"] * 3,
        }
    )


def make_accounts_for_dans():
    return pd.DataFrame(
        {
            "account_id": [1001, 1002],
            "customer_id": [100, 200],
            "account_type": ["Checking", "Savings"],
            "account_status": ["Active", "Active"],
            "current_balance": [5000.0, 10000.0],
            "ifw_effective_date": ["2024-10-01", "2024-10-01"],
        }
    )


def make_customers_for_dans():
    return pd.DataFrame(
        {
            "id": [100, 200],
            "sort_name": ["Smith John", "Jones Mary"],
            "ifw_effective_date": ["2024-10-01", "2024-10-01"],
        }
    )


def make_addresses_for_dans():
    return pd.DataFrame(
        {
            "customer_id": [100, 200],
            "city": ["New York", "Chicago"],
            "state_province": ["NY", "IL"],
            "postal_code": ["10001", "60601"],
            "start_date": ["2024-01-01", "2024-01-01"],
            "ifw_effective_date": ["2024-10-01", "2024-10-01"],
        }
    )


DANS_DETAILS_SQL = (
    "WITH deduped_addresses AS (SELECT customer_id, city, state_province, postal_code, "
    "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS rn "
    "FROM addresses) "
    "SELECT t.transaction_id, t.account_id, a.customer_id, c.sort_name, "
    "t.txn_timestamp, t.txn_type, t.amount, t.description, "
    "a.account_type, a.account_status, a.current_balance, "
    "da.city, da.state_province, da.postal_code, t.ifw_effective_date "
    "FROM transactions t "
    "LEFT JOIN accounts a ON t.account_id = a.account_id "
    "LEFT JOIN customers c ON a.customer_id = c.id "
    "LEFT JOIN deduped_addresses da ON a.customer_id = da.customer_id AND da.rn = 1 "
    "ORDER BY t.transaction_id"
)


def test_dans_transaction_special_denormalization():
    state = {
        "transactions": make_transactions_for_dans(),
        "accounts": make_accounts_for_dans(),
        "customers": make_customers_for_dans(),
        "addresses": make_addresses_for_dans(),
    }
    result = Transformation("transaction_details", DANS_DETAILS_SQL).execute(state)

    df = result["transaction_details"]
    assert len(df) == 3

    # Verify enrichment for transaction 1
    t1 = df[df["transaction_id"] == 1].iloc[0]
    assert int(t1["customer_id"]) == 100
    assert t1["sort_name"] == "Smith John"
    assert t1["account_type"] == "Checking"
    assert t1["city"] == "New York"
    assert t1["state_province"] == "NY"


def test_dans_transaction_special_ordered_by_transaction_id():
    state = {
        "transactions": make_transactions_for_dans(),
        "accounts": make_accounts_for_dans(),
        "customers": make_customers_for_dans(),
        "addresses": make_addresses_for_dans(),
    }
    result = Transformation("transaction_details", DANS_DETAILS_SQL).execute(state)

    df = result["transaction_details"]
    txn_ids = df["transaction_id"].tolist()
    assert txn_ids == [1, 2, 3]


def test_dans_transaction_special_address_dedup_keeps_most_recent():
    # Two addresses for customer 100, different start_dates
    addresses = pd.DataFrame(
        {
            "customer_id": [100, 100, 200],
            "city": ["Old City", "New York", "Chicago"],
            "state_province": ["OC", "NY", "IL"],
            "postal_code": ["00000", "10001", "60601"],
            "start_date": ["2023-01-01", "2024-06-01", "2024-01-01"],
            "ifw_effective_date": ["2024-10-01"] * 3,
        }
    )
    state = {
        "transactions": make_transactions_for_dans(),
        "accounts": make_accounts_for_dans(),
        "customers": make_customers_for_dans(),
        "addresses": addresses,
    }
    result = Transformation("transaction_details", DANS_DETAILS_SQL).execute(state)

    df = result["transaction_details"]
    # Customer 100 should have "New York" (most recent start_date)
    t1 = df[df["transaction_id"] == 1].iloc[0]
    assert t1["city"] == "New York"


def test_dans_transaction_special_state_province_aggregation_count_and_sum():
    state = {
        "transactions": make_transactions_for_dans(),
        "accounts": make_accounts_for_dans(),
        "customers": make_customers_for_dans(),
        "addresses": make_addresses_for_dans(),
    }
    # First, run the transaction_details transformation
    result1 = Transformation("transaction_details", DANS_DETAILS_SQL).execute(state)

    # Then, run the state/province aggregation
    result2 = Transformation(
        "transactions_by_state_province",
        "SELECT ifw_effective_date, state_province, COUNT(*) AS transaction_count, "
        "SUM(amount) AS total_amount FROM transaction_details "
        "GROUP BY ifw_effective_date, state_province "
        "ORDER BY ifw_effective_date, state_province",
    ).execute(result1)

    df = result2["transactions_by_state_province"]
    # 2 states: IL (1 txn, 200.0), NY (2 txns, 600.0)
    assert len(df) == 2

    il = df[df["state_province"] == "IL"].iloc[0]
    assert int(il["transaction_count"]) == 1
    assert round(il["total_amount"], 2) == 200.0

    ny = df[df["state_province"] == "NY"].iloc[0]
    assert int(ny["transaction_count"]) == 2
    assert round(ny["total_amount"], 2) == 600.0


def test_dans_transaction_special_null_address_null_state_province():
    # Transaction with no matching account -> no customer -> no address
    txns = pd.DataFrame(
        {
            "transaction_id": [1],
            "account_id": [9999],
            "txn_timestamp": ["2024-10-01T09:00:00"],
            "txn_type": ["Debit"],
            "amount": [100.0],
            "description": ["Test"],
            "ifw_effective_date": ["2024-10-01"],
        }
    )
    state = {
        "transactions": txns,
        "accounts": make_accounts_for_dans(),
        "customers": make_customers_for_dans(),
        "addresses": make_addresses_for_dans(),
    }
    result = Transformation("transaction_details", DANS_DETAILS_SQL).execute(state)

    df = result["transaction_details"]
    assert len(df) == 1
    assert pd.isna(df.iloc[0]["customer_id"])
    assert pd.isna(df.iloc[0]["sort_name"])
    assert pd.isna(df.iloc[0]["city"])
    assert pd.isna(df.iloc[0]["state_province"])


def test_dans_transaction_special_output_schema_all_columns():
    state = {
        "transactions": make_transactions_for_dans(),
        "accounts": make_accounts_for_dans(),
        "customers": make_customers_for_dans(),
        "addresses": make_addresses_for_dans(),
    }
    result = Transformation("transaction_details", DANS_DETAILS_SQL).execute(state)

    df = result["transaction_details"]
    expected = [
        "transaction_id", "account_id", "customer_id", "sort_name",
        "txn_timestamp", "txn_type", "amount", "description",
        "account_type", "account_status", "current_balance",
        "city", "state_province", "postal_code", "ifw_effective_date",
    ]
    for col in expected:
        assert col in df.columns
