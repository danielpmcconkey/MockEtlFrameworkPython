"""Tests for transformation.py — ported from TransformationTests.cs (10 tests)."""

import pandas as pd

from etl.modules.transformation import Transformation


def make_people_frame():
    return pd.DataFrame(
        {
            "Name": ["Alice", "Bob", "Charlie"],
            "Age": [25, 30, 35],
            "City": ["New York", "London", "New York"],
        }
    )


def test_execute_basic_select_returns_all_rows():
    state = {"people": make_people_frame()}
    result = Transformation("out", "SELECT * FROM people").execute(state)
    assert len(result["out"]) == 3


def test_execute_where_clause_filters_rows():
    state = {"people": make_people_frame()}
    result = Transformation("out", "SELECT * FROM people WHERE Age > 25").execute(state)
    assert len(result["out"]) == 2


def test_execute_select_columns_returns_only_specified_columns():
    state = {"people": make_people_frame()}
    result = Transformation("out", "SELECT Name, City FROM people").execute(state)
    df = result["out"]
    assert len(df.columns) == 2
    assert "Name" in df.columns
    assert "City" in df.columns


def test_execute_join_combines_two_frames():
    customers = pd.DataFrame({"Id": [1, 2], "Name": ["Alice", "Bob"]})
    orders = pd.DataFrame({"CustomerId": [1, 1], "Amount": [100, 200]})
    state = {"customers": customers, "orders": orders}
    result = Transformation(
        "out",
        "SELECT c.Name, o.Amount FROM customers c JOIN orders o ON c.Id = o.CustomerId",
    ).execute(state)
    assert len(result["out"]) == 2


def test_execute_group_by_aggregates_correctly():
    state = {"people": make_people_frame()}
    result = Transformation(
        "out", "SELECT City, COUNT(*) as cnt FROM people GROUP BY City"
    ).execute(state)
    assert len(result["out"]) == 2


def test_execute_preserves_existing_shared_state():
    state = {"people": make_people_frame()}
    result = Transformation("out", "SELECT * FROM people").execute(state)
    assert "people" in result
    assert "out" in result


def test_execute_only_dataframes_registered_as_tables():
    state = {"people": make_people_frame(), "someString": "not a dataframe"}
    result = Transformation("out", "SELECT * FROM people").execute(state)
    assert len(result["out"]) == 3


def test_execute_empty_dataframe_registers_table_with_schema():
    empty = pd.DataFrame(columns=["id", "name"])
    state = {"empty_table": empty}
    result = Transformation("out", "SELECT * FROM empty_table").execute(state)
    df = result["out"]
    assert len(df) == 0


def test_execute_left_join_empty_dataframe_returns_left_rows_with_nulls():
    scores = pd.DataFrame(
        {"customer_id": [1, 2], "bureau": ["Equifax", "Equifax"], "score": [750, 680]}
    )
    prior = pd.DataFrame(columns=["customer_id", "bureau", "score"])
    state = {"current": scores, "prior": prior}
    result = Transformation(
        "out",
        "SELECT c.customer_id, c.bureau, c.score AS current_score, p.score AS prior_score "
        "FROM current c LEFT JOIN prior p ON c.customer_id = p.customer_id AND c.bureau = p.bureau",
    ).execute(state)
    df = result["out"]
    assert len(df) == 2
    assert all(df["prior_score"].isna())


def test_execute_empty_dataframe_no_columns_skips_registration():
    empty = pd.DataFrame()
    people = make_people_frame()
    state = {"people": people, "empty": empty}
    result = Transformation("out", "SELECT * FROM people").execute(state)
    assert len(result["out"]) == 3
