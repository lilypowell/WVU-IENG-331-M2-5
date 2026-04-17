from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import duckdb
import polars as pl


def get_sql_path(filename: str) -> Path:
    """Return the absolute path to a SQL file in the sql/ directory.

    Args:
        filename: Name of the SQL file, such as "category_revenue_analysis.sql".

    Returns:
        Path object pointing to the SQL file.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
    """
    project_root = Path(__file__).resolve().parents[2]
    sql_path = project_root / "sql" / filename

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    return sql_path


def read_sql_file(filename: str) -> str:
    """Read a SQL file from the sql/ directory.

    Args:
        filename: Name of the SQL file.

    Returns:
        SQL query text as a string.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
        OSError: If the file cannot be read.
    """
    sql_path = get_sql_path(filename)
    return sql_path.read_text(encoding="utf-8")


def run_sql_query(
    db_path: str | Path,
    sql_filename: str,
    params: Sequence[Any] | None = None,
) -> pl.DataFrame:
    """Execute a parameterized SQL query from a file and return a Polars DataFrame.

    Args:
        db_path: Path to the DuckDB database file.
        sql_filename: Name of the SQL file in the sql/ directory.
        params: Optional sequence of parameter values for $1, $2, etc.

    Returns:
        Query results as a Polars DataFrame.

    Raises:
        FileNotFoundError: If the database or SQL file cannot be found.
        duckdb.Error: If DuckDB fails to execute the query.
        ValueError: If the query returns no rows.
    """
    db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    sql = read_sql_file(sql_filename)

    try:
        with duckdb.connect(str(db_path)) as conn:
            if params is None:
                result = conn.execute(sql).pl()
            else:
                result = conn.execute(sql, params).pl()
    except duckdb.Error as exc:
        raise duckdb.Error(
            f"Failed to execute query '{sql_filename}' with params {params}: {exc}"
        ) from exc

    if result.height == 0:
        raise ValueError(f"Query returned no rows: {sql_filename}")

    return result


def get_category_revenue_analysis(
    db_path: str | Path,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pl.DataFrame:
    """Run the category revenue analysis query with optional date filters.

    Args:
        db_path: Path to the DuckDB database file.
        start_date: Optional lower bound for order purchase date.
        end_date: Optional upper bound for order purchase date.

    Returns:
        Category revenue results as a Polars DataFrame.
    """
    return run_sql_query(
        db_path=db_path,
        sql_filename="category_revenue_analysis.sql",
        params=[start_date, end_date],
    )


def get_late_delivery_analysis(
    db_path: str | Path,
) -> pl.DataFrame:
    """Run the late delivery analysis query.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        Late delivery analysis as a Polars DataFrame.
    """
    return run_sql_query(
        db_path=db_path,
        sql_filename="late_delivery_analysis.sql",
    )


def get_customer_retention(
    db_path: str | Path,
) -> pl.DataFrame:
    """Run the customer retention analysis query.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        Customer retention results as a Polars DataFrame.
    """
    return run_sql_query(
        db_path=db_path,
        sql_filename="customer_retention.sql",
    )


def get_seller_scorecard(
    db_path: str | Path,
    seller_state: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pl.DataFrame:
    """Run the seller scorecard query with optional filters.

    Args:
        db_path: Path to the DuckDB database file.
        seller_state: Optional seller state filter.
        start_date: Optional lower bound for order date filter.
        end_date: Optional upper bound for order date filter.

    Returns:
        Seller scorecard results as a Polars DataFrame.
    """
    params = [seller_state, start_date, end_date]
    return run_sql_query(
        db_path=db_path,
        sql_filename="seller_scorecard.sql",
        params=params,
    )
