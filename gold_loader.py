from __future__ import annotations

import argparse
from pathlib import Path

import psycopg

from ingestion.config import load_settings


TABLE_LOAD_ORDER = [
    "dim_year",
    "dim_state",
    "dim_drug",
    "dim_prescriber",
    "fact_prescriptions",
]

TABLE_COLUMNS = {
    "dim_year": ["year_id", "year"],
    "dim_state": ["state_id", "state_abbr", "state_fips"],
    "dim_drug": ["drug_id", "brand_name", "generic_name"],
    "dim_prescriber": ["prescriber_id", "npi", "full_name", "city", "state", "specialty"],
    "fact_prescriptions": [
        "fact_id",
        "prescriber_id",
        "drug_id",
        "state_id",
        "year_id",
        "tot_claims",
        "tot_30day_fills",
        "tot_drug_cost",
        "tot_day_supply",
        "tot_beneficiaries",
        "ge65_claims",
        "ge65_drug_cost",
        "cost_per_claim",
        "claims_per_beneficiary",
        "is_tot_claims_suppressed",
        "is_ge65_suppressed",
    ],
}

MART_LOAD_ORDER = [
    "gold_state_year_spending",
    "gold_drug_year_spending",
    "gold_specialty_state_spending",
    "gold_prescriber_summary",
    "gold_anomaly_prescribers",
]

MART_COLUMNS = {
    "gold_state_year_spending": [
        "mart_id",
        "year",
        "state",
        "total_claims",
        "total_drug_cost",
        "total_beneficiaries",
        "prescriber_count",
        "drug_count",
        "avg_cost_per_claim",
    ],
    "gold_drug_year_spending": [
        "mart_id",
        "year",
        "brand_name",
        "generic_name",
        "total_claims",
        "total_drug_cost",
        "prescriber_count",
        "state_count",
        "avg_cost_per_claim",
    ],
    "gold_specialty_state_spending": [
        "mart_id",
        "year",
        "state",
        "specialty",
        "total_claims",
        "total_drug_cost",
        "prescriber_count",
        "drug_count",
        "avg_cost_per_claim",
    ],
    "gold_prescriber_summary": [
        "mart_id",
        "year",
        "npi",
        "full_name",
        "city",
        "state",
        "specialty",
        "total_claims",
        "total_drug_cost",
        "drug_count",
        "avg_cost_per_claim",
    ],
    "gold_anomaly_prescribers": [
        "mart_id",
        "year",
        "npi",
        "full_name",
        "city",
        "state",
        "specialty",
        "total_claims",
        "total_drug_cost",
        "avg_cost_per_claim",
        "peer_p95_cost_per_claim",
        "anomaly_reason",
    ],
}


def csv_part_files(table_dir: Path) -> list[Path]:
    return sorted(path for path in table_dir.glob("part-*.csv") if path.is_file())


def load_gold_exports(export_dir: Path, truncate: bool = False, load_set: str = "detailed") -> None:
    settings = load_settings()
    if not settings.postgres_dsn:
        raise ValueError("POSTGRES_DSN is required")

    table_order, table_columns = get_load_config(load_set)

    with psycopg.connect(settings.postgres_dsn) as conn:
        if truncate:
            truncate_tables(conn, table_order)

        for table_name in table_order:
            load_table(conn, table_name, export_dir / table_name, table_columns[table_name])

        conn.commit()


def get_load_config(load_set: str) -> tuple[list[str], dict[str, list[str]]]:
    if load_set == "detailed":
        return TABLE_LOAD_ORDER, TABLE_COLUMNS
    if load_set == "marts":
        return MART_LOAD_ORDER, MART_COLUMNS
    raise ValueError(f"Unsupported load set: {load_set}")


def truncate_tables(conn: psycopg.Connection, table_names: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {', '.join(table_names)} RESTART IDENTITY")


def load_table(conn: psycopg.Connection, table_name: str, table_dir: Path, columns: list[str]) -> None:
    part_files = csv_part_files(table_dir)
    if not part_files:
        raise FileNotFoundError(f"No CSV part files found for {table_name}: {table_dir}")

    staging_table = f"stg_{table_name}"

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS)")

        for part_file in part_files:
            copy_csv(cur, staging_table, columns, part_file)

        assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns[1:])
        cur.execute(
            f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            SELECT {", ".join(columns)}
            FROM {staging_table}
            ON CONFLICT ({columns[0]})
            DO UPDATE SET {assignments};
            """
        )
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        loaded_rows = cur.fetchone()[0]
        print(f"loaded {table_name}: {loaded_rows} staged rows")


def copy_csv(cur: psycopg.Cursor, table_name: str, columns: list[str], path: Path) -> None:
    copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
    with path.open("r", encoding="utf-8", newline="") as file_obj:
        with cur.copy(copy_sql) as copy:
            for line in file_obj:
                copy.write(line)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load Spark Gold CSV exports into PostgreSQL.")
    parser.add_argument("--export-dir", type=Path, default=Path("data/gold_export"))
    parser.add_argument("--load-set", choices=["detailed", "marts"], default="detailed")
    parser.add_argument("--truncate", action="store_true", help="Truncate Gold tables before loading.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_gold_exports(args.export_dir, truncate=args.truncate, load_set=args.load_set)


if __name__ == "__main__":
    main()
