from __future__ import annotations

import argparse

from processing.bronze_to_silver import create_spark
from processing.silver_to_gold import build_gold_tables, read_silver_parquet, write_gold_exports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold dimension and fact exports from Silver Parquet.")
    parser.add_argument("--input-path", default="/app/data/silver/part_d_prescribers")
    parser.add_argument("--output-path", default="/app/data/gold_export")
    parser.add_argument("--skip-count", action="store_true", help="Skip row counts for large datasets.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("silver-to-gold")

    try:
        silver_df = read_silver_parquet(spark, args.input_path)
        gold_tables = build_gold_tables(silver_df)

        if not args.skip_count:
            for table_name, df in gold_tables.items():
                print(f"{table_name} rows={df.count()}")

        write_gold_exports(gold_tables, args.output_path)
        print(f"Silver to Gold exports complete output={args.output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

