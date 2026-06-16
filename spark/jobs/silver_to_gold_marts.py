from __future__ import annotations

import argparse

from processing.bronze_to_silver import create_spark
from processing.gold_marts import build_gold_marts, write_gold_mart_exports
from processing.silver_to_gold import read_silver_parquet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build aggregate Gold mart exports from Silver Parquet.")
    parser.add_argument("--input-path", default="/app/data/silver/part_d_prescribers")
    parser.add_argument("--output-path", default="/app/data/gold_mart_export")
    parser.add_argument("--skip-count", action="store_true", help="Skip row counts for large datasets.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("silver-to-gold-marts")

    try:
        silver_df = read_silver_parquet(spark, args.input_path)
        marts = build_gold_marts(silver_df)

        if not args.skip_count:
            for table_name, df in marts.items():
                print(f"{table_name} rows={df.count()}")

        write_gold_mart_exports(marts, args.output_path)
        print(f"Silver to Gold mart exports complete output={args.output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

