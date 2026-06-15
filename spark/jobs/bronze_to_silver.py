from __future__ import annotations

import argparse

from processing.bronze_to_silver import create_spark, read_bronze_csv, transform_bronze_to_silver, write_silver_parquet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert CMS Part D Bronze CSV data to Silver Parquet.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--input-path", required=True, help="Bronze CSV path, for example data/raw/part_d_2022.csv")
    parser.add_argument("--output-path", default="data/silver/part_d_prescribers", help="Silver Parquet base path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark()

    try:
        raw_df = read_bronze_csv(spark, args.input_path)
        silver_df = transform_bronze_to_silver(raw_df, args.year)
        row_count = silver_df.count()
        write_silver_parquet(silver_df, args.output_path)
        print(f"Bronze to Silver complete year={args.year} rows={row_count} output={args.output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

