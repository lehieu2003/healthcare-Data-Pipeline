from __future__ import annotations

import argparse

from processing.bronze_to_silver import create_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect a Parquet dataset with Spark.")
    parser.add_argument("--path", required=True)
    parser.add_argument("--show", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark("inspect-parquet")

    try:
        df = spark.read.parquet(args.path)
        print(f"rows={df.count()}")
        print(f"columns={df.columns}")
        df.printSchema()
        df.show(args.show, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

