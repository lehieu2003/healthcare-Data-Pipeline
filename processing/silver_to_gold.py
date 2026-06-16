from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


NULL_TOKEN = "__NULL__"


def read_silver_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def build_prescriber_dim(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(
            prescriber_id_expr().alias("prescriber_id"),
            F.col("npi"),
            full_name_expr().alias("full_name"),
            F.col("city"),
            F.col("state"),
            F.col("specialty"),
        )
        .where(F.col("npi").isNotNull() & (F.col("npi") != ""))
        .dropDuplicates(["prescriber_id"])
    )


def build_drug_dim(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(
            drug_id_expr().alias("drug_id"),
            F.col("brand_name"),
            F.col("generic_name"),
        )
        .dropDuplicates(["drug_id"])
    )


def build_state_dim(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(
            F.col("state").alias("state_id"),
            F.col("state").alias("state_abbr"),
            F.col("state_fips"),
        )
        .where(F.col("state").isNotNull() & (F.col("state") != ""))
        .dropDuplicates(["state_id"])
    )


def build_year_dim(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(
            F.col("year").alias("year_id"),
            F.col("year"),
        )
        .dropDuplicates(["year_id"])
    )


def build_fact_prescriptions(silver_df: DataFrame) -> DataFrame:
    return silver_df.select(
        fact_id_expr().alias("fact_id"),
        prescriber_id_expr().alias("prescriber_id"),
        drug_id_expr().alias("drug_id"),
        F.col("state").alias("state_id"),
        F.col("year").alias("year_id"),
        F.col("tot_claims"),
        F.col("tot_30day_fills"),
        F.col("tot_drug_cost"),
        F.col("tot_day_supply"),
        F.col("tot_beneficiaries"),
        F.col("ge65_claims"),
        F.col("ge65_drug_cost"),
        F.col("cost_per_claim"),
        F.col("claims_per_beneficiary"),
        F.col("is_tot_claims_suppressed"),
        F.col("is_ge65_suppressed"),
    )


def build_gold_tables(silver_df: DataFrame) -> dict[str, DataFrame]:
    return {
        "dim_prescriber": build_prescriber_dim(silver_df),
        "dim_drug": build_drug_dim(silver_df),
        "dim_state": build_state_dim(silver_df),
        "dim_year": build_year_dim(silver_df),
        "fact_prescriptions": build_fact_prescriptions(silver_df),
    }


def write_gold_exports(gold_tables: dict[str, DataFrame], output_path: str) -> None:
    for table_name, df in gold_tables.items():
        (
            df.write.mode("overwrite")
            .option("header", "true")
            .option("emptyValue", "")
            .option("nullValue", "")
            .csv(f"{output_path}/{table_name}")
        )


def prescriber_id_expr() -> F.Column:
    return stable_hash("npi", "city", "state", "specialty")


def drug_id_expr() -> F.Column:
    return stable_hash("brand_name", "generic_name")


def fact_id_expr() -> F.Column:
    return stable_hash("year", "npi", "state", "brand_name", "generic_name")


def full_name_expr() -> F.Column:
    first = F.coalesce(F.col("prescriber_first_name"), F.lit(""))
    last = F.coalesce(F.col("prescriber_last_or_org_name"), F.lit(""))
    return F.trim(F.concat_ws(" ", first, last))


def stable_hash(*column_names: str) -> F.Column:
    values = [F.coalesce(F.col(name).cast("string"), F.lit(NULL_TOKEN)) for name in column_names]
    return F.sha2(F.concat_ws("||", *values), 256)

