from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from processing.silver_to_gold import full_name_expr, stable_hash


def build_state_year_spending(silver_df: DataFrame) -> DataFrame:
    grouped = silver_df.groupBy("year", "state").agg(
        F.sum("tot_claims").alias("total_claims"),
        F.sum("tot_drug_cost").alias("total_drug_cost"),
        F.sum("tot_beneficiaries").alias("total_beneficiaries"),
        F.countDistinct("npi").alias("prescriber_count"),
        F.countDistinct("brand_name", "generic_name").alias("drug_count"),
    )
    return grouped.withColumn("avg_cost_per_claim", safe_divide("total_drug_cost", "total_claims")).select(
        stable_hash("year", "state").alias("mart_id"),
        "year",
        "state",
        "total_claims",
        "total_drug_cost",
        "total_beneficiaries",
        "prescriber_count",
        "drug_count",
        "avg_cost_per_claim",
    )


def build_drug_year_spending(silver_df: DataFrame) -> DataFrame:
    grouped = silver_df.groupBy("year", "brand_name", "generic_name").agg(
        F.sum("tot_claims").alias("total_claims"),
        F.sum("tot_drug_cost").alias("total_drug_cost"),
        F.countDistinct("npi").alias("prescriber_count"),
        F.countDistinct("state").alias("state_count"),
    )
    return grouped.withColumn("avg_cost_per_claim", safe_divide("total_drug_cost", "total_claims")).select(
        stable_hash("year", "brand_name", "generic_name").alias("mart_id"),
        "year",
        "brand_name",
        "generic_name",
        "total_claims",
        "total_drug_cost",
        "prescriber_count",
        "state_count",
        "avg_cost_per_claim",
    )


def build_specialty_state_spending(silver_df: DataFrame) -> DataFrame:
    grouped = silver_df.groupBy("year", "state", "specialty").agg(
        F.sum("tot_claims").alias("total_claims"),
        F.sum("tot_drug_cost").alias("total_drug_cost"),
        F.countDistinct("npi").alias("prescriber_count"),
        F.countDistinct("brand_name", "generic_name").alias("drug_count"),
    )
    return grouped.withColumn("avg_cost_per_claim", safe_divide("total_drug_cost", "total_claims")).select(
        stable_hash("year", "state", "specialty").alias("mart_id"),
        "year",
        "state",
        "specialty",
        "total_claims",
        "total_drug_cost",
        "prescriber_count",
        "drug_count",
        "avg_cost_per_claim",
    )


def build_prescriber_summary(silver_df: DataFrame) -> DataFrame:
    enriched = silver_df.withColumn("full_name", full_name_expr())
    grouped = enriched.groupBy("year", "npi", "full_name", "city", "state", "specialty").agg(
        F.sum("tot_claims").alias("total_claims"),
        F.sum("tot_drug_cost").alias("total_drug_cost"),
        F.countDistinct("brand_name", "generic_name").alias("drug_count"),
    )
    return grouped.withColumn("avg_cost_per_claim", safe_divide("total_drug_cost", "total_claims")).select(
        stable_hash("year", "npi", "city", "state", "specialty").alias("mart_id"),
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
    )


def build_anomaly_prescribers(prescriber_summary_df: DataFrame) -> DataFrame:
    peer_thresholds = prescriber_summary_df.groupBy("year", "state", "specialty").agg(
        F.expr("percentile_approx(avg_cost_per_claim, 0.95, 100)").alias("peer_p95_cost_per_claim")
    )
    return (
        prescriber_summary_df.join(peer_thresholds, ["year", "state", "specialty"], "inner")
        .where(
            (F.col("total_claims") >= 30)
            & F.col("avg_cost_per_claim").isNotNull()
            & F.col("peer_p95_cost_per_claim").isNotNull()
            & (F.col("avg_cost_per_claim") > F.col("peer_p95_cost_per_claim"))
        )
        .withColumn("anomaly_reason", F.lit("avg_cost_per_claim_gt_peer_p95"))
        .select(
            F.col("mart_id"),
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
        )
    )


def build_gold_marts(silver_df: DataFrame) -> dict[str, DataFrame]:
    prescriber_summary = build_prescriber_summary(silver_df)
    return {
        "gold_state_year_spending": build_state_year_spending(silver_df),
        "gold_drug_year_spending": build_drug_year_spending(silver_df),
        "gold_specialty_state_spending": build_specialty_state_spending(silver_df),
        "gold_prescriber_summary": prescriber_summary,
        "gold_anomaly_prescribers": build_anomaly_prescribers(prescriber_summary),
    }


def write_gold_mart_exports(gold_marts: dict[str, DataFrame], output_path: str) -> None:
    for table_name, df in gold_marts.items():
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .option("emptyValue", "")
            .option("nullValue", "")
            .csv(f"{output_path}/{table_name}")
        )


def safe_divide(numerator_col: str, denominator_col: str) -> F.Column:
    return F.when(F.col(denominator_col) > 0, F.col(numerator_col) / F.col(denominator_col)).otherwise(F.lit(None))

