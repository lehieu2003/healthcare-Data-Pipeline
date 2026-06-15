from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


RAW_SCHEMA = StructType(
    [
        StructField("Prscrbr_NPI", StringType(), True),
        StructField("Prscrbr_Last_Org_Name", StringType(), True),
        StructField("Prscrbr_First_Name", StringType(), True),
        StructField("Prscrbr_City", StringType(), True),
        StructField("Prscrbr_State_Abrvtn", StringType(), True),
        StructField("Prscrbr_State_FIPS", StringType(), True),
        StructField("Prscrbr_Type", StringType(), True),
        StructField("Prscrbr_Type_Src", StringType(), True),
        StructField("Brnd_Name", StringType(), True),
        StructField("Gnrc_Name", StringType(), True),
        StructField("Tot_Clms", StringType(), True),
        StructField("Tot_30day_Fills", StringType(), True),
        StructField("Tot_Day_Suply", StringType(), True),
        StructField("Tot_Drug_Cst", StringType(), True),
        StructField("Tot_Benes", StringType(), True),
        StructField("GE65_Sprsn_Flag", StringType(), True),
        StructField("GE65_Tot_Clms", StringType(), True),
        StructField("GE65_Tot_30day_Fills", StringType(), True),
        StructField("GE65_Tot_Drug_Cst", StringType(), True),
        StructField("GE65_Tot_Day_Suply", StringType(), True),
        StructField("GE65_Bene_Sprsn_Flag", StringType(), True),
        StructField("GE65_Tot_Benes", StringType(), True),
    ]
)


NUMERIC_COLUMNS = {
    "tot_claims": LongType(),
    "tot_30day_fills": DoubleType(),
    "tot_drug_cost": DoubleType(),
    "tot_day_supply": LongType(),
    "tot_beneficiaries": LongType(),
    "ge65_claims": LongType(),
    "ge65_drug_cost": DoubleType(),
}


def create_spark(app_name: str = "bronze-to-silver") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def transform_bronze_to_silver(raw_df: DataFrame, year: int) -> DataFrame:
    df = raw_df.select(
        F.trim(F.col("Prscrbr_NPI")).alias("npi"),
        F.trim(F.col("Prscrbr_Last_Org_Name")).alias("prescriber_last_or_org_name"),
        F.trim(F.col("Prscrbr_First_Name")).alias("prescriber_first_name"),
        F.upper(F.trim(F.col("Prscrbr_City"))).alias("city"),
        F.upper(F.trim(F.col("Prscrbr_State_Abrvtn"))).alias("state"),
        F.trim(F.col("Prscrbr_State_FIPS")).alias("state_fips"),
        F.upper(F.trim(F.col("Prscrbr_Type"))).alias("specialty"),
        F.upper(F.trim(F.col("Brnd_Name"))).alias("brand_name"),
        F.upper(F.trim(F.col("Gnrc_Name"))).alias("generic_name"),
        F.trim(F.col("Tot_Clms")).alias("tot_claims_raw"),
        F.trim(F.col("Tot_30day_Fills")).alias("tot_30day_fills_raw"),
        F.trim(F.col("Tot_Drug_Cst")).alias("tot_drug_cost_raw"),
        F.trim(F.col("Tot_Day_Suply")).alias("tot_day_supply_raw"),
        F.trim(F.col("Tot_Benes")).alias("tot_beneficiaries_raw"),
        F.trim(F.col("GE65_Tot_Clms")).alias("ge65_claims_raw"),
        F.trim(F.col("GE65_Tot_Drug_Cst")).alias("ge65_drug_cost_raw"),
        F.upper(F.trim(F.col("GE65_Sprsn_Flag"))).alias("ge65_suppression_flag"),
    )

    df = df.withColumn("year", F.lit(year).cast(IntegerType()))
    df = df.withColumn("state", F.when(F.col("state").isNull() | (F.col("state") == ""), F.lit("UNKNOWN")).otherwise(F.col("state")))

    df = df.withColumn("is_tot_claims_suppressed", is_suppressed("tot_claims_raw"))
    df = df.withColumn(
        "is_ge65_suppressed",
        is_suppressed("ge65_claims_raw")
        | is_suppressed("ge65_drug_cost_raw")
        | F.col("ge65_suppression_flag").isin("Y", "*", "#"),
    )

    df = cast_numeric(df, "tot_claims_raw", "tot_claims", LongType())
    df = cast_numeric(df, "tot_30day_fills_raw", "tot_30day_fills", DoubleType())
    df = cast_numeric(df, "tot_drug_cost_raw", "tot_drug_cost", DoubleType())
    df = cast_numeric(df, "tot_day_supply_raw", "tot_day_supply", LongType())
    df = cast_numeric(df, "tot_beneficiaries_raw", "tot_beneficiaries", LongType())
    df = cast_numeric(df, "ge65_claims_raw", "ge65_claims", LongType())
    df = cast_numeric(df, "ge65_drug_cost_raw", "ge65_drug_cost", DoubleType())

    df = df.withColumn(
        "cost_per_claim",
        F.when(F.col("tot_claims") > 0, F.col("tot_drug_cost") / F.col("tot_claims")).otherwise(F.lit(None).cast(DoubleType())),
    )
    df = df.withColumn(
        "claims_per_beneficiary",
        F.when(F.col("tot_beneficiaries") > 0, F.col("tot_claims") / F.col("tot_beneficiaries")).otherwise(F.lit(None).cast(DoubleType())),
    )

    df = df.drop(
        "tot_claims_raw",
        "tot_30day_fills_raw",
        "tot_drug_cost_raw",
        "tot_day_supply_raw",
        "tot_beneficiaries_raw",
        "ge65_claims_raw",
        "ge65_drug_cost_raw",
    )

    return df.dropDuplicates(["year", "npi", "state", "brand_name", "generic_name"])


def is_suppressed(column_name: str) -> F.Column:
    value = F.upper(F.trim(F.col(column_name)))
    return value.isin("", "NULL", "N/A", "NA", "*", "#", "SUPPRESSED") | value.isNull()


def cast_numeric(df: DataFrame, source_col: str, target_col: str, target_type) -> DataFrame:
    cleaned = F.regexp_replace(F.trim(F.col(source_col)), ",", "")
    if isinstance(target_type, LongType):
        numeric_pattern = r"^[+-]?[0-9]+$"
    else:
        numeric_pattern = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)$"

    numeric_value = (
        F.when(is_suppressed(source_col), F.lit(None))
        .when(cleaned.rlike(numeric_pattern), cleaned.cast(target_type))
        .otherwise(F.lit(None))
    )
    return df.withColumn(target_col, numeric_value)


def write_silver_parquet(df: DataFrame, output_path: str) -> None:
    (
        df.write.mode("overwrite")
        .partitionBy("year", "state")
        .parquet(output_path)
    )
