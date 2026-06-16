from __future__ import annotations

import pytest
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

from processing.bronze_to_silver import create_spark
from processing.silver_to_gold import build_gold_tables


@pytest.fixture(scope="session")
def spark():
    session = create_spark("test-silver-to-gold")
    yield session
    session.stop()


def test_build_gold_tables_creates_dimensions_and_fact(spark) -> None:
    schema = StructType(
        [
            StructField("npi", StringType(), True),
            StructField("prescriber_last_or_org_name", StringType(), True),
            StructField("prescriber_first_name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("state_fips", StringType(), True),
            StructField("specialty", StringType(), True),
            StructField("brand_name", StringType(), True),
            StructField("generic_name", StringType(), True),
            StructField("ge65_suppression_flag", StringType(), True),
            StructField("is_tot_claims_suppressed", BooleanType(), True),
            StructField("is_ge65_suppressed", BooleanType(), True),
            StructField("tot_claims", LongType(), True),
            StructField("tot_30day_fills", DoubleType(), True),
            StructField("tot_drug_cost", DoubleType(), True),
            StructField("tot_day_supply", LongType(), True),
            StructField("tot_beneficiaries", LongType(), True),
            StructField("ge65_claims", LongType(), True),
            StructField("ge65_drug_cost", DoubleType(), True),
            StructField("cost_per_claim", DoubleType(), True),
            StructField("claims_per_beneficiary", DoubleType(), True),
            StructField("year", IntegerType(), True),
        ]
    )
    silver_df = spark.createDataFrame(
        [
            {
                "npi": "123",
                "prescriber_last_or_org_name": "Nguyen",
                "prescriber_first_name": "An",
                "city": "SAN JOSE",
                "state": "CA",
                "state_fips": "06",
                "specialty": "INTERNAL MEDICINE",
                "brand_name": "LIPITOR",
                "generic_name": "ATORVASTATIN CALCIUM",
                "ge65_suppression_flag": "Y",
                "is_tot_claims_suppressed": False,
                "is_ge65_suppressed": True,
                "tot_claims": 20,
                "tot_30day_fills": 25.5,
                "tot_drug_cost": 100.0,
                "tot_day_supply": 600,
                "tot_beneficiaries": 10,
                "ge65_claims": None,
                "ge65_drug_cost": None,
                "cost_per_claim": 5.0,
                "claims_per_beneficiary": 2.0,
                "year": 2022,
            }
        ],
        schema=schema,
    )

    tables = build_gold_tables(silver_df)

    assert set(tables) == {"dim_prescriber", "dim_drug", "dim_state", "dim_year", "fact_prescriptions"}
    assert tables["dim_prescriber"].count() == 1
    assert tables["dim_drug"].count() == 1
    assert tables["dim_state"].count() == 1
    assert tables["dim_year"].count() == 1
    assert tables["fact_prescriptions"].count() == 1

    prescriber = tables["dim_prescriber"].collect()[0].asDict()
    fact = tables["fact_prescriptions"].collect()[0].asDict()

    assert prescriber["full_name"] == "An Nguyen"
    assert prescriber["prescriber_id"] == fact["prescriber_id"]
    assert fact["year_id"] == 2022
    assert fact["state_id"] == "CA"
    assert fact["tot_claims"] == 20
    assert fact["cost_per_claim"] == 5.0
