from __future__ import annotations

import pytest
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType

from processing.bronze_to_silver import create_spark
from processing.gold_marts import build_gold_marts


@pytest.fixture(scope="session")
def spark():
    session = create_spark("test-gold-marts")
    yield session
    session.stop()


def silver_schema() -> StructType:
    return StructType(
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


def test_build_gold_marts_creates_dashboard_ready_aggregates(spark) -> None:
    silver_df = spark.createDataFrame(
        [
            row("123", "A", "CA", "CARDIOLOGY", "DRUG A", "GENERIC A", 100, 1000.0),
            row("123", "A", "CA", "CARDIOLOGY", "DRUG B", "GENERIC B", 50, 250.0),
            row("456", "B", "CA", "CARDIOLOGY", "DRUG A", "GENERIC A", 30, 3000.0),
        ],
        schema=silver_schema(),
    )

    marts = build_gold_marts(silver_df)

    assert marts["gold_state_year_spending"].count() == 1
    assert marts["gold_drug_year_spending"].count() == 2
    assert marts["gold_specialty_state_spending"].count() == 1
    assert marts["gold_prescriber_summary"].count() == 2

    state_row = marts["gold_state_year_spending"].collect()[0].asDict()
    assert state_row["year"] == 2022
    assert state_row["state"] == "CA"
    assert state_row["total_claims"] == 180
    assert state_row["total_drug_cost"] == 4250.0
    assert state_row["prescriber_count"] == 2
    assert state_row["drug_count"] == 2


def row(npi: str, first_name: str, state: str, specialty: str, brand: str, generic: str, claims: int, cost: float) -> dict:
    return {
        "npi": npi,
        "prescriber_last_or_org_name": "Nguyen",
        "prescriber_first_name": first_name,
        "city": "SAN JOSE",
        "state": state,
        "state_fips": "06",
        "specialty": specialty,
        "brand_name": brand,
        "generic_name": generic,
        "ge65_suppression_flag": None,
        "is_tot_claims_suppressed": False,
        "is_ge65_suppressed": False,
        "tot_claims": claims,
        "tot_30day_fills": float(claims),
        "tot_drug_cost": cost,
        "tot_day_supply": claims * 30,
        "tot_beneficiaries": 10,
        "ge65_claims": claims,
        "ge65_drug_cost": cost,
        "cost_per_claim": cost / claims,
        "claims_per_beneficiary": claims / 10,
        "year": 2022,
    }

