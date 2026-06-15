from __future__ import annotations

import pytest

from processing.bronze_to_silver import create_spark, transform_bronze_to_silver


@pytest.fixture(scope="session")
def spark():
    session = create_spark("test-bronze-to-silver")
    yield session
    session.stop()


def test_transform_bronze_to_silver_casts_and_flags_suppressed_values(spark) -> None:
    raw_df = spark.createDataFrame(
        [
            {
                "Prscrbr_NPI": " 123 ",
                "Prscrbr_Last_Org_Name": "Nguyen",
                "Prscrbr_First_Name": "An",
                "Prscrbr_City": "san jose",
                "Prscrbr_State_Abrvtn": "ca",
                "Prscrbr_State_FIPS": "06",
                "Prscrbr_Type": "Internal Medicine",
                "Brnd_Name": "Lipitor",
                "Gnrc_Name": "Atorvastatin Calcium",
                "Tot_Clms": "20",
                "Tot_30day_Fills": "25.5",
                "Tot_Drug_Cst": "100.00",
                "Tot_Day_Suply": "600",
                "Tot_Benes": "10",
                "GE65_Tot_Clms": "*",
                "GE65_Tot_Drug_Cst": "*",
                "GE65_Sprsn_Flag": "Y",
            }
        ]
    )

    rows = transform_bronze_to_silver(raw_df, 2022).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["year"] == 2022
    assert row["npi"] == "123"
    assert row["city"] == "SAN JOSE"
    assert row["state"] == "CA"
    assert row["specialty"] == "INTERNAL MEDICINE"
    assert row["brand_name"] == "LIPITOR"
    assert row["generic_name"] == "ATORVASTATIN CALCIUM"
    assert row["tot_claims"] == 20
    assert row["tot_drug_cost"] == 100.0
    assert row["ge65_claims"] is None
    assert row["ge65_drug_cost"] is None
    assert row["is_tot_claims_suppressed"] is False
    assert row["is_ge65_suppressed"] is True
    assert row["cost_per_claim"] == 5.0
    assert row["claims_per_beneficiary"] == 2.0


def test_transform_bronze_to_silver_keeps_tot_claims_suppressed_as_null(spark) -> None:
    raw_df = spark.createDataFrame(
        [
            {
                "Prscrbr_NPI": "456",
                "Prscrbr_Last_Org_Name": "Clinic",
                "Prscrbr_First_Name": "",
                "Prscrbr_City": "",
                "Prscrbr_State_Abrvtn": "",
                "Prscrbr_State_FIPS": "",
                "Prscrbr_Type": "Family Practice",
                "Brnd_Name": "Drug",
                "Gnrc_Name": "Generic",
                "Tot_Clms": "*",
                "Tot_30day_Fills": "",
                "Tot_Drug_Cst": "200",
                "Tot_Day_Suply": "",
                "Tot_Benes": "",
                "GE65_Tot_Clms": "",
                "GE65_Tot_Drug_Cst": "",
                "GE65_Sprsn_Flag": "",
            }
        ]
    )

    row = transform_bronze_to_silver(raw_df, 2022).collect()[0].asDict()

    assert row["state"] == "UNKNOWN"
    assert row["tot_claims"] is None
    assert row["is_tot_claims_suppressed"] is True
    assert row["cost_per_claim"] is None

