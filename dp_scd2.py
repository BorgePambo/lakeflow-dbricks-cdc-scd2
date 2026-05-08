from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

# ============================================================
# STAGING VIEW - HASH CHANGE DETECTION
# ============================================================

@dp.view
def stg_customers():

    return (
        spark.read.table("northwind.silver.customers")
        .select(
            F.col("customer_id"),
            F.col("company_name"),
            F.col("contact_name"),
            F.col("contact_title"),
            F.col("city"),
            F.col("country"),
            F.col("ingestion_timestamp"),

            # HASH para detectar mudanças
            F.md5(
                F.concat_ws(
                    "|",
                    F.col("company_name"),
                    F.col("contact_name"),
                    F.col("contact_title"),
                    F.col("city"),
                    F.col("country")
                )
            ).alias("change_hash")
        )
    )

# ============================================================
# SCD TYPE 2 - DIM_CUSTOMERS
# ============================================================

@dp.materialized_view(
    name="northwind.gold.dim_customers",
    table_properties={
        "quality": "gold",
        "dimension_type": "scd2"
    }
)
def dim_customers():

    source = spark.read.table("stg_customers")

    return (
        source
        .withColumn(
            "sk_customer",
            F.row_number().over(
                Window.orderBy("customer_id")
            )
        )

        .withColumn(
            "effective_start_date",
            F.current_timestamp()
        )

        .withColumn(
            "effective_end_date",
            F.lit(None).cast(TimestampType())
        )

        .withColumn(
            "is_current",
            F.lit(1)
        )

        .withColumn(
            "version",
            F.lit(1)
        )

        .select(
            "sk_customer",
            "customer_id",
            "company_name",
            "contact_name",
            "contact_title",
            "city",
            "country",
            "change_hash",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "version"
        )
    )