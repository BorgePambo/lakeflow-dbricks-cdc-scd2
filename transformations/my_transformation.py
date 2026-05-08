from pyspark import pipelines as dp
import pyspark.sql.functions as F

base_path = "/Volumes/northwind/raw/datasets/northwind_traders/"


@dp.table(name="bronze.customers")
def customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("encoding", "ISO-8859-1")
        .load(f"{base_path}customers")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )



# Tabela de staging (view temporária)
@dp.view
def stg_customers():
    return (
        spark.readStream.table("bronze.customers")
        .select(
            F.col("customerID").alias("customer_id"),
            F.col("companyName").alias("company_name"),
            F.col("contactName").alias("contact_name"),
            F.col("contactTitle").alias("contact_title"),
            "city",
            "country",
            "ingestion_timestamp"
        )
    )

# Criar tabela destino SCD2
dp.create_streaming_table("dim_customers")

# Auto CDC SCD Type 2
dp.create_auto_cdc_flow(
    target="dim_customers",
    source="stg_customers",
    keys=["customer_id"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=2
)




