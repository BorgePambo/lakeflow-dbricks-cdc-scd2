# utils.py

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window


# =============================================================================
# COLUMN STANDARDIZATION
# =============================================================================

def standardize_column_names(df):

    for col_name in df.columns:
        new_name = (
            col_name.strip()
            .lower()
            .replace(" ", "_")
        )

        df = df.withColumnRenamed(col_name, new_name)

    return df


# =============================================================================
# STRING CLEANING
# =============================================================================

def trim_string_columns(df):

    string_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for col_name in string_cols:
        df = df.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )

    return df


def standardize_text_case(df, columns):

    for col_name in columns:
        df = df.withColumn(
            col_name,
            F.initcap(F.col(col_name))
        )

    return df


# =============================================================================
# DATE CONVERSION
# =============================================================================

def convert_to_date(df, column_name, format="yyyy-MM-dd"):

    return df.withColumn(
        column_name,
        F.to_date(F.col(column_name), format)
    )


# =============================================================================
# NUMERIC STANDARDIZATION
# =============================================================================

def cast_decimal(df, columns):

    for col_name in columns:
        df = df.withColumn(
            col_name,
            F.col(col_name).cast(DecimalType(10, 2))
        )

    return df


# =============================================================================
# NULL HANDLING
# =============================================================================

def fill_null_strings(df, default="Unknown"):

    string_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    return df.fillna(default, subset=string_cols)


# =============================================================================
# AUDIT COLUMNS
# =============================================================================

def add_audit_columns(df, source_table):

    return (
        df.withColumn("created_at", F.current_timestamp())
          .withColumn("source_table", F.lit(source_table))
    )


# =============================================================================
# DEDUPLICATION
# =============================================================================

def remove_duplicates(df, key_columns):

    return df.dropDuplicates(key_columns)
	
	
	
	
	
	
	
	
from utils import *

@dp.table
def orders_silver():

    df = spark.read.table("northwind.bronze.orders")

    df = standardize_column_names(df)

    df = trim_string_columns(df)

    df = convert_to_date(df, "orderdate")

    df = convert_to_date(df, "requireddate")

    df = convert_to_date(df, "shippeddate")

    df = cast_decimal(df, ["freight"])

    df = add_audit_columns(df, "orders")

    df = remove_duplicates(df, ["orderid"])

    return df
	
	
northwind/
│
├── bronze.py
├── silver.py
├── gold.py
│
├── utils/
│   ├── cleaning.py
│   ├── casting.py
│   ├── audit.py
│   ├── dedup.py
│   └── validations.py
│
└── config.py