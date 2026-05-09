# utils.py

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window


# =============================================================================
# COLUMN STANDARDIZATION
# =============================================================================

def standardize_column_names(df):
    """
    Standardize column names:
    - lowercase
    - trim spaces
    - replace spaces with underscore
    """

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
    """
    Trim all string columns
    """

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
    """
    Apply InitCap to selected columns
    """

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.initcap(F.col(col_name))
        )

    return df


def remove_special_characters(df, columns):
    """
    Remove special characters from selected columns
    """

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.regexp_replace(
                F.col(col_name),
                "[^a-zA-Z0-9 ]",
                ""
            )
        )

    return df


# =============================================================================
# DATE CONVERSION
# =============================================================================

def convert_to_date(df, column_name, format="yyyy-MM-dd"):
    """
    Convert column to date
    """

    return df.withColumn(
        column_name,
        F.to_date(F.col(column_name), format)
    )


def convert_to_timestamp(df, column_name, format="yyyy-MM-dd HH:mm:ss"):
    """
    Convert column to timestamp
    """

    return df.withColumn(
        column_name,
        F.to_timestamp(F.col(column_name), format)
    )


# =============================================================================
# TYPE CASTING
# =============================================================================

def cast_decimal(df, columns, precision=10, scale=2):
    """
    Cast columns to decimal
    """

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.col(col_name).cast(DecimalType(precision, scale))
        )

    return df


def cast_integer(df, columns):
    """
    Cast columns to integer
    """

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.col(col_name).cast(IntegerType())
        )

    return df


def cast_boolean(df, columns):
    """
    Cast columns to boolean
    """

    for col_name in columns:

        df = df.withColumn(
            col_name,
            F.col(col_name).cast(BooleanType())
        )

    return df


# =============================================================================
# NULL HANDLING
# =============================================================================

def fill_null_strings(df, default="Unknown"):
    """
    Fill null values in string columns
    """

    string_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    return df.fillna(default, subset=string_cols)


def fill_null_numeric(df, default=0):
    """
    Fill null values in numeric columns
    """

    numeric_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(
            field.dataType,
            (
                IntegerType,
                DoubleType,
                FloatType,
                DecimalType,
                LongType
            )
        )
    ]

    return df.fillna(default, subset=numeric_cols)


# =============================================================================
# AUDIT COLUMNS
# =============================================================================

def add_audit_columns(df, source_table):
    """
    Add audit columns
    """

    return (
        df.withColumn(
            "created_at",
            F.current_timestamp()
        )
        .withColumn(
            "source_table",
            F.lit(source_table)
        )
    )


# =============================================================================
# DEDUPLICATION
# =============================================================================

def remove_duplicates(df, partition_columns, order_column):
    """
    Remove duplicates keeping latest record
    """

    window_spec = (
        Window
        .partitionBy(*partition_columns)
        .orderBy(F.col(order_column).desc())
    )

    return (
        df.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


# =============================================================================
# HASH / CDC
# =============================================================================

def create_row_hash(df, columns):
    """
    Create SHA256 hash for CDC
    """

    return df.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(c).cast("string"),
                        F.lit("")
                    )
                    for c in columns
                ]
            ),
            256
        )
    )


def detect_changes(source_df, target_df, business_key):
    """
    Detect inserts and updates using hash comparison
    """

    return (
        source_df.alias("src")
        .join(
            target_df.alias("tgt"),
            business_key,
            "left"
        )
        .filter(
            (F.col("tgt.row_hash").isNull()) |
            (F.col("src.row_hash") != F.col("tgt.row_hash"))
        )
    )


# =============================================================================
# SCD TYPE 2
# =============================================================================

def add_scd2_columns(df):
    """
    Add SCD Type 2 control columns
    """

    return (
        df.withColumn(
            "effective_date",
            F.current_date()
        )
        .withColumn(
            "end_date",
            F.lit(None).cast(DateType())
        )
        .withColumn(
            "is_current",
            F.lit(True)
        )
    )


# =============================================================================
# VALIDATIONS
# =============================================================================

def validate_not_null(df, columns):
    """
    Remove rows with null values in required columns
    """

    condition = None

    for col_name in columns:

        current_condition = F.col(col_name).isNotNull()

        if condition is None:
            condition = current_condition
        else:
            condition = condition & current_condition

    return df.filter(condition)


def validate_positive_values(df, columns):
    """
    Keep only positive numeric values
    """

    condition = None

    for col_name in columns:

        current_condition = F.col(col_name) >= 0

        if condition is None:
            condition = current_condition
        else:
            condition = condition & current_condition

    return df.filter(condition)


# =============================================================================
# COMMON HELPERS
# =============================================================================

def add_ingestion_date(df):
    """
    Add ingestion date column
    """

    return df.withColumn(
        "ingestion_date",
        F.current_date()
    )


def reorder_columns(df, columns):
    """
    Reorder dataframe columns
    """

    return df.select(columns)