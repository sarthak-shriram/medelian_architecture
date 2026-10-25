# Databricks notebook source
from pyspark.sql.functions import col, trim, upper, when, current_timestamp

# Read from Bronze
df_bronze = spark.table("project.default.bronze_bank")

# COMMAND ----------

from pyspark.sql.functions import lower

# Silver layer: clean, standardize, and enforce data quality
df_silver = (
    df_bronze
    # Trim whitespace and blanks
    .select(
        *[trim(col(c)).alias(c) if dict(df_bronze.dtypes)[c] == "string" else col(c)
          for c in df_bronze.columns]
    )
    # Convert columns to lowercase for consistency
    .withColumn("job", lower(col("job")))
    .withColumn("marital", lower(col("marital")))
    .withColumn("education", lower(col("education")))
    .withColumn("default", lower(col("default")))
    .withColumn("housing", lower(col("housing")))
    .withColumn("loan", lower(col("loan")))
    .withColumn("contact", lower(col("contact")))
    .withColumn("month", lower(col("month")))
    .withColumn("poutcome", lower(col("poutcome")))
    .withColumn("deposit", lower(col("deposit")))
    # Remove duplicates
    .dropDuplicates()
    # Drop rows where primary key column is null
    .na.drop(subset=["age", "job"])
    # Add timestamp for silver table 
    .withColumn("silver_processed_at", current_timestamp())
)


# COMMAND ----------


# Write to Silver table
(df_silver
    .write
    .mode("overwrite")
    .saveAsTable("project.default.silver_bank"))

print("Rows written to project.default.silver_bank:",
      spark.table("project.default.silver_bank").count())



# COMMAND ----------

display(spark.table("project.default.silver_bank"))

# COMMAND ----------

