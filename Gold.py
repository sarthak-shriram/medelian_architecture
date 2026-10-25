# Databricks notebook source
from pyspark.sql.functions import col, count, avg, sum as _sum, when, round as _round

# Gold layer: business-level aggregations from Silver
# Question: "What is the subscription (deposit) rate by job, education, and contact method?"
# This gold table is analytics-ready for dashboards and ML.

df_gold = (
    spark.table("project.default.silver_bank")
    .groupBy("job", "education", "contact")
    .agg(
        count("*").alias("total_contacts"),
        _sum(when(col("deposit") == "yes", 1).otherwise(0)).alias("total_subscriptions"),
        _round(_sum(when(col("deposit") == "yes", 1).otherwise(0)) / count("*") * 100, 2)
            .alias("subscription_rate_pct"),
        _round(avg("age"), 1).alias("avg_age"),
        _round(avg("balance"), 2).alias("avg_balance"),
        _round(avg("duration"), 1).alias("avg_call_duration"),
        _round(avg("campaign"), 1).alias("avg_campaign_contacts"),
    )
    .orderBy(col("subscription_rate_pct").desc())
)


# COMMAND ----------


# Write to Gold table
(df_gold
    .write
    .mode("overwrite")
    .saveAsTable("project.default.gold_bank_subscription_summary"))

print("Rows written to project.default.gold_bank_subscription_summary:",
      spark.table("project.default.gold_bank_subscription_summary").count()) 

# COMMAND ----------

display(spark.table("project.default.gold_bank_subscription_summary"))