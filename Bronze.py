# Databricks notebook source
# MAGIC %sql
# MAGIC select * from project.default.bank
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parse semicolon-delimited bank data into typed columns
from pyspark.sql.functions import split, col, regexp_replace

df_01 = spark.table("project.default.bank")

# The data is a single semicolon-delimited column with quoted string values,
# e.g. 44;"technician";"single";...  Split it into proper columns.
bank_columns = [
    "age", "job", "marital", "education", "default", "balance",
    "housing", "loan", "contact", "day", "month", "duration",
    "campaign", "pdays", "previous", "poutcome", "deposit",
]
int_columns = {"age", "balance", "day", "duration", "campaign", "pdays", "previous"}

base_col = df_01.columns[0]

exprs = []
for i, name in enumerate(bank_columns):
    # split on ";" and strip the surrounding double quotes
    value = regexp_replace(split(col(base_col), ";").getItem(i), "\"", "")
    if name in int_columns:
        value = value.cast("int")
    exprs.append(value.alias(name))

df_01 = df_01.select(*exprs)

display(df_01)

# COMMAND ----------

# DBTITLE 1,Write Bronze table
# Write the parsed data to the Bronze table
from pyspark.sql.functions import current_timestamp

(df_01
    .withColumn("ingested_at", current_timestamp())
    .write
    .mode("overwrite")
    .saveAsTable("project.default.bronze_bank"))

print("Rows written to project.default.bronze_bank:", spark.table("project.default.bronze_bank").count())

# COMMAND ----------


display(spark.table("project.default.bronze_bank"))