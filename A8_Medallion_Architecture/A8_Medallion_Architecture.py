# Databricks notebook source
# DBTITLE 1,a8_banking schema
# MAGIC %sql
# MAGIC -- Creating schema for A8 Medallion Architecture assignment
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS data_engineering_assignments.a8_banking;

# COMMAND ----------

# DBTITLE 1,check schema created
# MAGIC %sql
# MAGIC SHOW SCHEMAS IN data_engineering_assignments;

# COMMAND ----------

# DBTITLE 1,Generate the raw CSV
import json

raw_transactions = [
    {
        "transaction_id": "T001",
        "account_id": "A001",
        "transaction_timestamp": "2026-01-02 09:15:00",
        "transaction_type": "DEBIT",
        "amount": "250.00",
        "currency": "INR",
        "merchant": "Amazon",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T002",
        "account_id": "A001",
        "transaction_timestamp": "2026-01-02 10:30:00",
        "transaction_type": "CREDIT",
        "amount": "5000.00",
        "currency": "INR",
        "merchant": "Salary",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T003",
        "account_id": "A002",
        "transaction_timestamp": "2026-01-02 11:10:00",
        "transaction_type": "DEBIT",
        "amount": "1200.00",
        "currency": "INR",
        "merchant": "Retail Store",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T004",
        "account_id": "A002",
        "transaction_timestamp": "2026-01-03 12:45:00",
        "transaction_type": "DEBIT",
        "amount": "450.00",
        "currency": "INR",
        "merchant": "Fuel Station",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T005",
        "account_id": "A003",
        "transaction_timestamp": "2026-01-03 14:20:00",
        "transaction_type": "CREDIT",
        "amount": "10000.00",
        "currency": "INR",
        "merchant": "Transfer",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T006",
        "account_id": "A003",
        "transaction_timestamp": "2026-01-03 15:00:00",
        "transaction_type": "DEBIT",
        "amount": "800.00",
        "currency": "INR",
        "merchant": "Online Shopping",
        "status": "PENDING"
    },
    {
        "transaction_id": "T007",
        "account_id": "A001",
        "transaction_timestamp": "2026-01-04 09:00:00",
        "transaction_type": "DEBIT",
        "amount": "75.00",
        "currency": "INR",
        "merchant": "Coffee Shop",
        "status": "COMPLETED"
    },

    # Intentionally invalid amount
    {
        "transaction_id": "T008",
        "account_id": "A002",
        "transaction_timestamp": "2026-01-04 10:15:00",
        "transaction_type": "DEBIT",
        "amount": "-500.00",
        "currency": "INR",
        "merchant": "Utility",
        "status": "COMPLETED"
    },

    # Intentionally missing account
    {
        "transaction_id": "T009",
        "account_id": None,
        "transaction_timestamp": "2026-01-04 11:00:00",
        "transaction_type": "CREDIT",
        "amount": "1500.00",
        "currency": "INR",
        "merchant": "Transfer",
        "status": "COMPLETED"
    },

    # Intentionally invalid status
    {
        "transaction_id": "T010",
        "account_id": "A004",
        "transaction_timestamp": "2026-01-04 12:00:00",
        "transaction_type": "DEBIT",
        "amount": "300.00",
        "currency": "INR",
        "merchant": "Restaurant",
        "status": "UNKNOWN"
    },

    # Intentionally duplicated transaction
    {
        "transaction_id": "T003",
        "account_id": "A002",
        "transaction_timestamp": "2026-01-02 11:10:00",
        "transaction_type": "DEBIT",
        "amount": "1200.00",
        "currency": "INR",
        "merchant": "Retail Store",
        "status": "COMPLETED"
    }
]

len(raw_transactions)
# our raw source contains 11 records, including one duplicate on purpose.

# COMMAND ----------

# DBTITLE 1,Create a raw-data volume
# MAGIC %sql
# MAGIC -- we need a file location for our incoming CSV
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS data_engineering_assignments.a8_banking.raw_data
# MAGIC COMMENT 'Raw CSV banking transactions for Assignment A8';
# MAGIC
# MAGIC SHOW VOLUMES IN data_engineering_assignments.a8_banking;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Transaction Data
# MAGIC
# MAGIC Because no sample bank transaction data was provided, a small CSV dataset is created for development and testing.
# MAGIC
# MAGIC The dataset intentionally includes valid records as well as:
# MAGIC - A duplicate transaction
# MAGIC - A negative amount
# MAGIC - A missing account ID
# MAGIC - An invalid transaction status
# MAGIC
# MAGIC These records allow the Silver layer to demonstrate deduplication, type conversion, data-quality flags, and MERGE operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Raw CSV to Unity Catalog Volume
# MAGIC
# MAGIC The generated records are written as a CSV file to a Unity Catalog volume. This simulates the raw source file that would be received by the Bronze ingestion process.

# COMMAND ----------

# DBTITLE 1,Write the CSV file
# Writing the raw data to a CSV file
import csv

csv_path = "/Volumes/data_engineering_assignments/a8_banking/raw_data/transactions.csv"

csv_lines = []

# defining the column names in CSV
header = [
    "transaction_id",
    "account_id",
    "transaction_timestamp",
    "transaction_type",
    "amount",
    "currency",
    "merchant",
    "status"
]

csv_lines.append(",".join(header))

for transaction in raw_transactions:
    row = [
        "" if transaction[col] is None else str(transaction[col])
        for col in header
    ]
    csv_lines.append(",".join(row))

csv_content = "\n".join(csv_lines)

dbutils.fs.put(
    csv_path,
    csv_content,
    overwrite=True
)

print(f"Raw CSV created at: {csv_path}")

# COMMAND ----------

# DBTITLE 1,Checking raw  data
raw_transactions_df = (
    spark.read
    .option("header", True)
    .csv(csv_path)
)

display(raw_transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.      Bronze: Ingest raw CSV to Delta with append-only + metadata (_ingested_at, _source_file).

# COMMAND ----------

# DBTITLE 1,Read the CSV for ingestion
# target - data_engineering_assignments.a8_banking.bronze_transactions

bronze_source_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(csv_path)
)

bronze_source_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Bronze metadata
from pyspark.sql import functions as F

bronze_df = (
    bronze_source_df
    .withColumn("_ingested_at", F.current_timestamp()) # records when Spark performs the ingestion
    .withColumn("_source_file", F.col("_metadata.file_path")) # _metadata.file_path gives us the path of the source file from which the row was read.
)

# COMMAND ----------

# DBTITLE 1,check bronze data
display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Write Bronze as Delta
bronze_table = "data_engineering_assignments.a8_banking.bronze_transactions"

(
    bronze_df
    .write
    .format("delta") # the target is a Delta table
    .mode("overwrite")
    .saveAsTable(bronze_table)
)

# COMMAND ----------

# DBTITLE 1,Verify bronze data
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering_assignments.a8_banking.bronze_transactions
# MAGIC ORDER BY transaction_id;

# COMMAND ----------

# DBTITLE 1,To Verify that it is Delta
# MAGIC %sql
# MAGIC DESCRIBE DETAIL data_engineering_assignments.a8_banking.bronze_transactions;

# COMMAND ----------

# DBTITLE 1,To see an initial write operation
# MAGIC %sql
# MAGIC DESCRIBE HISTORY data_engineering_assignments.a8_banking.bronze_transactions;

# COMMAND ----------

# DBTITLE 1,Demonstrate Bronze append-only ingestion
# To create a second incoming batch

second_batch = [
    {
        "transaction_id": "T011",
        "account_id": "A001",
        "transaction_timestamp": "2026-01-05 09:30:00",
        "transaction_type": "DEBIT",
        "amount": "125.00",
        "currency": "INR",
        "merchant": "Grocery Store",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T012",
        "account_id": "A003",
        "transaction_timestamp": "2026-01-05 10:00:00",
        "transaction_type": "CREDIT",
        "amount": "2500.00",
        "currency": "INR",
        "merchant": "Transfer",
        "status": "COMPLETED"
    }
]

# We will write these into a second CSV shortly and append that file to Bronze.

# COMMAND ----------

# DBTITLE 1,Write as another CSV
second_csv_path = "/Volumes/data_engineering_assignments/a8_banking/raw_data/transactions_batch_2.csv"

header = [
    "transaction_id",
    "account_id",
    "transaction_timestamp",
    "transaction_type",
    "amount",
    "currency",
    "merchant",
    "status"
]

csv_lines = [",".join(header)]

for transaction in second_batch:
    row = [
        "" if transaction[col] is None else str(transaction[col])
        for col in header
    ]
    csv_lines.append(",".join(row))

dbutils.fs.put(
    second_csv_path,
    "\n".join(csv_lines),
    overwrite=True
)

print(second_csv_path)

# COMMAND ----------

# DBTITLE 1,read another csv
second_batch_df = (
    spark.read
    .option("header", True)
    .csv(second_csv_path)
)

# COMMAND ----------

# DBTITLE 1,Add the Bronze metadata
second_bronze_df = (
    second_batch_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Append-Only Demonstration
# MAGIC
# MAGIC The initial Bronze load is reset during notebook development to make the notebook safely rerunnable.
# MAGIC
# MAGIC The following step intentionally uses append mode to demonstrate the required append-only Bronze ingestion behavior. A second source batch is added without modifying the existing Bronze records.

# COMMAND ----------

# DBTITLE 1,append
(
    second_bronze_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(bronze_table)
)

# COMMAND ----------

# DBTITLE 1,bronze count
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_count
# MAGIC FROM data_engineering_assignments.a8_banking.bronze_transactions;

# COMMAND ----------

# DBTITLE 1,duplicated bronze
# The 26 count happened because I reran the notebook from the beginning while bronze_transactions already existed, and every run used .mode("append"). So the same 11 + 2 records were appended again.

# COMMAND ----------

# DBTITLE 1,Clean up
# MAGIC %sql
# MAGIC -- # Clean reset while reconnecting to server
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS data_engineering_assignments.a8_banking.bronze_transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.      Silver: Clean, deduplicate, apply types, add DQ flags. Use MERGE INTO.

# COMMAND ----------

# DBTITLE 1,Read bronze
bronze_df = spark.table(
    "data_engineering_assignments.a8_banking.bronze_transactions"
)

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Apply proper data types
# explicit business types for Silver staging DataFrame

silver_staging_df = (
    bronze_df
    .withColumn(
        "transaction_timestamp",
        F.to_timestamp("transaction_timestamp", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "amount",
        F.col("amount").cast("decimal(18,2)")
    )
)

silver_staging_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Data Quality flags
# Our dataset has three intentional problematic records:
# T008 → negative amount
# T009 → missing account_id
# T010 → invalid status

# to flag them

silver_dq_df = (
    silver_staging_df
    .withColumn(
        "dq_valid_account",
        F.col("account_id").isNotNull()
    )
    .withColumn(
        "dq_valid_amount",
        F.col("amount") > 0
    )
    .withColumn(
        "dq_valid_status",
        F.col("status").isin(
            "COMPLETED",
            "PENDING",
            "FAILED"
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Overall flag
# to create an overall flag

silver_dq_df = (
    silver_dq_df
    .withColumn(
        "dq_valid",
        F.col("dq_valid_account")
        & F.col("dq_valid_amount")
        & F.col("dq_valid_status")
    )
)

# inspect flag
display(
    silver_dq_df.select(
        "transaction_id",
        "account_id",
        "amount",
        "status",
        "dq_valid_account",
        "dq_valid_amount",
        "dq_valid_status",
        "dq_valid"
    ).orderBy("transaction_id")
)

# COMMAND ----------

# DBTITLE 1,clean business dataset
silver_valid_df = silver_dq_df.filter(
    F.col("dq_valid")
)

# COMMAND ----------

# DBTITLE 1,Deduplicate transactions
silver_valid_df.groupBy("transaction_id").count().filter(
    F.col("count") > 1
).show()

# COMMAND ----------

# DBTITLE 1,Deduplicate transactions -silver_cleaned_df
silver_clean_df = (
    silver_valid_df
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

# DBTITLE 1,verify count after deduplication
print("Before deduplication:", silver_valid_df.count())
print("After deduplication:", silver_clean_df.count())

# COMMAND ----------

# We now have two DataFrames:
    
# silver_dq_df    --> contains all records + DQ flags
# silver_clean_df -->only valid, deduplicated records

# I will use the clean records for our Silver business table, while the DQ-aware staging DataFrame demonstrates how invalid data was identified.

# COMMAND ----------

# DBTITLE 1,Create the Silver Delta table
# to create the initial Silver table from silver_clean_df
silver_table = "data_engineering_assignments.a8_banking.silver_transactions"

(
    silver_clean_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

# DBTITLE 1,silver_transactions table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering_assignments.a8_banking.silver_transactions
# MAGIC ORDER BY transaction_id;

# COMMAND ----------

# DBTITLE 1,Create a change batch
# to simulate a later transaction feed, I will update T001 and introduce a new transaction T013

silver_change_data = [
    {
        "transaction_id": "T001",
        "account_id": "A001",
        "transaction_timestamp": "2026-01-02 09:15:00",
        "transaction_type": "DEBIT",
        "amount": "275.00",
        "currency": "INR",
        "merchant": "Amazon",
        "status": "COMPLETED"
    },
    {
        "transaction_id": "T013",
        "account_id": "A004",
        "transaction_timestamp": "2026-01-05 11:30:00",
        "transaction_type": "DEBIT",
        "amount": "650.00",
        "currency": "INR",
        "merchant": "Electronics Store",
        "status": "COMPLETED"
    }
]

# COMMAND ----------

# DBTITLE 1,Turn the change batch into a typed DataFrame
# applying the same Silver rules to the incoming change batch

silver_change_df = spark.createDataFrame(silver_change_data)

silver_change_df = (
    silver_change_df
    .withColumn(
        "transaction_timestamp",
        F.to_timestamp("transaction_timestamp", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "amount",
        F.col("amount").cast("decimal(18,2)")
    )
    .withColumn(
        "dq_valid_account",
        F.col("account_id").isNotNull()
    )
    .withColumn(
        "dq_valid_amount",
        F.col("amount") > 0
    )
    .withColumn(
        "dq_valid_status",
        F.col("status").isin("COMPLETED", "PENDING", "FAILED")
    )
    .withColumn(
        "dq_valid",
        F.col("dq_valid_account")
        & F.col("dq_valid_amount")
        & F.col("dq_valid_status")
    )
)

# COMMAND ----------

# DBTITLE 1,adding the metadata to the incoming Silver source
silver_change_df = (
    silver_change_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("simulated_change_batch")) # provides a source identifier because this particular source is a simulated DataFrame rather than an actual CSV file.
)

# COMMAND ----------

# DBTITLE 1,Silver view
# to create a temporary view for silver_change_df as silver_change_source

silver_change_df.createOrReplaceTempView("silver_change_source")

# COMMAND ----------

# DBTITLE 1,Perform MERGE INTO
# MAGIC %sql
# MAGIC MERGE INTO data_engineering_assignments.a8_banking.silver_transactions AS target
# MAGIC USING silver_change_source AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.account_id = source.account_id,
# MAGIC     target.transaction_timestamp = source.transaction_timestamp,
# MAGIC     target.transaction_type = source.transaction_type,
# MAGIC     target.amount = source.amount,
# MAGIC     target.currency = source.currency,
# MAGIC     target.merchant = source.merchant,
# MAGIC     target.status = source.status,
# MAGIC     target.dq_valid_account = source.dq_valid_account,
# MAGIC     target.dq_valid_amount = source.dq_valid_amount,
# MAGIC     target.dq_valid_status = source.dq_valid_status,
# MAGIC     target.dq_valid = source.dq_valid,
# MAGIC     target._ingested_at = source._ingested_at,
# MAGIC     target._source_file = source._source_file
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     transaction_id,
# MAGIC     account_id,
# MAGIC     transaction_timestamp,
# MAGIC     transaction_type,
# MAGIC     amount,
# MAGIC     currency,
# MAGIC     merchant,
# MAGIC     status,
# MAGIC     dq_valid_account,
# MAGIC     dq_valid_amount,
# MAGIC     dq_valid_status,
# MAGIC     dq_valid,
# MAGIC     _ingested_at,
# MAGIC     _source_file
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.transaction_id,
# MAGIC     source.account_id,
# MAGIC     source.transaction_timestamp,
# MAGIC     source.transaction_type,
# MAGIC     source.amount,
# MAGIC     source.currency,
# MAGIC     source.merchant,
# MAGIC     source.status,
# MAGIC     source.dq_valid_account,
# MAGIC     source.dq_valid_amount,
# MAGIC     source.dq_valid_status,
# MAGIC     source.dq_valid,
# MAGIC     source._ingested_at,
# MAGIC     source._source_file
# MAGIC   )

# COMMAND ----------

# DBTITLE 1,verify silver_transactions
# MAGIC %sql
# MAGIC SELECT
# MAGIC     transaction_id,
# MAGIC     account_id,
# MAGIC     amount,
# MAGIC     status,
# MAGIC     dq_valid
# MAGIC FROM data_engineering_assignments.a8_banking.silver_transactions
# MAGIC ORDER BY transaction_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.      Gold: Build aggregated tables (e.g., daily transaction summary per account).

# COMMAND ----------

# MAGIC %md
# MAGIC Our Silver table is transaction-level data,
# MAGIC Gold should be business-ready aggregated data

# COMMAND ----------

# DBTITLE 1,Gold aggregation
# Taking clean Silver transaction data and turning it into a business-ready Gold table with one row per account per day.
# grain of your Gold table --> One row represents one account for one transaction date.



# Step 1: Read the persistent Silver Delta table
# spark.table() lets us reload the persistent Delta table, which is useful if our Serverless session reconnects.

silver_df = spark.table(
    "data_engineering_assignments.a8_banking.silver_transactions"
)


# Step 2: Keep only records that passed our DQ rules
# Invalid records should not contribute to business metrics.

silver_valid_df = (
    silver_df
    .filter(F.col("dq_valid") == True)
)

# Step 3: Extract the transaction date
# transaction_timestamp contains date + time.
# Gold needs DAILY metrics, so we extract only the date.

gold_source_df = (
    silver_valid_df
    .withColumn(
        "transaction_date",
        F.to_date("transaction_timestamp")
    )
)


# Step 4: Define the grain of the Gold table
# One Gold row will represent: ONE DATE + ONE ACCOUNT
# Therefore we group by: - transaction_date,  account_id

gold_daily_account_df = (
    gold_source_df
    .groupBy(
        "transaction_date",
        "account_id"
    )

    # Step 5: Calculate business metrics
    .agg(

        # Total number of valid transactions
        F.count("*").alias("transaction_count"),


        # Total value of DEBIT transactions - For DEBIT rows: use amount, For non-DEBIT rows: use 0
        F.sum(
            F.when(
                F.col("transaction_type") == "DEBIT",
                F.col("amount")
            ).otherwise(F.lit(0))
        ).alias("total_debit"),


        # Total value of CREDIT transactions
        F.sum(
            F.when(
                F.col("transaction_type") == "CREDIT",
                F.col("amount")
            ).otherwise(F.lit(0))
        ).alias("total_credit"),


        # Net amount
        F.sum(
            F.when(
                F.col("transaction_type") == "CREDIT",
                F.col("amount")
            )
            .when(
                F.col("transaction_type") == "DEBIT",
                -F.col("amount")
            )
            .otherwise(F.lit(0))
        ).alias("net_amount"),


        # Count how many transactions have COMPLETED status
        F.sum(
            F.when(
                F.col("status") == "COMPLETED",
                1
            ).otherwise(0)
        ).alias("completed_transactions")
    )

    # Sort the final Gold result
    .orderBy(
        "transaction_date",
        "account_id"
    )
)

# COMMAND ----------

# DBTITLE 1,Show gold_daily_account_df
display(gold_daily_account_df)

# COMMAND ----------

# DBTITLE 1,Save Gold as Delta
gold_table = "data_engineering_assignments.a8_banking.gold_daily_account_summary"

(
    gold_daily_account_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table)
)

# COMMAND ----------

# DBTITLE 1,verify gold delta table
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering_assignments.a8_banking.gold_daily_account_summary
# MAGIC ORDER BY transaction_date, account_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.      Enable Delta features: time travel, schema evolution, OPTIMIZE, ZORDER BY.

# COMMAND ----------

# DBTITLE 1,inspect the Gold history
# MAGIC %sql
# MAGIC DESCRIBE HISTORY data_engineering_assignments.a8_banking.gold_daily_account_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel

# COMMAND ----------

# DBTITLE 1,Create a new Gold version as version 1
# to make a harmless business change, lets add a column showing the current balance status as Postive or Negative based on net amount

gold_updated_df = (
    gold_daily_account_df
    .withColumn(
        "balance_direction",
        F.when(F.col("net_amount") > 0, "POSITIVE")
         .when(F.col("net_amount") < 0, "NEGATIVE")
         .otherwise("ZERO")
    )
)

display(gold_updated_df)

# COMMAND ----------

# DBTITLE 1,overwrite the Gold table
(
    gold_updated_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") # intentionally want the target table's schema to be replaced
    .saveAsTable(gold_table)
)

# COMMAND ----------

# DBTITLE 1,check the history
# MAGIC %sql
# MAGIC DESCRIBE HISTORY data_engineering_assignments.a8_banking.gold_daily_account_summary;

# COMMAND ----------

# DBTITLE 1,time-travel query - version 0
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering_assignments.a8_banking.gold_daily_account_summary
# MAGIC VERSION AS OF 0;

# COMMAND ----------

# DBTITLE 1,time-travel query - version 1
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering_assignments.a8_banking.gold_daily_account_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution

# COMMAND ----------

# DBTITLE 1,Create a schema-evolution demo table
catalog = "data_engineering_assignments"
schema = "a8_banking"

schema_demo_table = f"{catalog}.{schema}.schema_evolution_demo"

gold_df = spark.table(gold_table)

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(schema_demo_table)
)

display(spark.table(schema_demo_table))

# COMMAND ----------

# DBTITLE 1,Create incoming data with a NEW column
from pyspark.sql.functions import lit

evolution_source_df = (
    spark.table(schema_demo_table)
    .limit(2)
    .withColumn("channel", lit("ONLINE"))
)

display(evolution_source_df)

# COMMAND ----------

# DBTITLE 1,Append with schema evolution enabled
(
    evolution_source_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true") # --. allow the target schema to evolve to accommodate new columns from the incoming DataFrame.
    .saveAsTable(schema_demo_table)
)

# Databricks documents mergeSchema=true as the DataFrame write option for automatic schema evolution.

# COMMAND ----------

# DBTITLE 1,Verify the evolved schema
display(spark.table(schema_demo_table))

spark.table(schema_demo_table).printSchema()

# COMMAND ----------

# DBTITLE 1,inspect schema_demo_table
spark.sql(f"DESCRIBE TABLE {schema_demo_table}").show(truncate=False)

# COMMAND ----------

# reconstruct the DataFrame from the persistent Gold table
gold_daily_account_df = spark.table(
    "data_engineering_assignments.a8_banking.gold_daily_account_summary"
)

display(gold_daily_account_df)

# COMMAND ----------

schema_demo_table = (
    "data_engineering_assignments.a8_banking.schema_evolution_demo"
)

schema_demo_df = spark.table(schema_demo_table)

display(schema_demo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE + ZORDER BY

# COMMAND ----------

# after a session reconnect, running this one cell restores the variables

# A8 Table Configuration


catalog = "data_engineering_assignments"
schema = "a8_banking"

bronze_table = f"{catalog}.{schema}.bronze_transactions"
silver_table = f"{catalog}.{schema}.silver_transactions"
gold_table = f"{catalog}.{schema}.gold_daily_account_summary"

print("Bronze:", bronze_table)
print("Silver :", silver_table)
print("Gold   :", gold_table)

# COMMAND ----------

gold_table = "data_engineering_assignments.a8_banking.gold_daily_account_summary"

# Confirm that the table exists
display(spark.table(gold_table))

# COMMAND ----------

# DBTITLE 1,OPTIMIZE the Gold table
# OPTIMIZE reorganizes the physical files of a Delta table.
# The goal is to improve read performance by reducing the number of small files and creating better-sized data files.

spark.sql(f"""
OPTIMIZE {gold_table}
""")

# performs file compaction/reorganization

# COMMAND ----------

# DBTITLE 1,ZORDER BY
# ZORDER BY reorganizes the data so that values of the chosen column are colocated more effectively in the underlying files.
#
# This can improve data skipping when queries filter on account_id.

spark.sql(f"""
OPTIMIZE {gold_table}
ZORDER BY (account_id)
""")

# Z-Ordering tries to organize data so related values are physically colocated, which can make data skipping more effective.

# COMMAND ----------

# DBTITLE 1,Verify the table history
# Because OPTIMIZE is a Delta operation, it is recorded in Delta transaction history.

display(
    spark.sql(f"""
    DESCRIBE HISTORY {gold_table}
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold table has two Delta versions:
# MAGIC
# MAGIC - Version 0 — initial creation, 8 rows, 1 file written.
# MAGIC - Version 1 — the later CREATE OR REPLACE TABLE AS SELECT, with 8 rows and 1 file removed/replaced.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.      Implement CDC using Delta's readChangeFeed.

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed
# Let's use your Silver table for the CDC demonstration because Silver is where transactional row-level changes are especially meaningful.

# Enable Change Data Feed (CDF) on the Silver Delta table.
# CDF records row-level changes made to the table.

spark.sql(f"""
ALTER TABLE {silver_table}
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# DBTITLE 1,Verify Change Data feed
display(
    spark.sql(f"""
    DESCRIBE DETAIL {silver_table}
    """)
)

# COMMAND ----------

# DBTITLE 1,Make a change to Silver
# We need an actual change so CDF has something to report, to create a temporary source containing one changed transaction.
# We are intentionally modifying an existing transaction. For example, change T013 from 650 to 700

from pyspark.sql import functions as F

cdc_change_df = (
    spark.table(silver_table)
    .filter(F.col("transaction_id") == "T013")
    .withColumn("amount", F.lit(700).cast("decimal(18,2)"))
)
    
cdc_change_df.createOrReplaceTempView("cdc_change_source")

# COMMAND ----------

# DBTITLE 1,execute a MERGE
spark.sql(f"""
MERGE INTO {silver_table} AS target
USING cdc_change_source AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN UPDATE SET
    target.account_id = source.account_id,
    target.transaction_timestamp = source.transaction_timestamp,
    target.transaction_type = source.transaction_type,
    target.amount = source.amount,
    target.currency = source.currency,
    target.merchant = source.merchant,
    target.status = source.status,
    target.dq_valid_account = source.dq_valid_account,
    target.dq_valid_amount = source.dq_valid_amount,
    target.dq_valid_status = source.dq_valid_status,
    target.dq_valid = source.dq_valid,
    target._ingested_at = source._ingested_at,
    target._source_file = source._source_file
""")

# COMMAND ----------

# DBTITLE 1,Verify merge
display(
    spark.table(silver_table)
    .filter(F.col("transaction_id") == "T013")
)

# verify if amount is now 700

# COMMAND ----------

# DBTITLE 1,identify the Delta version created by this MERGE
display(
    spark.sql(f"""
    DESCRIBE HISTORY {silver_table}
    """)
)

# COMMAND ----------

# DBTITLE 1,observe  changes
# # we can observe here for Version 4, with operation = MERGE

# numTargetRowsUpdated: "1"
# numTargetChangeFilesAdded: "1"


# COMMAND ----------

# DBTITLE 1,Read the Change Data Feed
# To read row-level changes from the Delta table.
# readChangeFeed=True tells Delta that we want the changes, rather than simply reading the current table contents.

cdc_df = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 4)     # Start reading the change feed from Delta version 4
    .table(silver_table)
)

# If I used - option("startingVersion", 0), I get error as - Error getting change data for range [0 , 4] as change data was not recorded for version [0]
# Because CDF was enabled at Version 3, we can read changes from Version 4 onward.

display(cdc_df)

# COMMAND ----------

# For the T013 update, CDF shows 2 rows representing the update:

# update_preimage   → row before the update
# update_postimage  → row after the update

# COMMAND ----------

# DBTITLE 1,CDC output easier to understand
display(
    cdc_df.select(
        "transaction_id",
        "amount",
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    )
)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Gold table reflects the latest Silver state
# Gold output is still consistent with the latest Silver data
display(
    spark.table(gold_table)
    .orderBy("transaction_date", "account_id")
)

# COMMAND ----------

from pyspark.sql import functions as F

# Reload the latest Silver data from the persistent Delta table.
silver_df = spark.table(silver_table)

# Keep only records that passed our data-quality checks.
silver_valid_df = silver_df.filter(
    F.col("dq_valid") == True
)

# Derive the transaction date used for daily aggregation.
gold_source_df = silver_valid_df.withColumn(
    "transaction_date",
    F.to_date("transaction_timestamp")
)

# Create one Gold row per transaction_date + account_id.
gold_daily_account_df = (
    gold_source_df
    .groupBy("transaction_date", "account_id")
    .agg(
        F.count("*").alias("transaction_count"),

        # Sum only DEBIT transactions.
        F.sum(
            F.when(
                F.col("transaction_type") == "DEBIT",
                F.col("amount")
            ).otherwise(F.lit(0))
        ).alias("total_debit"),

        # Sum only CREDIT transactions.
        F.sum(
            F.when(
                F.col("transaction_type") == "CREDIT",
                F.col("amount")
            ).otherwise(F.lit(0))
        ).alias("total_credit"),

        # CREDIT increases the net amount;
        # DEBIT decreases it.
        F.sum(
            F.when(
                F.col("transaction_type") == "CREDIT",
                F.col("amount")
            )
            .when(
                F.col("transaction_type") == "DEBIT",
                -F.col("amount")
            )
            .otherwise(F.lit(0))
        ).alias("net_amount"),

        # Count completed transactions.
        F.sum(
            F.when(
                F.col("status") == "COMPLETED",
                1
            ).otherwise(0)
        ).alias("completed_transactions")
    )
    .withColumn(
        "balance_direction",
        F.when(F.col("net_amount") > 0, "POSITIVE")
         .when(F.col("net_amount") < 0, "NEGATIVE")
         .otherwise("ZERO")
    )
    .orderBy("transaction_date", "account_id")
)

display(gold_daily_account_df)

# COMMAND ----------

# Refresh the Gold Delta table using the latest Silver data.
(
    gold_daily_account_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

# COMMAND ----------

display(
    spark.table(gold_table)
    .orderBy("transaction_date", "account_id")
)

# COMMAND ----------

