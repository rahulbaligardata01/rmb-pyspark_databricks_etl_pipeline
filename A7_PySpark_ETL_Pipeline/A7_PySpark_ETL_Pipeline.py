# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment A7: PySpark ETL Pipeline
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC Process clickstream event data for user behavior analytics using PySpark.
# MAGIC
# MAGIC ### Pipeline Steps
# MAGIC
# MAGIC 1. Read raw JSON logs into a Spark DataFrame.
# MAGIC 2. Parse timestamps and derive session IDs.
# MAGIC 3. Enrich clickstream events with user dimension data.
# MAGIC 4. Compute session-level metrics:
# MAGIC    - Session duration
# MAGIC    - Pages per session
# MAGIC    - Bounce rate
# MAGIC 5. Compute daily aggregates by user segment.
# MAGIC 6. Write analytical output as partitioned Parquet by date.
# MAGIC 7. Compare groupBy, reduceByKey, and window-based approaches.
# MAGIC 8. Use explain() to inspect Spark's physical execution plan.
# MAGIC
# MAGIC ### Environment
# MAGIC
# MAGIC - Platform: Databricks Free Edition
# MAGIC - Language: PySpark / Python
# MAGIC - Storage format: JSON → Parquet

# COMMAND ----------

print("Spark version:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.      Read raw JSON logs into a Spark DataFrame.

# COMMAND ----------

# DBTITLE 1,Create User dimension
from pyspark.sql import Row

users = [
    Row(user_id="U001", user_name="User 001", segment="Premium",    signup_date="2025-08-15", country="IN"),
    Row(user_id="U002", user_name="User 002", segment="Standard",   signup_date="2025-10-20", country="US"),
    Row(user_id="U003", user_name="User 003", segment="Premium",    signup_date="2025-09-02", country="IN"),
    Row(user_id="U004", user_name="User 004", segment="Standard",   signup_date="2025-11-12", country="UK"),
    Row(user_id="U005", user_name="User 005", segment="Enterprise", signup_date="2025-06-01", country="IN"),
    Row(user_id="U006", user_name="User 006", segment="Premium",    signup_date="2025-07-21", country="US"),
    Row(user_id="U007", user_name="User 007", segment="Standard",   signup_date="2025-09-30", country="UK"),
    Row(user_id="U008", user_name="User 008", segment="Enterprise", signup_date="2025-05-18", country="IN")
]

user_df = spark.createDataFrame(users)

display(user_df)

# COMMAND ----------

user_df.show()

user_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Schema creation
# MAGIC %sql
# MAGIC -- I have created a catalog using databricks UI
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS data_engineering_assignments.a7_clickstream;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN data_engineering_assignments;

# COMMAND ----------

# DBTITLE 1,Creating Volume for raw data
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS data_engineering_assignments.a7_clickstream.raw_data
# MAGIC COMMENT 'Raw JSON clickstream data for Assignment A7';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN data_engineering_assignments.a7_clickstream;

# COMMAND ----------

# DBTITLE 1,Generate test data and write raw JSON
from datetime import datetime, timedelta
import json

raw_events = [
    # -------------------------
    # U001 - Premium
    # Session 1
    # -------------------------
    {
        "event_id": "E001",
        "user_id": "U001",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-02 09:00:00",
        "device": "mobile",
        "country": "IN"
    },
    {
        "event_id": "E002",
        "user_id": "U001",
        "event_type": "page_view",
        "page": "/products",
        "event_timestamp": "2026-01-02 09:05:00",
        "device": "mobile",
        "country": "IN"
    },
    {
        "event_id": "E003",
        "user_id": "U001",
        "event_type": "page_view",
        "page": "/product/101",
        "event_timestamp": "2026-01-02 09:10:00",
        "device": "mobile",
        "country": "IN"
    },
    {
        "event_id": "E004",
        "user_id": "U001",
        "event_type": "purchase",
        "page": "/checkout",
        "event_timestamp": "2026-01-02 09:12:00",
        "device": "mobile",
        "country": "IN"
    },

    # U001 - Session 2, >30 min gap
    {
        "event_id": "E005",
        "user_id": "U001",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-02 16:00:00",
        "device": "mobile",
        "country": "IN"
    },

    # -------------------------
    # U002 - Standard
    # Session 1
    # -------------------------
    {
        "event_id": "E006",
        "user_id": "U002",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-02 10:00:00",
        "device": "desktop",
        "country": "US"
    },
    {
        "event_id": "E007",
        "user_id": "U002",
        "event_type": "page_view",
        "page": "/pricing",
        "event_timestamp": "2026-01-02 10:04:00",
        "device": "desktop",
        "country": "US"
    },

    # -------------------------
    # U003 - Premium
    # Session 1
    # -------------------------
    {
        "event_id": "E008",
        "user_id": "U003",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-02 11:00:00",
        "device": "mobile",
        "country": "IN"
    },
    {
        "event_id": "E009",
        "user_id": "U003",
        "event_type": "page_view",
        "page": "/search",
        "event_timestamp": "2026-01-02 11:03:00",
        "device": "mobile",
        "country": "IN"
    },
    {
        "event_id": "E010",
        "user_id": "U003",
        "event_type": "page_view",
        "page": "/product/205",
        "event_timestamp": "2026-01-02 11:08:00",
        "device": "mobile",
        "country": "IN"
    },

    # U003 - Session 2
    {
        "event_id": "E011",
        "user_id": "U003",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-02 15:00:00",
        "device": "mobile",
        "country": "IN"
    },

    # U003 - Session 2 continued
    {
        "event_id": "E012",
        "user_id": "U003",
        "event_type": "purchase",
        "page": "/checkout",
        "event_timestamp": "2026-01-02 15:03:00",
        "device": "mobile",
        "country": "IN"
    },

    # -------------------------
    # U004 - Standard
    # Bounce session
    # -------------------------
    {
        "event_id": "E013",
        "user_id": "U004",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-02 12:00:00",
        "device": "desktop",
        "country": "UK"
    },

    # -------------------------
    # U005 - Enterprise
    # Session 1
    # -------------------------
    {
        "event_id": "E014",
        "user_id": "U005",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-03 08:00:00",
        "device": "desktop",
        "country": "IN"
    },
    {
        "event_id": "E015",
        "user_id": "U005",
        "event_type": "page_view",
        "page": "/dashboard",
        "event_timestamp": "2026-01-03 08:02:00",
        "device": "desktop",
        "country": "IN"
    },
    {
        "event_id": "E016",
        "user_id": "U005",
        "event_type": "page_view",
        "page": "/reports",
        "event_timestamp": "2026-01-03 08:06:00",
        "device": "desktop",
        "country": "IN"
    },

    # -------------------------
    # U006 - Premium
    # -------------------------
    {
        "event_id": "E017",
        "user_id": "U006",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-03 09:00:00",
        "device": "mobile",
        "country": "US"
    },
    {
        "event_id": "E018",
        "user_id": "U006",
        "event_type": "page_view",
        "page": "/products",
        "event_timestamp": "2026-01-03 09:07:00",
        "device": "mobile",
        "country": "US"
    },

    # -------------------------
    # U007 - Standard
    # -------------------------
    {
        "event_id": "E019",
        "user_id": "U007",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-03 10:00:00",
        "device": "desktop",
        "country": "UK"
    },
    {
        "event_id": "E020",
        "user_id": "U007",
        "event_type": "page_view",
        "page": "/blog",
        "event_timestamp": "2026-01-03 10:15:00",
        "device": "desktop",
        "country": "UK"
    },

    # -------------------------
    # U008 - Enterprise
    # -------------------------
    {
        "event_id": "E021",
        "user_id": "U008",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-04 14:00:00",
        "device": "mobile",
        "country": "IN"
    },
    {
        "event_id": "E022",
        "user_id": "U008",
        "event_type": "page_view",
        "page": "/pricing",
        "event_timestamp": "2026-01-04 14:05:00",
        "device": "mobile",
        "country": "IN"
    },

    # -------------------------
    # Intentionally bad record:
    # missing timestamp
    # -------------------------
    {
        "event_id": "E023",
        "user_id": "U002",
        "event_type": "page_view",
        "page": "/help",
        "event_timestamp": None,
        "device": "desktop",
        "country": "US"
    },

    # -------------------------
    # Intentionally unknown user
    # -------------------------
    {
        "event_id": "E024",
        "user_id": "U999",
        "event_type": "page_view",
        "page": "/home",
        "event_timestamp": "2026-01-04 15:00:00",
        "device": "mobile",
        "country": "IN"
    },

    # -------------------------
    # Intentionally duplicated event
    # -------------------------
    {
        "event_id": "E020",
        "user_id": "U007",
        "event_type": "page_view",
        "page": "/blog",
        "event_timestamp": "2026-01-03 10:15:00",
        "device": "desktop",
        "country": "UK"
    }
]

len(raw_events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Raw Clickstream Data
# MAGIC
# MAGIC Because no sample source data was provided for the assignment, a small synthetic dataset is generated for development and testing.
# MAGIC
# MAGIC The dataset intentionally includes:
# MAGIC - Multiple events within a session
# MAGIC - Multiple sessions for the same user
# MAGIC - Single-event sessions for bounce-rate analysis
# MAGIC - Duplicate event IDs
# MAGIC - Missing timestamps
# MAGIC - An unknown user ID
# MAGIC - Multiple dates and user segments
# MAGIC
# MAGIC The generated records are written as newline-delimited JSON to a Unity Catalog volume and then read using Spark to simulate raw-source ingestion.

# COMMAND ----------

# DBTITLE 1,write to Volume
import json

json_data = "\n".join(
    json.dumps(event)
    for event in raw_events
)

dbutils.fs.put(
    "/Volumes/data_engineering_assignments/a7_clickstream/raw_data/clickstream.json",
    json_data,
    overwrite=True
)

# COMMAND ----------

# DBTITLE 1,Ingest raw JSON using Spark
raw_events_df = spark.read.json(
    "/Volumes/data_engineering_assignments/a7_clickstream/raw_data/clickstream.json"
)

# COMMAND ----------

display(raw_events_df)

# COMMAND ----------

# to check record with missing timestamp

raw_events_df.filter(raw_events_df.event_timestamp.isNull()).show()

# COMMAND ----------

# to check duplicate records
raw_events_df.groupBy("event_id").count().filter("count > 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.      Parse timestamps, extract session IDs, and enrich with user dimension

# COMMAND ----------

# DBTITLE 1,Clean and parse timestamps
# Clean and parse timestamps
from pyspark.sql import functions as F

# COMMAND ----------

events_clean_df = (
    raw_events_df
    .dropDuplicates(["event_id"]) # to remove duplicates
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss") # to interpret the existing string using this timestamp format
    )
)

# COMMAND ----------

# To check if the event_timestamp is now a timestamp
events_clean_df.printSchema()

# COMMAND ----------

display(events_clean_df.orderBy("event_id"))

# COMMAND ----------

# DBTITLE 1,deduplicated count
# To check records count after deduplication
print("Records after deduplication:", events_clean_df.count())

# COMMAND ----------

# DBTITLE 1,Invalid timestamps
# To check if we have any invalid timestamps
events_clean_df.filter(
    F.col("event_timestamp").isNull()
).select(
    "event_id",
    "user_id",
    "event_timestamp"
).show()

# COMMAND ----------

# DBTITLE 1,Add a data-quality flag
# Adding a data quality flag to see how many invalid input data have we recieved

events_dq_df = (
    events_clean_df
    .withColumn(
        "dq_valid_timestamp",
        F.col("event_timestamp").isNotNull()
    )
)

# COMMAND ----------

# to see the data quality flag results
display(
    events_dq_df
    .select(
        "event_id",
        "user_id",
        "event_timestamp",
        "dq_valid_timestamp"
    )
    .orderBy("event_id")
)

# COMMAND ----------

# DBTITLE 1,use only valid timestamp records
# events safe for timestamp-dependent processing

events_valid_df = events_dq_df.filter(
    F.col("dq_valid_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleaning and Timestamp Parsing
# MAGIC
# MAGIC The raw clickstream contains duplicate events and potentially invalid timestamps.
# MAGIC
# MAGIC The following transformations are applied:
# MAGIC
# MAGIC 1. Deduplicate events using `event_id`.
# MAGIC 2. Convert `event_timestamp` from string to Spark `timestamp`.
# MAGIC 3. Create a data-quality flag for valid timestamps.
# MAGIC 4. Exclude invalid timestamps from downstream sessionization while retaining the DQ flag for monitoring.
# MAGIC
# MAGIC Record counts are validated after each stage.

# COMMAND ----------

print("Raw records:", raw_events_df.count())
print("After deduplication:", events_clean_df.count())
print("After valid-timestamp filtering:", events_valid_df.count())

# COMMAND ----------

# to compare the raw and cleaned dataframe schemas

raw_events_df.printSchema()

events_clean_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,SESSIONIZATION
# Our sessionization rule
# A new session starts when a user's current event occurs more than 30 minutes after their previous event. [30 minutes of inactivity starts a new session]

# Partition the data by user, and within each user, order events chronologically
# lag() — look at the previous row

from pyspark.sql.window import Window

user_time_window = (
    Window
    .partitionBy("user_id")
    .orderBy("event_timestamp")
)

events_with_previous_df = (
    events_valid_df
    .withColumn(
        "previous_event_timestamp",
        F.lag("event_timestamp").over(user_time_window)
    )
)

# COMMAND ----------

# DBTITLE 1,To see previous event timestamp
display(
    events_with_previous_df
    .select(
        "user_id",
        "event_id",
        "event_timestamp",
        "previous_event_timestamp"
    )
    .orderBy("user_id", "event_timestamp")
)

# COMMAND ----------

# DBTITLE 1,Calculate the inactivity gap
# To calculate the inactivity gap, we can use event_timestamp - previous_event_timestamp

events_with_gap_df = (
    events_with_previous_df
    .withColumn(
        "gap_seconds",
        F.col("event_timestamp").cast("long") - F.col("previous_event_timestamp").cast("long") # to convert timestamp to long, gives us the elapsed time in seconds
    )
)

# COMMAND ----------

# DBTITLE 1,Checking inactivity gap between events
display(
    events_with_gap_df
    .select(
        "user_id",
        "event_id",
        "event_timestamp",
        "previous_event_timestamp",
        "gap_seconds"
    )
    .orderBy("user_id", "event_timestamp")
)

# COMMAND ----------

# DBTITLE 1,Create a session-boundary flag
# To create session boundaries flags

# 1 means: Start a new session.
# 0 means: Continue the existing session.

events_with_boundary_df = (
    events_with_gap_df
    .withColumn(
        "new_session",
        F.when(
            F.col("previous_event_timestamp").isNull(),
            1 # since sessions starts here for a user
        )
        .when(
            F.col("gap_seconds") > 30 * 60,
            1 # starts new session after 30 minutes of inactivity
        )
        .otherwise(0) # continue the existing session as long as the gap is less than 30 minutes
    )
)

# COMMAND ----------

# DBTITLE 1,To see session-boundaries fllags
display(
    events_with_boundary_df
    .select(
        "user_id",
        "event_id",
        "event_timestamp",
        "gap_seconds",
        "new_session"
    )
    .orderBy("user_id", "event_timestamp")
)

# COMMAND ----------

# DBTITLE 1,Generate the session number
# With a cumulative window sum

# to create a session number
session_number_window = (
    Window
    .partitionBy("user_id")
    .orderBy("event_timestamp")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)


# adding a new column called session_number,
events_sessionized_df = (
    events_with_boundary_df
    .withColumn(
        "session_number",
        F.sum("new_session").over(session_number_window)
    )
)

# COMMAND ----------

# DBTITLE 1,To view session numbers
display(
    events_sessionized_df
    .select(
        "user_id",
        "event_id",
        "event_timestamp",
        "new_session",
        "session_number"
    )
    .orderBy("user_id", "event_timestamp")
)

# COMMAND ----------

# DBTITLE 1,Create the actual session_id
# create a readable session identifier

events_sessionized_df = (
    events_sessionized_df
    .withColumn(
        "session_id",
        F.concat(
            F.col("user_id"),
            F.lit("_session_"),
            F.col("session_number").cast("string")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,extract session IDs
display(
    events_sessionized_df
    .select(
        "event_id",
        "user_id",
        "event_timestamp",
        "session_id"
    )
    .orderBy("user_id", "event_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich with the user dimension

# COMMAND ----------

# DBTITLE 1,Convert signup_date to a date
# We have our user dimension - user_df

user_enriched_dim_df = (
    user_df
    .withColumn("signup_date", F.to_date("signup_date"))
)

# COMMAND ----------

# DBTITLE 1,verify schema for signup_date
user_enriched_dim_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Join the two DataFrames to enrich
# using left join for events_sessionized_df, user_enriched_dim_df

enriched_events_df = (
    events_sessionized_df
    .join(
        user_enriched_dim_df,
        on="user_id",
        how="left"
    )
)

# COMMAND ----------

# DBTITLE 1,To check enriched_events_df
display(
    enriched_events_df
    .select(
        "event_id",
        "user_id",
        "session_id",
        "segment",
        "signup_date"
    )
    .orderBy("user_id", "event_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### With above result, we have:
# MAGIC     
# MAGIC - U001 → Premium
# MAGIC - U002 → Standard
# MAGIC - U005 → Enterprise
# MAGIC
# MAGIC But for U999 is null for segment and signup_date as there is no matching dimension record
# MAGIC
# MAGIC ------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute session-level metrics: duration, pages per session, bounce rate.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session-level metrics

# COMMAND ----------

# DBTITLE 1,Aggregate events by session
# To calculate Session duration & Pages per session - Aggregate events by session
session_metrics_df = (
    enriched_events_df
    .groupBy("session_id", "user_id", "segment")
    .agg(
        F.min("event_timestamp").alias("session_start"),
        F.max("event_timestamp").alias("session_end"),
        F.sum(
            F.when(F.col("event_type") == "page_view", 1).otherwise(0)
        ).alias("pages_per_session")
    )
)

# COMMAND ----------

# DBTITLE 1,Calculate session duration
# To calculate the duration of each session, we need to convert the session start and end timestamps to long values and get the difference between them
session_metrics_df = (
    session_metrics_df
    .withColumn(
        "duration_seconds",
        F.col("session_end").cast("long") - F.col("session_start").cast("long")
    )
)

# COMMAND ----------

# DBTITLE 1,See session duration
display(
    session_metrics_df
    .orderBy("user_id", "session_start")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bounce rate

# COMMAND ----------

# DBTITLE 1,Session level flag
# Bounce as:
# A session containing exactly one event/page view [pages_per_session = 1]

# Session level flag for bounce
session_metrics_df = (
    session_metrics_df
    .withColumn(
        "is_bounce",
        F.when(F.col("pages_per_session") == 1, 1).otherwise(0)
    )
)

# COMMAND ----------

# DBTITLE 1,Checking session level flag
display(
    session_metrics_df
    .select(
        "session_id",
        "user_id",
        "segment",
        "duration_seconds",
        "pages_per_session",
        "is_bounce"
    )
    .orderBy("user_id")
)

# COMMAND ----------

# DBTITLE 1,handling null segment for u999
# U999 exists in clickstream data but not in user_df, so segment = null
# records with session_start = session_end have duration = 0

# U999 has no segment. We can keep NULL as an "Unknown" segment, so we don't silently lose valid clickstream activity

session_metrics_df = (
    session_metrics_df
    .withColumn(
        "segment",
        F.coalesce(F.col("segment"), F.lit("Unknown"))
    )
)


# COMMAND ----------

# DBTITLE 1,null segment for u999 as uknown
# verify null for U999
display(
    session_metrics_df.select(
        "session_id",
        "user_id",
        "segment",
        "duration_seconds",
        "pages_per_session",
        "is_bounce"
    ).orderBy("user_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.      Compute daily aggregates per user segment.

# COMMAND ----------

# DBTITLE 1,derive the date from the session start
# to derive the date from the session start
session_metrics_df = (
    session_metrics_df
    .withColumn(
        "date",
        F.to_date("session_start")
    )
)

# COMMAND ----------

# DBTITLE 1,aggregate values
# to get the daily metrics

daily_segment_metrics_df = (
    session_metrics_df
    .groupBy("date", "segment")
    .agg(
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("session_id").alias("sessions"),
        F.avg("duration_seconds").alias("avg_session_duration_seconds"),
        F.avg("pages_per_session").alias("avg_pages_per_session"),
        F.avg("is_bounce").alias("bounce_rate")
    )
    .orderBy("date", "segment")
)

# COMMAND ----------

# DBTITLE 1,To see daily metrics
display(daily_segment_metrics_df)

# COMMAND ----------

# DBTITLE 1,summary for daily segment metrics
# bounce rate is aggregated from the session-level is_bounce flag, while duration and pages are averages of the session-level metrics.

# Across Jan 2–4, Premium generated the most activity, with 4 sessions on Jan 2 and a 25% bounce rate; Standard had a higher 50% bounce rate on Jan 2.
# Enterprise showed longer/healthy engagement, while the Unknown segment represents the unmatched U999 user and has a 100% bounce rate

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.      Write output as partitioned Parquet (by date).

# COMMAND ----------

# DBTITLE 1,final DataFrame schema
# to inspect the final DataFrame schema
daily_segment_metrics_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write partitioned Parquet
# To write the DataFrame to a partitioned parquet file

output_path = "/Volumes/data_engineering_assignments/a7_clickstream/raw_data/daily_segment_metrics"

(
    daily_segment_metrics_df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(output_path)
)

# Instead of one large Parquet dataset, Spark organizes the files based on the partition column - date

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.      Compare performance: groupBy vs reduceByKey vs window functions.

# COMMAND ----------

# MAGIC %md
# MAGIC - groupBy + agg       --> DataFrame aggregation
# MAGIC - reduceByKey         --> Key-based RDD aggregation
# MAGIC - Window functions	--> Calculations involving related rows while retaining row-level detail
# MAGIC --------------------
# MAGIC - We have used a window function for sessionization
# MAGIC - groupBy(...).agg(...) for session and daily aggregation

# COMMAND ----------

# DBTITLE 1,groupBy
groupby_result = (
    events_valid_df
    .groupBy("user_id")
    .count()
)

groupby_result.show()

# COMMAND ----------

# DBTITLE 1,groupby results with explain
groupby_result.explain()

# COMMAND ----------

# DBTITLE 1,groupby results with explain(True)
groupby_result.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupBy plan analysis
# MAGIC --> The key insight is that Catalyst simplified your DataFrame lineage and the physical plan exposes two shuffle points: one for deduplication by event_id, and one for the final groupBy("user_id").

# COMMAND ----------

# DBTITLE 1,inspecting window-function
# events_with_previous_df --> used window-function for the sessionization logic
events_with_previous_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window function plan analysis
# MAGIC --> lag() needs the events for the same user together and in timestamp order. Therefore, unlike our simple aggregation, the window operation can involve both shuffle and sorting.
# MAGIC
# MAGIC The window-based sessionization introduces a shuffle on user_id followed by a sort on user_id and event_timestamp before applying lag(). This is necessary to establish the required ordering for previous-row calculations.

# COMMAND ----------

# DBTITLE 1,reduceByKey limitation
# For reduceByKey, we will document its behavior conceptually because Free Edition/serverless environment does not permit the RDD/SparkContext API 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Analysis: groupBy vs reduceByKey vs Window Functions
# MAGIC
# MAGIC ### Dataset and benchmark limitation
# MAGIC
# MAGIC The functional pipeline was developed using a small synthetic dataset. Runtime measurements on this dataset are not representative of the assignment's target workload of approximately 50 GB.
# MAGIC
# MAGIC Therefore, the performance comparison focuses primarily on Spark execution characteristics, physical plans, shuffle behavior, and suitability of each API.
# MAGIC
# MAGIC ### groupBy
# MAGIC
# MAGIC `groupBy` is a DataFrame aggregation approach. The physical plan shows a shuffle using `hashpartitioning(user_id, 16)` before the final aggregation.
# MAGIC
# MAGIC ### reduceByKey
# MAGIC
# MAGIC `reduceByKey` is an RDD-based aggregation API. It can perform map-side combining before the shuffle, reducing the amount of intermediate data transferred. A direct RDD benchmark was not executed because the current Databricks Free Edition serverless environment restricts direct SparkContext/RDD APIs.
# MAGIC
# MAGIC ### Window Functions
# MAGIC
# MAGIC The sessionization implementation uses a window partitioned by `user_id` and ordered by `event_timestamp`. The physical plan shows a shuffle by `user_id`, followed by sorting by `user_id` and `event_timestamp`, before applying `lag()`.
# MAGIC
# MAGIC ### Summary
# MAGIC
# MAGIC `groupBy` is appropriate when the desired result is an aggregated dataset. `reduceByKey` is useful for RDD-based key aggregation and benefits from map-side combining. Window functions are appropriate when calculations depend on relationships between rows while retaining row-level detail, but may require both redistribution and sorting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results and Conclusion
# MAGIC
# MAGIC The A7 PySpark ETL pipeline successfully demonstrates:
# MAGIC
# MAGIC - Ingestion of raw JSON clickstream data into a Spark DataFrame.
# MAGIC - Deduplication and timestamp parsing.
# MAGIC - Sessionization using a 30-minute inactivity threshold.
# MAGIC - User-dimension enrichment.
# MAGIC - Session-level metrics including duration, pages per session, and bounce indication.
# MAGIC - Daily aggregates by user segment.
# MAGIC - Partitioned Parquet output by date.
# MAGIC - Comparison of DataFrame aggregation and window-function approaches, with a conceptual analysis of the RDD-based reduceByKey approach due to Databricks Free Edition serverless limitations.
# MAGIC - Physical-plan inspection using explain().
# MAGIC
# MAGIC ### Key assumptions
# MAGIC
# MAGIC 1. A new session begins when the inactivity gap exceeds 30 minutes.
# MAGIC 2. A bounce is defined as a session containing exactly one event.
# MAGIC 3. Unknown users are retained using a left join and assigned to the `Unknown` segment.
# MAGIC 4. The development dataset is synthetic and small; therefore its runtime is not representative of the assignment's target ~50 GB workload.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final validation

# COMMAND ----------

# DBTITLE 1,Validate session-level results
print("Total sessions:", session_metrics_df.count())
print("Total bounced sessions:", session_metrics_df.filter(F.col("is_bounce") == 1).count())

# COMMAND ----------

# DBTITLE 1,Validate Final Daily output
print("Daily aggregate rows:", daily_segment_metrics_df.count())

display(
    daily_segment_metrics_df.orderBy("date", "segment")
)

# COMMAND ----------

# DBTITLE 1,verify the Parquet output can actually be read back
parquet_check_df = spark.read.parquet(
    "/Volumes/data_engineering_assignments/a7_clickstream/raw_data/daily_segment_metrics"
)

display(parquet_check_df.orderBy("date", "segment"))

# COMMAND ----------

