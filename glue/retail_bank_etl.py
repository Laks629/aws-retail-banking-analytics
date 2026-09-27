"""AWS Glue (PySpark) job: raw -> validated/quarantine -> curated retail-banking lake.

Pipeline per run (full, idempotent reprocess of raw/):
  1. Read raw CSV batches + Kinesis/Lambda micro-batches (recursive) with an explicit schema
  2. Standardise types and codes (trim/upper-case, decimals, timestamps)
  3. Row-level validation with reason codes (completeness, validity, timeliness,
     referential integrity, uniqueness) -> quarantine/ with reasons
  4. AWS Glue Data Quality (DQDL ruleset) scores the raw and curated datasets;
     results are written to dq-results/ and published to CloudWatch / Glue console
  5. Quality gate: if quarantine rate > MAX_QUARANTINE_RATE, curated/ is NOT refreshed
     and the job fails (Step Functions alerts via SNS)
  6. Enrich + write curated star-schema tables as partitioned Parquet

The transformation functions are pure PySpark so they can be unit-tested locally
(tests/test_etl_logic.py) without the awsglue libraries.

Job parameters: --BUCKET --PREFIX --MAX_QUARANTINE_RATE --DQ_RULESET_S3
"""
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_TXN_COLUMNS = [
    "transaction_id", "transaction_timestamp", "source_account_id", "counterparty_id",
    "transaction_type", "amount", "channel", "merchant_category", "branch_id",
    "transaction_status", "balance_before", "balance_after", "is_fraud_flag",
]
RAW_ACCOUNT_COLUMNS = ["account_id", "customer_id", "account_type", "open_date", "account_status", "current_balance"]
RAW_CUSTOMER_COLUMNS = ["customer_id", "customer_since", "state", "customer_segment", "risk_tier",
                        "digital_enrolled_flag", "home_branch_id"]
RAW_BRANCH_COLUMNS = ["branch_id", "branch_name", "city", "state", "region"]

VALID_TYPES = ["DEPOSIT", "WITHDRAWAL", "PAYMENT", "TRANSFER", "DEBIT"]
VALID_CHANNELS = ["MOBILE", "ONLINE", "ATM", "BRANCH", "POS"]
VALID_STATUSES = ["POSTED", "FAILED", "REVERSED"]
VALID_ACCOUNT_STATUSES = ["ACTIVE", "DORMANT", "CLOSED", "FROZEN"]
DIGITAL_CHANNELS = ["MOBILE", "ONLINE"]
TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
MONEY = "decimal(18,2)"


# --------------------------------------------------------------------------- helpers
def configure_spark(spark: SparkSession) -> None:
    # Deterministic parsing: invalid values become NULL (and are then quarantined)
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # INT64 microsecond timestamps: readable by Athena AND Snowflake COPY INTO
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


def string_schema(columns) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns])


def read_raw_csv(spark: SparkSession, path: str, columns) -> DataFrame:
    return (spark.read.option("header", "true")
            .option("recursiveFileLookup", "true")
            .option("mode", "PERMISSIVE")
            .schema(string_schema(columns))
            .csv(path)
            .withColumn("source_file", F.input_file_name()))


def clean_code(col_name: str):
    """Trim + upper-case; blank strings become NULL."""
    c = F.upper(F.trim(F.col(col_name)))
    return F.when(c == "", F.lit(None)).otherwise(c)


def clean_text(col_name: str):
    c = F.trim(F.col(col_name))
    return F.when(c == "", F.lit(None)).otherwise(c)


# --------------------------------------------------------------------------- transactions
def standardize_transactions(raw: DataFrame) -> DataFrame:
    return raw.select(
        clean_code("transaction_id").alias("transaction_id"),
        F.to_timestamp(F.trim("transaction_timestamp"), TS_FORMAT).alias("transaction_ts"),
        clean_code("source_account_id").alias("source_account_id"),
        clean_code("counterparty_id").alias("counterparty_id"),
        clean_code("transaction_type").alias("transaction_type"),
        F.trim("amount").cast(MONEY).alias("amount"),
        clean_code("channel").alias("channel"),
        clean_code("merchant_category").alias("merchant_category"),
        clean_code("branch_id").alias("branch_id"),
        clean_code("transaction_status").alias("transaction_status"),
        F.trim("balance_before").cast(MONEY).alias("balance_before"),
        F.trim("balance_after").cast(MONEY).alias("balance_after"),
        F.trim("is_fraud_flag").cast("int").alias("is_fraud_flag"),
        F.when(F.col("source_file").contains("source=stream"), "STREAM").otherwise("BATCH").alias("ingest_source"),
        "source_file",
        F.col("transaction_timestamp").alias("raw_transaction_timestamp"),
        F.col("amount").alias("raw_amount"),
    )


def flag_transactions(std: DataFrame, known_account_ids: DataFrame) -> DataFrame:
    """Adds dq_fail_reasons (array<string>); an empty array means the row is valid."""
    accts = known_account_ids.select(F.col("account_id").alias("_known_acct")).distinct()
    df = std.join(F.broadcast(accts), std.source_account_id == accts._known_acct, "left")

    checks = [
        (F.col("transaction_id").isNull(), "NULL_TXN_ID"),
        (F.col("source_account_id").isNull(), "NULL_ACCOUNT"),
        (F.col("source_account_id").isNotNull() & F.col("_known_acct").isNull(), "ORPHAN_ACCOUNT"),
        (F.col("amount").isNull(), "INVALID_AMOUNT"),
        (F.col("amount") <= 0, "NON_POSITIVE_AMOUNT"),
        (F.col("transaction_ts").isNull(), "INVALID_TIMESTAMP"),
        (F.col("transaction_ts") > F.current_timestamp(), "FUTURE_TIMESTAMP"),
        (F.col("transaction_type").isNull() | ~F.col("transaction_type").isin(VALID_TYPES), "INVALID_TYPE"),
        (F.col("channel").isNull(), "NULL_CHANNEL"),
        (F.col("channel").isNotNull() & ~F.col("channel").isin(VALID_CHANNELS), "INVALID_CHANNEL"),
        (F.col("transaction_status").isNull() | ~F.col("transaction_status").isin(VALID_STATUSES), "INVALID_STATUS"),
    ]
    reasons = F.array(*[F.when(cond, F.lit(code)) for cond, code in checks])
    df = df.withColumn("dq_fail_reasons", F.filter(reasons, lambda x: x.isNotNull())).drop("_known_acct")

    # Uniqueness: among otherwise-valid rows keep the latest version of each transaction_id.
    df = df.withColumn("_row_ok", F.size("dq_fail_reasons") == 0)
    w = (Window.partitionBy("transaction_id", "_row_ok")
         .orderBy(F.col("transaction_ts").desc_nulls_last(), F.col("source_file")))
    df = df.withColumn("_rn", F.row_number().over(w))
    is_dup = F.col("_row_ok") & F.col("transaction_id").isNotNull() & (F.col("_rn") > 1)
    df = df.withColumn(
        "dq_fail_reasons",
        F.when(is_dup, F.array_union(F.col("dq_fail_reasons"), F.array(F.lit("DUPLICATE_TXN_ID"))))
         .otherwise(F.col("dq_fail_reasons")),
    )
    return df.drop("_row_ok", "_rn")


def split_valid_quarantine(flagged: DataFrame, run_id: str):
    valid = flagged.filter(F.size("dq_fail_reasons") == 0).drop("dq_fail_reasons", "raw_transaction_timestamp", "raw_amount")
    quarantine = (flagged.filter(F.size("dq_fail_reasons") > 0)
                  .withColumn("dq_fail_reasons", F.concat_ws(",", "dq_fail_reasons"))
                  .withColumn("run_id", F.lit(run_id))
                  .withColumn("quarantined_at", F.current_timestamp()))
    return valid, quarantine


def enrich_transactions(valid: DataFrame, accounts: DataFrame, run_id: str) -> DataFrame:
    acct = accounts.select(F.col("account_id").alias("source_account_id"), "customer_id", "account_type")
    inflow = F.col("transaction_type") == "DEPOSIT"
    return (valid.join(F.broadcast(acct), "source_account_id", "left")
            .withColumn("transaction_date", F.to_date("transaction_ts"))
            .withColumn("transaction_hour", F.hour("transaction_ts"))
            .withColumn("flow_direction", F.when(inflow, "INFLOW").otherwise("OUTFLOW"))
            .withColumn("signed_amount", F.when(inflow, F.col("amount")).otherwise(-F.col("amount")).cast(MONEY))
            .withColumn("digital_channel_flag", F.col("channel").isin(DIGITAL_CHANNELS))
            .withColumn("is_failed", F.col("transaction_status") == "FAILED")
            .withColumn("run_id", F.lit(run_id))
            .withColumn("year", F.year("transaction_ts"))
            .withColumn("month", F.month("transaction_ts"))
            .select("transaction_id", "transaction_ts", "transaction_date", "transaction_hour",
                    "source_account_id", "customer_id", "account_type", "counterparty_id",
                    "transaction_type", "flow_direction", "amount", "signed_amount", "channel",
                    "digital_channel_flag", "merchant_category", "branch_id", "transaction_status",
                    "is_failed", "balance_before", "balance_after", "is_fraud_flag",
                    "ingest_source", "source_file", "run_id", "year", "month"))


# --------------------------------------------------------------------------- dimensions
def standardize_accounts(raw: DataFrame, run_id: str):
    std = raw.select(
        clean_code("account_id").alias("account_id"),
        clean_code("customer_id").alias("customer_id"),
        clean_code("account_type").alias("account_type"),
        F.to_date(F.trim("open_date"), "yyyy-MM-dd").alias("open_date"),
        clean_code("account_status").alias("account_status"),
        F.trim("current_balance").cast(MONEY).alias("current_balance"),
    )
    known_ids = std.filter(F.col("account_id").isNotNull()).select("account_id")
    reason = (F.when(F.col("account_id").isNull(), "NULL_ACCOUNT_ID")
               .when(F.col("account_status").isNull() | ~F.col("account_status").isin(VALID_ACCOUNT_STATUSES),
                     "INVALID_ACCOUNT_STATUS"))
    std = std.withColumn("dq_fail_reason", reason)
    valid = std.filter(F.col("dq_fail_reason").isNull()).drop("dq_fail_reason")
    quarantine = (std.filter(F.col("dq_fail_reason").isNotNull())
                  .withColumn("run_id", F.lit(run_id))
                  .withColumn("quarantined_at", F.current_timestamp()))
    return valid, quarantine, known_ids, std.drop("dq_fail_reason")


def standardize_customers(raw: DataFrame) -> DataFrame:
    since = F.to_date(F.trim("customer_since"), "yyyy-MM-dd")
    return (raw.select(
        clean_code("customer_id").alias("customer_id"),
        since.alias("customer_since"),
        clean_code("state").alias("state"),
        clean_code("customer_segment").alias("customer_segment"),
        clean_code("risk_tier").alias("risk_tier"),
        (clean_code("digital_enrolled_flag") == "Y").alias("digital_enrolled_flag"),
        clean_code("home_branch_id").alias("home_branch_id"))
        .withColumn("tenure_years", F.round(F.months_between(F.current_date(), "customer_since") / 12, 1))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"]))


def standardize_branches(raw: DataFrame) -> DataFrame:
    return raw.select(clean_code("branch_id").alias("branch_id"), clean_text("branch_name").alias("branch_name"),
                      clean_text("city").alias("city"), clean_code("state").alias("state"),
                      clean_code("region").alias("region")).filter(F.col("branch_id").isNotNull())


def quarantine_reason_counts(quarantine: DataFrame, run_id: str, run_ts) -> DataFrame:
    return (quarantine.select(F.explode(F.split("dq_fail_reasons", ",")).alias("reason"), "ingest_source")
            .groupBy("reason", "ingest_source").count().withColumnRenamed("count", "row_count")
            .withColumn("run_id", F.lit(run_id)).withColumn("run_ts", F.lit(run_ts).cast("timestamp")))


# --------------------------------------------------------------------------- Glue entrypoint
def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from awsgluedq.transforms import EvaluateDataQuality
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "PREFIX", "MAX_QUARANTINE_RATE", "DQ_RULESET_S3"])
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)
    configure_spark(spark)
    log = glue_ctx.get_logger()

    base = f"s3://{args['BUCKET']}/{args['PREFIX']}"
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = run_ts.strftime("%Y%m%dT%H%M%SZ")
    max_q_rate = float(args["MAX_QUARANTINE_RATE"])

    bucket, key = args["DQ_RULESET_S3"].replace("s3://", "").split("/", 1)
    ruleset = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

    def evaluate_dq(df: DataFrame, context: str) -> DataFrame:
        dyf = DynamicFrame.fromDF(df, glue_ctx, context)
        outcomes = EvaluateDataQuality.apply(
            frame=dyf, ruleset=ruleset,
            publishing_options={
                "dataQualityEvaluationContext": context,
                "enableDataQualityCloudWatchMetrics": True,
                "enableDataQualityResultsPublishing": True,
            },
        ).toDF()
        cols = {c.lower(): c for c in outcomes.columns}
        out = outcomes.select(
            F.lit(run_id).alias("run_id"), F.lit(run_ts).cast("timestamp").alias("run_ts"),
            F.lit(context).alias("dataset"),
            F.col(cols["rule"]).alias("rule"), F.col(cols["outcome"]).alias("outcome"),
            F.col(cols["failurereason"]).alias("failure_reason") if "failurereason" in cols else F.lit(None).cast("string").alias("failure_reason"),
            F.to_json(F.col(cols["evaluatedmetrics"])).alias("evaluated_metrics") if "evaluatedmetrics" in cols else F.lit(None).cast("string").alias("evaluated_metrics"),
        )
        return out

    def dq_score(outcomes: DataFrame) -> float:
        r = outcomes.agg(F.avg(F.when(F.col("outcome") == "Passed", 1.0).otherwise(0.0)).alias("s")).collect()[0]["s"]
        return round(float(r or 0.0), 4)

    # ---- dimensions
    acct_valid, acct_quarantine, known_ids, acct_all = standardize_accounts(
        read_raw_csv(spark, f"{base}/raw/accounts/", RAW_ACCOUNT_COLUMNS), run_id)
    customers = standardize_customers(read_raw_csv(spark, f"{base}/raw/customers/", RAW_CUSTOMER_COLUMNS))
    branches = standardize_branches(read_raw_csv(spark, f"{base}/raw/branches/", RAW_BRANCH_COLUMNS))
    known_ids = known_ids.cache()
    acct_all = acct_all.cache()

    # ---- transactions
    std = standardize_transactions(read_raw_csv(spark, f"{base}/raw/transactions/", RAW_TXN_COLUMNS)).persist()
    flagged = flag_transactions(std, known_ids).persist()
    valid, quarantine = split_valid_quarantine(flagged, run_id)
    raw_rows = flagged.count()
    quarantined_rows = quarantine.count()
    curated_rows = raw_rows - quarantined_rows
    q_rate = round(quarantined_rows / raw_rows, 6) if raw_rows else 0.0
    log.info(f"run_id={run_id} raw={raw_rows} quarantined={quarantined_rows} rate={q_rate}")

    # ---- Glue Data Quality on the standardised raw data (expected < 100%)
    dq_cols = ["transaction_id", "transaction_ts", "source_account_id", "transaction_type",
               "amount", "channel", "transaction_status"]
    raw_outcomes = evaluate_dq(std.select(*dq_cols), "raw_transactions").cache()
    raw_score = dq_score(raw_outcomes)

    # ---- quarantine + DQ history (always written, even when the gate blocks)
    quarantine.coalesce(4).write.mode("overwrite").parquet(f"{base}/quarantine/quarantine_transactions/")
    acct_quarantine.coalesce(1).write.mode("overwrite").parquet(f"{base}/quarantine/quarantine_accounts/")
    quarantine_reason_counts(quarantine, run_id, run_ts).coalesce(1).write.mode("append") \
        .parquet(f"{base}/dq-results/dq_quarantine_reasons/")

    gate_passed = q_rate <= max_q_rate
    curated_score = None
    if gate_passed:
        fact = enrich_transactions(valid, acct_all, run_id).persist()
        (fact.repartition("year", "month").write.mode("overwrite")
             .partitionBy("year", "month").parquet(f"{base}/curated/fact_transactions/"))
        acct_valid.coalesce(1).write.mode("overwrite").parquet(f"{base}/curated/dim_account/")
        customers.coalesce(1).write.mode("overwrite").parquet(f"{base}/curated/dim_customer/")
        branches.coalesce(1).write.mode("overwrite").parquet(f"{base}/curated/dim_branch/")
        curated_outcomes = evaluate_dq(fact.select(*dq_cols), "curated_transactions")
        curated_score = dq_score(curated_outcomes)
        raw_outcomes = raw_outcomes.unionByName(curated_outcomes)

    raw_outcomes.coalesce(1).write.mode("append").parquet(f"{base}/dq-results/dq_rule_outcomes/")

    summary = spark.createDataFrame([(
        run_id, run_ts, int(raw_rows), int(curated_rows), int(quarantined_rows), float(q_rate),
        float(max_q_rate), float(raw_score), float(curated_score) if curated_score is not None else None,
        int(acct_quarantine.count()), "PUBLISHED" if gate_passed else "BLOCKED_BY_QUALITY_GATE",
    )], "run_id string, run_ts timestamp, raw_rows long, curated_rows long, quarantined_rows long, "
       "quarantine_rate double, max_quarantine_rate double, raw_dq_score double, curated_dq_score double, "
       "accounts_quarantined long, pipeline_status string")
    summary.coalesce(1).write.mode("append").parquet(f"{base}/dq-results/dq_run_summary/")

    if not gate_passed:
        raise RuntimeError(f"Quality gate failed: quarantine rate {q_rate:.4%} > {max_q_rate:.2%}. "
                           f"Curated layer NOT refreshed (run_id={run_id}).")
    job.commit()


if __name__ == "__main__":
    main()
