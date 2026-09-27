"""End-to-end local test: generate data -> inject known defects -> run the Glue
transformation functions on local Spark -> quarantine must equal the injected defects.

Runs in CI (GitHub Actions) without AWS: the awsglue imports live inside main().
"""
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F  # noqa: E402


def load_etl():
    spec = importlib.util.spec_from_file_location("etl", ROOT / "glue" / "retail_bank_etl.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("etl-tests")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session")
def data_dir(tmp_path_factory):
    out = tmp_path_factory.mktemp("data")
    env = {**os.environ, "DATA_OUTPUT_DIR": str(out)}
    gen = ROOT / "data_generator"

    def run(*args):
        subprocess.run([sys.executable, *map(str, args)], check=True, env=env, cwd=ROOT)

    run(gen / "make_sample_paysim.py", "--rows", "60000", "--out", out / "paysim.csv")
    run(gen / "generate_dimensions.py", "--customers", "2000", "--accounts", "3000")
    run(gen / "prepare_transactions.py", "--input", out / "paysim.csv", "--chunk-size", "25000",
        "--stream-holdout", "1000")
    run(gen / "inject_dq_issues.py")
    return out


def test_quarantine_matches_injected_defects(spark, data_dir):
    etl = load_etl()
    etl.configure_spark(spark)
    raw = data_dir / "raw"
    manifest = json.loads((data_dir / "injected_defects.json").read_text())

    _, acct_q, known_ids, acct_all = etl.standardize_accounts(
        etl.read_raw_csv(spark, str(raw / "accounts"), etl.RAW_ACCOUNT_COLUMNS), "test")
    std = etl.standardize_transactions(etl.read_raw_csv(spark, str(raw / "transactions"), etl.RAW_TXN_COLUMNS))
    flagged = etl.flag_transactions(std, known_ids).cache()
    valid, quarantine = etl.split_valid_quarantine(flagged, "test")

    assert flagged.count() == manifest["raw_rows"]
    assert quarantine.count() == manifest["expected_quarantined_transactions"]
    assert acct_q.count() == manifest["account_status_defects"]

    reasons = {r["reason"]: r["row_count"] for r in
               etl.quarantine_reason_counts(quarantine, "test", None).groupBy("reason")
               .agg(F.sum("row_count").alias("row_count")).collect()}
    for code, n in manifest["transaction_defects"].items():
        if n:
            assert reasons.get(code) == n, (code, reasons)

    fact = etl.enrich_transactions(valid, acct_all, "test")
    assert fact.count() == manifest["raw_rows"] - manifest["expected_quarantined_transactions"]
    # uniqueness + standardisation guarantees on the curated output
    assert fact.select("transaction_id").distinct().count() == fact.count()
    assert fact.filter(~F.col("channel").isin(etl.VALID_CHANNELS)).count() == 0
    assert fact.filter(~F.col("transaction_type").isin(etl.VALID_TYPES)).count() == 0
    assert fact.filter(F.col("customer_id").isNull()).count() == 0
