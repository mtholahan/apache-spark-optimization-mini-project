#!/usr/bin/env python3
# optimize.py
#
# Run either the baseline (inefficient) Spark SQL query,
# or the optimized query, or run both back-to-back in "compare" mode.
# Each run saves an EXPLAIN plan, times the action, and (in baseline/optimized
# modes) fetches shuffle metrics from the Spark History Server (port 18080)
# using the event logs written to /tmp/spark-events.

import csv, sys, os, time, logging, contextlib, atexit, pathlib, argparse, json
from pathlib import Path
from datetime import datetime
from urllib.request import urlopen  # used for History Server REST calls

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ===================== CONFIGURATION =====================
LOG_FILE_NAME = "benchmark_log.csv"  # CSV where results get appended

# Create folders to store explain plans and logs (safe to call every run)
PLANS_DIR = pathlib.Path("plans"); PLANS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR  = pathlib.Path("logs");  LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Console + file logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler(LOGS_DIR / "run.log", mode="a", encoding="utf-8")],
)

# ===================== HELPERS =====================
def fetch_history_metrics(app_id, history_host="http://localhost:18080"):
    """
    Look up per-application totals from the Spark History Server.
    NOTE: This sums all stages for the *entire app*. In compare mode we run
    two queries inside one app, so the totals would mix them. Therefore we
    only call this in baseline/optimized modes.
    Returns: (shuffle_read_MB, shuffle_write_MB, total_duration_ms)
    """
    with urlopen(f"{history_host}/api/v1/applications/{app_id}/stages") as r:
        stages = json.load(r)
    srd = sum(s.get("shuffleReadBytes", 0)  for s in stages)
    swr = sum(s.get("shuffleWriteBytes", 0) for s in stages)
    dur = sum((s.get("completionTimeEpoch", 0) or 0) -
              (s.get("submissionTimeEpoch", 0) or 0) for s in stages)
    return round(srd / (1024*1024), 3), round(swr / (1024*1024), 3), int(dur)

def save_plan(df, filename, mode="formatted"):
    """Write df.explain(mode) to plans/<filename>."""
    path = PLANS_DIR / filename
    with open(path, "w", encoding="utf-8") as f, contextlib.redirect_stdout(f):
        try:
            df.explain(mode)  # Spark 3
        except TypeError:
            df.explain(True)  # fallback

def time_action(label, action):
    """Run a zero-arg callable, time it, and return (elapsed_s, result)."""
    t0 = time.perf_counter()
    result = action()
    elapsed = time.perf_counter() - t0
    logging.info(f"{label}_elapsed_s={elapsed:.3f}")
    return elapsed, result

def print_selected_spark_confs(spark):
    """Log a few Spark confs so runs are comparable."""
    for k in [
        "spark.sql.shuffle.partitions",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.executor.memory",
        "spark.driver.memory",
        "spark.sql.adaptive.enabled",
    ]:
        logging.info(f"conf {k} = {spark.conf.get(k, '<unset>')}")

# Optional: keep live UI up at the end if KEEP_SPARK_UI_OPEN=1
def _pause_for_ui_if_requested():
    if os.environ.get("KEEP_SPARK_UI_OPEN", "0") == "1":
        try:
            input("Press Enter to end (UI at http://localhost:4040)…")
        except EOFError:
            pass
atexit.register(_pause_for_ui_if_requested)

def run_query(mode, answersDF, questionsDF):
    """
    Run baseline or optimized.
    - baseline: NO manual pruning (matches original behavior)
    - optimized: explicit column pruning before heavy ops
    """
    if mode == "baseline":
        # BAD: join first, then aggregate (intentionally unchanged)
        df = (questionsDF.alias("q")
              .join(answersDF.alias("a"), "question_id", "left")
              .withColumn("month", F.date_trunc("month", F.col("a.creation_date")))
              .groupBy("question_id", "month")
              .agg(F.count("*").alias("cnt")))
        save_plan(df, "before_formatted.txt", "formatted")

    else:  # optimized
        # 👇 explicit column pruning only here
        a_small = answersDF.select("question_id", "creation_date")
        q_small = questionsDF.select("question_id", "title")

        # GOOD: aggregate first, then join (plus broadcast)
        answers_monthly = (a_small
                           .withColumn("month", F.date_trunc("month", F.col("creation_date")))
                           .groupBy("question_id", "month")
                           .agg(F.count("*").alias("cnt")))

        df = (F.broadcast(q_small)
              .join(answers_monthly, "question_id", "left")
              .select("question_id", "month", "cnt", "title"))

        save_plan(df, "after_formatted.txt", "formatted")

    elapsed_s, rows = time_action(mode, lambda: df.count())
    return mode, elapsed_s, rows


# ===================== ARGUMENTS =====================
parser = argparse.ArgumentParser()
parser.add_argument("--mode", choices=["baseline", "optimized", "compare"], required=True,
                    help="Run a single query or both back-to-back in one session.")
args = parser.parse_args()

# ===================== SPARK SESSION =====================
spark = (SparkSession.builder
         .appName(f"SOMiniProject-{args.mode}")
         .config("spark.ui.enabled", "true")
         .config("spark.ui.port", "4040")
         # local-friendly defaults; AQE can further coalesce shuffles
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB
         .getOrCreate())

print(f"Spark version: {spark.version}")
print(f"Spark master:  {spark.sparkContext.master}")
print("Spark UI:      ", spark.sparkContext.uiWebUrl)
print_selected_spark_confs(spark)

# ===================== LOAD DATA =====================
here = Path(__file__).resolve().parent
answers_path   = here / "data" / "answers"
questions_path = here / "data" / "questions"
if not answers_path.exists():   raise FileNotFoundError(answers_path)
if not questions_path.exists(): raise FileNotFoundError(questions_path)

answersDF   = spark.read.parquet(str(answers_path)).select("question_id", "creation_date")
questionsDF = spark.read.parquet(str(questions_path)).select("question_id", "title")

# ===================== RUN =====================
if args.mode in ("baseline", "optimized"):
    # Run a single query; we can attribute History Server metrics to this app run.
    label, elapsed_s, rows = run_query(args.mode, answersDF, questionsDF)

    # Capture app id BEFORE stopping Spark
    app_id = spark.sparkContext.applicationId
    spark.stop()

    # Fetch per-application metrics from History Server (may take a moment to index)
    srd_mb = swr_mb = dur_ms = None
    for _ in range(20):
        try:
            srd_mb, swr_mb, dur_ms = fetch_history_metrics(app_id)
            break
        except Exception:
            time.sleep(0.5)
    if srd_mb is None:
        logging.warning("History Server metrics not yet available.")
        srd_mb = swr_mb = dur_ms = -1

    # Append one row with timing + shuffle metrics
    log_path = here / LOG_FILE_NAME
    new_file = not log_path.exists()
    with log_path.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp","mode","spark_app_id","spark_master",
                        "elapsed_s","shuffle_read_mb","shuffle_write_mb","duration_ms","rows"])
        w.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    label, app_id, "<stopped>",
                    round(elapsed_s, 3), srd_mb, swr_mb, dur_ms, int(rows)])
    print(f"Benchmark logged to: {log_path}")

else:
    # compare mode: run BOTH queries in one Spark app to remove session startup noise
    b_label, b_secs, b_rows = run_query("baseline",  answersDF, questionsDF)
    o_label, o_secs, o_rows = run_query("optimized", answersDF, questionsDF)

    # In one app the History Server will SUM both jobs; that's not useful per-query.
    # So in compare mode we *only* log the timings and row counts.
    log_path = here / LOG_FILE_NAME
    new_file = not log_path.exists()
    with log_path.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp","mode","spark_master",
                        "baseline_secs","optimized_secs","baseline_rows","optimized_rows"])
        w.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "compare", spark.sparkContext.master,
                    round(b_secs, 3), round(o_secs, 3), int(b_rows), int(o_rows)])
    print(f"Compare results logged to: {log_path}")

    # Optional pause for UI if KEEP_SPARK_UI_OPEN=1
    # (atexit handler above will read the env var right before exit)
    app_id = spark.sparkContext.applicationId
    spark.stop()
