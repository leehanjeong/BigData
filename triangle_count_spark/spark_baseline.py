"""
Spark Triangle Count - Baseline (Node ID ordering)
===================================================

Algorithm:
1. Normalize edges: (u, v) where u < v
2. Generate wedges: Self-Join to find (v1, v2) pairs sharing same u
3. Find triangles: Check if edge exists for each wedge (v1, v2)

Usage:
  spark-submit spark_baseline.py wiki
  spark-submit spark_baseline.py /full/hdfs/path/to/data.txt
  
Options:
  --wedge-only: Stop after counting wedges (don't find triangles)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType
import time
import sys

# ============================================================
# Spark Session
# ============================================================
spark = SparkSession.builder \
    .appName("TriangleCount-Baseline") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Triangle Count - Baseline")
print("=" * 60)

# ============================================================
# Parse Arguments
# ============================================================
WEDGE_ONLY = "--wedge-only" in sys.argv
args = [a for a in sys.argv[1:] if not a.startswith("--")]
DATASET = args[0] if args else "wiki"

# HDFS path
HDFS_USER = spark.sparkContext.sparkUser()
HDFS_BASE = f"/user/{HDFS_USER}"

if DATASET == "wiki":
    DATA_PATH = f"{HDFS_BASE}/data/wiki-topcats.txt"
    SEPARATOR = " "
elif DATASET == "paysim":
    DATA_PATH = f"{HDFS_BASE}/data/edges.csv"
    SEPARATOR = ","
elif DATASET.startswith("/"):
    DATA_PATH = DATASET
    SEPARATOR = " "
else:
    DATA_PATH = f"{HDFS_BASE}/data/{DATASET}"
    SEPARATOR = " "

print(f"Data path: {DATA_PATH}")
print(f"HDFS user: {HDFS_USER}")
print(f"Wedge only mode: {WEDGE_ONLY}")

# ============================================================
# Load Data
# ============================================================
schema = StructType([
    StructField("src", LongType(), True),
    StructField("dst", LongType(), True)
])

edges_raw = spark.read.csv(DATA_PATH, schema=schema, header=False, sep=SEPARATOR)
print(f"\n[1] Raw edge count: {edges_raw.count():,}")

# Remove self-loops
edges_no_loop = edges_raw.filter(F.col("src") != F.col("dst"))
print(f"[2] After removing self-loops: {edges_no_loop.count():,}")

# ============================================================
# Baseline Algorithm
# ============================================================
start_time = time.time()

# Step 1: Normalize edges (u < v)
print("\n[Step 1] Normalizing edges...")
edges_normalized = edges_no_loop.select(
    F.least(F.col("src"), F.col("dst")).alias("u"),
    F.greatest(F.col("src"), F.col("dst")).alias("v")
).distinct()

edges_normalized.cache()
edge_count = edges_normalized.count()
print(f"Normalized edge count: {edge_count:,}")

# Step 2: Generate Wedges (Self-Join)
print("\n[Step 2] Generating wedges...")
edges_a = edges_normalized.alias("a")
edges_b = edges_normalized.alias("b")

wedges = edges_a.join(
    edges_b,
    (F.col("a.u") == F.col("b.u")) & (F.col("a.v") < F.col("b.v")),
    "inner"
).select(
    F.col("a.v").alias("v1"),
    F.col("b.v").alias("v2"),
    F.col("a.u").alias("center")
)

wedges.cache()
wedge_count = wedges.count()
print(f"Generated wedge count: {wedge_count:,}")

# Estimate wedge data size
wedge_size_gb = wedge_count * 24 / (1024**3)  # 3 longs × 8 bytes
print(f"Estimated wedge size: {wedge_size_gb:.2f} GB")

if WEDGE_ONLY:
    elapsed = time.time() - start_time
    print(f"\n[Wedge Only Mode] Stopping here.")
    print(f"Time elapsed: {elapsed:.2f}s")
    
    edges_normalized.unpersist()
    wedges.unpersist()
    spark.stop()
    sys.exit(0)

# Step 3: Find Triangles
print("\n[Step 3] Finding triangles...")
triangles = wedges.join(
    edges_normalized,
    (wedges.v1 == edges_normalized.u) & (wedges.v2 == edges_normalized.v),
    "inner"
).select("v1", "v2", "center")

triangle_count = triangles.count()
elapsed = time.time() - start_time

print(f"\n" + "=" * 60)
print(f"Results - Baseline")
print(f"=" * 60)
print(f"Edges:     {edge_count:,}")
print(f"Wedges:    {wedge_count:,}")
print(f"Triangles: {triangle_count:,}")
print(f"Time:      {elapsed:.2f}s")
print(f"=" * 60)

edges_normalized.unpersist()
wedges.unpersist()
spark.stop()
