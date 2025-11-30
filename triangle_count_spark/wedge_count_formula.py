"""
Wedge Count Calculator - Using Degree Formula
==============================================

Mathematical Background:
- A wedge is a path of length 2: (v1) - (center) - (v2)
- For a vertex with degree d, it can be center of C(d,2) = d*(d-1)/2 wedges
- Total wedges = Σ C(degree_v, 2) for all vertices v

This calculates:
1. Baseline wedge count (original graph)
2. Degree-optimized wedge count (reoriented graph)

Usage:
  spark-submit wedge_count_formula.py wiki
  spark-submit wedge_count_formula.py /full/hdfs/path/to/data.txt
  
  Or locally with Python:
  python wedge_count_formula.py local datasets/wiki-topcats.txt
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType
import time
import sys
import math

# ============================================================
# Spark Session
# ============================================================
spark = SparkSession.builder \
    .appName("WedgeCountFormula") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Wedge Count Calculator (using Degree Formula)")
print("=" * 60)
print()
print("Formula: Total Wedges = Σ C(degree_v, 2) = Σ d*(d-1)/2")
print()

# ============================================================
# Parse Arguments
# ============================================================
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

# ============================================================
# Load Data
# ============================================================
schema = StructType([
    StructField("src", LongType(), True),
    StructField("dst", LongType(), True)
])

start_time = time.time()

edges_raw = spark.read.csv(DATA_PATH, schema=schema, header=False, sep=SEPARATOR)
raw_count = edges_raw.count()
print(f"\n[1] Raw edge count: {raw_count:,}")

# Remove self-loops
edges_no_loop = edges_raw.filter(F.col("src") != F.col("dst"))
print(f"[2] After removing self-loops: {edges_no_loop.count():,}")

# ============================================================
# Step 1: Normalize edges (u < v)
# ============================================================
print("\n[Step 1] Normalizing edges...")
edges_normalized = edges_no_loop.select(
    F.least(F.col("src"), F.col("dst")).alias("u"),
    F.greatest(F.col("src"), F.col("dst")).alias("v")
).distinct()

edges_normalized.cache()
edge_count = edges_normalized.count()
print(f"Normalized edge count: {edge_count:,}")

# ============================================================
# Step 2: Calculate Degree for each vertex
# ============================================================
print("\n[Step 2] Calculating degrees...")

# Count degree from both endpoints
degree_u = edges_normalized.groupBy("u").count() \
    .withColumnRenamed("u", "node").withColumnRenamed("count", "deg")
degree_v = edges_normalized.groupBy("v").count() \
    .withColumnRenamed("v", "node").withColumnRenamed("count", "deg")

degrees = degree_u.union(degree_v) \
    .groupBy("node").agg(F.sum("deg").alias("degree"))

degrees.cache()
vertex_count = degrees.count()

# ============================================================
# Step 3: Calculate BASELINE Wedge Count (Original Graph)
# ============================================================
print("\n[Step 3] Calculating BASELINE wedge count...")

# C(d, 2) = d * (d-1) / 2 for each vertex
baseline_stats = degrees.agg(
    F.count("*").alias("vertex_count"),
    F.sum("degree").alias("total_degree"),
    F.max("degree").alias("max_degree"),
    F.avg("degree").alias("avg_degree"),
    # Wedge count formula: Σ C(d, 2) = Σ d*(d-1)/2
    F.sum((F.col("degree") * (F.col("degree") - 1) / 2)).alias("total_wedges")
).collect()[0]

baseline_wedges = int(baseline_stats["total_wedges"])
max_degree = int(baseline_stats["max_degree"])
avg_degree = float(baseline_stats["avg_degree"])

print(f"Vertex count: {vertex_count:,}")
print(f"Max degree: {max_degree:,}")
print(f"Avg degree: {avg_degree:.2f}")
print(f"BASELINE Wedges: {baseline_wedges:,}")

# ============================================================
# Step 4: Reorient edges (low-degree -> high-degree)
# ============================================================
print("\n[Step 4] Reorienting edges (low-degree -> high-degree)...")

edges_with_deg = edges_normalized \
    .join(degrees.withColumnRenamed("node", "u").withColumnRenamed("degree", "deg_u"), "u") \
    .join(degrees.withColumnRenamed("node", "v").withColumnRenamed("degree", "deg_v"), "v")

edges_oriented = edges_with_deg.select(
    F.when(
        (F.col("deg_u") < F.col("deg_v")) | 
        ((F.col("deg_u") == F.col("deg_v")) & (F.col("u") < F.col("v"))),
        F.col("u")
    ).otherwise(F.col("v")).alias("u"),
    F.when(
        (F.col("deg_u") < F.col("deg_v")) | 
        ((F.col("deg_u") == F.col("deg_v")) & (F.col("u") < F.col("v"))),
        F.col("v")
    ).otherwise(F.col("u")).alias("v")
)

edges_oriented.cache()

# ============================================================
# Step 5: Calculate OUT-DEGREE after reorientation
# ============================================================
print("\n[Step 5] Calculating OUT-DEGREE after reorientation...")

out_degrees = edges_oriented.groupBy("u").count() \
    .withColumnRenamed("count", "out_degree")

out_degree_stats = out_degrees.agg(
    F.count("*").alias("vertex_with_out"),
    F.max("out_degree").alias("max_out_degree"),
    F.avg("out_degree").alias("avg_out_degree"),
    # Wedge count after degree optimization
    F.sum((F.col("out_degree") * (F.col("out_degree") - 1) / 2)).alias("degree_opt_wedges")
).collect()[0]

degree_opt_wedges = int(out_degree_stats["degree_opt_wedges"])
max_out_degree = int(out_degree_stats["max_out_degree"])
theoretical_max_out = 2 * math.sqrt(edge_count)

print(f"Max out-degree: {max_out_degree:,} (theoretical max: {theoretical_max_out:.0f})")
print(f"DEGREE-OPT Wedges: {degree_opt_wedges:,}")

# ============================================================
# Calculate Reduction Rate
# ============================================================
reduction_rate = (1 - degree_opt_wedges / baseline_wedges) * 100 if baseline_wedges > 0 else 0

elapsed = time.time() - start_time

# ============================================================
# Final Results
# ============================================================
print()
print("=" * 70)
print("RESULTS - Wedge Count Comparison")
print("=" * 70)
print(f"{'Metric':<30} {'Value':>20}")
print("-" * 70)
print(f"{'Edges':<30} {edge_count:>20,}")
print(f"{'Vertices':<30} {vertex_count:>20,}")
print(f"{'Max Degree (original)':<30} {max_degree:>20,}")
print(f"{'Max Out-Degree (reoriented)':<30} {max_out_degree:>20,}")
print("-" * 70)
print(f"{'BASELINE Wedges':<30} {baseline_wedges:>20,}")
print(f"{'DEGREE-OPT Wedges':<30} {degree_opt_wedges:>20,}")
print(f"{'Reduction':<30} {reduction_rate:>19.1f}%")
print("-" * 70)
print(f"{'Time':<30} {elapsed:>19.2f}s")
print("=" * 70)

print()
print("Note: Bloom Filter optimization further reduces wedges by filtering")
print("      those without closing edges (typically 50-90% additional reduction)")
print()

# Estimate Bloom Filter effect (rough approximation)
# Assuming ~3-5% of wedges form triangles (typical for social networks)
estimated_triangles_rate = 0.03  # 3% is typical
estimated_bloom_wedges = int(degree_opt_wedges * estimated_triangles_rate * 1.01)  # +1% for false positives

print(f"Estimated BLOOM-FILTER Wedges (assuming ~3% triangle rate): {estimated_bloom_wedges:,}")
print(f"Estimated additional reduction: {(1 - estimated_bloom_wedges / degree_opt_wedges) * 100:.1f}%")

# Cleanup
edges_normalized.unpersist()
degrees.unpersist()
edges_oriented.unpersist()
spark.stop()
