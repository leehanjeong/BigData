"""
Spark Triangle Count - Degree Optimized
=======================================

Algorithm:
1. Normalize edges: (u, v) where u < v
2. Calculate degree for each vertex
3. Reorient edges: low-degree -> high-degree
4. Generate wedges: Self-Join (much fewer wedges!)
5. Find triangles: Check if edge exists

Optimization Effect: Reduces wedge count by 50-80%

Usage:
  spark-submit spark_degree_opt.py wiki
  spark-submit spark_degree_opt.py /full/hdfs/path/to/data.txt
  
Options:
  --wedge-only: Stop after counting wedges (don't find triangles)
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
    .appName("TriangleCount-DegreeOpt") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Triangle Count - Degree Optimized")
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
# Degree Optimized Algorithm
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

# Step 2: Calculate degree for each vertex
print("\n[Step 2] Calculating degrees...")
degree_u = edges_normalized.groupBy("u").count() \
    .withColumnRenamed("u", "node").withColumnRenamed("count", "deg")
degree_v = edges_normalized.groupBy("v").count() \
    .withColumnRenamed("v", "node").withColumnRenamed("count", "deg")

degrees = degree_u.union(degree_v) \
    .groupBy("node").agg(F.sum("deg").alias("degree"))
degrees.cache()

max_degree = degrees.agg(F.max("degree")).collect()[0][0]
avg_degree = degrees.agg(F.avg("degree")).collect()[0][0]
print(f"Max degree: {max_degree:,}, Avg degree: {avg_degree:.2f}")

# Step 3: Reorient edges (low-degree -> high-degree)
print("\n[Step 3] Reorienting edges...")
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

# Verify max out-degree after reorientation
out_degrees = edges_oriented.groupBy("u").count()
max_out_degree = out_degrees.agg(F.max("count")).collect()[0][0]
theoretical_max = 2 * math.sqrt(edge_count)
print(f"Max out-degree after reorientation: {max_out_degree:,} (theoretical max: {theoretical_max:.0f})")

# Step 4: Generate Wedges (Self-Join)
print("\n[Step 4] Generating wedges...")
edges_a = edges_oriented.alias("a")
edges_b = edges_oriented.alias("b")

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
wedge_size_gb = wedge_count * 24 / (1024**3)
print(f"Estimated wedge size: {wedge_size_gb:.2f} GB")

if WEDGE_ONLY:
    elapsed = time.time() - start_time
    print(f"\n[Wedge Only Mode] Stopping here.")
    print(f"Time elapsed: {elapsed:.2f}s")
    
    edges_normalized.unpersist()
    edges_oriented.unpersist()
    degrees.unpersist()
    wedges.unpersist()
    spark.stop()
    sys.exit(0)

# Step 5: Find Triangles
print("\n[Step 5] Finding triangles...")
triangles = wedges.join(
    edges_normalized,
    (F.least(wedges.v1, wedges.v2) == edges_normalized.u) & 
    (F.greatest(wedges.v1, wedges.v2) == edges_normalized.v),
    "inner"
).select("v1", "v2", "center")

triangle_count = triangles.count()
elapsed = time.time() - start_time

print(f"\n" + "=" * 60)
print(f"Results - Degree Optimized")
print(f"=" * 60)
print(f"Edges:     {edge_count:,}")
print(f"Wedges:    {wedge_count:,}")
print(f"Triangles: {triangle_count:,}")
print(f"Time:      {elapsed:.2f}s")
print(f"=" * 60)

edges_normalized.unpersist()
edges_oriented.unpersist()
degrees.unpersist()
wedges.unpersist()
spark.stop()
