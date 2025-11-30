# Triangle Count - Apache Spark

In-memory triangle counting using PySpark DataFrame API.

## 🚀 Usage

```bash
# Degree Optimized (main)
spark-submit spark_degree_opt.py wiki

# With stage-wise timing
spark-submit spark_degree_opt_timed.py wiki

# Wedge count only (formula-based)
spark-submit wedge_count_formula.py wiki
```

## 📊 Performance (Wiki-topcats: 25M edges)

| Framework | Algorithm | Wedges | Triangles | Time |
|-----------|-----------|--------|-----------|------|
| Hadoop | Degree Opt | 315M | 52,106,893 | 30m 54s |
| Hadoop | Bloom Filter | 53M | 52,106,893 | 23m 49s |
| **Spark** | **Degree Opt** | 315M | 52,106,893 | **5m 13s** |

**Spark is 5.9x faster than Hadoop** (in-memory vs disk-based)

## 📁 Files

```
├── spark_degree_opt.py        # Main: Degree optimization
└── wedge_count_formula.py     # Wedge count using degree formula
```

## 🔄 Algorithm Pipeline

```
Raw Data → Normalize → Degree Calc → Reorient → Wedges → Triangles
              │            │            │          │         │
            u < v      count per    low→high    self-join   join
                        vertex      degree
```

## 🧮 Key Optimization: Degree Reorientation

- Reorient edges from **low-degree → high-degree** vertices
- Max out-degree ≤ 2√m (m = number of edges)
- **99.6% wedge reduction** (74.6B → 315M)

## ⚠️ Note on Bloom Filter

Bloom Filter optimization was attempted but failed on large datasets due to:
- **PySpark lacks distributed Bloom Filter API** (`df.stat.bloomFilter()` exists only in Scala/Java)
- Custom implementation required `toLocalIterator()` → all data collected to Driver → OOM
- Hadoop succeeds because Bloom Filter is built in distributed Reducers (Java)
