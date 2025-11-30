# Triangle Count - Hadoop MapReduce

MapReduce implementation for counting triangles in large-scale graphs with optimizations.

## 🚀 Usage

```bash
# Baseline - Wedge count only (formula-based, avoids disk overflow)
hadoop jar triangle_count.jar trianglecount.WedgeCountDriver <input>

# Degree Optimized - 5 steps
hadoop jar triangle_count.jar trianglecount.DegreeOptDriver <input>

# Bloom Filter Optimized - 6 steps (fastest)
hadoop jar triangle_count.jar trianglecount.BloomFilterDriver <input>
```

## 📊 Performance (Wiki-topcats: 25M edges)

| Algorithm | Wedges | Triangles | Time |
|-----------|--------|-----------|------|
| Baseline | 74,635,128,954 | - | ❌ Disk Overflow |
| Degree Opt | 315,738,168 | 52,106,893 | 30m 54s |
| **Bloom Filter** | **53,061,724** | 52,106,893 | **23m 49s** |

- Degree Optimization: 99.6% wedge reduction
- Bloom Filter: Additional 83% reduction

## 🔄 Algorithm Pipeline

### DegreeOptDriver (5 steps)
```
Raw → Normalize → Degree Calc → Reorient → Wedges → Triangles
       Step1       Step2         Step3      Step4     Step5
```

### BloomFilterDriver (6 steps)
```
Raw → BloomFilter → Normalize → Degree → Reorient → Wedges+BF → Triangles
        Step1        Step2       Step3    Step4       Step5       Step6
```

## 📁 Key Files

```
src/main/java/trianglecount/
├── DegreeOptDriver.java      # Degree optimization driver
├── BloomFilterDriver.java    # Bloom Filter optimization driver
├── WedgeCountDriver.java     # Wedge count (formula-based)
│
├── NormalizeMapper/Reducer   # Edge normalization (u < v)
├── DegreeMapper/Reducer      # Degree calculation
├── ReorientMapper/Reducer    # Edge reorientation (low→high)
├── WedgeReducer              # Wedge generation
├── WedgeBloomReducer         # Wedge generation with Bloom Filter
├── TriangleReducer           # Triangle verification
│
├── BloomFilterBuilder.java   # Bloom Filter utility
└── IntPairWritable.java      # Custom Writable
```

## 🧮 Optimization Principles

### 1. Degree Optimization
- Reorient edges from **low-degree → high-degree** vertices
- Max out-degree ≤ 2√m (m = number of edges)
- Dramatically reduces wedge count

### 2. Bloom Filter
- Store all edges in Bloom Filter
- Pre-filter wedges by checking closing edge existence
- 1% false positive rate → removes most unnecessary wedges
