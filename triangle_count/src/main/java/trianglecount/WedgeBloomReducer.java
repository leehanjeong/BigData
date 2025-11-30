package trianglecount;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * WedgeBloomReducer - Generate wedges with Bloom Filter pre-filtering
 * 
 * For each center node v, receives all its neighbors [u1, u2, u3, ...]
 * Generates wedges (ui, uj) for all pairs where i < j
 * 
 * KEY DIFFERENCE from WedgeReducer:
 * - Before emitting wedge (ui, uj), check Bloom Filter for edge existence
 * - If Bloom Filter says edge (ui, uj) doesn't exist → skip (no triangle possible)
 * - This significantly reduces output wedges
 * 
 * Input:  (v, [u1, u2, u3, ...]) - center node and its neighbors
 * Output: ((ui, uj), v) - potential wedge with center v, only if edge might exist
 */
public class WedgeBloomReducer extends Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable> {
	
	IntPairWritable ok = new IntPairWritable();
	IntWritable ov = new IntWritable();
	
	private BloomFilter bloomFilter;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Load Bloom Filter from Distributed Cache
		bloomFilter = new BloomFilter(BloomFilterBuilder.VECTOR_SIZE, 
				BloomFilterBuilder.NB_HASH, Hash.MURMUR_HASH);
		
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles != null && cacheFiles.length > 0) {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path bloomPath = new Path(cacheFiles[0]);
			
			try (FSDataInputStream in = fs.open(bloomPath)) {
				bloomFilter.readFields(in);
			}
		}
	}
	
	/**
	 * Check if edge (u, v) might exist using Bloom Filter
	 * @return true if edge might exist (need to verify), false if definitely not
	 */
	private boolean mightEdgeExist(int u, int v) {
		// Normalize: smaller node first
		if (u > v) {
			int temp = u;
			u = v;
			v = temp;
		}
		String edgeKey = u + "," + v;
		return bloomFilter.membershipTest(new Key(edgeKey.getBytes()));
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		// Collect all neighbors
		List<Integer> neighbors = new ArrayList<>();
		for (IntWritable v : values) {
			neighbors.add(v.get());
		}
		
		int center = key.get();
		ov.set(center);
		
		// Generate wedges for all pairs
		for (int i = 0; i < neighbors.size(); i++) {
			for (int j = i + 1; j < neighbors.size(); j++) {
				int u = neighbors.get(i);
				int w = neighbors.get(j);
				
				// ★ KEY: Check Bloom Filter before emitting wedge
				// If edge (u, w) definitely doesn't exist, skip this wedge
				if (!mightEdgeExist(u, w)) {
					continue;  // No triangle possible, skip
				}
				
				// Edge might exist, emit wedge for verification
				if (u < w) {
					ok.set(u, w);
				} else {
					ok.set(w, u);
				}
				context.write(ok, ov);
			}
		}
	}
}
