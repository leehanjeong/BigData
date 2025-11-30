package trianglecount;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * BloomFilterBuilder - Build Bloom Filter from all edges
 * 
 * Purpose: Create a Bloom Filter containing all edges in the graph.
 *          This filter will be used to quickly check edge existence
 *          during wedge generation phase.
 * 
 * Output: Bloom Filter file saved to HDFS (distributed cache)
 */
public class BloomFilterBuilder {
	
	// Bloom Filter parameters
	// For 28M edges with 1% false positive rate:
	// vectorSize = -n * ln(p) / (ln(2)^2) ≈ 10 * n
	public static final int VECTOR_SIZE = 300000000;  // 300M bits = 37.5 MB
	public static final int NB_HASH = 7;  // optimal for 1% FP rate
	
	/**
	 * Mapper: Emit each edge as (u, v) where u < v
	 */
	public static class EdgeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private Text edgeKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String[] nodes = value.toString().trim().split("\\s+");
			if (nodes.length < 2) return;
			
			try {
				int u = Integer.parseInt(nodes[0]);
				int v = Integer.parseInt(nodes[1]);
				
				// Normalize: smaller node first
				if (u > v) {
					int temp = u;
					u = v;
					v = temp;
				}
				
				edgeKey.set(u + "," + v);
				context.write(edgeKey, NullWritable.get());
			} catch (NumberFormatException e) {
				// Skip invalid lines
			}
		}
	}
	
	/**
	 * Reducer: Collect all edges and build Bloom Filter
	 */
	public static class BloomFilterReducer extends Reducer<Text, NullWritable, NullWritable, NullWritable> {
		
		private BloomFilter bloomFilter;
		private String outputPath;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			bloomFilter = new BloomFilter(VECTOR_SIZE, NB_HASH, Hash.MURMUR_HASH);
			outputPath = context.getConfiguration().get("bloom.filter.output.path");
		}
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
			// Add edge to Bloom Filter
			bloomFilter.add(new Key(key.toString().getBytes()));
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Save Bloom Filter to HDFS
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path path = new Path(outputPath);
			
			try (FSDataOutputStream out = fs.create(path, true)) {
				bloomFilter.write(out);
			}
		}
	}
	
	/**
	 * Build Bloom Filter from input edges
	 */
	public static void buildBloomFilter(org.apache.hadoop.conf.Configuration conf, 
			String inputPath, String bloomFilterPath) throws Exception {
		
		Job job = Job.getInstance(conf);
		job.setJobName("BloomFilter-Step0-BuildFilter");
		job.setJarByClass(BloomFilterBuilder.class);
		
		job.setMapperClass(EdgeMapper.class);
		job.setReducerClass(BloomFilterReducer.class);
		job.setNumReduceTasks(1);  // Single reducer to build one filter
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.getConfiguration().set("bloom.filter.output.path", bloomFilterPath);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		job.waitForCompletion(true);
	}
}
