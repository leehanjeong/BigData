package trianglecount;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * BloomFilterDriver - Triangle Count with Bloom Filter Optimization
 * 
 * Algorithm: 6 steps (DegreeOpt + Bloom Filter pre-filtering)
 * - Step 0: Build Bloom Filter from all edges (NEW!)
 * - Step 1: Normalize edges (same as DegreeOpt)
 * - Step 2: Calculate degrees (same as DegreeOpt)
 * - Step 3: Reorient edges (same as DegreeOpt)
 * - Step 4: Generate wedges WITH Bloom Filter check (KEY DIFFERENCE!)
 * - Step 5: Find triangles (same as DegreeOpt)
 * 
 * Bloom Filter Benefit:
 * - During wedge generation, check if closing edge might exist
 * - If Bloom Filter says NO → skip wedge (no triangle possible)
 * - Reduces wedge count significantly → less I/O, faster processing
 * 
 * Usage: hadoop jar triangle_count.jar trianglecount.BloomFilterDriver <input>
 */
public class BloomFilterDriver extends Configured implements Tool {
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BloomFilterDriver(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputPath = args[0]; 
		String bloomFilterPath = inputPath + ".bloomfilter";
		String normalizedPath = inputPath + ".normalized";
		String degreePath = inputPath + ".degree";
		String reorientedPath = inputPath + ".reoriented";
		String wedgePath = inputPath + ".wedges_bloom";
		String outputPath = inputPath + ".out_bloom";
		
		// Step 0: Build Bloom Filter (NEW!)
		System.out.println("=== Step 0: Building Bloom Filter ===");
		BloomFilterBuilder.buildBloomFilter(getConf(), inputPath, bloomFilterPath);
		
		// Step 1-3: Same as DegreeOpt
		System.out.println("=== Step 1: Normalize Edges ===");
		runStep1_Normalize(inputPath, normalizedPath);
		
		System.out.println("=== Step 2: Calculate Degrees ===");
		runStep2_CalculateDegree(normalizedPath, degreePath);
		
		System.out.println("=== Step 3: Reorient Edges ===");
		runStep3_ReorientEdges(degreePath, reorientedPath);
		
		// Step 4: Generate wedges WITH Bloom Filter (KEY DIFFERENCE!)
		System.out.println("=== Step 4: Generate Wedges with Bloom Filter ===");
		runStep4_GenerateWedgesWithBloom(reorientedPath, wedgePath, bloomFilterPath);
		
		// Step 5: Find triangles (same as DegreeOpt)
		System.out.println("=== Step 5: Find Triangles ===");
		runStep5_FindTriangles(inputPath, wedgePath, outputPath);
		
		return 0;
	}
	
	/**
	 * Step 1: Normalize edges
	 */
	private void runStep1_Normalize(String inputPath, String outputPath) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Bloom-Step1-Normalize");
		job.setJarByClass(BloomFilterDriver.class);
		
		job.setMapperClass(NormalizeMapper.class);
		job.setReducerClass(NormalizeReducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(IntPairIntPartitioner.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
	
	/**
	 * Step 2: Calculate degrees
	 */
	private void runStep2_CalculateDegree(String inputPath, String outputPath) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Bloom-Step2-CalculateDegree");
		job.setJarByClass(BloomFilterDriver.class);
		
		job.setMapperClass(DegreeMapper.class);
		job.setReducerClass(DegreeReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntPairWritable.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntPairWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
	
	/**
	 * Step 3: Reorient edges
	 */
	private void runStep3_ReorientEdges(String inputPath, String outputPath) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Bloom-Step3-ReorientEdges");
		job.setJarByClass(BloomFilterDriver.class);
		
		job.setMapperClass(ReorientMapper.class);
		job.setReducerClass(ReorientReducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntPairWritable.class);
		job.setPartitionerClass(IntPairIntPairPartitioner.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
	
	/**
	 * Step 4: Generate wedges WITH Bloom Filter pre-filtering
	 * 
	 * KEY DIFFERENCE from DegreeOpt:
	 * - Uses WedgeBloomReducer instead of WedgeReducer
	 * - Bloom Filter is loaded via Distributed Cache
	 * - Wedges are only emitted if closing edge might exist
	 */
	private void runStep4_GenerateWedgesWithBloom(String inputPath, String outputPath, 
			String bloomFilterPath) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Bloom-Step4-GenerateWedgesWithBloom");
		job.setJarByClass(BloomFilterDriver.class);
		
		// Add Bloom Filter to Distributed Cache
		job.addCacheFile(new URI(bloomFilterPath));
		
		// Use same mapper as DegreeOpt
		job.setMapperClass(WedgeSeqMapper.class);
		// Use Bloom Filter reducer (KEY DIFFERENCE!)
		job.setReducerClass(WedgeBloomReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}

	/**
	 * Step 5: Find triangles (identical to DegreeOpt)
	 */
	private void runStep5_FindTriangles(String inputPath, String wedgePath, String outputPath) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("Bloom-Step5-FindTriangles");
		job.setJarByClass(BloomFilterDriver.class);
		
		// Use same reducer as DegreeOpt
		job.setReducerClass(TriangleReducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(IntPairIntPartitioner.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, EdgeMarkerMapper.class);
		MultipleInputs.addInputPath(job, new Path(wedgePath), SequenceFileInputFormat.class, WedgeMarkerMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
}
