package trianglecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * DegreeOptDriver - Degree-Optimized Triangle Count (Task2 reorientation + triangle count)
 * 
 * Course Requirements:
 * - Task 1: Normalize edges (u < v) + remove duplicates/loops
 * - Task 2: Reorient edges by degree (low-degree -> high-degree)
 * - Task 3: Triangle counting
 * 
 * Algorithm: 5 steps
 * - Step 1: Normalize (NormalizeMapper -> NormalizeReducer)
 * - Step 2: Calculate Degree (DegreeMapper -> DegreeReducer)
 * - Step 3: Reorient Edges (ReorientMapper -> ReorientReducer)
 * - Step 4: Generate Wedges (WedgeSeqMapper -> WedgeReducer)
 * - Step 5: Find Triangles (EdgeMarkerMapper + WedgeMarkerMapper -> TriangleReducer)
 * 
 * Optimization Effect: Reduces wedge count by 50-80%
 * 
 * Usage: hadoop jar triangle_count.jar trianglecount.DegreeOptDriver <input>
 */
public class DegreeOptDriver extends Configured implements Tool {
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DegreeOptDriver(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputPath = args[0]; 
		String normalizedPath = inputPath + ".normalized";   // Step 1 output
		String degreePath = inputPath + ".degree";           // Step 2 output
		String reorientedPath = inputPath + ".reoriented";   // Step 3 output
		String wedgePath = inputPath + ".wedges_opt";        // Step 4 output
		String outputPath = inputPath + ".out_degree";       // Final output
		
		runStep1_Normalize(inputPath, normalizedPath);
		runStep2_CalculateDegree(normalizedPath, degreePath);
		runStep3_ReorientEdges(degreePath, reorientedPath);
		runStep4_GenerateWedges(reorientedPath, wedgePath);
		runStep5_FindTriangles(inputPath, wedgePath, outputPath);
		
		return 0;
	}
	
	/**
	 * Step 1: Normalize edges (NormalizeMapper -> NormalizeReducer)
	 */
	private void runStep1_Normalize(String inputPath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("DegreeOpt-Step1-Normalize");
		job.setJarByClass(DegreeOptDriver.class);
		
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
	 * Step 2: Calculate degree for each vertex (DegreeMapper -> DegreeReducer)
	 */
	private void runStep2_CalculateDegree(String inputPath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("DegreeOpt-Step2-CalculateDegree");
		job.setJarByClass(DegreeOptDriver.class);
		
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
	 * Step 3: Reorient edges from low-degree to high-degree (ReorientMapper -> ReorientReducer)
	 */
	private void runStep3_ReorientEdges(String inputPath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("DegreeOpt-Step3-ReorientEdges");
		job.setJarByClass(DegreeOptDriver.class);
		
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
	 * Step 4: Generate wedges from reoriented edges (WedgeSeqMapper -> WedgeReducer)
	 */
	private void runStep4_GenerateWedges(String inputPath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("DegreeOpt-Step4-GenerateWedges");
		job.setJarByClass(DegreeOptDriver.class);
		
		job.setMapperClass(WedgeSeqMapper.class);
		job.setReducerClass(WedgeReducer.class);
		
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
	 * Step 5: Find triangles (original edges + wedges -> TriangleReducer)
	 */
	private void runStep5_FindTriangles(String inputPath, String wedgePath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("DegreeOpt-Step5-FindTriangles");
		job.setJarByClass(DegreeOptDriver.class);
		
		job.setReducerClass(TriangleReducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(IntPairIntPartitioner.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(inputPath), TextInputFormat.class, EdgeMarkerMapper.class);
		MultipleInputs.addInputPath(job, new Path(wedgePath), SequenceFileInputFormat.class, WedgeMarkerMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
}
