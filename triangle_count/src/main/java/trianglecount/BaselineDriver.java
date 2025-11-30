package trianglecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * BaselineDriver - Baseline Triangle Count (Task1 normalization + triangle count)
 * 
 * Course Requirements:
 * - Task 1: Normalize edges (u < v) + remove duplicates/loops
 * - Task 3: Triangle counting
 * 
 * Algorithm: 3 steps
 * - Step 1: Normalize (NormalizeMapper -> NormalizeReducer) <- Task 1
 * - Step 2: Wedge Generation (WedgeSeqMapper -> WedgeReducer)
 * - Step 3: Triangle Finding (normalized edges + wedges -> TriangleReducer)
 * 
 * Usage: hadoop jar triangle_count.jar trianglecount.BaselineDriver <input>
 */
public class BaselineDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BaselineDriver(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputPath = args[0];
		String normalizedPath = inputPath + ".normalized";  // Task1 result (normalized edges)
		String wedgePath = inputPath + ".wedges";           // Wedges
		String outputPath = inputPath + ".out";             // Final output
		
		runStep1_Normalize(inputPath, normalizedPath);
		runStep2_GenerateWedges(normalizedPath, wedgePath);
		runStep3_FindTriangles(normalizedPath, wedgePath, outputPath);
		
		return 0;
	}
	
	/**
	 * Step 1: Task 1 - Normalize edges (NormalizeMapper -> NormalizeReducer)
	 */
	private void runStep1_Normalize(String inputPath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("Baseline-Step1-Normalize");
		job.setJarByClass(BaselineDriver.class);
		
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
	 * Step 2: Generate wedges from normalized edges (SequenceFile input)
	 */
	private void runStep2_GenerateWedges(String inputPath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("Baseline-Step2-GenerateWedges");
		job.setJarByClass(BaselineDriver.class);
		
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
	 * Step 3: Find triangles (normalized edges + wedges)
	 */
	private void runStep3_FindTriangles(String normalizedPath, String wedgePath, String outputPath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJobName("Baseline-Step3-FindTriangles");
		job.setJarByClass(BaselineDriver.class);
		
		job.setReducerClass(TriangleReducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(IntPairIntPartitioner.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Normalized edges + Wedges (both SequenceFile)
		MultipleInputs.addInputPath(job, new Path(normalizedPath), SequenceFileInputFormat.class, EdgeMarkerSeqMapper.class);
		MultipleInputs.addInputPath(job, new Path(wedgePath), SequenceFileInputFormat.class, WedgeMarkerMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
	}
}
