package trianglecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Task3_TriangleCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Task3_TriangleCount(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputpath = args[0];
		String tmppath = inputpath + ".tmp";
		String outpath = inputpath + ".out";
		
		runStep1(inputpath, tmppath);
		runStep2(inputpath, tmppath, outpath);
		
		return 0;
	}
	
	private void runStep1(String inputpath, String tmppath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task3_TriangleCount.class);
		
		job.setMapperClass(TriangleCountMapper1.class);
		job.setReducerClass(TriangleCountReducer1.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath));
		
		job.waitForCompletion(true);
		
	}

	private void runStep2(String inputpath, String tmppath, String outpath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task3_TriangleCount.class);
		
		job.setReducerClass(TriangleCountReducer2.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(IntPairIntPartitioner.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(inputpath), TextInputFormat.class, TriangleCountMapperForEdges.class);
		MultipleInputs.addInputPath(job, new Path(tmppath), SequenceFileInputFormat.class, TriangleCountMapperForWedges.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.waitForCompletion(true);
		
	}

	

}
