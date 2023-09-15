package trianglecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import trianglecount.IntPairWritable;


public class Task2_GraphMining2 extends Configured implements Tool {
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Task2_GraphMining2(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputpath = args[0]; 
		String tmppath1 = inputpath + "1.tmp"; 
		String tmppath2 = inputpath + "2.tmp"; 
		String outpath = inputpath + ".out"; 
		
		runStep1(inputpath, tmppath1);
		runStep2(tmppath1, tmppath2);
		runStep3(tmppath2, outpath);
		
		return 0;
	}
	
	private void runStep1(String inputpath, String tmppath1) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task2_GraphMining2.class);
		
		job.setMapperClass(GraphMiningMapper.class);
		job.setReducerClass(GraphMiningReducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(IntPairIntPartitioner.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); // 출력 SequenceFile
		
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(tmppath1));
		
		job.waitForCompletion(true);
		
	}
	
	private void runStep2(String tmppath1, String tmppath2) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task2_GraphMining2.class);
		
		job.setMapperClass(GraphMiningMapper2.class);
		job.setReducerClass(GraphMiningReducer2.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntPairWritable.class);
		
		// job.setPartitionerClass(TriCntPartitioner.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntPairWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class); 
		
		FileInputFormat.addInputPath(job, new Path(tmppath1));
		FileOutputFormat.setOutputPath(job, new Path(tmppath2));
		
		job.waitForCompletion(true);
	}
	
	private void runStep3(String tmppath2, String outpath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task2_GraphMining2.class);
		
		job.setMapperClass(GraphMiningMapper3.class);
		job.setReducerClass(GraphMiningReducer3.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntPairWritable.class);
		
		job.setPartitionerClass(IntPairIntPairPartitioner.class);
		
//		job.setOutputKeyClass(IntPairWritable.class);
//		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); 
		
		FileInputFormat.addInputPath(job, new Path(tmppath2));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.waitForCompletion(true);
	}
}
