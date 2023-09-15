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


public class Task5_RunAllWithTask1 extends Configured implements Tool {
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Task5_RunAllWithTask1(), args);
	}
	
	public int run(String[] args) throws Exception {
		
		String inputpath = args[0]; 
		String tmppath1 = inputpath + "1.tmp"; 
		String tmppath2 = inputpath + "2.tmp";  
		String outpath = inputpath + ".out"; 
		
		runStep1(inputpath, tmppath1);
		runStep2(tmppath1, tmppath2, outpath);
		runStep3(inputpath, tmppath2, outpath);
		
		return 0;
	}
	
	private void runStep1(String inputpath, String tmppath1) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task5_RunAllWithTask1.class);
		
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
	private void runStep2(String tmppath1, String tmppath2, String outpath) throws Exception{
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task5_RunAllWithTask1.class);
		
		job.setMapperClass(TriStep1Mapper.class);
		job.setReducerClass(TriStep1Reducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntPairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(tmppath1));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.waitForCompletion(true);
		
	}

	private void runStep3(String inputpath, String tmppath2, String outpath) throws Exception {
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task5_RunAllWithTask1.class);
		
		job.setReducerClass(TriStep2Reducer.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(IntPairIntPartitioner.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(inputpath), TextInputFormat.class, TriStep2MapperForEdges.class);
		MultipleInputs.addInputPath(job, new Path(tmppath2), SequenceFileInputFormat.class, TriStep2MapperForWedges.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.waitForCompletion(true);
		
	}
}
