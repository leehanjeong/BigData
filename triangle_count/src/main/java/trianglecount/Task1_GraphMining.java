package trianglecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import trianglecount.IntPairWritable;


public class Task1_GraphMining extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Job myjob = Job.getInstance(getConf());
		myjob.setJarByClass(Task1_GraphMining.class);
		
		myjob.setMapperClass(GraphMiningMapper.class);
		myjob.setReducerClass(GraphMiningReducer.class);
		
		myjob.setMapOutputKeyClass(IntPairWritable.class);
		myjob.setMapOutputValueClass(IntWritable.class);
		
		myjob.setPartitionerClass(IntPairIntPartitioner.class);
		
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[0]).suffix(".out"));
		
		myjob.waitForCompletion(true);
		return 0;
	}
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Task1_GraphMining(), args);
	}
}
