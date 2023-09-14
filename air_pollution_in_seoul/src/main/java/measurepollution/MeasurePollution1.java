package measurepollution;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

public class MeasurePollution1 extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Job myjob = Job.getInstance(getConf());
		myjob.setJarByClass(MeasurePollution1.class);
		
		myjob.setMapperClass(MeasurePollutionMapper1.class);
		myjob.setReducerClass(MeasurePollutionReducer1.class);
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(FloatWritable.class);
		
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[0]).suffix(".out"));
		
		myjob.waitForCompletion(true);
		return 0;
	}
	 
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MeasurePollution1(), args);
	}
}
