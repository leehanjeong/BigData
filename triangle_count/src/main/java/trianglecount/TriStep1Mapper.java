package trianglecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriStep1Mapper extends Mapper<IntPairWritable, Text, IntWritable, IntWritable> {
	
	IntWritable ok = new IntWritable();
	IntWritable ov = new IntWritable();
	
	@Override
	protected void map(IntPairWritable key, Text value, Mapper<IntPairWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		ok.set(key.getFirst());
		ov.set(key.getSecond());
		
		context.write(ok, ov);
	}
}
