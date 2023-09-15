package trianglecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleCountMapper1_ForAll extends Mapper<IntPairWritable, Text, IntWritable, IntWritable> {
	
	IntWritable ok = new IntWritable();
	IntWritable ov = new IntWritable();
	
	@Override
	// input value: 1	77
	// output key: 1, output value: 77
	protected void map(IntPairWritable key, Text value, Mapper<IntPairWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int u = key.getFirst(); // degree 작은 애
		int v = key.getSecond(); // degree 큰 애
		
		ok.set(u);
		ov.set(v);
//		System.out.println(Integer.toString(u)+ ", "+ Integer.toString(v));
		context.write(ok, ov);
		
	}
}
