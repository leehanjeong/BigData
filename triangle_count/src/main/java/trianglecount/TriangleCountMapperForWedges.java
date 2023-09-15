package trianglecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleCountMapperForWedges extends Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
	
	@Override
	// input key: (4, 77), input value: 1
	// output key: (4, 77), output value: 1
	protected void map(IntPairWritable key, IntWritable value, Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
