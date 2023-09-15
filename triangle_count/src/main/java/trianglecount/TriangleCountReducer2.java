package trianglecount;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleCountReducer2 extends Reducer<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
	
	IntWritable ov = new IntWritable();
	
	@Override
	// input  key: (3, 4), input value[-1, 5]
	// output key: (3, 4), output value: 5
	protected void reduce(IntPairWritable key, Iterable<IntWritable> values,
			Reducer<IntPairWritable, IntWritable, IntPairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		
		boolean edge_exists = false; 
		ArrayList<Integer> nodes = new ArrayList<Integer>();

		for(IntWritable v : values) {
			if(v.get() == -1) {
				edge_exists = true;
			}
			else {
				nodes.add(v.get());
			}
		}
		
		if(edge_exists) {
			for(int v : nodes) {
				ov.set(v);
				context.write(key, ov);
			}
		}
	}
}
