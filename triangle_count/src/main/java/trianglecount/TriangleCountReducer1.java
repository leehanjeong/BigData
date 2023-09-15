package trianglecount;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleCountReducer1 extends Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable>{
	IntPairWritable ok = new IntPairWritable();
	
	@Override
	// input key: 1, input value: [77, 4, 156 ..]
	// output key: (4, 77), output value: 1
	// ...
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		
		
		ArrayList<Integer> neighbors = new ArrayList<Integer>(); // key의 이웃들
		for(IntWritable v : values) {
//			System.out.println("reducer1 input: "+Integer.toString(key.get())+ ", "+ Integer.toString(v.get()));
			neighbors.add(v.get());
		}
		
		for(int u : neighbors) {
			for(int v : neighbors) {
				if (u < v) {
					ok.set(u, v);
//					System.out.println("reducer1 output: "+ok.toString()+ ", "+ Integer.toString(key.get()));
					context.write(ok, key);
				}
			}
		}
		
	}
}
