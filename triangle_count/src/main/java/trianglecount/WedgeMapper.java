package trianglecount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WedgeMapper - Parse edges from text file (Task 3, Step 1)
 * 
 * Purpose: Read edge from text and emit (source, target) for wedge generation.
 * 
 * Input:  (offset, "u v") - edge from text file
 * Output: (u, v) - source vertex as key, target as value
 */
public class WedgeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	IntWritable ok = new IntWritable();
	IntWritable ov = new IntWritable();
	
	@Override
	// input value: 1	77
	// output key: 1, output value: 77
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		
		int u = Integer.parseInt(st.nextToken()); // degree 작은 애
		int v = Integer.parseInt(st.nextToken()); // degree 큰 애
		
		ok.set(u);
		ov.set(v);
//		System.out.println(Integer.toString(u)+ ", "+ Integer.toString(v));
		context.write(ok, ov);
		
	}
}
