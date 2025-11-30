package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import trianglecount.IntPairWritable;

/**
 * DegreeMapper - Distribute edges for degree counting (Task 2, Step 2)
 * 
 * Purpose: Emit each edge twice (keyed by each endpoint) to count degrees.
 * 
 * Input:  ((u, v), "") - normalized edge from SequenceFile
 * Output: (u, (u, v)) and (v, (u, v)) - edge sent to both endpoints
 */
public class DegreeMapper extends Mapper<IntPairWritable, Text, IntWritable, IntPairWritable>{
	IntWritable out_key = new IntWritable();
	IntPairWritable out_value = new IntPairWritable();
	
	int u = -1;
	int v = -1;
	protected void map(IntPairWritable key, Text value, Mapper<IntPairWritable, Text, IntWritable, IntPairWritable>.Context context) throws IOException, InterruptedException {
		
		out_value = key;
		
		u = key.getFirst();
		v = key.getSecond();
		
		out_key.set(u);
//		System.out.println("mapper2-1, "+Integer.toString(out_key.get())+", "+out_value.toString());
		context.write(out_key, out_value);
		
		out_key.set(v);
//		System.out.println("mapper2-2, "+Integer.toString(out_key.get())+", "+out_value.toString());
		context.write(out_key, out_value);
	}
}
