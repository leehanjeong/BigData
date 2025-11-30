package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import trianglecount.IntPairWritable;

/**
 * NormalizeReducer - Remove duplicate edges (Task 1)
 * 
 * Purpose: Deduplicate edges by outputting each unique edge once.
 * 
 * Input:  ((u, v), [-1, -1, ...]) - same edge may appear multiple times
 * Output: ((u, v), "") - deduplicated edge
 */
public class NormalizeReducer extends Reducer<IntPairWritable, IntWritable, IntPairWritable, Text> {
	Text result = new Text("");

	protected void reduce(IntPairWritable key, Iterable<IntWritable> values, Reducer<IntPairWritable, IntWritable, IntPairWritable, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("reducer1: " + key);	
		context.write(key,  result);
	}
}
