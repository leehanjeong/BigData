package trianglecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WedgeMarkerMapper - Pass-through for wedges (Task 3, Step 2)
 * 
 * Purpose: Identity mapper to pass wedges to the triangle finding reducer.
 *          Wedges have positive center values (vs -1 for edge markers).
 * 
 * Input:  ((u, v), center) - wedge from SequenceFile
 * Output: ((u, v), center) - same wedge passed through
 */
public class WedgeMarkerMapper extends Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
	
	@Override
	// input key: (4, 77), input value: 1
	// output key: (4, 77), output value: 1
	protected void map(IntPairWritable key, IntWritable value, Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
