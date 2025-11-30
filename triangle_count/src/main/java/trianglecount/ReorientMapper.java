package trianglecount;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import trianglecount.IntPairWritable;

/**
 * ReorientMapper - Pass-through mapper (Task 2, Step 3)
 * 
 * Purpose: Identity mapper to shuffle edges by key for reorientation.
 * 
 * Input:  ((u, v), (deg_u, deg_v)) - edge with degree info
 * Output: ((u, v), (deg_u, deg_v)) - same, passed to reducer
 */
public class ReorientMapper extends Mapper<IntPairWritable, IntPairWritable, IntPairWritable, IntPairWritable>{
	protected void map(IntPairWritable key, IntPairWritable value, Mapper<IntPairWritable, IntPairWritable, IntPairWritable, IntPairWritable>.Context context) throws IOException, InterruptedException {

		context.write(key, value);
	}
}