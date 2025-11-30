package trianglecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * EdgeMarkerSeqMapper - Mark normalized edges from SequenceFile
 * 
 * Purpose: Emit normalized edges with -1 marker to indicate edge existence.
 *          Used with MultipleInputs alongside wedges.
 * 
 * Input:  ((u, v), "") - normalized edge from SequenceFile
 * Output: ((u, v), -1) - edge with marker
 */
public class EdgeMarkerSeqMapper extends Mapper<IntPairWritable, Text, IntPairWritable, IntWritable> {

	private IntWritable minusOne = new IntWritable(-1);

	@Override
	protected void map(IntPairWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 엣지에 -1 마킹해서 출력
		context.write(key, minusOne);
	}
}
