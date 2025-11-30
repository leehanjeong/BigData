package trianglecount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * EdgeMarkerMapper - Mark original edges from text file (Task 3, Step 2)
 * 
 * Purpose: Emit original edges with -1 marker to indicate edge existence.
 *          Used with MultipleInputs alongside wedges.
 * 
 * Input:  (offset, "u v") - edge from text file
 * Output: ((min(u,v), max(u,v)), -1) - normalized edge with marker
 */
public class EdgeMarkerMapper extends Mapper<Object, Text, IntPairWritable, IntWritable>{
	
	IntPairWritable ok = new IntPairWritable();
	IntWritable ov = new IntWritable(-1);
	@Override
	// input value: 1	77
	// output key: (1, 77), output value: -1 (숫자 작은거에서 큰거로 가도록)
	protected void map(Object key, Text value, Mapper<Object, Text, IntPairWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		int u = Integer.parseInt(st.nextToken());
		int v = Integer.parseInt(st.nextToken());
		
		if (u < v) {
			ok.set(u, v);
		}
		else {
			ok.set(v,  u);
		}

		context.write(ok, ov);
		
	}
}
