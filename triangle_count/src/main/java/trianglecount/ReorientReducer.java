package trianglecount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import trianglecount.IntPairWritable;

/**
 * ReorientReducer - Reorient edges by degree (Task 2, Step 3)
 * 
 * Purpose: Reorient edges from low-degree to high-degree vertex.
 *          This optimization reduces the number of wedges generated.
 * 
 * Input:  ((u, v), [(deg_u, -1), (-1, deg_v)]) - edge with both degrees
 * Output: ((low_deg, high_deg), "") - reoriented edge (low → high degree)
 *         If same degree, use vertex ID as tiebreaker (small → large)
 */
public class ReorientReducer extends Reducer<IntPairWritable, IntPairWritable, IntPairWritable, Text> {
	IntPairWritable out_key = new IntPairWritable();
	Text out_value = new Text("");
	
	protected void reduce(IntPairWritable key, Iterable<IntPairWritable> values, Reducer<IntPairWritable, IntPairWritable, IntPairWritable, Text>.Context context) throws IOException, InterruptedException {
		int v1 = -1;
		int v2 = -1;
		
		for (IntPairWritable v: values) {
//			System.out.println("key: "+key.toString()+", value: "+ v.toString());
			if (v.getFirst() != -1) {
				v1 = v.getFirst();
			}
			else {
				v2 = v.getSecond();
			}
//			System.out.println("reducer3, v1" + Integer.toString(v1) + ", v2" + Integer.toString(v2));
		}
		
		
		if (v1 < v2) {
			out_key = key;
		}
		else if (v1 == v2) {
			if (key.getFirst() < key.getSecond()) {
				out_key = key;
			}
			else {
				out_key.set(key.getSecond(), key.getFirst());
			}
		}
		else {
			out_key.set(key.getSecond(), key.getFirst());
		}
//		System.out.println("result: "+out_key.toString());
		context.write(out_key, out_value);

	}
}