package trianglecount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import trianglecount.IntPairWritable;

// input key: (1, 7), input value: [(5, -1), (-1, 3)]
// output key: (7, 1), output value: ""
// 붙일 때 degree 작은게 왼쪽 큰게 오른쪽, id 작은게 왼쪽 큰게 오른쪽
public class GraphMiningReducer3 extends Reducer<IntPairWritable, IntPairWritable, IntPairWritable, Text> {
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