package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

import trianglecount.IntPairWritable;

// input key: 1, input value: [(1, 4562), (1, 7), (1, 565643) ..]
// output key1: (1, 4562), output value1: (5, -1)
// output key2: (1, 7), output value2: (5, -1)
public class GraphMiningReducer2 extends Reducer<IntWritable, IntPairWritable, IntPairWritable, IntPairWritable> {
	IntPairWritable out_key = new IntPairWritable();
	IntPairWritable out_value = new IntPairWritable();
	
	protected void reduce(IntWritable key, Iterable<IntPairWritable> values, Reducer<IntWritable, IntPairWritable, IntPairWritable, IntPairWritable>.Context context) throws IOException, InterruptedException {
		
		int cnt = 0;
		ArrayList<IntPairWritable> edges = new ArrayList<IntPairWritable>();
		
		for (IntPairWritable v: values) {
//			System.out.println("reducer2 value, "+v.toString());
			cnt += 1; // value 개수 세기
			IntPairWritable new_v = new IntPairWritable(); // 이렇게 안 하고 edges.add(new_v); 이렇게하면 마지막값으로 이전 값들이 다 바뀜. 왜냐하면 USER_DEFINE_CLASS의 멤버변수가 static이면 value 업데이트가 안 되기때문.
			new_v.set(v.getFirst(), v.getSecond());
			edges.add(new_v);
		}
		
		for (IntPairWritable e: edges) {
//			System.out.println("reducer2 edge, "+e.toString());
			out_key = e;
			if (key.get() == e.getFirst()) { // value의 첫 자리에 cnt를 써줌
				out_value.set(cnt, -1);
			}
			else { // value의 두번째 자리에 cnt를 써줌
				out_value.set(-1,  cnt);
			}
			System.out.println("reducer2, "+e.toString()+", "+out_value.toString());
			context.write(out_key,  out_value);
		}

	}
}