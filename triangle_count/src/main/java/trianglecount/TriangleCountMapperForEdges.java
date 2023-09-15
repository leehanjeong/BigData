package trianglecount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleCountMapperForEdges extends Mapper<Object, Text, IntPairWritable, IntWritable>{
	
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
