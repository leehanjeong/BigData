package trianglecount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriStep2MapperForEdges extends Mapper<Object, Text, IntPairWritable, IntWritable>{
	
	IntPairWritable ok = new IntPairWritable();
	IntWritable ov = new IntWritable(-1);
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntPairWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		int u = Integer.parseInt(st.nextToken());
		int v = Integer.parseInt(st.nextToken());
		
		ok.set(u, v);
		
		context.write(ok, ov);
		
	}
}
