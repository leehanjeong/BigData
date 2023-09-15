package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import trianglecount.IntPairWritable;

// task: 각각 내보내기 for degree 구하기
// input key: (1, 4562), input value: ""
// output key1: 1, output value1: (1, 4562)
// output key2: 4562, output value2: (1, 4562)
public class GraphMiningMapper2 extends Mapper<IntPairWritable, Text, IntWritable, IntPairWritable>{
	IntWritable out_key = new IntWritable();
	IntPairWritable out_value = new IntPairWritable();
	
	int u = -1;
	int v = -1;
	protected void map(IntPairWritable key, Text value, Mapper<IntPairWritable, Text, IntWritable, IntPairWritable>.Context context) throws IOException, InterruptedException {
		
		out_value = key;
		
		u = key.getFirst();
		v = key.getSecond();
		
		out_key.set(u);
//		System.out.println("mapper2-1, "+Integer.toString(out_key.get())+", "+out_value.toString());
		context.write(out_key, out_value);
		
		out_key.set(v);
//		System.out.println("mapper2-2, "+Integer.toString(out_key.get())+", "+out_value.toString());
		context.write(out_key, out_value);
	}
}
