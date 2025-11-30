package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;
import trianglecount.IntPairWritable;

/**
 * NormalizeMapper - Edge normalization (Task 1)
 * 
 * Purpose: Normalize edges so that u < v for all edges (u, v)
 *          This removes duplicate edges and self-loops.
 * 
 * Input:  (offset, "u v") - raw edge from text file
 * Output: ((min(u,v), max(u,v)), -1) - normalized edge
 */
public class NormalizeMapper extends Mapper<Object, Text, IntPairWritable, IntWritable>{
	IntPairWritable out_key = new IntPairWritable();
	IntWritable out_value = new IntWritable(-1);
	//Text out_value = new Text("$");
	
	int v1 = -1;
	int v2 = -1;
	
	protected void map(Object key, Text value, Mapper<Object, Text, IntPairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		System.out.println("mapper1: "+ value);
		v1 = Integer.parseInt(st.nextToken());
		v2 = Integer.parseInt(st.nextToken());
		
		if (v1 < v2) {
			out_key.set(v1, v2);
			context.write(out_key, out_value);
		}
		else if (v1 > v2){ // 중복 에지 제거
			out_key.set(v2,  v1);
			context.write(out_key, out_value);
		}
	}
}
