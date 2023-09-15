package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import trianglecount.IntPairWritable;

// task: key 출력
// input: key (1, 3), values [-1, -1] 
// output: key (1, 3), value ""
public class GraphMiningReducer extends Reducer<IntPairWritable, IntWritable, IntPairWritable, Text> {
	Text result = new Text("");

	protected void reduce(IntPairWritable key, Iterable<IntWritable> values, Reducer<IntPairWritable, IntWritable, IntPairWritable, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("reducer1: " + key);	
		context.write(key,  result);
	}
}
