package measurepollution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MeasurePollutionReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable result = new IntWritable();
	
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0; 
		
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		result.set(sum);
		
		context.write(key,  result);
	}
}

// 2. 카운트까지만 하고 최종 답은 스크립트 작성해서 구하기