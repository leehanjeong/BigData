package measurepollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MeasurePollutionReducer1 extends Reducer<Text, FloatWritable, Text, Text> {
	Text result = new Text();

	protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		float float_value;
		float sum = 0;
		int cnt = 0;
		float min_value = Float.MAX_VALUE;
		float max_value = Float.MIN_VALUE;
		
		for (FloatWritable value : values) {
			float_value = value.get();
			sum += float_value;
			cnt += 1;
			if (float_value < min_value) {
				min_value = float_value;
			}
			if (float_value > max_value) {
				max_value = float_value;
			}
		}
		 
		result.set(String.format("Average: %.3f, Min: %.3f, Max: %.3f", sum/cnt, min_value, max_value));
		
		context.write(key,  result);
	}
}

// 1. 지역별로 최대 최소 평균 구하기