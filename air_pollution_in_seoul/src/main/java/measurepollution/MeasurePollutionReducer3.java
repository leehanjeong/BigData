package measurepollution;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringTokenizer;

public class MeasurePollutionReducer3 extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();

	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		String one = "";
		String three = "";
		String five = "";
		String six = "";
		String eight = "";
		String nine = "";
		String item_code = "";
		
		for (Text value : values) {
			StringTokenizer st = new StringTokenizer(value.toString(), ",");
			item_code = st.nextToken();
					
			if (item_code.equals("1")) {
				one = st.nextToken();
			}
			else if (item_code.equals("3")) {
				three = st.nextToken();
			}
			else if (item_code.equals("5")) {
				five = st.nextToken();
			}
			else if (item_code.equals("6")) {
				six = st.nextToken();
			}
			else if (item_code.equals("8")) {
				eight = st.nextToken();
			}
			else if (item_code.equals("9")) {
				nine = st.nextToken();
			}
		}
		
		result.set(String.format("%s %s %s %s %s %s", one, three, five, six, eight, nine));
		
		context.write(key,  result);
	}
}

// 1. 지역별로 최대 최소 평균 구하기