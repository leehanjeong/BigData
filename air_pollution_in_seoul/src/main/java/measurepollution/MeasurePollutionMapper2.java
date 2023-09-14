package measurepollution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;

public class MeasurePollutionMapper2 extends Mapper<Object, Text, Text, IntWritable>{
	Text station = new Text();
	IntWritable one = new IntWritable(1);
	
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String item_code = "-1";
		float avg_value = 0;
		StringTokenizer st = new StringTokenizer(value.toString(), ",");
		
		int col = 0;
		while (st.hasMoreTokens())  {
			if (col == 0) { // Measurement date
				if (st.nextToken().equals("Measurement date")) { // 첫 행 무시
					break;
				} 
			} 
			else if (col == 1) { // Station code
				station.set(st.nextToken()); 
			}
			else if (col == 2) { // Item code
				item_code = st.nextToken();
				if (!(item_code.equals("8") || item_code.equals("9"))) { // item code 8, 9 아니면 무시
					break;
				}
			}
			else if (col == 3) { // Average value
				avg_value = Float.parseFloat(st.nextToken());
				if (item_code.equals("8")) {
					if (avg_value > 30) {
						break;
					}
				}
				else if (item_code.equals("9")) {
					if (avg_value > 15) {
						break;
					}
				}
				else {
					break;
				}
			}
			else if (col == 4) { // Instrument status
				if (st.nextToken().equals("0")) { // 0(normal) 아니면 무시
					context.write(station, one); 
				}
			}
			col += 1;
		}
	}
}



// 3. 시간, 지역코드 묶기 2022-10-03 10:00 101 0.1 0.01(여러줄에 있던 아이템코드 값들을 한줄로)