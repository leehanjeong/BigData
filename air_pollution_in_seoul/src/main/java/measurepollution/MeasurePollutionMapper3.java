package measurepollution;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;

public class MeasurePollutionMapper3 extends Mapper<Object, Text, Text, Text>{
	Text date_station = new Text();
	Text item_value = new Text();
	
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString(), ",");
		String date = "";
		String station = "";
		String item_code = "";
		String avg_value = "";
		
		int col = 0;
		while (st.hasMoreTokens()) {
			if (col == 0) { // Measurement date
				date = st.nextToken();
				if (date.equals("Measurement date")) { // 첫 행 무시
					break;
				}
			}
			else if (col == 1) { // Station code
				station = st.nextToken();
			}
			else if (col == 2) { // Item code
				item_code = st.nextToken();
			}
			else if (col == 3) { // Average value
				avg_value = st.nextToken();
			}
			else if (col == 4) { // Instrument status
				if (st.nextToken().equals("0")) { // 0(normal) 아니면 무시
					date_station.set(date + "," + station);
					item_value.set(item_code + "," + avg_value);
					context.write(date_station, item_value); 
				}
			}
			col += 1;
		}
	}
}


// 3. 시간, 지역코드 묶기 2022-10-03 10:00 101 0.1 0.01(여러줄에 있던 아이템코드 값들을 한줄로)