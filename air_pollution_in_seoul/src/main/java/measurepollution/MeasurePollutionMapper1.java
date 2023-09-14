package measurepollution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;

public class MeasurePollutionMapper1 extends Mapper<Object, Text, Text, FloatWritable>{
	Text station = new Text();
	FloatWritable avg_value = new FloatWritable();
	
	protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString(), ",");
		
		int col = 0;
		while (st.hasMoreTokens()) { 
			if (col == 0) { // Measurement date
				if (st.nextToken().equals("Measurement date")) { // 첫 행 무시
					break;
				}
			}
			else if (col == 1) { // Station code
				station.set(st.nextToken());  
			}
			else if (col == 2) { // Item code
				if (!st.nextToken().equals("8")) { // item code 8 아니면 무시
					break;
				}
			}
			else if (col == 3) { // Average value
				avg_value.set(Float.parseFloat(st.nextToken()));
			}
			else if (col == 4) { // Instrument status
				if (st.nextToken().equals("0")) { // 0(normal) 아니면 무시
					context.write(station, avg_value); 
				}
			}
			col += 1;
		}
	}
}
