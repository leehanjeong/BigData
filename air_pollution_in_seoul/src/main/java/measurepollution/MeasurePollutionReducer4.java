package measurepollution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeasurePollutionReducer4 extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();

	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		ArrayList<Float> ones = new ArrayList<Float>();
		ArrayList<Float> threes = new ArrayList<Float>();
		ArrayList<Float> fives = new ArrayList<Float>();
		ArrayList<Float> sixes = new ArrayList<Float>();
		ArrayList<Float> eights = new ArrayList<Float>();
		ArrayList<Float> nines = new ArrayList<Float>();

		String item_code = "";
		float sum;
		float array_size = 0;
		float avg;
		String avgs = "";
		
		for (Text value : values) {
			StringTokenizer st = new StringTokenizer(value.toString(), ",");
			item_code = st.nextToken();
					
			if (item_code.equals("1")) {
				ones.add(Float.parseFloat(st.nextToken()));
			}
			else if (item_code.equals("3")) {
				threes.add(Float.parseFloat(st.nextToken()));
			}
			else if (item_code.equals("5")) {
				fives.add(Float.parseFloat(st.nextToken()));
			}
			else if (item_code.equals("6")) {
				sixes.add(Float.parseFloat(st.nextToken()));
			}
			else if (item_code.equals("8")) {
				eights.add(Float.parseFloat(st.nextToken()));
			}
			else if (item_code.equals("9")) {
				nines.add(Float.parseFloat(st.nextToken()));
			}
		}
		
		
		array_size = ones.size();
		sum = 0;
		for (int i=0; i<array_size; i++) {
			sum += ones.get(i);
			System.out.println(ones.get(i));
		}
		avg = sum / array_size;
		avgs = avgs + String.format("%.6f", avg) + " ";
		
		array_size = threes.size();
		sum = 0;
		for (int i=0; i<array_size; i++) {
			sum += threes.get(i);
		}
		avg = sum / array_size;
		avgs = avgs + String.format("%.6f", avg) + " ";
		
		array_size = fives.size();
		sum = 0;
		for (int i=0; i<array_size; i++) {
			sum += fives.get(i);
		}
		avg = sum / array_size;
		avgs = avgs + String.format("%.6f", avg) + " ";
		
		array_size = sixes.size();
		sum = 0;
		for (int i=0; i<array_size; i++) {
			sum += sixes.get(i);
		}
		avg = sum / array_size;
		avgs = avgs + String.format("%.6f", avg) + " ";
		
		array_size = eights.size();
		sum = 0;
		for (int i=0; i<array_size; i++) {
			sum += eights.get(i);
		}
		avg = sum / array_size;
		avgs = avgs + String.format("%.6f", avg) + " ";
		
		array_size = nines.size();
		sum = 0;
		for (int i=0; i<array_size; i++) {
			sum += nines.get(i);
		}
		avg = sum / array_size;
		avgs = avgs + String.format("%.6f", avg) + " ";

//		for (ArrayList<Float> list : lists) {
//			sum = 0;
//			array_size = list.size();
//			int cnt = 0;
//			for (int i=0; i<array_size; i++) {
//				if (cnt == 0) {
//					System.out.println(list.get(i));
//				}
//				
//				sum += list.get(i);
//				cnt += 1;
//			}
//			avg = sum / array_size;
//			avgs = avgs + String.format("%.7f", avg) + " ";
//		}

		result.set(avgs);
		
		context.write(key,  result);
	}
}

// 1. 지역별로 최대 최소 평균 구하기