package measurepollution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

public class MeasurePollution2 extends Configured implements Tool {
	public static void get_good_station() {
		String good_station = "";
		int max_value = Integer.MIN_VALUE;
		
		File[] files = new File[3];
		files[0] = new File("src/test/resources/Measurement_info.csv.out/part-r-00000");
		files[1] = new File("src/test/resources/Measurement_info.csv.out/part-r-00001");
		files[2] = new File("src/test/resources/Measurement_info.csv.out/part-r-00002");
		
		for (int i=0; i<3; i++) {
			FileReader fileReader;
			try {
				fileReader = new FileReader(files[i]);
				BufferedReader bufReader = new BufferedReader(fileReader);
				String line = "";
				
				while((line = bufReader.readLine()) != null) {
					int cnt = 0;
					String station = "";
					int value = 0;
					StringTokenizer st = new StringTokenizer(line);
					
					while (st.hasMoreTokens()) {
						if (cnt == 0) {
							station = st.nextToken();
						}
						else if (cnt == 1) {
							value = Integer.parseInt(st.nextToken());
							if (value > max_value) {
								max_value = value;
								good_station = station;
							}
						}
						cnt += 1;
					}
				}
				bufReader.close();
				fileReader.close();
			} 
			catch (FileNotFoundException e) {
				System.out.println("Error1");
			} 
			catch(IOException e) {
				System.out.println("Error2");
			}	
		}
		System.out.println(String.format("good air pollution station is %s", good_station));		
	}
	
	public int run(String[] args) throws Exception {
		Job myjob = Job.getInstance(getConf());
		myjob.setJarByClass(MeasurePollution2.class);
		
		myjob.setMapperClass(MeasurePollutionMapper2.class);
		myjob.setReducerClass(MeasurePollutionReducer2.class);
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(IntWritable.class);
		
		myjob.setOutputFormatClass(TextOutputFormat.class);
		myjob.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(args[0]).suffix(".out"));
		
		myjob.waitForCompletion(true);
		get_good_station();
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MeasurePollution2(), args);
		
	}
}
