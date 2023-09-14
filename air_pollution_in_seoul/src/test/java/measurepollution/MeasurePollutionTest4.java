package measurepollution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MeasurePollutionTest4 {
	
	public static void main(String[] not_used) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] args = {"src/test/resources/Measurement_info.csv"};
		ToolRunner.run(conf,  new MeasurePollution4(), args);
	}

}
