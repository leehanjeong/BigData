package measurepollution;

import measurepollution.MeasurePollution1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


public class MeasurePollutionTest1 {
	public static void main(String[] not_used) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] args = {"src/test/resources/Measurement_info.csv"};
		ToolRunner.run(conf,  new MeasurePollution1(), args);

	}

}
