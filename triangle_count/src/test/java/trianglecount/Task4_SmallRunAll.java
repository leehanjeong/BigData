package trianglecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Task4_SmallRunAll {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] params = {"src/test/resources/email-Eu-core.txt"};
		
		ToolRunner.run(conf, new Task4_RunAll(), params);
	}
}