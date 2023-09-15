package trianglecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Task2_TriangleCountTest {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] params = {"src/test/resources/wiki-topcats.txt"};
		
		ToolRunner.run(conf, new Task2_GraphMining2(), params);
	}
}