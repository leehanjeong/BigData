package trianglecount;

import trianglecount.Task1_GraphMining;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Task1_SmallTriangleCountTest {
	public static void main(String[] not_used) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] args = {"src/test/resources/email-Eu-core.txt"};
		ToolRunner.run(conf,  new Task1_GraphMining(), args);

	}

}

