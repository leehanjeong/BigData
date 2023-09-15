package trianglecount;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import trianglecount.IntPairWritable;


// input key: (1, 7), input value: (5, -1)
// output key: (1, 7), output value: (5, -1)
public class GraphMiningMapper3 extends Mapper<IntPairWritable, IntPairWritable, IntPairWritable, IntPairWritable>{
	protected void map(IntPairWritable key, IntPairWritable value, Mapper<IntPairWritable, IntPairWritable, IntPairWritable, IntPairWritable>.Context context) throws IOException, InterruptedException {

		context.write(key, value);
	}
}