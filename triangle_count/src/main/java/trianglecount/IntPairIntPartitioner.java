package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class IntPairIntPartitioner extends Partitioner<IntPairWritable, IntWritable>{
	public int getPartition(IntPairWritable key, IntWritable value, int numReduceTasks) {
		return (key.u * 31 + key.v) % numReduceTasks; // 정확하게 하려면 31 대신에 노드 수가 들어가야 함
	}
}

// 1791489 이거 곱하면 왜 에러나지??