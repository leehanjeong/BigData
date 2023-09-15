package trianglecount;

import org.apache.hadoop.mapreduce.Partitioner;

public class IntPairIntPairPartitioner extends Partitioner<IntPairWritable, IntPairWritable>{
	public int getPartition(IntPairWritable key, IntPairWritable value, int numReduceTasks) {
		return (key.u * 31 + key.v) % numReduceTasks; // 정확하게 하려면 31 대신에 노드 수가 들어가야 함
	}
}
