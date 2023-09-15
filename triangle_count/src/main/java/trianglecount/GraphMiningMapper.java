package trianglecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;
import trianglecount.IntPairWritable;


// task: 그래프에서 중복된 간선과 loop를 모두 제거하는 기능을 MapReduce로 구현하기
//		 - 간선 (u, v)와 (v, u)는 동일한 간선으로 봅니다.
//		 - loop는 (u, u)처럼 동일한 정점을 연결하는 간선입니다.
//		 - 모든 간선 (u, v) 가 u < v 를 만족하도록 저장하세요.

// input: 0 10772 (v1 v2)

public class GraphMiningMapper extends Mapper<Object, Text, IntPairWritable, IntWritable>{
	IntPairWritable out_key = new IntPairWritable();
	IntWritable out_value = new IntWritable(-1);
	//Text out_value = new Text("$");
	
	int v1 = -1;
	int v2 = -1;
	
	protected void map(Object key, Text value, Mapper<Object, Text, IntPairWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		System.out.println("mapper1: "+ value);
		v1 = Integer.parseInt(st.nextToken());
		v2 = Integer.parseInt(st.nextToken());
		
		if (v1 < v2) {
			out_key.set(v1, v2);
			context.write(out_key, out_value);
		}
		else if (v1 > v2){ // 중복 에지 제거
			out_key.set(v2,  v1);
			context.write(out_key, out_value);
		}
	}
}
