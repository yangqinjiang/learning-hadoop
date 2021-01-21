package mapreduce.nline;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	LongWritable v = new LongWritable();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		//
		long sum = 0;
		// 1 汇总
		for (LongWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		// 2输出
		context.write(key, v);
	}
}
