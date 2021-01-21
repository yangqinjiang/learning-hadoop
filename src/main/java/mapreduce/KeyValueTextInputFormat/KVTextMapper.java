package mapreduce.KeyValueTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

	// 1 设置value
	LongWritable v = new LongWritable(1);

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// banzhang ni hao
		// 写出
		context.write(key, v);
	}
}
