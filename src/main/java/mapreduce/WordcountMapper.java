package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordcountMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
	Text k=new Text();
	IntWritable v = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 1��ȡһ��
		String line = value.toString();
		//2�и�
		String[] words =  line.split(" ");
		//3���
		for (String word : words) {
			// ѭ�����
			k.set(word);
			context.write(k, v);
		}
	}
}
