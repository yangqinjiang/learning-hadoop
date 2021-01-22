package mapreduce.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "e:/mr/input/kvtext/", "e:/mr/output/kvtext2" };
		Configuration conf = new Configuration();
		// 设置切割符号
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

		// 1获取job对象
		Job job = Job.getInstance(conf);
		// 2 设置jar包位置,关联mapper和reducer
		job.setJarByClass(KVTextDriver.class);
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);

		// 3设置map输出kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 4 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 设置输入格式 如果不设置InputFormat，它默认用的是TextInputFormat.class
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 5 设置输入输出数据的路径(可以是多个路径参数)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 6设置输出数据的路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}

}
