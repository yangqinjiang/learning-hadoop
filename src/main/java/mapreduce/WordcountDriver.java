package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.combiner.WordcountCombiner;

public class WordcountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Word count driver");
		args = new String[] { "e:/mr/input/wordcount2.txt", "e:/mr/output/wordcount1" };
		// 1 获取配置信息以及封闭任务
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置jar加载路径
		job.setJarByClass(WordcountDriver.class);

		// 3设置map和reduce类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 4设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

//		--------------Combine------------
		// 如果不设置InputFormat，它默认用的是TextInputFormat.class
//		job.setInputFormatClass(CombineTextInputFormat.class);
		// 虚拟存储切片最大值设置1024b
		// 4194304 4mb
//		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		// --------------Combiner------------
		// 指定需要使用Combiner，以及用哪个类作为Combiner的逻辑
		job.setCombinerClass(WordcountCombiner.class);
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
