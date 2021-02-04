package mapreduce.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneIndexDriver {

	public static void main(String[] args) throws Exception {

		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "e:/mr/input/oneindex", "e:/mr/output/oneindex" };

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(OneIndexDriver.class);

		job.setMapperClass(OneIndexMapper.class);
		job.setReducerClass(OneIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		/**
		 * （4）查看第一次输出结果 atguigu--a.txt 3 atguigu--b.txt 2 atguigu--c.txt 2
		 * pingping--a.txt 1 pingping--b.txt 3 pingping--c.txt 1 ss--a.txt 2 ss--b.txt 1
		 * ss--c.txt 1
		 */
	}

}
