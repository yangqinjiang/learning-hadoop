package mapreduce.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoIndexDriver {

	public static void main(String[] args) throws Exception {

		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "e:/mr/output/oneindex", "e:/mr/output/twoindex" };

		Configuration config = new Configuration();
		Job job = Job.getInstance(config);

		job.setJarByClass(TwoIndexDriver.class);
		job.setMapperClass(TwoIndexMapper.class);
		job.setReducerClass(TwoIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
/**
 * （4）第二次查看最终结果 
 * atguigu c.txt-->2 b.txt-->2 a.txt-->3 
 * pingping c.txt-->1 b.txt-->3 a.txt-->1 
 * ss c.txt-->1 b.txt-->1 a.txt-->2
 */