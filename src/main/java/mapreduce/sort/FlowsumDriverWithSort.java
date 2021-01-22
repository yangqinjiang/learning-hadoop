package mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.flowsum.FlowBean;

public class FlowsumDriverWithSort {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "e:/mr/output/flow-sum/", "e:/mr/output/flow-sum-sort/" };

		// 1 获取配置信息，或者job对象实例
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 6 指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowsumDriverWithSort.class);

		// 2 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);

		// 3 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		// 4 指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// A: 这里可以是全排序(只有一个reduce), 也可以是区内排序(多个reduce)
//		// ---------------自定义数据分区 以及 区内排序 ------------------
//		// 8 指定自定义数据分区
//		job.setPartitionerClass(mapreduce.sort.ProvincePartitioner.class);
//		// 9 同时指定相应数量的reduce task
//		job.setNumReduceTasks(5);

		// 5 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
