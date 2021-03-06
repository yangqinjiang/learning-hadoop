package mapreduce.outputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream atguiguOut = null;
	FSDataOutputStream otherOut = null;

	public FilterRecordWriter(TaskAttemptContext job) {
		// 1 获取文件系统
		try {
			FileSystem fs = FileSystem.get(job.getConfiguration());
			// 2 创建 输出文件路径
			Path atguiguPath = new Path("e:/mr/output/filter/atguigu.log");
			Path otherPath = new Path("e:/mr/output/filter/other.log");
			// 3 创建输出流
			atguiguOut = fs.create(atguiguPath);
			otherOut = fs.create(otherPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// 判断是否包含 atguigu输出到不同文件
		if (key.toString().contains("atguigu")) {
			atguiguOut.write(key.toString().getBytes());
		} else {
			otherOut.write(key.toString().getBytes());
		}

	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// 关闭资源
		IOUtils.closeStream(atguiguOut);
		IOUtils.closeStream(otherOut);
	}

}
