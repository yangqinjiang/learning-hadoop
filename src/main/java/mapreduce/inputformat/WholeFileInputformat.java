package mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//定义类继承FileInputFormat
//将整个文件输入到系统内
public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// //返回false, 说明输入的文件不可分割
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// 自定义 RecordReader
		WholeRecordReader recordReader = new WholeRecordReader();
		recordReader.initialize(split, context);
		return recordReader;
	}

}
