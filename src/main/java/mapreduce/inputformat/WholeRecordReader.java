package mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//自定义RecordReader类
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

	private Configuration conf;
	private FileSplit split;
	// 标志当前输入的文件是否被处理
	private boolean isProgress = true;

	private Text k = new Text();
	private BytesWritable value = new BytesWritable();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.split = (FileSplit) split; //输入切片的文件对象
		this.conf = context.getConfiguration();// 配置
	}

	// 函数 nextKeyValue 会被mr框架里 循环调用,所有在此需求里,使用isProgress作为标志值,用于只调用一次nextKeyValue
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (isProgress) {
			// 1 定义缓存区, 从文件读取数据至 内存
			byte[] contents = new byte[(int) split.getLength()];

			FileSystem fs = null;
			FSDataInputStream fis = null;
			try {
				// 2 获取文件系统
				Path path = split.getPath();
				fs = path.getFileSystem(conf);
				// 3读取数据
				fis = fs.open(path);
				// 4 读取文件内容,注意,是整个文件内容
				IOUtils.readFully(fis, contents, 0, contents.length);
				// 5 输出文件内容 ,
				value.set(contents, 0, contents.length); // 整个文件作为 value
				// 6获取文件路径及名称
				String name = split.getPath().toString();
				k.set(name); // 以文件路径及名称, 作为key
			} catch (Exception e) {
				// ignore
			} finally {
				IOUtils.closeStream(fis);
			}
			// 设置标志值,下次回来执行时,跳过此函数
			isProgress = false;
			return true;

		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		//
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		//
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {

	}

}
