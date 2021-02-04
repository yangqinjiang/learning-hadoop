package mapreduce.weblog;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1获取第一行
		String line=value.toString();
		//2解析日志
		boolean result = parseLog(line,context);
		//3 日志不合法退出
		if(!result) {
			return;
		}
		//4,设置key
		Text k =new Text();
		k.set(line);
		//5写出数据
		context.write(k,NullWritable.get());
	}

	//解析日志
	private boolean parseLog(String line, Context context) {
		//1 截取
		String[] fields = line.split(" ");
		//2 日志长度大于11的,合法
		if(11 < fields.length){
			//系统计数器
			context.getCounter("map","true").increment(1);
			return true;
		}else{
			context.getCounter("map","false").increment(1);
			return false;
		}
	}
}
