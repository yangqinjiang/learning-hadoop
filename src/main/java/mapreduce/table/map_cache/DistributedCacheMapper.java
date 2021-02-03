package mapreduce.table.map_cache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Map<String, String> pdMap = new HashMap<>();
	Text k = new Text();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 1, 获取缓存的文件
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		System.out.println("DDDDDDDDDD:"+path);
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

		String line;
		while (StringUtils.isNotEmpty(line = reader.readLine())) {
			// 2 切割
			String[] fields = line.split("\t");
			// 3 缓存数据到集合
			pdMap.put(fields[0], fields[1]);
		}
		// 4 关闭流
		reader.close();

	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 1 获取一行
		String line = value.toString();

		// 2 截取
		String[] fields = line.split("\t");

		// 3 获取产品id
		String pId = fields[1];

		// 4 获取商品名称
		String pdName = pdMap.get(pId);

		// 5 拼接
		k.set(line + "\t" + pdName);

		// 6 写出
		context.write(k, NullWritable.get());
	}
}
