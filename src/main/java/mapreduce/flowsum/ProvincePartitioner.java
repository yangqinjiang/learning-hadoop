package mapreduce.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 1．需求 将统计结果按照手机归属地不同省份输出到不同文件中（分区） （2）期望输出数据
 * 手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
 * 
 * @author ABC
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		// 1 获取电话号码的前三位
		String preNum = key.toString().substring(0, 3);
		int partition = 4; // 其余分区号是 4
		// 2 判断前三位号码是什么?
		if ("136".equals(preNum)) {
			partition = 0;
		} else if ("137".equals(preNum)) {
			partition = 1;
		} else if ("138".equals(preNum)) {
			partition = 2;
		} else if ("139".equals(preNum)) {
			partition = 3;
		}
		return partition;
	}

}
