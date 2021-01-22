package mapreduce.sort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import mapreduce.flowsum.FlowBean;

public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// 循环输出，避免总流量相同情况
		for (Text value : values) {
			context.write(value, key); // 反转 key和value
		}
	}
}
