package mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo.Bean;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

// 1 实现writable接口
public class FlowBean implements WritableComparable<FlowBean> {

	private long upFlow;
	private long downFlow;
	private long sumFlow;

	// 2 反序列化时，需要反射调用空参构造函数，所以必须有
	public FlowBean() {
		super();
	}

	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	// 3 写序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);

	}

	// 4 反序列化方法
	// 5 反序列化方法读顺序必须和写序列化方法的写顺序必须一致
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	// 6 编写toString方法，方便后续打印到文本
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	public void set(long upFlow, long downFlow) {
		this.setUpFlow(upFlow);
		this.setDownFlow(downFlow);
		this.setSumFlow(upFlow + downFlow);
	}

	@Override
	public int compareTo(FlowBean o) {
		// 按照总流量大小,倒序排列
		int result;
		if (sumFlow > o.getSumFlow()) {
			result = -1;
		} else if (sumFlow < o.getSumFlow()) {
			result = 1;
		} else {
			result = 0; // sumFlow值相同
		}
		return result;
	}

}
