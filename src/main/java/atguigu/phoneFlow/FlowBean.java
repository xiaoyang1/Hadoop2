package atguigu.phoneFlow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean>{

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    // 反序列化时，需要无参构造函数
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow, long sunFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sunFlow;
    }

    // 写序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
        out.writeLong(this.sumFlow);
    }

    // 反序列化方法， 读顺序必须和写序列化方法的写顺序必须一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public void set(long upFlow, long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

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


    @Override
    public int compareTo(FlowBean o) {
        // 倒序排列， 从大到小
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }
}
