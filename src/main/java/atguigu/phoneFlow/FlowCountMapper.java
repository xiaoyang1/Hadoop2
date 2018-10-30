package atguigu.phoneFlow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
    FlowBean bean = new FlowBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");

        // 封装对象， 取出手机号， 取出上下行流量
        String phoneNum = fields[1];
        System.out.println(phoneNum);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        text.set(phoneNum);
        bean.set(upFlow, downFlow);
        context.write(text, bean);
    }
}
