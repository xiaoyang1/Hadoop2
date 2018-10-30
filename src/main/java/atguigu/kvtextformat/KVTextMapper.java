package atguigu.kvtextformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable>{

    final Text k = new Text();
    final LongWritable v = new LongWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // banzhang ni hao
        // 1 设置 key 和 value
        // banzhang
        k.set(key);
        // 设置 key 的个数
        v.set(1);
        // 2 写出
        context.write(k, v);
    }
}
