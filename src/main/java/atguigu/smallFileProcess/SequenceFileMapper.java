package atguigu.smallFileProcess;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable>{

    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1. 获取文件切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 2. 获取切片信息
        String name = inputSplit.getPath().toString();
        // 3. 设置key的输出
        k.set(name);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(k, value);
    }
}
