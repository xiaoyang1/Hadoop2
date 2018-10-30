package atguigu.descSortIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *   第一次 mapper ，
 */
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    IntWritable one = new IntWritable(1);

    String filename;

    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获得对应的文件名
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        filename = inputSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 2 切割
        String[] fields = line.split(" ");

        // 3 连接
        for(String word : fields){
            k.set(word + "--" + filename);
            context.write(k, one);
        }
    }
}
