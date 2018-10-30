package atguigu.kvtextformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class KVTextDriver {

    public static final String pathInput = "hdfs://192.168.1.81:9000/user/hadoop/input";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
//        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(pathOutput), true);

        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "KV Text");

        // 设置 jar 包位置，关联 mapper 和 reducer
        job.setJarByClass(KVTextDriver.class);

        job.setMapperClass(KVTextMapper.class);
        job.setOutputValueClass(LongWritable.class);
        // 设置 map 输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 设置最终输出 kv 类型
        job.setReducerClass(KVTextReducer.class);
        job.setOutputKeyClass(Text.class);
        // 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(pathInput));

        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        // 设置输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交 job
        job.waitForCompletion(true);
    }
}
