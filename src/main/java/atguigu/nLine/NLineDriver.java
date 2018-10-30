package atguigu.nLine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class NLineDriver {

    public static final String pathInput = "hdfs://192.168.1.81:9000/user/hadoop/input";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
//        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(pathOutput), true);

        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "NLine Text");

        // 设置每个切片 InputSplit 中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        // 使用 NLineInputFormat 处理记录数
        job.setInputFormatClass(NLineInputFormat.class);

        // 设置 jar 包位置，关联 mapper 和 reducer
        job.setJarByClass(NLineDriver.class);
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);
        // 设置 map 输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 设置最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 设置输入输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交 job
        job.waitForCompletion(true);

    }
}
