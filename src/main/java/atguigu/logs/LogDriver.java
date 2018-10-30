package atguigu.logs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class LogDriver {
    public static final String pathInput = "hdfs://192.168.1.81:9000/user/hadoop/input";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(pathOutput), true);

        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "log job");

        // 2 指定本程序的 jar 包所在的本地路径
        job.setJarByClass(LogDriver.class);
        // 3 关联 map
        job.setMapperClass(LogMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(pathInput));
        FileOutputFormat.setOutputPath(job, new Path(pathOutput));
        // 6 提交
        job.waitForCompletion(true);
    }
}
