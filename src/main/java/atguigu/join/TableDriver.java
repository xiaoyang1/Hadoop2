package atguigu.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class TableDriver {

    public static final String pathInput = "hdfs://192.168.1.81:9000/user/hadoop/input";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

//        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(pathOutput), true);

        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf, "reduce join");

        // 2 指定本程序的 jar 包所在的本地路径
        job.setJarByClass(TableDriver.class);
        // 3 指定本业务 job 要使用的 mapper/Reducer 业务类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);


        // 4 指定 mapper 输出数据的 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 5 指定最终输出的数据的 kv 类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 6 指定 job 的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(pathInput));
        FileOutputFormat.setOutputPath(job, new Path(pathOutput));

        // 7 将 job 中配置的相关参数，以及 job 所用的 java 类所在的 jar 包， 提交给 yarn 去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
