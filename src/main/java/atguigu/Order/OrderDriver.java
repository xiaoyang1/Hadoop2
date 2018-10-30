package atguigu.Order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class OrderDriver {
    public static final String pathInput = "hdfs://192.168.1.81:9000/user/hadoop/order";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/order";

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

//        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(pathOutput), true);

        conf.set("mapreduce.app-submission.cross-platform", "true");


        Job job = Job.getInstance(conf, "OrderDriver");
        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // 4 设置 map 输出数据 key 和 value 类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出数据的 key 和 value 类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(pathInput));
        FileOutputFormat.setOutputPath(job, new Path(pathOutput));

        // 10 设置 reduce 端的分组，而分组是只有reduce阶段调用reduce之前才会分组，combiner是不会的。
        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        // 这个是设置map和reduce 的shuffle阶段排序用的，如果不设置，就会用bean的compare方法
        job.setSortComparatorClass(OrderGroupingComparator.class);

        // 7 设置分区
        job.setPartitionerClass(OrderPartitioner.class);

        // 8 设置 reduce 个数
        job.setNumReduceTasks(2);
        // 9 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
