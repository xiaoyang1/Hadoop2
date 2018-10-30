package youtube;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class VideoETLRunner {
    public static final String pathInput = "hdfs://192.168.1.81:9000/home/hadoop/data/youtube/video.txt";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

//        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(pathOutput), true);

        conf.set("mapreduce.app-submission.cross-platform", "true");


        Job job = Job.getInstance(conf, "ETLVideoClean");
        job.setNumReduceTasks(0);

        job.setJarByClass(VideoETLRunner.class);
        job.setMapperClass(VideoETLMapper.class);

        // 4 设置 map 输出数据 key 和 value 类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(pathInput));
        FileOutputFormat.setOutputPath(job, new Path(pathOutput));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
