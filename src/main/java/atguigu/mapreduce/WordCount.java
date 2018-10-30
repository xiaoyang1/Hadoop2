package atguigu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class WordCount {

    public static final String pathInput = "hdfs://192.168.1.81:9000/input";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";
    public static Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        // 删除任务的结果文件
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), configuration, "hadoop");
        fs.delete(new Path(pathOutput), true);
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        // 开启小作业的优化
        configuration.set("mapreduce.job.ubertask.enable", "true");
        Job job = Job.getInstance(configuration, "wordcount");
        job.setUser("hadoop");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);

        //job.setInputFormatClass(CombineTextInputFormat.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(pathInput));
        FileOutputFormat.setOutputPath(job, new Path(pathOutput));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            logger.info("map key =  " + key);
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class WordCountPartitioner extends Partitioner<Text, IntWritable>{

        @Override
        public int getPartition(Text key, IntWritable intWritable, int i) {
           int result = key.toString().charAt(0);

           return result % 2 == 0 ? 0 : 1;
        }

    }

}
