package atguigu.multiJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class Avg2 {
    private static final Text TEXT_SUM = new Text("SUM");
    private static final Text TEXT_COUNT = new Text("COUNT");
    private static final Text TEXT_AVG = new Text("AVG");

    //计算Sum
    public static class SumMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private long sum = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            sum += Long.parseLong(value.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 在最后再把统计的东西写出去。
            context.write(TEXT_SUM, new LongWritable(sum));
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private long sum = 0;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values){
                sum += value.get();
            }

            context.write(TEXT_SUM, new LongWritable(sum));
        }
    }

    //计算Count
    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private long count = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            count += 1;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_COUNT, new LongWritable(count));
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private long count = 0;

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values){
                count += value.get();
            }

            context.write(TEXT_SUM, new LongWritable(count));
        }
    }

    //计算Avg
    public static class AvgMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        private long sum = 0;
        private long count = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] v = value.toString().split("\t");
            if(v[0].equals("COUNT")){
                count += Long.parseLong(v[1]);
            }else if (v[0].equals("SUM")) {
                sum += Long.parseLong(v[1]);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(sum), new LongWritable(count));
        }
    }

    public static class AvgReducer extends Reducer<LongWritable, LongWritable, Text, DoubleWritable> {

        private long sum = 0;
        public long count = 0;

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            sum += key.get();
            for (LongWritable value : values){
                count += value.get();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(TEXT_AVG, new DoubleWritable(new Double(sum) / count));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String inputPath = "/input/duplicate.txt";
        String maxOutputPath = "/output/max/";
        String countOutputPath = "/output/count/";
        String avgOutputPath = "/output/avg/";

        //删除输出目录(可选,省得多次运行时,总是报OUTPUT目录已存在)
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        fs.delete(new Path(inputPath), true);
        fs.delete(new Path(countOutputPath), true);
        fs.delete(new Path(avgOutputPath), true);

        Job job1 = Job.getInstance(conf, "Sum");
        job1.setJarByClass(Avg2.class);
        job1.setMapperClass(SumMapper.class);
        job1.setCombinerClass(SumReducer.class);
         job1.setReducerClass(SumReducer.class);
         job1.setOutputKeyClass(Text.class);
         job1.setOutputValueClass(LongWritable.class);
         FileInputFormat.addInputPath(job1, new Path(inputPath));
         FileOutputFormat.setOutputPath(job1, new Path(maxOutputPath));


         Job job2 = Job.getInstance(conf, "Count");
         job2.setJarByClass(Avg2.class);
         job2.setMapperClass(CountMapper.class);
         job2.setCombinerClass(CountReducer.class);
         job2.setReducerClass(CountReducer.class);
         job2.setOutputKeyClass(Text.class);
         job2.setOutputValueClass(LongWritable.class);
         FileInputFormat.addInputPath(job2, new Path(inputPath));
         FileOutputFormat.setOutputPath(job2, new Path(countOutputPath));

         Job job3 = Job.getInstance(conf, "Average");
         job3.setJarByClass(Avg2.class);
         job3.setMapperClass(AvgMapper.class);
         job3.setReducerClass(AvgReducer.class);
         job3.setMapOutputKeyClass(LongWritable.class);
         job3.setMapOutputValueClass(LongWritable.class);
         job3.setOutputKeyClass(Text.class);
         job3.setOutputValueClass(DoubleWritable.class);

         //将job1及job2的输出为做job3的输入
         FileInputFormat.addInputPath(job3, new Path(maxOutputPath));
         FileInputFormat.addInputPath(job3, new Path(countOutputPath));
         FileOutputFormat.setOutputPath(job3, new Path(avgOutputPath));

         //提交job1及job2,并等待完成
         if (job1.waitForCompletion(true) && job2.waitForCompletion(true)) {
                 System.exit(job3.waitForCompletion(true) ? 0 : 1);
         }
    }

}
