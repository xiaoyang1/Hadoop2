package atguigu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class WordCount2 {
    public static final String pathInput = "hdfs://192.168.1.81:9000/user/hadoop";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output";


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.job.jar", "E:\\idea_code\\Hadoop2\\out\\artifacts\\Hadoop\\Hadoop.jar");
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);

        String[] remainingArgs = optionsParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if("-skip".equals(remainingArgs[i])){
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"), conf, "hadoop");
        // 删除结果文件
        fs.delete(new Path(otherArgs.get(1)), true);
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        public static enum CounterEnum{ INPUT_WORDS}

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        // 大小写是否敏感
        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<>();

        private Configuration configuration;
        private BufferedReader fis;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            configuration = context.getConfiguration();
            // 默认值是true
            caseSensitive = configuration.getBoolean("wordcount.case.sensitive", true);
            if(configuration.getBoolean("wordcount.skip.patterns", false)){
                URI[] pattersURIs = Job.getInstance(configuration).getCacheFiles();
                for(URI uri : pattersURIs){
                    Path patternsPath = new Path(uri.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName){
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null){
                    patternsToSkip.add(pattern);
                }
            } catch (IOException e){
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(e));
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = caseSensitive ? value.toString() : value.toString().toLowerCase();

            for(String pattern : patternsToSkip){
                line = line.replaceAll(pattern, "");
            }

            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);

                Counter counter = context.getCounter(CounterEnum.class.getName(), CounterEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class WordCountPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable intWritable, int i) {
            int result = key.toString().charAt(0);

            return result % 2 == 0 ? 0 : 1;
        }
    }
}
