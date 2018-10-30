package atguigu.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class BloomFilteringDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        System.out.println("================ " + otherArgs[0]);
        if (otherArgs.length != 3) {
            System.err.println("Usage: BloomFiltering <in> <out>");
            System.exit(1);
        }

        FileSystem.get(conf).delete(new Path(otherArgs[2]), true);

        Job job = Job.getInstance(conf, "TestBloomFiltering");
        job.setJarByClass(BloomFilteringDriver.class);
        job.setMapperClass(BloomFilteringMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));


        job.addCacheFile(new Path("/tmp/decli/bloom.bin").toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class BloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable>{

        private BloomFilter filter = new BloomFilter();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            try{
                // 从当前作业中获取要缓存的文件
                URI[] localPaths = context.getCacheFiles();
                for(URI each : localPaths){
                    Path path = new Path(each);
                    if (path.toString().contains("bloom.bin")) {
                        DataInputStream strm = new DataInputStream(
                                new FileInputStream(path.toString()));
                        // Read into our Bloom filter.
                        filter.readFields(strm);
                        strm.close();
                    }
                }
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Get the value for the comment
            String comment = value.toString();

            // If it is null, skip this record
            if (comment == null || comment.isEmpty()) {
                return;
            }

            StringTokenizer tokenizer = new StringTokenizer(comment);
            // For each word in the comment

            while (tokenizer.hasMoreTokens()){
                // Clean up the words
                String cleanWord = tokenizer.nextToken().replaceAll("'", "")
                        .replaceAll("[^a-zA-Z]", " ");

                // If the word is in the filter, output it and break
                if (cleanWord.length() > 0
                        && filter.membershipTest(new Key(cleanWord.getBytes("UTF-8")))) {
                    context.write(new Text(cleanWord), NullWritable.get());
                    // break;
                }
            }

        }
    }
}
