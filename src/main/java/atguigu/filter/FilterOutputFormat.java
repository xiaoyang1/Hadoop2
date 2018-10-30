package atguigu.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;

public class FilterOutputFormat extends OutputFormat<Text, NullWritable>{
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // // 创建一个 RecordWriter
        return new FilterRecordWriter(taskAttemptContext);
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws IOException, InterruptedException {
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        } else {
            TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outDir}, job.getConfiguration());
            if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
                throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
            }
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        Path output = getOutputPath(context);
        return new FileOutputCommitter(output, context);
    }

    private Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        return name == null ? null : new Path(name);
    }
}
