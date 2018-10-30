package atguigu.filter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguiguOut = null;
    FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext context) {
        // 获得文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(context.getConfiguration());

            // 2 创建输出文件路径
            Path atguiguPath = new Path("/user/atguigu.log");
            Path otherPath = new Path("/user/other.log");

            atguiguOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        // 判断是否包含“atguigu”输出到不同文件
        if(key.toString().contains("atguigu")){
            atguiguOut.write(key.toString().getBytes());
        } else {
            otherOut.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if(atguiguOut != null){
            atguiguOut.close();
        }

        if(otherOut != null) {
            otherOut.close();
        }
    }
}
