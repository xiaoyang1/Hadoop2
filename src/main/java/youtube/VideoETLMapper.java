package youtube;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VideoETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    NullWritable k = NullWritable.get();
    Text valueClean = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String cleaned = ETLUtils.getETLString(value.toString());
        if(cleaned != null){
            valueClean.set(cleaned);
            context.write(k, valueClean);
        }
    }
}
