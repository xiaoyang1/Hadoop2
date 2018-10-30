package project.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class WeatherMapper extends Mapper<LongWritable, Text, KeyPair, Text>{

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        //每一行数据由制表符分割，所以需要分割

        String[]ss = line.split("\t");



        //只处理符合条件的数据, 后面的简单就不写了

    }
}
