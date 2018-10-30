package project.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartition extends Partitioner<KeyPair, Text>{
    @Override
    public int getPartition(KeyPair keyPair, Text value, int nums) {
        return keyPair.getYear() * 127 % nums;
    }
}
