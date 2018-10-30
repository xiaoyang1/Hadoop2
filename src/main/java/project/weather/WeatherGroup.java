package project.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroup extends WritableComparator {
    public WeatherGroup() {
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyPair key1 = (KeyPair) a;
        KeyPair key2 = (KeyPair) b;

        return Integer.compare(key1.getYear(), key2.getYear());
    }
}
