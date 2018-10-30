package project.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyPair implements WritableComparable<KeyPair>{

    private int year;
    private int temperature;

    public KeyPair(){

    }

    public KeyPair(int year, int temperature) {
        this.year = year;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(KeyPair o) {
        int result = Integer.compare(this.year, o.getYear());
        if(result != 0){
            return result;
        }

        return Integer.compare(this.temperature, o.getTemperature());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.temperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.temperature = in.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    //另外，还需重写tostring

    @Override

    public String toString() {

        return year + "\t" + temperature;

    }

    //重写hashcode

    @Override

    public int hashCode() {

        return new Integer(year+temperature).hashCode();

    }
}
