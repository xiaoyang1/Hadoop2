package kmeans.mr;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataVector implements Writable {

    private double []ndVector; // n 维向量

    public DataVector(double[] ndVector) {
        this.ndVector = ndVector;
    }

    public DataVector() {
    }



    public double[] getNdVector() {
        return ndVector;
    }

    public void setNdVector(double[] ndVector) {
        this.ndVector = ndVector;
    }


    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(this.ndVector.length);
        for(int i = 0; i < ndVector.length; i++){
            output.writeDouble(ndVector[i]);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int ndim = input.readInt();
        this.ndVector = new double[ndim];
        for(int i = 0; i < ndim; i++){
            ndVector[i] = input.readDouble();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < ndVector.length; i++){
            sb.append(ndVector[i]);
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
}
