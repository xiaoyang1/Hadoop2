package atguigu.Order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean>{

    private int order_id;
    private double price;

    public OrderBean() {
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.order_id = input.readInt();
        this.price = input.readDouble();
    }


    // 二次排序
    @Override
    public int compareTo(OrderBean o) {
        if(order_id > o.getOrder_id()){
            return 1;
        } else if(order_id < o.getOrder_id()){
            return -1;
        } else {
            return price > o.getPrice() ? -1 : 1;
        }
    }

    @Override
    public String toString(){
        return order_id + "\t" + price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
