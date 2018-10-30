package atguigu.Order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {
    protected OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        int result;

        // 在这里不再像OrderBean一样二次排序，这个分组排序是在reduce阶段，调用reduce方法之前用的。
        // 目的是骗过reduce，不再像OrderBean一样，只需要id相同就行，
        // 因为reduce阶段只会取第一个key，其他相同的key只会记录值，而第一个key由于在reduce 端的merge
        // 之后merge就保证了最大了
        if(aBean.getOrder_id() > bBean.getOrder_id()){
            result = 1;
        }else if(aBean.getOrder_id() < bBean.getOrder_id()){
            result = -1;
        } else {
            result = 0;
        }

        return result;
    }
}
