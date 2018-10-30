package atguigu.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 1 准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        // 2 准备 bean 对象
        TableBean pdBean = new TableBean();

        for(TableBean bean : values){
            // 判断是订单还是产品
            if("0".equals(bean.getFlag())){
                // 表示是订单
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            } else {

                try {
                    // 拷贝传递过来的产品表到内存中
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // 3 表的拼接
        for(TableBean bean:orderBeans){
            bean.setPname (pdBean.getPname());
            // 4 数据写出去
            context.write(bean, NullWritable.get());
        }
    }
}
