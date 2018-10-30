package kmeans.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *  输出还是一个标签， 一个向量
 */
public class KMeansReducer extends Reducer<IntWritable, DataVector, DataVector, NullWritable> {
    Logger logger = LoggerFactory.getLogger(KMeansReducer.class);

    @Override
    protected void reduce(IntWritable key, Iterable<DataVector> values, Context context) throws IOException, InterruptedException {
        // int ndim = values.iterator().next().getNdVector().length;
        // 千万不要用上面的方法，否则的话由于for each也是用迭代器，会每个key都吃掉一个。
        int ndim = context.getConfiguration().getInt(Constants.DIM_OF_VECTOR, 2);
        double[] center = new double[ndim];
        int count = 0;

        logger.info("这个时候的key 为 : " + key.get());
        for(DataVector dataVector : values){
            logger.info(dataVector.toString());
            double[] temp = dataVector.getNdVector();
            for(int i = 0; i < ndim; i++){
                center[i] += temp[i];
            }
            count++;
        }
        logger.info("这个时候的count: " + count);
        for(int i = 0; i < ndim; i++){
            center[i] /= count;
        }
        DataVector newCenter = new DataVector(center);
        logger.info("本次聚类的label 为： " + key.get() + " ， 中心向量为： " + newCenter);
        context.write(newCenter, NullWritable.get());
    }
}
