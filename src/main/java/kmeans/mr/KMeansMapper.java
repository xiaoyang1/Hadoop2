package kmeans.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 *  思路： 本质上。不用mr的话， 就需要对每个点的左边进行进行label的缓存，因为最后要用到这些进行簇点的更新。
 *  有了mr以后，自动就会对label 用key来表示。
 *
 *  在hdfs的地方， 应该保存簇点文件， 格式是 0 \t 1.0,1.0 所以需要解析，并在setup函数中加载到内存中。
 *
 *  输入格式： 一行一行的坐标向量数据输入，
 *  输出是 label， 加数据本身
 */
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, DataVector> {

    private double[][] centroids;
    private int _nrows, _ndims; // the number of rows and dimensions
    Logger logger = LoggerFactory.getLogger(KMeansMapper.class);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取聚类的 簇点
        Configuration conf = context.getConfiguration();
        URI[] centroidsFile = context.getCacheFiles();

        for(URI each : centroidsFile){
            logger.info("URI 的地址 ; " + each);
        }
        Path path = new Path(centroidsFile[0].toString());

        try {
            FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.81:9000"),conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            // get the number of rows
            _nrows = conf.getInt(Constants.CENTROID_NUMBER_ARG, 2);
            _ndims = conf.getInt(Constants.DIM_OF_VECTOR, 2);
            logger.info("读取到的聚类簇点个数为： " + _nrows);
            logger.info("聚类簇点维度为： " + _nrows + " * " + _ndims);

            centroids = Utils.getCentroids(br, _nrows, _ndims);

            logger.info("聚类点的显示： " + Utils.getFormattedCentroids(centroids));

            if(br != null) {
                br.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 对于每一行数据， 生成一个 DataVector， 并且对每个向量计算距离
        String[] data = value.toString().trim().split(",");
        if(data.length == _ndims){
            // 过滤掉维数不对的垃圾数据
            double[] ndimData = new double[_ndims];
            for(int i = 0; i< _ndims; i++){
                ndimData[i] = Double.parseDouble(data[i]);
            }
            DataVector vector = new DataVector(ndimData);

            // 比较距离，得到最小的标签
            int label = getClosest(ndimData);
            context.write(new IntWritable(label), vector);


            logger.info("读取的数据 vector:  " + vector + "  ； 标签为 " + label);
            if(label == 0){
                context.getCounter(Catagory.ONE_LABEL).increment(1);
            }else {
                context.getCounter(Catagory.OTHER_LABEL).increment(1);
            }
        }
    }

    private int getClosest(double[] ndimData) {
        double minDistance = dist(ndimData, centroids[0]);
        int label = 0;
        for(int i = 1; i < _nrows; i++){
            for(int j = 0; j < _ndims; j++){
                double temp = dist(ndimData, centroids[i]);
                if(temp < minDistance){
                    label = i;
                    minDistance = temp;
                }
            }
        }
        return label;
    }

    // compute Euclidean distance between two vectors v1 and v2
    private double dist(double [] v1, double [] v2){
        double sum=0;
        for (int i=0; i<_ndims; i++){
            double d = v1[i]-v2[i];
            sum += d*d;
        }
        return Math.sqrt(sum);
    }

    public static enum  Catagory{
        ONE_LABEL,OTHER_LABEL;

        private Catagory(){

        }
    }

}
