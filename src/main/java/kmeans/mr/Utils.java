package kmeans.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Utils {

    public static Logger logger = LoggerFactory.getLogger(Utils.class);
    public static double[][] getCentroids(BufferedReader br, int _nrows, int _ndims) throws IOException {

        double[][] centroids;
        try {

            centroids = new double[_nrows][];
            String line;

            for (int i = 0; i < _nrows; i++)
                centroids[i] = new double[_ndims];

            int nrow = 0;

            while ((line = br.readLine()) != null) {
                logger.info("读取缓存文件的日志： " + line);
                String[] nDim = line.split(",");
                double[] dv = new double[_ndims];
                for (int i = 0; i < _ndims; i++) {
                    dv[i] = Double.parseDouble(nDim[i]);
                }
                centroids[nrow] = dv;
                nrow++;
            }

            return centroids;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        } finally {
            if(br != null)
                br.close();
        }
    }

    public static double[][] randomPickCentroids(int _numClusters, int _ndims){
        double[][] _centroids = new double[_numClusters][];

        for (int i=0; i < _numClusters; i++){
            // copy the value from _data[c]
            _centroids[i] = new double[_ndims];
            for (int j=0; j<_ndims; j++) {
                _centroids[i][j] = Math.random() * 2;
            }
        }

        return _centroids;
    }
    public static void writeCentroids(Configuration configuration, String _centroids) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fin = fs.create(new Path(Constants.CENTOIDS_FILE));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));

        logger.info("本次聚类的输出字符串：  " + _centroids);
        bw.write(_centroids);
        bw.close();
    }

    public static void writeCentroids(Configuration configuration, double[][]_centroids) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fin = fs.create(new Path(Constants.CENTOIDS_FILE));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        String formattedString = getFormattedCentroids(_centroids);
        logger.info("本次聚类的输出字符串：  " + formattedString);
        bw.write(formattedString);
        bw.close();

    }

    public static String getFormattedCentroids(double[][]_centroids){
        StringBuilder sb = new StringBuilder();
        for(double[] eachCentroid : _centroids){
            for(int j = 0; j < eachCentroid.length; j++){
                sb.append(eachCentroid[j]);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String readReducerOutput(Configuration conf)throws IOException{
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(conf.get(Constants.OUTPUT_FILE_ARG) + "/part-r-00000")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
        StringBuilder content = new StringBuilder();

        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }

        return content.toString();
    }
}
