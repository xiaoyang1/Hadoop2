package kmeans.mr;

public class Constants {
    public static final String INPUT_FILE_ARG = "input_file";
    public static final String OUTPUT_FILE_ARG = "output_file";

    public static final String CENTROID_NUMBER_ARG = "centroids_number"; // 聚类数k
    public static final String DIM_OF_VECTOR = "dim_of_data"; // 向量维数

    public static final String CENTOIDS_FILE = "hdfs://192.168.1.81:9000/input/cluster/centroids_file.txt"; // 聚类中心文件
}
