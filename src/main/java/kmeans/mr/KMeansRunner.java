package kmeans.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansRunner {
    public static final String pathInput = "hdfs://192.168.1.81:9000/input/cluster/rawdata.txt";
    public static final String pathOutput = "hdfs://192.168.1.81:9000/output/cluster";
    public static void main(String[] args) throws Exception{



        int centroidsNumber = 2;
        int ndim = 2;
        if(args.length == 1){
            centroidsNumber = Integer.parseInt(args[0]);
        }
        if(args.length == 2){
            centroidsNumber = Integer.parseInt(args[0]);
            ndim = Integer.parseInt(args[1]);
        }

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("hdfs://192.168.1.81:9000/output" ), true);

        conf.setInt(Constants.CENTROID_NUMBER_ARG, centroidsNumber);
        conf.setInt(Constants.DIM_OF_VECTOR, ndim);
        conf.set(Constants.INPUT_FILE_ARG, pathInput);

        double[][] initCluster = Utils.randomPickCentroids(centroidsNumber, ndim);
        String centroidFile = Utils.getFormattedCentroids(initCluster);
        // String centroidFile = "1,1\n11,11";
        Utils.writeCentroids(conf, centroidFile);

        boolean hasConverged = false;
        int iteration = 0;

        do{
            conf.set(Constants.OUTPUT_FILE_ARG, pathOutput + "-" + iteration);
            conf.set("mapreduce.app-submission.cross-platform", "true");
            // executes hadoop job
            if (!launchJob(conf)) {
                // if an error has occurred stops iteration and terminates
                System.exit(1);
            }

            // reads reducer output file
            String newCentroids = Utils.readReducerOutput(conf);
            if(centroidFile.equals(newCentroids)){
                hasConverged = true;
            }else {
                // writes the reducers output to distributed cache
                Utils.writeCentroids(conf, newCentroids);
            }

            centroidFile = newCentroids;
            iteration ++;

        }while (!hasConverged);

        Utils.writeCentroids(conf, centroidFile);
    }

    private static boolean launchJob(Configuration configuration) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJobName("KMeans");
        job.addCacheFile(new Path(Constants.CENTOIDS_FILE).toUri());

        job.setJarByClass(KMeansRunner.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DataVector.class);

        job.setOutputKeyClass(DataVector.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(configuration.get(Constants.INPUT_FILE_ARG)));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get(Constants.OUTPUT_FILE_ARG)));

        return job.waitForCompletion(true);
    }
}
