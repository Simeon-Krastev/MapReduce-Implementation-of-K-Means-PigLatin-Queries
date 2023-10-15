import Resources.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KMeans {

    public static List<double[]> clusterCenters = new ArrayList<>();
    public static List<List<double[]>> clusterCenterHistory = new ArrayList<>();
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                if (clusterCenters.isEmpty()) {
                    URI[] cacheFiles = context.getCacheFiles();
                    Path path = new Path(cacheFiles[0]);
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    FSDataInputStream fis = fs.open(path);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
                    String line;
                    while (StringUtils.isNotEmpty(line = reader.readLine())) {
                        String[] seedPointLine = line.split(",");
                        seedPointLine[0] = seedPointLine[0].replace("\uFEFF", ""); //remove Byte Order Mark
                        if (seedPointLine[0].trim().equals("X")) {
                            continue;
                        }

                        double[] seedPoint = new double[seedPointLine.length];

                        for (int i = 0; i < seedPointLine.length; i++) {
                            seedPoint[i] = Integer.parseInt(seedPointLine[i]);
                        }

                        clusterCenters.add(seedPoint);
                        clusterCenterHistory.add(clusterCenters);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] dataPointLine = value.toString().split(",");

                if (dataPointLine[0].equals("X")) return;

                int[] dataPoint = new int[dataPointLine.length];
                for (int i = 0; i < dataPointLine.length; i++) {
                    dataPoint[i] = Integer.parseInt(dataPointLine[i]);
                }

                int nearestClusterIndex = findNearestCluster(dataPoint);

                context.write(new Text(Integer.toString(nearestClusterIndex)), new Text(dataPointLine[0] + "," + dataPointLine[1]));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private int findNearestCluster(int[] dataPoint) {
            int nearestClusterIndex = 0;
            double minDistance = Double.MAX_VALUE;

            for (int i = 0; i < clusterCenters.size(); i++) {
                double distance = calculateDistance(dataPoint, clusterCenters.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestClusterIndex = i;
                }
            }

            return nearestClusterIndex;
        }

        private double calculateDistance(int[] dataPoint, double[] centerPoint) {
            int x1 = dataPoint[0];
            int y1 = dataPoint[1];
            double x2 = centerPoint[0];
            double y2 = centerPoint[1];
            return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            clusterCenters = new ArrayList<>();
        }
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<int[]> dataPoints = new ArrayList<>();
            for (Text value : values) {
                String[] dataPointLine = value.toString().split(",");
                int[] dataPoint = new int[dataPointLine.length];
                for (int i = 0; i < dataPointLine.length; i++) {
                    dataPoint[i] = Integer.parseInt(dataPointLine[i]);
                }
                dataPoints.add(dataPoint);
            }
            double[] newClusterCenter = calculateNewClusterCenter(dataPoints);

            context.write(key, new Text(newClusterCenter[0] + "," + newClusterCenter[1]));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                List<int[]> dataPoints = new ArrayList<>();

                for (Text value : values) {
                    String[] dataPointLine = value.toString().split(",");
                    int[] dataPoint = new int[dataPointLine.length];
                    for (int i = 0; i < dataPointLine.length; i++) {
                        dataPoint[i] = Integer.parseInt(dataPointLine[i]);
                    }
                    dataPoints.add(dataPoint);
                }
                double[] newClusterCenter = calculateNewClusterCenter(dataPoints);

                clusterCenters.add(newClusterCenter);

                context.write(key, new Text(newClusterCenter[0] + "," + newClusterCenter[1]));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static double[] calculateNewClusterCenter(List<int[]> dataPoints) {
        int dimensions = dataPoints.get(0).length;
        double[] sums = new double[dimensions];

        for (int[] dataPoint : dataPoints) {
            for (int i = 0; i < dimensions; i++) {
                sums[i] += dataPoint[i];
            }
        }

        for (int i = 0; i < dimensions; i++) {
            sums[i] /= dataPoints.size();
        }
        System.out.println(Arrays.toString(sums));
        return sums;
    }

    public static boolean checkConvergence(double threshold) {

        int clusterHistorySize = clusterCenterHistory.size();

        if (clusterHistorySize < 2) return false;

        List<double[]> oldClusters = clusterCenterHistory.get(clusterHistorySize-2);
        List<double[]> newClusters = clusterCenterHistory.get(clusterHistorySize-1);

        for (int i = 0; i < oldClusters.size() ; i++) {
            System.out.println("Convergence check - Centroid " + i);
            double deltaX = Math.abs(oldClusters.get(i)[0]-newClusters.get(i)[0])/oldClusters.get(i)[0];
            double deltaY = Math.abs(oldClusters.get(i)[1]-newClusters.get(i)[1])/oldClusters.get(i)[1];
            System.out.println(deltaX);
            System.out.println(deltaY);
            if (deltaX > threshold || deltaY > threshold) return false;
        }

        return true;
    }

    public static void debug(String[] args) throws Exception {

        int maxIterations = Integer.parseInt(args[2]);

        for (int i = 0; i < maxIterations; i++) {

            File output = new File(args[1].substring(8));
            Utils.deleteDirectory(output);

            Configuration conf = new Configuration();
            System.out.println("Iteration " + i);
            Job job = Job.getInstance(conf, "KMeans MapReduce");

            job.setMapperClass(KMeansMapper.class);
            //job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new URI("file:///C:/Hadoop/SeedPoints.csv"));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean jobSuccess = job.waitForCompletion(true);
            clusterCenterHistory.add(clusterCenters);

            if (!jobSuccess) {
                System.out.println("K-Means iteration failed.");
                System.exit(1);
            }

            boolean convergence = checkConvergence(0.01);

            if (convergence) {
                System.out.println("Convergence achieved after " + i + " iterations. Exiting.");
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {

    }
}
