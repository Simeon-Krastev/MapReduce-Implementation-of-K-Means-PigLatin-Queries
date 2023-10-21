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
import java.util.HashMap;
import java.util.List;

public class KMeans {

    public static List<double[]> clusterCenters = new ArrayList<>();
    public static List<List<double[]>> clusterCenterHistory = new ArrayList<>();
    public static enum ConvergenceCounter {
        CONVERGED
    }
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

                double[] nearestCluster = findNearestCluster(dataPoint);
                String clusterPoint = "(" + nearestCluster[0] + ", " + nearestCluster[1] + ")";
                context.write(new Text(clusterPoint), new Text(dataPointLine[0] + "," + dataPointLine[1]));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            clusterCenters = new ArrayList<>();
        }
    }

    private static double[] findNearestCluster(int[] dataPoint) {
        int nearestClusterIndex = 0;
        double minDistance = Double.MAX_VALUE;

        for (int i = 0; i < clusterCenters.size(); i++) {
            double distance = calculateDistance(dataPoint, clusterCenters.get(i));
            if (distance < minDistance) {
                minDistance = distance;
                nearestClusterIndex = i;
            }
        }

        return clusterCenters.get(nearestClusterIndex);
    }

    private static double calculateDistance(int[] dataPoint, double[] centerPoint) {
        int x1 = dataPoint[0];
        int y1 = dataPoint[1];
        double x2 = centerPoint[0];
        double y2 = centerPoint[1];
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumX = 0;
            double sumY = 0;
            int count = 0;

            for (Text value : values) {
                String[] dataPointLine = value.toString().split(",");
                double x = Double.parseDouble(dataPointLine[0]);
                double y = Double.parseDouble(dataPointLine[1]);

                sumX += x;
                sumY += y;
                count++;
            }

            context.write(key, new Text(sumX + "," + sumY + "," + count));
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

                context.write(new Text(newClusterCenter[0] + "," + newClusterCenter[1]), new Text(""));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            clusterCenterHistory.add(clusterCenters);
            double threshold = context.getConfiguration().getDouble("threshold", 0.0);
            if (threshold >= 0.0) {
                boolean convergence_reached = checkConvergence(threshold);
                if (convergence_reached) {
                    context.getCounter(ConvergenceCounter.CONVERGED).increment(1);
                    int iteration = context.getConfiguration().getInt("iteration", 0);
                    context.write(new Text("Convergence Reached. Number of Iterations: "), new Text(String.valueOf(iteration)));
                }
            }
        }
    }

    public static class KMeansReducerOptimized extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalSumX = 0;
            double totalSumY = 0;
            int totalCount = 0;

            for (Text value : values) {
                String[] combinedLine = value.toString().split(",");
                double sumX = Double.parseDouble(combinedLine[0]);
                double sumY = Double.parseDouble(combinedLine[1]);
                int count = Integer.parseInt(combinedLine[2]);

                totalSumX += sumX;
                totalSumY += sumY;
                totalCount += count;
            }

            double newClusterCenterX = totalSumX / totalCount;
            double newClusterCenterY = totalSumY / totalCount;

            double[] center = new double[]{newClusterCenterX, newClusterCenterY};
            clusterCenters.add(center);
            System.out.println(Arrays.toString(center));
            context.write(new Text(newClusterCenterX + "," + newClusterCenterY), new Text(""));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            clusterCenterHistory.add(clusterCenters);
            double threshold = context.getConfiguration().getDouble("threshold", 0.0);
            int maxIterations = context.getConfiguration().getInt("maxIterations", 0);
            int iteration = context.getConfiguration().getInt("iteration", 0);

            boolean convergence_reached = checkConvergence(threshold);
            if (convergence_reached) {
                context.getCounter(ConvergenceCounter.CONVERGED).increment(1);
                context.write(new Text("Convergence Reached. Number of iterations: "), new Text(String.valueOf(iteration)));
            } else if (iteration == maxIterations) {
                context.write(new Text("Could not reach convergence based on given threshold. Total number of iterations: "), new Text(String.valueOf(iteration)));
            }
        }
    }

    public static class PointsGrouperReducer extends Reducer<Text, Text, Text, Text> {

        HashMap<String, String> output;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            output = new HashMap<>();
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            try {
                for (Text value : values) {
                    String ky = "Center " + key.toString() + ". Points: ";
                    String val = "(" + value.toString() + ")";
                    if (output.containsKey(ky)) {
                        output.put(ky, output.get(ky) + "; " + val);
                    } else {
                        output.put(ky, val);
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (HashMap.Entry<String, String> entry : output.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                context.write(new Text(key), new Text(value));
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

        for (int i = 0; i < oldClusters.size(); i++) {
            int centroidLabel = i + 1;
            System.out.println("Convergence check - Centroid " + centroidLabel);
            double deltaX = Math.abs(oldClusters.get(i)[0]-newClusters.get(i)[0])/oldClusters.get(i)[0];
            double deltaY = Math.abs(oldClusters.get(i)[1]-newClusters.get(i)[1])/oldClusters.get(i)[1];
            System.out.println(deltaX);
            System.out.println(deltaY);
            if (deltaX > threshold || deltaY > threshold) return false;
        }

        return true;
    }

    public static void debugA(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        File output = new File(args[1].substring(8));
        Utils.deleteDirectory(output);

        Configuration conf = new Configuration();
        System.out.println("Single Iteration K-Means");
        Job job = Job.getInstance(conf, "Single Iteration K-Means");

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///C:/Hadoop/SeedPoints.csv"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void debugB(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int maxIterations = Integer.parseInt(args[2]);

        for (int i = 1; i < maxIterations + 1; i++) {

            File output = new File(args[1].substring(8));
            Utils.deleteDirectory(output);

            Configuration conf = new Configuration();
            System.out.println("Iteration " + i);
            conf.setDouble("threshold", -1.0);
            Job job = Job.getInstance(conf, "Basic KMeans MapReduce");

            job.setMapperClass(KMeansMapper.class);
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
        }
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void debugC(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int maxIterations = Integer.parseInt(args[2]);
        double threshold = Double.parseDouble(args[3]);

        for (int i = 1; i < maxIterations + 1; i++) {

            File output = new File(args[1].substring(8));
            Utils.deleteDirectory(output);

            Configuration conf = new Configuration();
            System.out.println("Iteration " + i);
            conf.setDouble("threshold", threshold);
            conf.setInt("iteration", i);

            Job job = Job.getInstance(conf, "Convergence KMeans MapReduce");

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new URI("file:///C:/Hadoop/SeedPoints.csv"));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean jobSuccess = job.waitForCompletion(true);

            if (!jobSuccess) {
                System.out.println("K-Means iteration failed.");
                System.exit(1);
            }
            long convergedReducers = job.getCounters().findCounter(ConvergenceCounter.CONVERGED).getValue();

            if (convergedReducers == 1) {
                System.out.println("Convergence achieved!");
                break;
            }
        }
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void debugD(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int maxIterations = Integer.parseInt(args[2]);
        double threshold = Double.parseDouble(args[3]);

        for (int i = 1; i < maxIterations + 1; i++) {

            File output = new File(args[1].substring(8));
            Utils.deleteDirectory(output);

            Configuration conf = new Configuration();
            System.out.println("Iteration " + i);
            conf.setDouble("threshold", threshold);
            conf.setInt("maxIterations", maxIterations);
            conf.setInt("iteration", i);

            Job job = Job.getInstance(conf, "Convergence KMeans MapReduce");

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducerOptimized.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new URI("file:///C:/Hadoop/SeedPoints.csv"));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean jobSuccess = job.waitForCompletion(true);

            if (!jobSuccess) {
                System.out.println("K-Means iteration failed.");
                System.exit(1);
            }
            long convergedReducers = job.getCounters().findCounter(ConvergenceCounter.CONVERGED).getValue();

            if (convergedReducers == 1) {
                System.out.println("Convergence achieved!");
                break;
            }
        }
        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void debugE(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        int maxIterations = Integer.parseInt(args[3]);
        double threshold = Double.parseDouble(args[4]);

        for (int i = 1; i < maxIterations + 1; i++) {

            File output_i = new File(args[1].substring(8));
            File output_ii = new File(args[2].substring(8));
            Utils.deleteDirectory(output_i);
            Utils.deleteDirectory(output_ii);

            Configuration conf = new Configuration();
            System.out.println("Iteration " + i);
            conf.setDouble("threshold", threshold);
            conf.setInt("maxIterations", maxIterations);
            conf.setInt("iteration", i);

            Job job = Job.getInstance(conf, "Convergence KMeans MapReduce");

            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducerOptimized.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new URI("file:///C:/Hadoop/SeedPoints.csv"));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            boolean jobSuccess = job.waitForCompletion(true);

            if (!jobSuccess) {
                System.out.println("K-Means iteration failed.");
                System.exit(1);
            }
            long convergedReducers = job.getCounters().findCounter(ConvergenceCounter.CONVERGED).getValue();

            if (convergedReducers == 1) {
                System.out.println("Convergence achieved!");
                break;
            }
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Final Output K MeansMapreduce");

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(PointsGrouperReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new URI("file:///C:/Hadoop/SeedPoints.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean jobSuccess = job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        String elapsed = String.format("%.2f", (end - start) * 0.001);
        System.out.println("Elapsed Time: " + elapsed + "s");
    }

    public static void main(String[] args) throws Exception {

    }
}
