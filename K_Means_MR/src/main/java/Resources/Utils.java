package Resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Utils {

    public static boolean deleteDirectory(String directoryPath) throws IOException, URISyntaxException {
        if (directoryPath.startsWith("hdfs://")) {
            Configuration configuration = new Configuration();
            Path hdfsPath = new Path(directoryPath);
            FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);
            if (hdfs.exists(hdfsPath)) {
                return hdfs.delete(hdfsPath, true);
            }
        } else {
            System.out.println(directoryPath.substring(8));
            File directory = new File(directoryPath.substring(8));
            if (directory.exists()) {
                File[] files = directory.listFiles();

                if (files != null) {
                    for (File file : files) {
                        if (file.isDirectory()) {
                            deleteDirectory(file.getPath());
                        } else {
                            file.delete();
                        }
                    }
                }
                return directory.delete();
            }
        }
        return false;
    }

    public static double calculateDistance(int[] dataPoint, int[] dataPoint2) {
        int x1 = dataPoint[0];
        int y1 = dataPoint[1];
        int x2 = dataPoint2[0];
        int y2 = dataPoint2[1];
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }

    public static double calculateDistance(int[] dataPoint, double[] dataPoint2) {
        int x1 = dataPoint[0];
        int y1 = dataPoint[1];
        double x2 = dataPoint2[0];
        double y2 = dataPoint2[1];
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }

    public static double calculateDistance(double[] dataPoint, double[] centerPoint) {
        double x1 = dataPoint[0];
        double y1 = dataPoint[1];
        double x2 = centerPoint[0];
        double y2 = centerPoint[1];
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }
}