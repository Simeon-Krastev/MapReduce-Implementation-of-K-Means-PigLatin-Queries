import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SilhouetteTest {

    @Test
    public void debug() throws Exception {

        String filePath = "C:/Hadoop/points.txt"; // replace with your file's path
        ArrayList<List<int[]>> clusters = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;

            while ((line = reader.readLine()) != null) {
                ArrayList<int[]> pointsInCluster = new ArrayList<>();
                String clusterPoints = line.split("Points: \t")[1];
                String[] points = clusterPoints.split("; ");
                for (String point : points) {
                    System.out.println(point);
                    String[] coords = point.substring(1,point.length()-1).split(",");
                    int[] pt = new int[]{Integer.parseInt(coords[0]), Integer.parseInt(coords[1])};
                    pointsInCluster.add(pt);
                }
                clusters.add(pointsInCluster);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(Silhouette.computeSilhouette(clusters));
    }
}