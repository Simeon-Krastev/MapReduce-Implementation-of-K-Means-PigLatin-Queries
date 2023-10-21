import Resources.Utils;

import java.util.List;

public class Silhouette {
    private static double computeA(int[] dataPoint, List<int[]> sameClusterDataPoints) {
        double sumDistances = 0.0;
        for (int[] point : sameClusterDataPoints) {
            sumDistances += Utils.calculateDistance(dataPoint, point);
        }
        return sumDistances / (sameClusterDataPoints.size() - 1);  // Exclude the point itself
    }

    private static double computeB(int[] dataPoint, List<List<int[]>> allClusters, List<int[]> currentCluster) {
        double minAvgDistance = Double.MAX_VALUE;

        for (List<int[]> cluster : allClusters) {
            if (cluster == currentCluster) continue;

            double sumDistances = 0.0;
            for (int[] point : cluster) {
                sumDistances += Utils.calculateDistance(dataPoint, point);
            }
            double avgDistance = sumDistances / cluster.size();
            minAvgDistance = Math.min(minAvgDistance, avgDistance);
        }

        return minAvgDistance;
    }

    public static double computeSilhouette(List<List<int[]>> allClusters) {
        double silhouetteSum = 0.0;
        int totalDataPoints = 0;

        for (List<int[]> cluster : allClusters) {
            for (int[] dataPoint : cluster) {
                double a = computeA(dataPoint, cluster);
                double b = computeB(dataPoint, allClusters, cluster);
                double silhouetteForDataPoint = (b - a) / Math.max(a, b);
                silhouetteSum += silhouetteForDataPoint;
                totalDataPoints++;
            }
        }

        return silhouetteSum / totalDataPoints;
    }
}
