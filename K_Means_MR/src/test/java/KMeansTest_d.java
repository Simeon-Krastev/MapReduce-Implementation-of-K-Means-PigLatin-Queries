import org.junit.Test;

public class KMeansTest_d {

    @Test
    public void debugD() throws Exception {
        String[] input = new String[5];

//        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/points.csv";
//        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/seeds.csv";
//        input[2] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/Task_2d_output";
        input[0] = "hdfs://localhost:9000/project2/K-Means_MapReduce/points.csv";
        input[1] = "hdfs://localhost:9000/project2/K-Means_MapReduce/seeds.csv";
        input[2] = "hdfs://localhost:9000/project2/K-Means_MapReduce/Task_2d_output";
        input[3] = "20";
        input[4] = "0.01";

        KMeans km = new KMeans();
        try {
            km.debugD(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}