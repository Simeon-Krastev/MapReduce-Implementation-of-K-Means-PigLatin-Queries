import org.junit.Test;

public class KMeansTest_b {

    @Test
    public void debugB() throws Exception {
        String[] input = new String[4];

//        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/points.csv";
//        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/seeds.csv";
//        input[2] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/Task_2b_output";
        input[0] = "hdfs://localhost:9000/project2/K-Means_MapReduce/points.csv";
        input[1] = "hdfs://localhost:9000/project2/K-Means_MapReduce/seeds.csv";
        input[2] = "hdfs://localhost:9000/project2/K-Means_MapReduce/Task_2b_output";
        input[3] = "20";

        KMeans km = new KMeans();
        try {
            km.debugB(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}