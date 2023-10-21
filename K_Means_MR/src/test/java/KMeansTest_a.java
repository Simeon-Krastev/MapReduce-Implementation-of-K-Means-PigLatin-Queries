import org.junit.Test;

public class KMeansTest_a {

    @Test
    public void debugA() {
        String[] input = new String[3];

//        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/points.csv";
//        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/seeds.csv";
//        input[2] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/Task_2a_output";
        input[0] = "hdfs://localhost:9000/project2/K-Means_MapReduce/points.csv";
        input[1] = "hdfs://localhost:9000/project2/K-Means_MapReduce/seeds.csv";
        input[2] = "hdfs://localhost:9000/project2/K-Means_MapReduce/Task_2a_output";

        KMeans km = new KMeans();
        try {
            km.debugA(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}