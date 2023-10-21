import org.junit.Test;

public class KMeansTest_b {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/DataSet.csv";
        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/2_Output_b";
        input[2] = "20";

        KMeans km = new KMeans();
        try {
            km.debugB(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}