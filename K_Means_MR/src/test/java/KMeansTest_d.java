import org.junit.Test;

public class KMeansTest_d {

    @Test
    public void debug() throws Exception {
        String[] input = new String[4];

        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/DataSet.csv";
        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/2_Output_d";
        input[2] = "10";
        input[3] = "0.05";

        KMeans km = new KMeans();
        try {
            km.debugD(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}