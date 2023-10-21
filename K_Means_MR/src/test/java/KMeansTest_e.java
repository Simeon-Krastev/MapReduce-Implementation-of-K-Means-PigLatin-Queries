import org.junit.Test;

public class KMeansTest_e {

    @Test
    public void debug() throws Exception {
        String[] input = new String[5];

        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/DataSet.csv";
        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/2_Output_e_i";
        input[2] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/2_Output_e_ii";
        input[3] = "10";
        input[4] = "0.1";

        KMeans km = new KMeans();
        try {
            km.debugE(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}