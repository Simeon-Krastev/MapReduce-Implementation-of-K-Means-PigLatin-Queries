import org.junit.Test;

public class KMeansTest_a {

    @Test
    public void debug() {
        String[] input = new String[2];

        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/DataSet.csv";
        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/2_Output_a";

        KMeans km = new KMeans();
        try {
            km.debugA(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}