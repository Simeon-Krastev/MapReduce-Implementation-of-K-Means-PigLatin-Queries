import Resources.Utils;
import org.junit.Test;

import java.io.File;

public class KMeansTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];

        input[0] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/DataSet.csv";
        input[1] = "file:///C:/Users/Simeon Krastev/Desktop/WPI/DS503/Project_2/K_Means_MR/2_Output";
        input[2] = "20";

        KMeans km = new KMeans();
        try {
            km.debug(input);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}