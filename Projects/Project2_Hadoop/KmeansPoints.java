package DS503_Project2;

import java.util.*;
import java.lang.*;
import java.io.FileWriter;

public class KmeansPoints {
    public static void main(String [] args) throws Exception
    {
        try {
            int minVal = 1;
            int maxVal = 10000;
            Random rand = new Random();
            FileWriter txtWriter = new FileWriter("KmeansPoints.txt");
            for (int i = 0; i < 10; i++) {
                int x = rand.nextInt(maxVal - minVal + 1) + minVal;
                int y = rand.nextInt(maxVal - minVal + 1) + minVal;
                String p = String.valueOf(x) + "," + String.valueOf(y);
                txtWriter.write(String.valueOf(p)+"\n");
            }
            txtWriter.flush();
            txtWriter.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
