package DS503_Project2;

import java.util.*;
import java.lang.*;
import java.io.FileWriter;

public class CreateRectangleDataset {
    public static void main(String [] args) throws Exception
    {
        try {
            int minVal = 1;
            int maxVal = 10000;
            int maxHeight = 20;
            int maxWidth = 5;
            Random rand = new Random();
            FileWriter txtWriter = new FileWriter("Rectangle.txt");
            for (int i = 0; i <= 4000000; i++) {
                int h = rand.nextInt(maxHeight - minVal + 1) + minVal;
                int w = rand.nextInt(maxWidth - minVal + 1) + minVal;
                int x = rand.nextInt((maxVal-maxWidth) - minVal + 1) + minVal;
                int y = rand.nextInt((maxVal-maxHeight) - minVal + 1) + minVal;

                String r = "r"+String.valueOf(i)+","+String.valueOf(x) + "," + String.valueOf(y)+","+ String.valueOf(h)+","+ String.valueOf(w);
                txtWriter.write(String.valueOf(r)+"\n");
            }
            txtWriter.flush();
            txtWriter.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
