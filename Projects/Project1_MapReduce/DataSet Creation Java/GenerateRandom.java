import java.util.*;
import java.lang.*;

public class GenerateRandom {

  public static String generateRandomString(Random rand,int minLen,int maxLen){
    int l_Limit = 97; //char a
    int r_Limit = 122; //char z
    int len = rand.nextInt(maxLen - minLen +1) + minLen;
    StringBuilder buffer = new StringBuilder(len);
    for (int j = 0; j < len; j++) {
        int randomLimitedInt = l_Limit + (int)
          (rand.nextFloat() * (r_Limit - l_Limit + 1));
        buffer.append((char) randomLimitedInt);
    }
    String stringValue = buffer.toString();
    return stringValue;
  }
  public static int getRandomNumber(Random rand,int minVal,int maxVal){
    int value = rand.nextInt(maxVal - minVal +1) + minVal;
    return value;
  }
}
