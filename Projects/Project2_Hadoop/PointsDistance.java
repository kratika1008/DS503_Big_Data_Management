package DS503_Project2;

import java.lang.Math.*;

public class PointsDistance {
    public static Double getDistance(Float x2, Float y2, Float x1, Float y1){
        double distance=0;
        distance=Math.sqrt((x2-x1)*(x2-x1) + (y2-y1)*(y2-y1));
        return distance;
    }
}
