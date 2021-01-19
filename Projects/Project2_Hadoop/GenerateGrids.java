package DS503_Project2;

import java.util.ArrayList;
import java.util.List;

public class GenerateGrids {

    public static List<String> divideIntoGrids(Integer minPoint, Integer maxPoint, Integer gridSize) {
        List<String> grids = new ArrayList<>();
        int gridCount = 1;
        for (int i = 0; i < maxPoint; i = i + gridSize) {
            for (int j = 0; j < maxPoint; j = j + gridSize) {
                String grid = "g";
                int gridX = i;
                int gridY = j;
                int gridX2 = gridX+gridSize;
                int gridY2 = gridY+gridSize;
                grid = grid + String.valueOf(gridCount) + "," + String.valueOf(gridX) + "," + String.valueOf(gridY) + "," + String.valueOf(gridX2) + "," + String.valueOf(gridY2);
                grids.add(grid);
                gridCount++;
            }

        }
        return grids;
    }
}
