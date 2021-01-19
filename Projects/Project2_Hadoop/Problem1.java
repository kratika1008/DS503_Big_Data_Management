package DS503_Project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem1 {

    public static void main(String [] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("window",args[3]);
        Job job = Job.getInstance(conf, "Problem1");
        FileSystem.get(conf).delete(new Path("/home/arpit/IdeaProjects/DS503/Output/Problem1"), true);
        job.setJarByClass(Problem1.class);
        job.setJobName("Problem1");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(Problem1.SpatialJoinReducer.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Problem1.PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Problem1.RectangleMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

    public static class PointMapper extends Mapper<Object, Text, Text, Text> {
        private String window;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            window = conf.get("window");
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> gridPartitions = GenerateGrids.divideIntoGrids(1,10000,2500);

            //defined a space window
            int windowX1=0;
            int windowY1=0;
            int windowX2=0;
            int windowY2=0;
            if(window!=null && window!="") {
                String[] windowAtt = window.split(",");
                windowX1 = Integer.parseInt(windowAtt[0]);
                windowY1 = Integer.parseInt(windowAtt[1]);
                windowX2 = Integer.parseInt(windowAtt[2]);
                windowY2 = Integer.parseInt(windowAtt[3]);
            }

            String[] eachPoint = value.toString().split(",");
            int x = Integer.parseInt(eachPoint[0]);
            int y = Integer.parseInt(eachPoint[1]);
            for(int i=0;i<gridPartitions.size();i++) {
                String[] gridDef = gridPartitions.get(i).split(",");
                String gridNum = gridDef[0];
                int gridX1 = Integer.parseInt(gridDef[1]);
                int gridY1 = Integer.parseInt(gridDef[2]);
                int gridX2 = Integer.parseInt(gridDef[3]);
                int gridY2 = Integer.parseInt(gridDef[4]);
                if ((x >= gridX1) && (x <= gridX2) && (y >= gridY1) && (y <= gridY2)){
                    if(window != null && window != ""){
                        if((x >= windowX1) && (x <= windowX2) && (y >= windowY1) && (y <= windowY2)){
                            context.write(new Text(gridNum), value);
                            break;
                        }
                    }
                    else{
                        context.write(new Text(gridNum), value);
                        break;
                    }
                }
            }
        }
    }

    public static class RectangleMapper extends Mapper<Object, Text, Text, Text> {
        private String window;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            window = conf.get("window");
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> gridPartitions = GenerateGrids.divideIntoGrids(1,10000,2500);
            //fetch window coordinates
            int windowX1=0;
            int windowY1=0;
            int windowX2=0;
            int windowY2=0;
            if(window!=null && window!="") {
                String[] windowAtt = window.split(",");
                windowX1 = Integer.parseInt(windowAtt[0]);
                windowY1 = Integer.parseInt(windowAtt[1]);
                windowX2 = Integer.parseInt(windowAtt[2]);
                windowY2 = Integer.parseInt(windowAtt[3]);
            }

            String[] eachRec = value.toString().split(",");
            int x1 = Integer.parseInt(eachRec[1]);
            int y1 = Integer.parseInt(eachRec[2]);
            int h = Integer.parseInt(eachRec[3]);
            int w = Integer.parseInt(eachRec[4]);
            int x2 = x1+w;
            int y2 = y1;
            int x3 = x1;
            int y3 = y1+h;
            int x4 = x2;
            int y4 = y3;

            for(int i=0;i<gridPartitions.size();i++) {
                String[] gridDef = gridPartitions.get(i).split(",");
                String gridNum = gridDef[0];
                int gridX1 = Integer.parseInt(gridDef[1]);
                int gridY1 = Integer.parseInt(gridDef[2]);
                int gridX2 = Integer.parseInt(gridDef[3]);
                int gridY2 = Integer.parseInt(gridDef[4]);

                if (((x1 >= gridX1) && (x1 <= gridX2) && (y1 >= gridY1) && (y1 <= gridY2)) ||
                        ((x2 >= gridX1) && (x2 <= gridX2) && (y2 >= gridY1) && (y2 <= gridY2)) ||
                        ((x3 >= gridX1) && (x3 <= gridX2) && (y3 >= gridY1) && (y3 <= gridY2)) ||
                        ((x4 >= gridX1) && (x4 <= gridX2) && (y4 >= gridY1) && (y4 <= gridY2)))
                {
                    if(window != null && window != "")
                    {
                        if(((x1 >= windowX1) && (x1 <= windowX2) && (y1 >= windowY1) && (y1 <= windowY2)) ||
                                ((x2 >= windowX1) && (x2 <= windowX2) && (y2 >= windowY1) && (y2 <= windowY2)) ||
                                ((x3 >= windowX1) && (x3 <= windowX2) && (y3 >= windowY1) && (y3 <= windowY2)) ||
                                ((x4 >= windowX1) && (x4 <= windowX2) && (y4 >= windowY1) && (y4 <= windowY2))) {
                            String rectangleCoords = eachRec[0] + "," + String.valueOf(x1) + "," + String.valueOf(y1) + "," + String.valueOf(x4) + "," + String.valueOf(y4);
                            context.write(new Text(gridNum), new Text(rectangleCoords));
                        }
                    }
                    else{
                        String rectangleCoords = eachRec[0] + "," + String.valueOf(x1) + "," + String.valueOf(y1) + "," + String.valueOf(x4) + "," + String.valueOf(y4);
                        context.write(new Text(gridNum), new Text(rectangleCoords));
                    }
                }
            }
        }
    }

    public static class SpatialJoinReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> pointList = new ArrayList<>();
            List<String> rectangleList = new ArrayList<>();
            for(Text value: values){
                String[] valueArr = value.toString().split(",");
                if(valueArr.length==2){
                    pointList.add(value.toString());
                }
                else{
                    rectangleList.add(value.toString());
                }
            }
            for(String point:pointList){
                String[] pointCoord = point.split(",");
                int pointX = Integer.parseInt(pointCoord[0]);
                int pointY = Integer.parseInt(pointCoord[1]);
                for(String rectangle:rectangleList){
                    String[] rectCoord = rectangle.split(",");
                    int recX1 = Integer.parseInt(rectCoord[1]);
                    int recY1 = Integer.parseInt(rectCoord[2]);
                    int recX2 = Integer.parseInt(rectCoord[3]);
                    int recY2 = Integer.parseInt(rectCoord[4]);
                    if ((pointX >= recX1) && (pointX <= recX2) && (pointY >= recY1) && (pointY <= recY2)) {
                        String resultVal = "("+pointCoord[0]+","+pointCoord[1]+")";
                        context.write(new Text(rectCoord[0]), new Text(resultVal));
                    }
                }
            }
        }
    }
}
