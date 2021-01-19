package DS503_Project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem4 {

    public static void main(String [] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("k",args[2]);
        conf.set("r",args[3]);
        Job job = Job.getInstance(conf, "Problem4");
        FileSystem.get(conf).delete(new Path("/home/arpit/IdeaProjects/DS503/Output/Problem4"), true);
        job.setJarByClass(Problem4.class);
        job.setJobName("Problem4");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(Problem4.OutlierDetectMapper.class);
        job.setReducerClass(Problem4.OutlierDetectReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static class OutlierDetectMapper extends Mapper<Object, Text, Text, Text> {
        Float radius;
        Integer threshold;
        Integer minGridVal = 0;
        Integer maxGridVal = 10000;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String error = "Either of the input argument radius r or threshold k is missing.";
            try
            {
                radius = Float.parseFloat(conf.get("r"));
                threshold = Integer.parseInt(conf.get("k"));
            }
            catch(NullPointerException e)
            {
                throw new NullPointerException(error);
            }
            catch(NumberFormatException e){
                throw new NumberFormatException(error);
            }
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> gridPartitions = GenerateGrids.divideIntoGrids(1, 10000, 2500);

            String[] eachPoint = value.toString().split(",");
            int x = Integer.parseInt(eachPoint[0]);
            int y = Integer.parseInt(eachPoint[1]);
            for (int i = 0; i < gridPartitions.size(); i++) {
                String[] gridDef = gridPartitions.get(i).split(",");
                String gridNum = gridDef[0];
                float gridX1 = Integer.parseInt(gridDef[1]);
                float gridY1 = Integer.parseInt(gridDef[2]);
                float gridX2 = Integer.parseInt(gridDef[3]);
                float gridY2 = Integer.parseInt(gridDef[4]);
                float supRegX1 = 0;
                float supRegY1 = 0;
                float supRegX2 = 0;
                float supRegY2 = 0;
                if (gridX1 != minGridVal) {
                    supRegX1 = gridX1 - radius;
                }
                if (gridY1 != minGridVal) {
                    supRegY1 = gridY1 - radius;
                }
                if (gridX2 != maxGridVal) {
                    supRegX2 = gridX2 + radius;
                }
                if (gridY2 != maxGridVal) {
                    supRegY2 = gridY2 + radius;
                }
                if ((x >= gridX1) && (x <= gridX2) && (y >= gridY1) && (y <= gridY2)) {
                    String valString = "M~" + String.valueOf(value);
                    context.write(new Text(gridNum), new Text(valString));
                }
                if ((x >= supRegX1) && (x <= supRegX2) && (y >= supRegY1) && (y <= supRegY2) && ((x < gridX1) || (x > gridX2) || (y < gridY1) || (y > gridY2))) {
                    String valString = "S~" + String.valueOf(value);
                    context.write(new Text(gridNum), new Text(valString));
                }

            }

        }
    }

    public static class OutlierDetectReducer extends Reducer<Text, Text, Text, NullWritable> {
        Float radius;
        Integer threshold;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String error = "Either of the input argument radius r or threshold k is missing.";
            try
            {
                radius = Float.parseFloat(conf.get("r"));
                threshold = Integer.parseInt(conf.get("k"));
            }
            catch(NullPointerException e)
            {
                throw new NullPointerException(error);
            }
            catch(NumberFormatException e){
                throw new NumberFormatException(error);
            }
        }
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> MainGridPoints = new ArrayList<>();
            List<String> AllPoints = new ArrayList<>();
            String[] valueArr = new String[0];

            for (Text value : values) {
                valueArr = value.toString().split("~");
                if (valueArr[0].equalsIgnoreCase("M")) {
                    MainGridPoints.add(valueArr[1]);
                    AllPoints.add(valueArr[1]);
                } else {
                    AllPoints.add(valueArr[1]);
                }
            }
            for (String point : MainGridPoints) {
                List<String> distanceList = new ArrayList<>();
                String[] pointCoord = point.split(",");
                float pointX = Float.parseFloat(pointCoord[0]);
                float pointY = Float.parseFloat(pointCoord[1]);
                for (String neighbourPt : AllPoints) {
                    String[] Ncoord = neighbourPt.split(",");
                    float nPointX1 = Float.parseFloat(Ncoord[0]);
                    float nPointY1 = Float.parseFloat(Ncoord[1]);
                    Double distance = PointsDistance.getDistance(nPointX1, nPointY1, pointX, pointY);
                    if (distance <= radius) {
                        distanceList.add(neighbourPt);
                    }
                }
                if ((distanceList.size() - 1) < threshold) {
                    context.write(new Text(point), NullWritable.get());
                }
            }
        }
    }
}