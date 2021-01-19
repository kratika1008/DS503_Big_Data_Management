package DS503_Project2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Problem2 {

    public static void main(String [] args) throws Exception {

        Configuration conf = new Configuration();
        //set max number of iterations to stop the job
        int iterations = 6;
        String outputFileName = "part-r-00000";
        //for the first iteration, the job will consider centroid file that we randomly generate
        URI inPath = new URI(args[0]);
        Path outPath =  null;
        for (int i = 0; i<iterations; ++i) {
            outPath = new Path(args[2]+i);
            //Job configuration
            Job job = Job.getInstance(conf, "Problem2");
            job.setJarByClass(Problem2.class);
            job.setJobName("Problem2");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapperClass(Problem2.KMeansMapper.class);
            job.setReducerClass(Problem2.KMeansReducer.class);
            job.addCacheFile(inPath);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, outPath);
            job.waitForCompletion(true);

            //check if current centroids are same as the centroids found in the last iteration
            FileSystem fs1 = FileSystem.get(conf);
            String outfileString = outPath.toString() + "/" + outputFileName;
            BufferedReader bfreader1 = new BufferedReader(new InputStreamReader(fs1.open(new Path(inPath))));
            BufferedReader bfreader2 = new BufferedReader(new InputStreamReader(fs1.open(new Path(outfileString))));

            ArrayList<String> currentCentroids = new ArrayList<String>();
            ArrayList<String> LastCentroids = new ArrayList<String>();
            while (bfreader1.readLine() != null) {
                currentCentroids.add(bfreader1.readLine());
            }
            while (bfreader2.readLine() != null) {
                LastCentroids.add(bfreader2.readLine());
            }
            if(currentCentroids.containsAll(LastCentroids)){
                break;
            }
            //replace the path of new centroid Broadcast file to the path of last output file
            inPath = new URI(outPath+"/"+outputFileName);

        }
    }

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            URI[] kmeansPoints = context.getCacheFiles();
            ArrayList<String> centroids = new ArrayList<String>();
            if (kmeansPoints != null && kmeansPoints.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(kmeansPoints[0].toString());
                BufferedReader bfreader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String centroidPoint = "";
                while ((centroidPoint = bfreader.readLine()) != null) {
                    centroids.add(centroidPoint);
                }
            }
            String[] EachPointString = value.toString().split(",");
            float x = Float.parseFloat(EachPointString[0]);
            float y = Float.parseFloat(EachPointString[1]);
            double minimumDistance = 100000000.0;
            Map<Double,String> centroid_Distance_Map = new HashMap<Double,String>();
            for(int i=0;i<centroids.size();i++){
                String[] centroidpt = centroids.get(i).split(",");
                float centroidX = Float.parseFloat(centroidpt[0]);
                float centroidY = Float.parseFloat(centroidpt[1]);
                double distance = PointsDistance.getDistance(centroidX,centroidY,x,y);
                centroid_Distance_Map.put(distance,centroids.get(i));
                minimumDistance = Math.min(minimumDistance,distance);
            }
            String nearestCentroid = centroid_Distance_Map.get(minimumDistance);
            context.write(new Text(nearestCentroid), new Text(value.toString()));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, NullWritable> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float newCentroidX = 0;
            float newCentroidY = 0;
            String newCentroid = "";
            int count = 0;
            float X_Total=0;
            float Y_Total=0;
            DecimalFormat dformat = new DecimalFormat("#.##");
            for (Text value : values)
            {
                String[] eachPoint = value.toString().split(",");
                X_Total += Float.parseFloat(eachPoint[0]);
                Y_Total += Float.parseFloat(eachPoint[1]);
                count++;
            }
            newCentroidX = Float.parseFloat(dformat.format(X_Total/count));
            newCentroidY = Float.parseFloat(dformat.format(Y_Total/count));
            newCentroid = String.valueOf(newCentroidX)+","+String.valueOf(newCentroidY);
            context.write(new Text(newCentroid), NullWritable.get());
        }
    }

}
