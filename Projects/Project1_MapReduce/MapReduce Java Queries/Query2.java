package project1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Query2{

    public static void main(String [] args) throws Exception {

        Configuration c = new Configuration();
        Job conf = Job.getInstance(c, "Query2");
        FileSystem.get(c).delete(new Path("/home/arpit/IdeaProjects/DS503/outputQuery2"), true);
        conf.setJarByClass(Query2.class);
        conf.setJobName("Query2");
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);
        conf.setMapperClass(Query2.MapperQuery2.class);
        conf.setCombinerClass(Query2.CombinerQuery2.class);
        conf.setReducerClass(Query2.ReduceQuery2.class);
        conf.addCacheFile(new URI(args[0]));
        FileInputFormat.addInputPath(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        conf.waitForCompletion(true);
    }


    public static class MapperQuery2 extends Mapper<Object, Text, IntWritable, Text> {
        Map<String,String>  custIdToCustomer = new TreeMap<String,String>();

        public void setup(Context con) throws IOException, InterruptedException {
            URI[] custFile = con.getCacheFiles();
            if (custFile != null && custFile.length > 0) {
                FileSystem fs = FileSystem.get(con.getConfiguration());
                Path path = new Path(custFile[0].toString());
                BufferedReader bfreader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String custRec = "";
                while ((custRec = bfreader.readLine()) != null) {
                    String[] custData = custRec.split(",");
                    String custId = custData[0];
                    if(!custIdToCustomer.containsKey(custId))
                        custIdToCustomer.put(custId,custRec);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String transString = value.toString();
            String[] transValues = transString.split(",");
            String[] CustData = custIdToCustomer.get(transValues[1]).split(",");
            String custName = CustData[1];
            String Val = transValues[1]+","+custName+","+transValues[2];
            context.write(new IntWritable(Integer.parseInt(transValues[1])), new Text(Val));
        }
    }

    public static class CombinerQuery2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float transSum = 0;
            int noOfTrans = 0;
            String resultString="";

            for (Text value : values)
            {
                String[] valSet = value.toString().split(",");
                transSum+=Float.parseFloat(valSet[2]);
                noOfTrans++;
                resultString=valSet[0]+","+valSet[1]+","+String.valueOf(noOfTrans)+","+String.valueOf(transSum);
            }

            context.write(key,new Text(resultString));

        }
    }
    public static class ReduceQuery2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float transSum=0;
            int noOfTrans = 0;
            String resultString;
            String custId="";
            String custName="";
            for (Text value : values)
            {
                String[] valSet = value.toString().split(",");
                transSum+=Float.parseFloat(valSet[3]);
                noOfTrans += Integer.parseInt(valSet[2]);
                custId=valSet[0];
                custName=valSet[1];
            }
            resultString=custId+","+custName+","+String.valueOf(noOfTrans)+","+String.valueOf(transSum);
            context.write(key,new Text(resultString));
        }
    }

}
