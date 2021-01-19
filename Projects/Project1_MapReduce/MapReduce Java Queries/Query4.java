package project1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

public class Query4{

    public static void main(String [] args) throws Exception {

        Configuration c = new Configuration();
        Job conf = Job.getInstance(c, "Query4");
        FileSystem.get(c).delete(new Path("/home/arpit/IdeaProjects/DS503/OutputQuery4"), true);
        conf.setJarByClass(Query4.class);
        conf.setJobName("Query4");
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);
        conf.setMapperClass(Query4.MapperQuery4.class);
        conf.setReducerClass(Query4.ReduceQuery4.class);
        conf.addCacheFile(new URI(args[0]));
        FileInputFormat.addInputPath(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        conf.waitForCompletion(true);
    }


    public static class MapperQuery4 extends Mapper<Object, Text, IntWritable, Text> {
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
            String countryCode = CustData[4];
            String Val = transValues[1]+","+transValues[2];
            context.write(new IntWritable(Integer.parseInt(countryCode)), new Text(Val));
        }
    }

    public static class ReduceQuery4 extends Reducer<IntWritable, Text, IntWritable, Text> {

        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String resultString="";
            float minTrans=1000000000;
            float maxTrans=-1;
            Set<String> custIds = new HashSet<String>();
            for (Text value : values)
            {
                String[] valSet = value.toString().split(",");
                custIds.add(valSet[0]);
                float transTotal = Float.parseFloat(String.valueOf(valSet[1]));
                minTrans = Math.min(minTrans,transTotal);
                maxTrans = Math.max(maxTrans,transTotal);

            }
            resultString=custIds.size()+","+String.valueOf(minTrans)+","+String.valueOf(maxTrans);
            context.write(key,new Text(resultString));

        }
    }
}
