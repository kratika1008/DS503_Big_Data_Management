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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Query5{

    public static void main(String [] args) throws Exception {

        Configuration c = new Configuration();
        Job conf = Job.getInstance(c, "Query5");
        FileSystem.get(c).delete(new Path("/home/arpit/IdeaProjects/DS503/OutputQuery5"), true);
        conf.setJarByClass(Query5.class);
        conf.setJobName("Query5");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);
        conf.setMapperClass(Query5.MapperQuery5.class);
        conf.setReducerClass(Query5.ReduceQuery5.class);
        conf.addCacheFile(new URI(args[0]));
        FileInputFormat.addInputPath(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        conf.waitForCompletion(true);
    }


    public static class MapperQuery5 extends Mapper<Object, Text, Text, Text> {
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
            String[] custValues = custIdToCustomer.get(transValues[1]).split(",");
            int custAge = Integer.parseInt(custValues[2]);
            String gender = custValues[3];
            String ageRange="";

            if(custAge>=10&&custAge<20)
                ageRange = "[10,20)";
            else if(custAge>=20&&custAge<30)
                ageRange = "[20,30)";
            else if(custAge>=30&&custAge<40)
                ageRange = "[30,40)";
            else if(custAge>=40&&custAge<50)
                ageRange = "[40,50)";
            else if(custAge>=50&&custAge<60)
                ageRange = "[50,60)";
            else if(custAge>=60&&custAge<=70)
                ageRange = "[60,70]";
            String new_key = ageRange+"-"+gender;
            String valueString = new_key+";"+transValues[2];
            context.write(new Text(new_key), new Text(valueString));
        }
    }

    public static class ReduceQuery5 extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String new_key="";
            float maxTrans = 0;
            float minTrans = 100000000;
            int noOfTrans = 0;
            float totalTransSum = 0;
            float avgTrans = 0;
            String result;
            for(Text value : values){
                String[] transData = value.toString().split(";");
                float transTotal = Float.parseFloat(transData[1]);
                totalTransSum += transTotal;
                noOfTrans++;
                maxTrans = Math.max(transTotal, maxTrans);
                minTrans = Math.min(transTotal, minTrans);
            }
            avgTrans = totalTransSum/noOfTrans;
            result = String.valueOf(minTrans)+";"+String.valueOf(maxTrans)+";"+String.valueOf(avgTrans);
            context.write(key,new Text(result));
        }
    }
}
