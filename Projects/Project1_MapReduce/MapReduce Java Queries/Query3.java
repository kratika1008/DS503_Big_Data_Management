package project1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3{

    public static void main(String [] args) throws Exception {

        Configuration c = new Configuration();
        Job conf = Job.getInstance(c, "Query3");
        FileSystem.get(c).delete(new Path("/home/arpit/IdeaProjects/DS503/OutputQuery3"), true);
        conf.setJarByClass(Query3.class);
        conf.setJobName("Query3");
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormatClass(TextInputFormat.class);
        conf.setOutputFormatClass(TextOutputFormat.class);
        conf.setReducerClass(Query3.ReduceQuery3.class);
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Query3.CustMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Query3.TransMapper.class);
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        conf.waitForCompletion(true);
    }


    public static class CustMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String custString = value.toString();
            String[] custValues = custString.split(",");
            String valueString = "C~"+custString;
            context.write(new IntWritable(Integer.parseInt(custValues[0])), new Text(valueString));
        }
    }

    public static class TransMapper extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String transString = value.toString();
            String[] transValues = transString.split(",");
            String valueString = "T~"+transString;
            context.write(new IntWritable(Integer.parseInt(transValues[1])), new Text(valueString));
        }
    }
    public static class ReduceQuery3 extends Reducer<IntWritable, Text, IntWritable, Text> {
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float transSum = 0;
            int noOfTrans = 0;
            String tag;
            String[] valueSet;
            String resultString="";
            String custName="";
            String salary="";
            int leastMinItem=11;

            for (Text value : values)
            {
                valueSet = value.toString().split("~");
                tag = valueSet[0];
                if(tag.equalsIgnoreCase("C")){
                    String[] custData  = valueSet[1].split(",");
                    custName = custData[1];
                    salary = custData[5];
                }
                else if(tag.equalsIgnoreCase("T")){
                    String[] transData  = valueSet[1].split(",");
                    transSum+=Float.parseFloat(transData[2]);
                    noOfTrans++;
                    int min_item = Integer.parseInt(transData[3]);
                    leastMinItem = Math.min(min_item, leastMinItem);

                }
            }
            resultString=String.valueOf(key)+","+custName+","+salary+","+String.valueOf(noOfTrans)+","+String.valueOf(transSum)+","+String.valueOf(leastMinItem);
            context.write(key,new Text(resultString));

        }
    }

}
