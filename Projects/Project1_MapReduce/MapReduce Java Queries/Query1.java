package project1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 {

    public static void main(String [] args) throws Exception
    {
        Configuration c=new Configuration();
        Job conf = Job.getInstance(c,"query1");
        FileSystem.get(c).delete(new Path("/home/arpit/IdeaProjects/DS503/output"), true);
        conf.setJarByClass(Query1.class);
        conf.setJobName("JobQuery1");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(MapQuery1.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        System.exit(conf.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapQuery1 extends Mapper<Object, Text, IntWritable, Text>{



        public void map(Object key, Text value, Context con) throws IOException, InterruptedException
        {
            String custDataString = value.toString();
            String[] custDataSet =custDataString.split(",");
            int custAge = Integer.parseInt(custDataSet[2]);
            if(custAge>=20 && custAge<=50){
                con.write(new IntWritable(Integer.parseInt(custDataSet[0])),new Text(custDataString));
            }
        }
    }

}
