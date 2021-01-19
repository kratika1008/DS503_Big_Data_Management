package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Problem3 {
        public static class ReadJsonMapper extends Mapper<Object, Text, Text, NullWritable> {
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                Text word = new Text();
                word.set(value);
                context.write(word, NullWritable.get());
            }
        }

        public static void main(String[] args) throws Exception {
            Path outPath = new Path(args[1]);
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "JobreadJson");
            FileSystem.get(conf).delete(new Path("C:/Users/DELL/Desktop/ds503/Assignment_2/output"),true);
            job.setJarByClass(Problem3.class);
            job.setMapperClass(ReadJsonMapper.class);
            job.setInputFormatClass(JSONInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
            String outputFile = outPath.toString()+"/"+"part-r-00000";

            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2,"aggregateJson");
            FileSystem.get(conf2).delete(new Path("C:/Users/DELL/Desktop/ds503/Assignment_2/output2"),true);
            job2.setJarByClass(Problem3.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setMapperClass(AggregateMapper.class);
            job2.setReducerClass(AggregateReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(outputFile));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            boolean res = job2.waitForCompletion(true);
            System.exit(res ? 0 : 1);
        }
        public static class AggregateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] words = line.split(",");
                String[] elevation = words[8].split(":");
                context.write(new Text(words[5]), new IntWritable(Integer.parseInt(elevation[1])));
            }
        }

        public static class AggregateReducer extends Reducer<Text, IntWritable, Text, Text> {
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int maxElevation = -1;
                int minElevation = 1000000000;
                for (IntWritable value : values) {
                    int elevation = value.get();
                    maxElevation = Math.max(elevation,maxElevation);
                    minElevation = Math.min(elevation,minElevation);
                }
                String result = String.valueOf(maxElevation)+","+String.valueOf(minElevation);
                context.write(key, new Text(result));
            }
        }
    }
