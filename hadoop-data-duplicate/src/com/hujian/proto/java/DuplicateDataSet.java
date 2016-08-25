package com.hujian.proto.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class DuplicateDataSet {
    //Mapper
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        //implement map
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                context.write(new Text(itr.nextToken()),new Text(""));
            }
        }
    }
    //Reducer
    public static class DuplicateReducer extends Reducer<Text,Text,Text,Text> {
        //implement reduce
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }
    public static void main(String[] args) throws Exception {
        //setup the job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DuplicateDataSet");
        job.setJarByClass(DuplicateDataSet.class);

        //the mapper/reducer/combiner
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(DuplicateReducer.class);
        job.setReducerClass(DuplicateReducer.class);

        //set the reducer output key/value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //set the in/out path
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //run this job!
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}