package com.empresa.transactions;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransactionsPerCategory {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text catKey = new Text();

        @Override
        public void map(Object key, Text value, Context ctx)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Country")) return;
            String[] cols = line.split(";");
            if (cols.length != 10) return;
            String cat = cols[9].trim();
            if (!cat.isEmpty()) {
                catKey.set(cat);
                ctx.write(catKey, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : vals) sum += v.get();
            result.set(sum);
            ctx.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Transactions Per Category");
        job.setJarByClass(TransactionsPerCategory.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
