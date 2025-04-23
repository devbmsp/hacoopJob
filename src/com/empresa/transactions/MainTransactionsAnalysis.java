package com.empresa.transactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainTransactionsAnalysis {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Uso: MainTransactionsAnalysis <input> <outBrazil> <outYear> <outCategory>");
            System.exit(2);
        }
        Path in  = new Path(args[0]);
        Path bOut= new Path(args[1]);
        Path yOut= new Path(args[2]);
        Path cOut= new Path(args[3]);
        Configuration conf = new Configuration();

        // Job 1
        Job job1 = Job.getInstance(conf, "Brasil");
        job1.setJarByClass(MainTransactionsAnalysis.class);
        job1.setMapperClass(BrazilTransactions.Map.class);
        job1.setCombinerClass(BrazilTransactions.Reduce.class);
        job1.setReducerClass(BrazilTransactions.Reduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, in);
        FileOutputFormat.setOutputPath(job1, bOut);
        if (!job1.waitForCompletion(true)) System.exit(1);

        // Job 2
        Job job2 = Job.getInstance(conf, "PorAno");
        job2.setJarByClass(MainTransactionsAnalysis.class);
        job2.setMapperClass(TransactionsPerYear.Map.class);
        job2.setCombinerClass(TransactionsPerYear.Reduce.class);
        job2.setReducerClass(TransactionsPerYear.Reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, in);
        FileOutputFormat.setOutputPath(job2, yOut);
        if (!job2.waitForCompletion(true)) System.exit(1);

        // Job 3
        Job job3 = Job.getInstance(conf, "PorCategoria");
        job3.setJarByClass(MainTransactionsAnalysis.class);
        job3.setMapperClass(TransactionsPerCategory.Map.class);
        job3.setCombinerClass(TransactionsPerCategory.Reduce.class);
        job3.setReducerClass(TransactionsPerCategory.Reduce.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, in);
        FileOutputFormat.setOutputPath(job3, cOut);
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
