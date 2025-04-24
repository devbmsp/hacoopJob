import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTransactionBrazil {
    public static class BrazilMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text year = new Text();
        private DoubleWritable price = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("Country")) return;
            String[] fields = value.toString().split(";");
            if (fields.length != 10) return;

            if (!fields[0].equalsIgnoreCase("Brazil")) return;

            try {
                year.set(fields[1]);
                price.set(Double.parseDouble(fields[5]));
                context.write(year, price);
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double average = sum / count;
                context.write(key, new DoubleWritable(average));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Transaction in Brazil");

        job.setJarByClass(AverageTransactionBrazil.class);
        job.setMapperClass(BrazilMapper.class);
        job.setReducerClass(AverageReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
