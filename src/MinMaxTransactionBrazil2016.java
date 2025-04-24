import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxTransactionBrazil2016 {

    public static class Brazil2016Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static Text minKey = new Text("MIN");
        private final static Text maxKey = new Text("MAX");

        public void map(Object key, Text val, Context context) throws IOException, InterruptedException {
            if (val.toString().startsWith("Country")) return;
            String[] fields = val.toString().split(";");
            if (fields.length != 10) return;

            if (fields[0].equalsIgnoreCase("Brazil") && fields[1].equals("2016")) {
                try {
                    double price = Double.parseDouble(fields[5]);
                    context.write(minKey, new DoubleWritable(price));
                    context.write(maxKey, new DoubleWritable(price));
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("MIN")) {
                double min = Double.MAX_VALUE;
                for (DoubleWritable val : values) {
                    if (val.get() < min) min = val.get();
                }
                context.write(new Text("Transação mais barata em 2016 no Brasil"), new DoubleWritable(min));
            } else {
                double max = Double.MIN_VALUE;
                for (DoubleWritable val : values) {
                    if (val.get() > max) max = val.get();
                }
                context.write(new Text("Transação mais cara em 2016 no Brasil"), new DoubleWritable(max));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Min Max Transaction Brazil 2016");

        job.setJarByClass(MinMaxTransactionBrazil2016.class);
        job.setMapperClass(Brazil2016Mapper.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
