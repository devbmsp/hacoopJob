package com.empresa.transactions;

import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvarageExportValueBrazil {
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> {
        private final Text yearKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Country")) return; // Pula o cabeçalho
            String[] cols = line.split(";");    //  Divide as linhas
            if (cols.length != 10) return; // Ignora linhas incompletas, valida apenas as com 10 colunas
            if ("Brazil".equals(cols[0].trim()) && "Export".equals(cols[4].trim())) { // Filtra exportacoes do Brasil
                String year = cols[1].trim();//  Pega o ano
                String amountStr = cols[8].trim(); // Pega o valor
                if (amountStr.isEmpty()) return; // Ignora linhas sem valor
                double amount = Double.parseDouble(amountStr); // Converte para double
                yearKey.set(year);
                context.write(yearKey, new DoubleWritable(amount)); // Emite chave-valor
            }
        }
    }
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            // Soma os valores e conta o numero de ocorrencias
            for (DoubleWritable value : values) {
                sum += value.get(); // Soma
                count++; // Conta
            }
            double average = sum / count; // Calculo da media
            context.write(key, new DoubleWritable(average)); // Chave e o valor médio
        }
    }
    // Define a classe principal, Mapper Reducer
    // Configura entrada e saida
    // Define os caminhos
    // Executa e finaliza
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Export Value Brazil");
        job.setJarByClass(AvarageExportValueBrazil.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
