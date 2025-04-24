package com.empresa.transactions;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxPricePerYearCountry {
    public static class Map extends Mapper<Object, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text> {
        private final org.apache.hadoop.io.Text yearCountryKey = new org.apache.hadoop.io.Text();
        private final org.apache.hadoop.io.Text priceAmount = new org.apache.hadoop.io.Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           // Chave: combinacao de ano e pais separados por ponto e virgula
            String line = value.toString();
            if (line.startsWith("Country")) return; // Ignora o cabecalho
            String[] cols = line.split(";"); // Separa as linhas em colunas
            if (cols.length != 10) return; // Valida se ha 10 colunas

            String year = cols[1].trim();
            String country = cols[0].trim();
            String price = cols[5].trim();
            String amount = cols[8].trim();

            if (price.isEmpty() || amount.isEmpty()) return;
            // Emite as chaves ano ; pais e o valor preco ; quantidade
            yearCountryKey.set(year + ";" + country);
            priceAmount.set(price + ";" + amount);
            context.write(yearCountryKey, priceAmount);
        }
    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            // Inicializa minPrice e maxPrice com valores extremos
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;
            // Divide valor em preco e quantidade
            String minAmount = "";
            String maxAmount = "";

            for (Text value : values) {
                String[] parts = value.toString().split(";");
                double price = Double.parseDouble(parts[0]);
                String amount = parts[1];
                // Atualiza minPrice e maxPrice conforme necessario
                if (price < minPrice) {
                    minPrice = price;
                    minAmount = amount;
                }
                if (price > maxPrice) {
                    maxPrice = price;
                    maxAmount = amount;
                }
            }
            // Formata o resultado com precos e quantidades maximas e minimas
            String result = "MinPrice: " + minPrice + " (Amount: " + minAmount +"), " +
                    "MaxPrice: " + maxPrice + " (Amount: " + maxAmount + ")";
            // Emite o resultado "Min Price: XXX  (Amount: YYY), Max Price: XXX (Amount: YYY)"
            context.write (key, new Text(result));
        }
    }
   // Configura os tipos de entrada e saida e executa
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Min Max Price Per Year Country");
        job.setJarByClass(MinMaxPricePerYearCountry.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
