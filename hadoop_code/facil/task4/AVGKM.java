package hadoop_code.facil.task4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class AVGKM {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        String input_file = files[0];

        String output_dir = files[1];

        // arquivo de entrada
        Path input = new Path(input_file);

        // arquivo de saida
        Path output = new Path(output_dir);

        // criacao do job e seu nome
        Job j = new Job(c, "media de Km cidade e rodovia");

        // registro das classes
        j.setJarByClass(AVGKM.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (!line.startsWith("file")) {
                String[] colunas = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                double km = (!colunas[13].isEmpty()) ? Double.parseDouble(colunas[13]) : 0;
                con.write(new Text("AVG KM/L"), new DoubleWritable(km));
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            int contador = 0;
            double sum = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                contador += 1;
            }

            double avg = sum / contador;

            con.write(key, new DoubleWritable(avg));
        }
    }
}