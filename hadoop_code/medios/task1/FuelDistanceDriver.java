package hadoop_code.medios.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class FuelDistanceDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new FuelDistanceDriver(), args);

        System.exit(result);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(strings[0]);
        Path output = new Path(strings[1]);

        Job job = Job.getInstance(conf);
        job.setJobName("Fuel Distance Driver");


        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(FuelDistanceDriver.class);
        job.setMapperClass(FuelDistanceMapper.class);
        job.setCombinerClass(FuelDistanceCombiner.class);
        job.setReducerClass(FuelDistanceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ModelKmWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FuelDistanceMapper extends Mapper<LongWritable, Text, Text, ModelKmWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.startsWith("file")) {
                String[] colunas = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if(colunas.length >= 14) {
                    double km = (!colunas[13].isEmpty()) ? Double.parseDouble(colunas[13]) : 0;
                    String fuel = colunas[10];
                    String model = colunas[3];
                    con.write(new Text(fuel), new ModelKmWritable(model, km));
                }
            }
        }
    }

    public static class FuelDistanceCombiner extends Reducer<Text, ModelKmWritable, Text, ModelKmWritable> {
        public void reduce(Text key, Iterable <ModelKmWritable> values, Context con) throws IOException, InterruptedException {
            double maior = 0;
            String model = "";

            for (ModelKmWritable val : values) {
                if(val.getKm() > maior) {
                    maior = val.getKm();
                    model = val.getModel();
                }
            }

            con.write(key, new ModelKmWritable(model, maior));
        }
    }

    public static class FuelDistanceReducer extends Reducer<Text, ModelKmWritable, Text, Text> {
        public void reduce(Text key, Iterable <ModelKmWritable> values, Context con) throws IOException, InterruptedException {
            double maior = 0;
            String model = "";

            for (ModelKmWritable val : values) {
                if(val.getKm() > maior) {
                    maior = val.getKm();
                    model = val.getModel();
                }
            }

            con.write(key, new Text(model));
        }
    }
}
