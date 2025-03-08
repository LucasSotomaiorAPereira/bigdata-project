package hadoop_code.medios.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AVGCO2MANUFACTUREdriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new AVGCO2MANUFACTUREdriver(), args);

        System.exit(result);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(strings[0]);
        Path output = new Path(strings[1]);

        Job job = Job.getInstance(conf);
        job.setJobName("Fuel ");


        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(AVGCO2MANUFACTUREdriver.class);
        job.setMapperClass(FuelDistanceMapper.class);
        job.setCombinerClass(FuelDistanceCombiner.class);
        job.setReducerClass(FuelDistanceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CO2ManufactureWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FuelDistanceMapper extends Mapper<LongWritable, Text, Text, CO2ManufactureWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.startsWith("file")) {
                String[] colunas = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if(colunas.length >= 14) {
                    String manufacturer = colunas[2];
                    double co2 = Double.parseDouble(colunas[18]);
                    con.write(new Text(manufacturer), new CO2ManufactureWritable(co2, 1));
                }
            }
        }
    }

    public static class FuelDistanceCombiner extends Reducer<Text, CO2ManufactureWritable, Text, CO2ManufactureWritable> {
        public void reduce(Text key, Iterable <CO2ManufactureWritable> values, Context con) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;

            for (CO2ManufactureWritable co2 : values) {
                count += co2.getCount();
                sum += co2.getCo2();
            }

            con.write(key, new CO2ManufactureWritable(sum, count));
        }
    }

    public static class FuelDistanceReducer extends Reducer<Text, CO2ManufactureWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable <CO2ManufactureWritable> values, Context con) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;

            for(CO2ManufactureWritable val : values) {
                sum += val.getCo2();
                count += val.getCount();
            }

            con.write(key, new DoubleWritable(sum / count));
        }
    }
}
