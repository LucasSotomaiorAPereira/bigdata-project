package hadoop_code.dificil.task10;

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
import java.util.TreeMap;

public class ManufacturersCO2ReductionDriver extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(strings[0]);
        Path intermediate = new Path(strings[1]);
        Path output = new Path(strings[2]);

        Job job1 = Job.getInstance(conf, "CO2 Reduction Job 1");
        job1.setJarByClass(ManufacturersCO2ReductionDriver.class);

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        job1.setMapperClass(CO2Mapper.class);
        job1.setReducerClass(CO2Reducer.class);

        job1.setMapOutputKeyClass(ManufacturerYearKey.class);
        job1.setMapOutputValueClass(CarData.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(YearCO2Writable.class);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "CO2 Reduction Job 2");
            job2.setJarByClass(ManufacturersCO2ReductionDriver.class);

            FileInputFormat.addInputPath(job2, intermediate);
            FileSystem.get(conf).delete(output, true);
            FileOutputFormat.setOutputPath(job2, output);

            job2.setMapperClass(CO2Mapper2.class);
            job2.setReducerClass(CO2Reducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(YearCO2Writable.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);

            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new ManufacturersCO2ReductionDriver(), args);
        System.exit(result);
    }

    public static class CO2Mapper extends Mapper<LongWritable, Text, ManufacturerYearKey, CarData> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.startsWith("file")) {
                String[] colunas = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                int year = Integer.parseInt(colunas[1]);
                String manufacturer = colunas[2];
                double co2 = Double.parseDouble(colunas[18]);
                context.write(new ManufacturerYearKey(manufacturer, year), new CarData(manufacturer, year, co2));
            }
        }
    }

    public static class CO2Reducer extends Reducer<ManufacturerYearKey, CarData, Text, YearCO2Writable> {
        @Override
        public void reduce(ManufacturerYearKey key, Iterable<CarData> values, Context context)
                throws IOException, InterruptedException {
            double sumCO2 = 0;
            int count = 0;
            for (CarData cd : values) {
                sumCO2 += cd.getCo2Emissions();
                count++;
            }
            double avgCO2 = sumCO2 / count;
            context.write(new Text(key.getManufacturer().toString()), new YearCO2Writable(key.getYear(), avgCO2));
        }
    }

    public static class CO2Mapper2 extends Mapper<LongWritable, Text, Text, YearCO2Writable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String manufacturer = tokens[0];
            String[] yearCO2 = tokens[1].split("_");
            int year = Integer.parseInt(yearCO2[0]);
            double avgCO2 = Double.parseDouble(yearCO2[1]);
            context.write(new Text(manufacturer), new YearCO2Writable(year, avgCO2));
        }
    }

    public static class CO2Reducer2 extends Reducer<Text, YearCO2Writable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<YearCO2Writable> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<Integer, Double> yearCO2Map = new TreeMap<>();
            for (YearCO2Writable value : values) {
                yearCO2Map.put(value.getYear(), value.getAvgCO2());
            }

            double prevCO2 = 0;
            double totalReduction = 0;
            int count = 0;
            for (Double avgCO2 : yearCO2Map.values()) {
                if (count > 0) {
                    totalReduction += prevCO2 - avgCO2;
                }
                prevCO2 = avgCO2;
                count++;
            }

            double avgReduction = count > 1 ? totalReduction / (count - 1) : 0;
            context.write(key, new DoubleWritable(avgReduction));
        }
    }
}