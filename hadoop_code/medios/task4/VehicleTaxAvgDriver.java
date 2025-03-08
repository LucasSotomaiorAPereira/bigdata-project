package hadoop_code.medios.task4;

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

public class VehicleTaxAvgDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(conf);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(VehicleTaxAvgDriver.class);
        job.setMapperClass(Task7Mapper.class);
        job.setCombinerClass(Task7Combiner.class);
        job.setReducerClass(Task7Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VehicleTaxWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VehicleTaxWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new VehicleTaxAvgDriver(), args);
        System.exit(result);
    }

    public static class Task7Mapper extends Mapper<LongWritable, Text, Text, VehicleTaxWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.startsWith("file,")) {
                String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if (columns.length > 29) {
                    String model = columns[3];
                    model = model.replace("\"", "");
                    String default12months = columns[27];
                    String default6months = columns[26];
                    String first12months = columns[29];
                    String first6months = columns[28];
                    double d1 = (!default6months.isEmpty()) ? Double.parseDouble(default6months) : 0;
                    double d2 = (!default12months.isEmpty()) ? Double.parseDouble(default12months) : 0;
                    double f1 = (!first6months.isEmpty()) ? Double.parseDouble(first6months) : 0;
                    double f2 = (!first12months.isEmpty()) ? Double.parseDouble(first12months) : 0;
                    con.write(new Text(model), new VehicleTaxWritable(d1, d2, f1, f2));
                }
            }
        }

    }

    public static class Task7Combiner extends Reducer<Text, VehicleTaxWritable, Text, VehicleTaxWritable> {

        public void reduce(Text key, Iterable<VehicleTaxWritable> values, Context con) throws IOException, InterruptedException {
            double totald6m = 0;
            double totald12m = 0;
            double totalf6m = 0;
            double totalf12m = 0;
            for (VehicleTaxWritable v : values) {
                totald6m += v.getDefault6Months();
                totald12m += v.getDefault12Months();
                totalf6m += v.getFirst6Months();
                totalf12m += v.getFirst12Months();
            }
            con.write(key, new VehicleTaxWritable(totald6m, totald12m, totalf6m, totalf12m));
        }

    }

    public static class Task7Reducer extends Reducer<Text, VehicleTaxWritable, Text, VehicleTaxWritable> {

        public void reduce(Text key, Iterable<VehicleTaxWritable> values, Context con) throws IOException, InterruptedException {
            double totald6m = 0;
            double totald12m = 0;
            double totalf6m = 0;
            double totalf12m = 0;
            int count = 0;
            for (VehicleTaxWritable v : values) {
                count++;
                totald6m += v.getDefault6Months();
                totald12m += v.getDefault12Months();
                totalf6m += v.getFirst6Months();
                totalf12m += v.getFirst12Months();
            }
            double avgd6m = totald6m / count;
            double avgd12m = totald12m / count;
            double avgf6m = totalf6m / count;
            double avgf12m = totalf12m / count;
            if (avgd6m != 0 && avgd12m != 0 && avgf6m != 0 && avgf12m != 0) {
                con.write(key, new VehicleTaxWritable(avgd6m, avgd12m, avgf6m, avgf12m));
            }
        }

    }

}
