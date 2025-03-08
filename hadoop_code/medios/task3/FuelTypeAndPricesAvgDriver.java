package hadoop_code.medios.task3;

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

public class FuelTypeAndPricesAvgDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = Job.getInstance(conf);

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(FuelTypeAndPricesAvgDriver.class);
        job.setMapperClass(Task8Mapper.class);
        job.setCombinerClass(Task8Combiner.class);
        job.setReducerClass(Task8Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FuelWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FuelWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new FuelTypeAndPricesAvgDriver(), args);
        System.exit(result);
    }

    public static class Task8Mapper extends Mapper<LongWritable, Text, Text, FuelWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.startsWith("file,")) {
                String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                if (columns.length > 25) {
                    String type = columns[10];
                    String price12000 = columns[24];
                    String price6000 = columns[25];
                    double p1 = (!price6000.isEmpty()) ? Double.parseDouble(price6000) : 0;
                    double p2 = (!price12000.isEmpty()) ? Double.parseDouble(price12000) : 0;
                    con.write(new Text(type), new FuelWritable(type, p1, p2));
                }
            }
        }

    }

    public static class Task8Combiner extends Reducer<Text, FuelWritable, Text, FuelWritable> {

        public void reduce(Text key, Iterable<FuelWritable> values, Context con) throws IOException, InterruptedException {
            int count = 0;
            double total6000 = 0, total12000 = 0;
            for (FuelWritable v : values) {
                count += 1;
                total6000 += v.getPrice6000miles();
                total12000 += v.getPrice12000miles();
            }
            double avg6000 = total6000 / count;
            double avg12000 = total12000 / count;
            FuelWritable f = new FuelWritable(key.toString(), avg6000, avg12000);
            con.write(key, f);
        }

    }

    public static class Task8Reducer extends Reducer<Text, FuelWritable, Text, FuelWritable> {

        public void reduce(Text key, Iterable<FuelWritable> values, Context con) throws IOException, InterruptedException {
            int count = 0;
            double total6000 = 0, total12000 = 0;
            for (FuelWritable v : values) {
                count += 1;
                total6000 += v.getPrice6000miles();
                total12000 += v.getPrice12000miles();
            }
            double avg6000 = total6000 / count;
            double avg12000 = total12000 / count;
            FuelWritable f = new FuelWritable(key.toString(), avg6000, avg12000);
            con.write(new Text("avg"), f);
        }

    }

}
