package hadoop_code.dificil.task9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class BiggerEuroStandardCountDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path intermediate = new Path(args[1]);
        Path output = new Path(args[2]);

        Job job1 = Job.getInstance(conf);

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(intermediate, true);
        FileOutputFormat.setOutputPath(job1, intermediate);

        job1.setJarByClass(BiggerEuroStandardCountDriver.class);
        job1.setMapperClass(Task9Job1Mapper.class);
        job1.setReducerClass(Task9Job1Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf);
            FileInputFormat.addInputPath(job2, intermediate);
            FileSystem.get(conf).delete(output, true);
            FileOutputFormat.setOutputPath(job2, output);

            job2.setJarByClass(BiggerEuroStandardCountDriver.class);
            job2.setMapperClass(Task9Job2Mapper.class);
            job2.setCombinerClass(Task9Job2Combiner.class);
            job2.setReducerClass(Task9Job2Reducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(EuroStandardWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(EuroStandardWritable.class);

            return job2.waitForCompletion(true) ? 0 : 1;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new BiggerEuroStandardCountDriver(), args);
        System.exit(result);
    }

    public static class Task9Job1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.startsWith("file,")) {
                String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                String euroStandard = columns[5];
                if (!euroStandard.isEmpty()) {
                    con.write(new Text(euroStandard), new IntWritable(1));
                }
            }
        }

    }

    public static class Task9Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v : values) {
                count += v.get();
            }
            con.write(key, new IntWritable(count));
        }

    }

    public static class Task9Job2Mapper extends Mapper<LongWritable, Text, Text, EuroStandardWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String line = value.toString();
            if (!line.isEmpty()) {
                String[] values = line.split("\t");
                con.write(new Text(values[0]), new EuroStandardWritable(values[0], Integer.parseInt(values[1])));
            }
        }

    }

    public static class Task9Job2Combiner extends Reducer<Text, EuroStandardWritable, Text, EuroStandardWritable> {

        public void reduce(Text key, Iterable<EuroStandardWritable> values, Context con) throws IOException, InterruptedException {
            int count = 0;
            for (EuroStandardWritable v : values) {
                count += v.getV();
            }
            EuroStandardWritable w = new EuroStandardWritable(key.toString(), count);
            con.write(new Text("count"), w);
        }

    }

    public static class Task9Job2Reducer extends Reducer<Text, EuroStandardWritable, Text, EuroStandardWritable> {

        public void reduce(Text key, Iterable<EuroStandardWritable> values, Context con) throws IOException, InterruptedException {
            String keyMax = "";
            int max = -1;
            for (EuroStandardWritable v : values) {
                if (v.getV() > max) {
                    max = v.getV();
                    keyMax = v.getK();
                }
            }
            EuroStandardWritable f = new EuroStandardWritable(keyMax, max);
            con.write(new Text("bigger_euro_standard_count"), f);
        }

    }

}
