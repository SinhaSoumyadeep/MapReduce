package wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Cardinality extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MaxFilter.class);

    public static class CardinalityMapper extends Mapper<Object, Text, Text, Text> {

        private String maxUser = "100";


        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] users = value.toString().split(",");

            if (MaxFilterUtil.check(users[0], users[1], maxUser)) {
                context.write(new Text(users[0]), new Text("O"));
                context.write(new Text(users[1]), new Text("I"));
            }


        }

        @Override
        public void setup(Context context) throws IOException {

            maxUser = context.getConfiguration().get("max.filter");

        }
    }

    public static class CardinalityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            long Ocount =0;
            long ICount = 0;
            for (Text v : values) {
                if (v.toString().equals("O")) {
                    Ocount++;
                }
                else if (v.toString().equals("I")) {
                    ICount++;
                }
                else {
                    System.out.println(v.toString());
                }
            }

            context.write(key, new Text((Ocount * ICount) + ""));

        }
    }

    public static class ComMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] userCount = value.toString().split(",");
            context.write(new Text("Sum"), new Text(userCount[1]));
        }

    }

    public static class ComReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
                long count =0;
                for(Text t: values){
                    count = count+Long.parseLong(t.toString());
                }
                context.write(new Text("the cardinality is"), new Text(count+""));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("max.filter", args[2]);
        final Job job = Job.getInstance(conf, "Word Count");
        final Job job1 = Job.getInstance(conf, "Word Count 1");


        job.setJarByClass(Cardinality.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(CardinalityMapper.class);
        job.setReducerClass(CardinalityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp"));
        job.waitForCompletion(true);

        job1.setJarByClass(Cardinality.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", ",");
        job1.setMapperClass(ComMapper.class);
        job1.setReducerClass(ComReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("temp"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        return job1.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {


        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <max-filter>");
        }

        try {
            ToolRunner.run(new Cardinality(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
