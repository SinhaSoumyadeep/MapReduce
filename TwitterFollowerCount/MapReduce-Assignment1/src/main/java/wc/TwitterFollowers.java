package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * This class is used to count the number of Twitter followers each user has.
 */
public class TwitterFollowers extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TwitterFollowers.class);

    /**
     * This inner class extends the Mapper Class and is  used to implement the mapper function.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        /**
         * The map function parses a line to extract the users which includes the follower and the user the follower is
         * following: for example let’s say the data in the edge.csv is 1, 2. This means that the user 1 is following
         * user 2. So in My mapper I have split the input using “,” and then assign a value/ the count of followers to
         * 1 for each user being followed( the user after “,”) and writes to the context.
         *
         * @param key     the key.
         * @param value   the value.
         * @param context the context.
         * @throws IOException          when an Input or output exception occurs.
         * @throws InterruptedException when the thread is Interrupted.
         */
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                itr.nextToken();
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * This inner class extends the Reducer Class and is  used to implement the reduce function.
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        /**
         * The intermediate output which is produced by the mapper is then grouped by the user and then the reducer
         * function takes the sum of the count for each user and calculates the number of followers for each
         * group/ user and writes it to the context.
         *
         * @param key     the key.
         * @param values  the value.
         * @param context the context.
         * @throws IOException          when an Input or output exception occurs.
         * @throws InterruptedException when the thread is Interrupted.
         */
        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
	 * This method is used to run the Tool with the provided configuration.
     * @param args the arguments.
     * @return the exit code.
     * @throws Exception if exception is thrown by the method.
     */
    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Count");
        job.setJarByClass(TwitterFollowers.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * This is the start of the program.
     *
     * @param args the arguments i.e the input and the output directories.
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new TwitterFollowers(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}