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

public class MaxFilter extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(MaxFilter.class);


	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final Text from = new Text();
		private Integer maxUsers = 1000;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) {
				String frmStr = itr.nextToken();
				String toStr = itr.nextToken();

				if(Integer.parseInt(frmStr)<maxUsers && Integer.parseInt(toStr) <maxUsers){
					from.set(frmStr);
					context.write(from, new IntWritable(Integer.parseInt(toStr)));
				}
			}
		}

		@Override
		public void setup(Context context) throws IOException {
			this.maxUsers = Integer.parseInt(context.getConfiguration().get("max.user.filter"));
		}
		
		
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {

			for (final IntWritable val : values) {
				context.write(key, val);
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("max.user.filter", args[2]);
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(MaxFilter.class);
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

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new MaxFilter(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}