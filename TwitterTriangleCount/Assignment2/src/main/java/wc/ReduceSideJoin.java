package wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class ReduceSideJoin extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(ReduceSideJoin.class);




    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text from = new Text();
        private final Text to = new Text();
        private String maxUser = "100";

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                String frmStr = itr.nextToken();
                String toStr = itr.nextToken();
                from.set(frmStr);
                to.set(toStr);

                if(MaxFilterUtil.check(frmStr, toStr, maxUser)) {

                    context.write(from, value);
                    context.write(to, value);
                }

            }
        }

        @Override
        public void setup(Context context) throws IOException {
            maxUser = context.getConfiguration().get("max.filter");
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            HashMap<String, List<String>> intermediate = new HashMap<>();

            for (final Text val : values) {
                //context.write(key, val);


                String[] keyVal = val.toString().split(",");


                if (intermediate.containsKey(keyVal[0])) {
                    List<String> valArr = intermediate.get(keyVal[0]);
                    valArr.add(keyVal[1]);
                    intermediate.put(keyVal[0], valArr);

                } else {
                    ArrayList<String> valArr = new ArrayList<>();
                    valArr.add(keyVal[1]);
                    intermediate.put(keyVal[0], valArr);
                }

                //intermediate.put(key.toString(), x);

            }

            /*System.out.println(intermediate);*/

            for (String k : intermediate.keySet()) {
                List<String> x = intermediate.get(k);
                for (String d : x) {
                    if (intermediate.containsKey(d)) {

                        for (String q : intermediate.get(d)) {
                            String wr = d + "," + q;
                            /*System.out.println(k + ", " + wr);*/
                            context.write(new Text(k), new Text(wr));
                        }

                    }
                }
            }


        }
    }


    public static class Mapper2 extends Mapper<Object, Text, Text, Text> {

        private String maxUser = "100";

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] x = value.toString().split(",");

            String from = x[0];
            String to = x[x.length - 1];
            if(MaxFilterUtil.check(from, to, maxUser)) {
                context.write(new Text(from), value);
                context.write(new Text(to), value);
            }

        }

        @Override
        public void setup(Context context) throws IOException {
            maxUser = context.getConfiguration().get("max.filter");
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            HashMap<String, List<String>> threePathHash = new HashMap<>();
            HashMap<String, List<String>> twoPathHash = new HashMap<>();
            for (Text val : values) {

                String[] path = val.toString().split(",");

                if (path.length == 3) {
                    //three path
                    if (path[0].equals(key.toString())) {

                        if (threePathHash.containsKey(path[0])) {
                            List<String> temp = threePathHash.get(path[0]);
                            temp.add(path[path.length - 1]);
                            threePathHash.put(path[0], temp);
                        } else {
                            List<String> temp = new ArrayList<>();
                            temp.add(path[path.length - 1]);
                            threePathHash.put(path[0], temp);
                        }

                    }


                } else {
                    //two path
                    if (path[path.length - 1].equals(key.toString())) {

                        if (twoPathHash.containsKey(path[path.length - 1])) {
                            List<String> temp = twoPathHash.get(path[path.length - 1]);
                            temp.add(path[path.length - 1]);
                            twoPathHash.put(path[0], temp);
                        } else {
                            List<String> temp = new ArrayList<>();
                            temp.add(path[path.length - 1]);
                            twoPathHash.put(path[0], temp);
                        }

                    }

                }

            }

            for (String k : threePathHash.keySet()) {
                List<String> m = threePathHash.get(k);
                for (String v : m) {
                    if (twoPathHash.containsKey(v)&&twoPathHash.get(v).contains(k)) {
                        context.write(new Text("Triangle: "/*+k+" -> "+v*/),new Text("1"));
                    }
                }

            }

        }
    }

    public static class Mapper3 extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] x = value.toString().split(",");
            context.write(new Text(x[0]), new Text(x[1]));
        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Text v: values){
                count++;
            }


            context.write(new Text("Reduce Side Join: number of triangle"), new Text(count/3+""));

        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("max.filter", args[2]);
        final Job job = Job.getInstance(conf, "Word Count");
        final Job job1 = Job.getInstance(conf, "Word Count2");
        final Job job2 = Job.getInstance(conf, "Word Count3");


        job.setJarByClass(ReduceSideJoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp_file"));
        job.waitForCompletion(true);

        job1.setJarByClass(ReduceSideJoin.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", ",");
        job1.setMapperClass(Mapper2.class);
        //job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(Reducer2.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("temp_file"));
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp_intermediate"));
        job1.waitForCompletion(true);

        job2.setJarByClass(ReduceSideJoin.class);
        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", ":");
        job2.setMapperClass(Mapper3.class);
        //job.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(Reducer3.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("temp_intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        return job2.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir> <max-filter>");
        }

        try {
            ToolRunner.run(new ReduceSideJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}