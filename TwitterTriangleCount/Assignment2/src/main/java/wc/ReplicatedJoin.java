package wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class ReplicatedJoin extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MaxFilter.class);

    public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {

        private String service_id_file_location = null;
        private String maxUser = "100";
        private HashMap<String, List<String>> adjList = new HashMap<>();


        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

            String[] path = value.toString().split(",");
            String from = path[0].trim();
            String to = path[1].trim();
            if(MaxFilterUtil.check(from, to, maxUser)) {

                List<String> neigh = adjList.get(to);


                for (String n : neigh) {
                    List<String> neighOfneigh = adjList.get(n);
                    if (neighOfneigh.contains(from)) {
                        //System.out.println(from+" -> "+to+" -> "+n);
                        context.write(new Text("Triangle"), new Text("1"));
                    }
                }
            }

        }

        @Override
        public void setup(Context context) throws IOException {

            service_id_file_location = context.getConfiguration().get("service.id.file.path");
            maxUser = context.getConfiguration().get("max.filter");

            Configuration conf1 = context.getConfiguration();

            FileSystem fs = FileSystem.get(URI.create(service_id_file_location), conf1);
            FileStatus[] files = fs.listStatus(new Path(service_id_file_location));

            if (files == null || files.length == 0) {
                throw new RuntimeException(
                        "no files");
            }

            for (FileStatus s : files) {

                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(fs.open(s.getPath())));

                String line;

                while ((line = rdr.readLine()) != null) {
                    Pattern pat = Pattern.compile("[0-9]*,[0-9]*");
                    Matcher mat = pat.matcher(line);
                    if (mat.find()) {
                        String[] path = line.split(",");



                        String from = path[0];
                        String to = path[1];


                        if(!MaxFilterUtil.check(from, to, maxUser)){
                            continue;
                        }

                        if (adjList.containsKey(from)) {
                            List<String> temp = adjList.get(from);
                            temp.add(to);
                            adjList.put(from, temp);

                        } else {
                            List<String> temp = new ArrayList<>();
                            temp.add(to);
                            adjList.put(from, temp);
                        }
                        if (!adjList.containsKey(to)) {
                            List<String> temp = new ArrayList<>();
                            adjList.put(to, temp);

                        }
                    }

                }

            }

        }
    }

    public static class ReplicatedJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            int count = 0;
            for (final Text val : values) {
                count++;
            }
            context.write(new Text("Replicated Join: the number of triangles are"), new Text(count / 3 + ""));


        }
    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("service.id.file.path", args[0]);
        conf.set("max.filter", args[2]);
        final Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(ReplicatedJoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setReducerClass(ReplicatedJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {


        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <max-filter>");
        }

        try {
            ToolRunner.run(new ReplicatedJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}