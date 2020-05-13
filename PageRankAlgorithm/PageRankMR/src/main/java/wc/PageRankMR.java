package wc;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class PageRankMR extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRankMR.class);
    private static Integer k = 0;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> adjList = new HashMap<>();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] links = value.toString().split(",");
            String rankOfDest = adjList.get(links[0]);
            context.write(new Text(links[1]), new Text(rankOfDest));
        }

        @Override
        public void setup(Context context) throws IOException {
            String service_id_file_location = context.getConfiguration().get("service.id.file.path");

            Configuration conf1 = context.getConfiguration();

            FileSystem fs = FileSystem.get(URI.create(service_id_file_location), conf1);
            FileStatus[] files = fs.listStatus(new Path(service_id_file_location));

            if (files == null || files.length == 0) {
                throw new RuntimeException(
                        "User information is not set in DistributedCache");
            }

            // Read all files in the DistributedCache
            for (FileStatus s : files) {

                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(fs.open(s.getPath())));

                String line;
                // For each record in the user file
                while ((line = rdr.readLine()) != null) {
                    Pattern pat = Pattern.compile("[0-9]*,[0-9]*");
                    Matcher mat = pat.matcher(line);
                    if (mat.find()) {
                        String[] path = line.split(",");
                        String from = path[0];
                        String to = path[1];

                        adjList.put(from, to);
                    }

                }

            }

        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (final Text val : values) {
                sum = sum + Double.parseDouble(val.toString());
            }
            context.write(key, new Text(sum.toString()));
        }
    }

    public static class Mapper3 extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> adjList2 = new HashMap<>();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] ver = value.toString().split(",");
            Double delta = Double.parseDouble(adjList2.get("0")) / (k * k);
            Double val = 0.0;
            if (adjList2.containsKey(ver[0])) {

                val = Double.parseDouble(adjList2.get(ver[0])) + delta;
            } else {
                val = delta;
            }
            context.write(new Text(ver[0]), new Text(val.toString()));
        }

        @Override
        public void setup(Context context) throws IOException {

            String service_id_temp = context.getConfiguration().get("service.id.temp.file");
            Configuration conf1 = context.getConfiguration();

            FileSystem fs = FileSystem.get(URI.create(service_id_temp), conf1);
            FileStatus[] files = fs.listStatus(new Path(service_id_temp));

            if (files == null || files.length == 0) {
                throw new RuntimeException(
                        "User information is not set in DistributedCache");
            }

            // Read all files in the DistributedCache
            for (FileStatus s : files) {

                BufferedReader rdr = new BufferedReader(
                        new InputStreamReader(fs.open(s.getPath())));

                String line;
                // For each record in the user file
                while ((line = rdr.readLine()) != null) {
                    Pattern pat = Pattern.compile("[0-9]*,[0-9]*");
                    Matcher mat = pat.matcher(line);
                    if (mat.find()) {
                        String[] path = line.split(",");
                        String from = path[0];
                        String to = path[1];

                        adjList2.put(from, to);
                    }

                }

            }

        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            if (key.toString().equals("0")) {
                context.write(new Text(key), new Text("0.0"));
            } else {
                for (final Text val : values) {

                    context.write(new Text(key), new Text(val));
                }
            }


        }
    }

    @Override
    public int run(final String[] args) throws Exception {


        final Configuration conf = getConf();


        for (int i = 0; i < Integer.parseInt(args[3]); i++) {
            conf.set("service.id.file.path", args[0] + "/rank" + i + ".txt");
            conf.set("service.id.temp.file", "temp_intermediate_" + i);

            final Job job = Job.getInstance(conf, "Page Rank");
            final Job job2 = Job.getInstance(conf, "Word Count2");


            job.setJarByClass(PageRankMR.class);
            final Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", ",");
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0] + "/graph.txt"));
            FileOutputFormat.setOutputPath(job, new Path("temp_intermediate_" + i));
            job.waitForCompletion(true);

            job2.setJarByClass(PageRankMR.class);
            final Configuration jobConf2 = job2.getConfiguration();
            jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
            job2.setMapperClass(Mapper3.class);
            job2.setReducerClass(Reducer3.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[0] + "/vertex.txt"));
            String path = "";
            if (i == Integer.parseInt(args[3])-1) {
                path = args[1];
            } else {
                path = args[0];
            }
            FileOutputFormat.setOutputPath(job2, new Path(path + "/rank" + (i + 1) + ".txt"));
            job2.waitForCompletion(true);


        }

        return 1;

    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 5) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        k = Integer.parseInt(args[2]);

        if(args[4].equals("false")) {
            generateGraph(args, k);
        }
        try {
            ToolRunner.run(new PageRankMR(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    public static void generateGraph(String[] args, int k) throws IOException {
        StringBuilder graph = new StringBuilder();
        for (int a = 1; a <= (k * k); a++) {
            if (a % k == 0) {
                graph.append(a + "," + "0" + "\n");
            } else {
                graph.append(a + "," + (a + 1) + "\n");
            }
        }

        StringBuilder rank = new StringBuilder();;
        for (int a = 1; a <= (k * k); a++) {
            if (a == 0) {
                rank.append(a + "," + (0) + "\n");
            } else {
                rank .append(a + "," + ((double)1 / (k * k)) + "\n");
            }

        }

       StringBuilder v = new StringBuilder();
        for (int a = 1; a <= (k * k); a++) {
            v.append(a + "," + (0) + "\n");
        }

        writeFile(args[0]+"/graph.txt", graph.toString());
        writeFile(args[0]+"/rank0.txt", rank.toString());
        writeFile(args[0]+"/vertex.txt", v.toString());

    }

    public static void writeFile(String fileName, String content)
            throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        writer.write(content);

        writer.close();
    }

}