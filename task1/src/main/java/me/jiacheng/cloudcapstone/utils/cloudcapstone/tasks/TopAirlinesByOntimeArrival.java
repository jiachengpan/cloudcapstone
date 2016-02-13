package me.jiacheng.cloudcapstone.utils.cloudcapstone.tasks;

/**
 * Created by jiacheng on 30/01/16.
 */

import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.ReduceSum;
import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.TextArrayWritable;
import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.TopCount;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

/**
 * Group 1 (Answer any 2):
 * Rank the top 10 most popular airports by numbers of flights to/from the airport.
 * Rank the top 10 airlines by on-time arrival performance.
 * Rank the days of the week by on-time arrival performance.
 */

public class TopAirlinesByOntimeArrival extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = TopAirlinesByOntimeArrival.class.getClassLoader();
        URL resource = classLoader.getResource("org/apache/hadoop/util/PlatformName.class");
        System.out.println(resource);
        int res = ToolRunner.run(new Configuration(), new TopAirlinesByOntimeArrival(), args);
        System.exit(res);
    }

    public static class MapCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int[] columns = {Setup.COL_AIRLINEID, Setup.COL_ARRDELAYM};

        public void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(new StringReader(val.toString()), CSVFormat.DEFAULT);

            for (CSVRecord record: parser) {
                String airline = record.get(columns[0]);
                String arrdelay= record.get(columns[1]);
                if (airline.isEmpty() || arrdelay.isEmpty()) continue;

                context.write(
                        new Text(airline),
                        new IntWritable((int) Float.parseFloat(arrdelay))
                );
            }
        }
    }

    @Override
    public final int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job;
        FileSystem fs = FileSystem.get(conf);

        conf.set("input", args[0]);
        conf.set("output", args[1]);

        Path tmpPath = new Path("tmp");
        fs.delete(tmpPath, true);

        // airline delay minutes sum
        job = new Job(conf, "airline delay minutes sum");
        job.setJarByClass(TopAirlinesByOntimeArrival.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(MapCount.class);
        job.setReducerClass(ReduceSum.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.waitForCompletion(true);

        // top airlines by arrival delay minutes
        job = new Job(conf, "top airlines by arrival delay minutes");
        job.setJarByClass(TopAirlinesByOntimeArrival.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(TopCount.MapReverse.class);
        job.setReducerClass(TopCount.ReduceReverse.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, tmpPath);
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.waitForCompletion(true);


        return 0;
    }

}
