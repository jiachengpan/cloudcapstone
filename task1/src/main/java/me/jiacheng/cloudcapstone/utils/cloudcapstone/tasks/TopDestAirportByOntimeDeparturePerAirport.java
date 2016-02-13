package me.jiacheng.cloudcapstone.utils.cloudcapstone.tasks;

/**
 * Created by jiacheng on 30/01/16.
 */

import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.ReduceCompositeSum;
import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.TextArrayWritable;
import me.jiacheng.cloudcapstone.utils.cloudcapstone.utils.TopCountComposite;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

/**
 * Group 2 (Answer any 3):
 * Clarification 1/27/2016: For questions 1 and 2 below, we are asking you to find, for each airport, the top 10 carriers and destination airports from that airport with respect to on-time departure performance. We are not asking you to rank the overall top 10 carriers/airports.
 * * For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X. See Task 1 Queries for specific queries.
 * * For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X. See Task 1 Queries for specific queries.
 * * For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X. See Task 1 Queries for specific queries.
 * * For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y. See Task 1 Queries for specific queries.
 */

public class TopDestAirportByOntimeDeparturePerAirport extends Configured implements Tool {

     public static class MapCompositeCount extends Mapper<LongWritable, Text, TextArrayWritable, IntWritable> {
        private static final int[] columns = {Setup.COL_ORIGIN, Setup.COL_DEST, Setup.COL_DEPDELAYM};
        public void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(new StringReader(val.toString()), CSVFormat.DEFAULT);

            for (CSVRecord record: parser) {
                String orig     = record.get(columns[0]);
                String dest     = record.get(columns[1]);
                String delay    = record.get(columns[2]);

                if (orig.isEmpty() || dest.isEmpty() || delay.isEmpty()) continue;

                context.write(
                        new TextArrayWritable(new String[] {orig, dest}),
                        new IntWritable((int)Float.parseFloat(delay))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopDestAirportByOntimeDeparturePerAirport(), args);
        System.exit(res);
    }

    @Override
    public final int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job;
        FileSystem fs = FileSystem.get(conf);

        conf.set("input", args[0]);
        conf.set("output", args[1]);
        conf.set("header_file", args[2]);

        Path tmpPath = new Path("tmp");
        fs.delete(tmpPath, true);

        conf.setStrings("columns", "Origin", "Dest", "DepDelayMinutes");
        job = new Job(conf, "airport-dest-airport departure ontime performance");
        job.setJarByClass(TopDestAirportByOntimeDeparturePerAirport.class);

        job.setMapOutputKeyClass(TextArrayWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(MapCompositeCount.class);
        job.setReducerClass(ReduceCompositeSum.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.waitForCompletion(true);


        job = new Job(conf, "top airport-dest-airport departure ontime performance");
        job.setJarByClass(TopDestAirportByOntimeDeparturePerAirport.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(TopCountComposite.Map.class);
        job.setReducerClass(TopCountComposite.ReduceReverse.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, tmpPath);
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.waitForCompletion(true);


        return 0;
    }

}
