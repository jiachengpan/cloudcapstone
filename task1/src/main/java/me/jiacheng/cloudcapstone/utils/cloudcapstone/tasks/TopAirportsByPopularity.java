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

/**
 * Group 1 (Answer any 2):
 * Rank the top 10 most popular airports by numbers of flights to/from the airport.
 * Rank the top 10 airlines by on-time arrival performance.
 * Rank the days of the week by on-time arrival performance.
 */

public class TopAirportsByPopularity extends Configured implements Tool {
    public static class MapPopularity extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int[] columns = {Setup.COL_ORIGIN, Setup.COL_DEST};
        private static final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
            CSVParser parser = new CSVParser(new StringReader(val.toString()), CSVFormat.DEFAULT);

            for (CSVRecord record: parser) {
                String orig = record.get(columns[0]);
                String dest = record.get(columns[1]);
                context.write(new Text(orig), one);
                context.write(new Text(dest), one);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopAirportsByPopularity(), args);
        System.exit(res);
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

        // airport count
        conf.setStrings("columns", "Origin", "Dest");
        job = new Job(conf, "airport count");
        job.setJarByClass(TopAirportsByPopularity.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setMapperClass(MapPopularity.class);
        job.setReducerClass(ReduceSum.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.waitForCompletion(true);

        // top airports
        job = new Job(conf, "top airports");
        job.setJarByClass(TopAirportsByPopularity.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);

        job.setMapperClass(TopCount.Map.class);
        job.setReducerClass(TopCount.Reduce.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, tmpPath);
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        job.waitForCompletion(true);

        return 0;
    }

}
